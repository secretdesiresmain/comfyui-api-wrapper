# generation_worker
import asyncio
import aiohttp
import json
import time
from typing import Optional, Dict, Any
from datetime import datetime

from config import COMFYUI_API_PROMPT, COMFYUI_API_HISTORY, COMFYUI_API_INTERRUPT, COMFYUI_API_WEBSOCKET, COMFYUI_RETRIES
from config.logging_config import get_logger, ErrorMetrics, ENGINE_NAME, extract_endpoint, set_log_endpoint, clear_log_endpoint

logger = get_logger(__name__)


class GenerationWorker:
    """
    Send payload to ComfyUI and await completion using WebSocket
    """
    def __init__(self, worker_id, kwargs):
        self.worker_id = worker_id
        self.preprocess_queue = kwargs["preprocess_queue"]
        self.generation_queue = kwargs["generation_queue"]
        self.postprocess_queue = kwargs["postprocess_queue"]
        self.request_store = kwargs["request_store"]
        self.response_store = kwargs["response_store"]
        
        # Configuration
        self.max_wait_time = 3600  # 1 hour maximum wait
        self.ws_url = COMFYUI_API_WEBSOCKET
        self.client_id = f"worker_{worker_id}_{datetime.now().timestamp()}"

    async def work(self):
        logger.info(
            "Worker starting",
            extra={"worker_id": self.worker_id, "worker_type": "generation"}
        )
        while True:
            # Get a task from the job queue
            request_id = await self.generation_queue.get()
            if request_id is None:
                # None is a signal that there are no more tasks
                break

            # Process the job
            start_time = time.time()
            comfyui_job_id = None
            is_cached = False
            
            logger.info(
                f"Processing job: {request_id}",
                extra={
                    "worker_id": self.worker_id,
                    "worker_type": "generation",
                    "request_id": request_id,
                    "stage": "generation_start"
                }
            )
            
            try:
                # Get request and result from stores
                request = await self.request_store.get(request_id)
                result = await self.response_store.get(request_id)
                
                if not request:
                    raise Exception(f"Request {request_id} not found in store")
                if not result:
                    raise Exception(f"Result {request_id} not found in store")

                # Set endpoint from request when ENGINE_NAME env var is not set
                if not ENGINE_NAME:
                    set_log_endpoint(extract_endpoint(request))

                # Check for cancellation
                if result and getattr(result, 'status', '') == 'cancelled':
                    logger.info(
                        f"Skipping cancelled job: {request_id}",
                        extra={
                            "worker_id": self.worker_id,
                            "request_id": request_id,
                            "status": "cancelled"
                        }
                    )
                    await self.postprocess_queue.put(request_id)
                    self.generation_queue.task_done()
                    continue
                    
                # Submit workflow to ComfyUI
                comfyui_job_id = await self.post_workflow(request, request_id)
                logger.info(
                    f"Submitted job to ComfyUI",
                    extra={
                        "worker_id": self.worker_id,
                        "request_id": request_id,
                        "comfyui_job_id": comfyui_job_id
                    }
                )
                
                # Update status to show generation started
                result.status = "generating"
                result.message = f"Generation started (ComfyUI job: {comfyui_job_id})"
                await self.response_store.set(request_id, result)

                # Check if job is already complete (cached result)
                is_cached = await self.check_if_cached(comfyui_job_id, request_id)
                
                if is_cached:
                    logger.info(
                        f"Job completed immediately (cached)",
                        extra={
                            "worker_id": self.worker_id,
                            "request_id": request_id,
                            "comfyui_job_id": comfyui_job_id,
                            "cached": True
                        }
                    )
                    execution_result = {
                        "prompt_id": comfyui_job_id,
                        "nodes_executed": [],
                        "progress_updates": [],
                        "completed": True,
                        "cached": True,
                        "error": None
                    }
                else:
                    # Wait for completion using WebSocket
                    execution_result = await self.wait_for_completion_websocket(
                        comfyui_job_id, 
                        request_id
                    )
                
                # Get the final result from ComfyUI history
                comfyui_response = await self.get_result(comfyui_job_id, request_id)
                logger.info(
                    f"ComfyUI response structure: {json.dumps(comfyui_response, indent=2)[:500]}...",
                    extra={"request_id": request_id}
                )
                
                # Update result with success
                result.status = "generated"
                result.message = "Generation complete. Queued for post-processing."
                result.comfyui_response = comfyui_response
                # Store execution details in the comfyui_response if needed
                if execution_result:
                    # Merge execution details into the response
                    if isinstance(result.comfyui_response, dict):
                        result.comfyui_response["execution_details"] = execution_result
                await self.response_store.set(request_id, result)
                
                # Send for post-processing
                await self.postprocess_queue.put(request_id)
                
                # Log timing
                duration_ms = (time.time() - start_time) * 1000
                ErrorMetrics.log_request_timing(
                    logger, request_id, "generation", duration_ms, "success",
                    worker_id=self.worker_id,
                    comfyui_job_id=comfyui_job_id,
                    cached=is_cached
                )
                
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                ErrorMetrics.log_error(
                    logger, e, "generation",
                    request_id=request_id,
                    worker_id=self.worker_id,
                    comfyui_job_id=comfyui_job_id,
                    duration_ms=duration_ms
                )
                
                try:
                    # Update result to show failure
                    result = await self.response_store.get(request_id)
                    if result:
                        result.status = "failed"
                        result.message = f"Generation failed: {str(e)}"
                        await self.response_store.set(request_id, result)
                    
                    # Send job to postprocess for cleanup
                    await self.postprocess_queue.put(request_id)
                    
                except Exception as store_error:
                    ErrorMetrics.log_error(
                        logger, store_error, "store_update",
                        request_id=request_id,
                        worker_id=self.worker_id
                    )
            
            finally:
                if not ENGINE_NAME:
                    clear_log_endpoint()
                # Mark the job as complete
                self.generation_queue.task_done()

        logger.info(f"GenerationWorker {self.worker_id} finished", extra={"worker_id": self.worker_id, "worker_type": "generation"})

    async def post_workflow(self, request, request_id: str = None) -> str:
        """Submit workflow to ComfyUI API. Retries on network errors and timeouts."""
        logger.info(f"Posting workflow to ComfyUI API, with request : {request}", extra={"request_id": request_id})
        payload = {
            "prompt": request.input.workflow_json,
            "client_id": self.client_id  # Use our worker's client ID
        }
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        timeout = aiohttp.ClientTimeout(total=30)  # 30 second timeout
        
        last_error = None
        for attempt in range(1, COMFYUI_RETRIES + 1):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    logger.info(f"Posting workflow to {COMFYUI_API_PROMPT} (attempt {attempt}/{COMFYUI_RETRIES})", extra={"request_id": request_id})
                    logger.debug(f"Workflow keys: {list(request.input.workflow_json.keys()) if isinstance(request.input.workflow_json, dict) else 'not a dict'}", extra={"request_id": request_id})
                    
                    async with session.post(
                        COMFYUI_API_PROMPT, 
                        data=json.dumps(payload),
                        headers=headers
                    ) as response:
                        
                        response_text = await response.text()
                        logger.debug(f"ComfyUI API response status: {response.status}", extra={"request_id": request_id})
                        logger.debug(f"ComfyUI API response: {response_text[:500]}...", extra={"request_id": request_id})  # First 500 chars
                        
                        # 5xx = ComfyUI server error, retry
                        if response.status >= 500:
                            last_error = f"ComfyUI server error (status {response.status}): {response_text}"
                            logger.warning(
                                f"ComfyUI post attempt {attempt}/{COMFYUI_RETRIES} failed ({last_error})",
                                extra={"request_id": request_id}
                            )
                            if attempt < COMFYUI_RETRIES:
                                delay = 2 ** (attempt - 1)
                                await asyncio.sleep(delay)
                                continue
                            raise Exception(last_error)
                        
                        # 4xx = client error, don't retry
                        if response.status >= 400:
                            raise aiohttp.ClientResponseError(
                                request_info=response.request_info,
                                history=response.history,
                                status=response.status,
                                message=f"ComfyUI API error: {response_text}"
                            )
                        
                        response_data = json.loads(response_text)
                        
                        if "prompt_id" in response_data:
                            if attempt > 1:
                                logger.info(f"ComfyUI post succeeded on attempt {attempt}/{COMFYUI_RETRIES}", extra={"request_id": request_id})
                            return response_data["prompt_id"]
                        elif "node_errors" in response_data:
                            error_details = json.dumps(response_data["node_errors"], indent=2)
                            raise Exception(f"ComfyUI node errors: {error_details}")
                        elif "error" in response_data:
                            raise Exception(f"ComfyUI error: {response_data['error']}")
                        else:
                            raise Exception(f"Unexpected response from ComfyUI: {response_data}")

            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                # Network errors and timeouts are retryable
                last_error = str(e)
                logger.warning(
                    f"ComfyUI post attempt {attempt}/{COMFYUI_RETRIES} error: {last_error}",
                    extra={"request_id": request_id}
                )
                if attempt < COMFYUI_RETRIES:
                    delay = 5 * (2 ** (attempt - 1))  # 5s, 10s, 20s, 40s, …
                    logger.info(f"Retrying in {delay}s…", extra={"request_id": request_id})
                    await asyncio.sleep(delay)
                    continue
                raise Exception(f"Network error posting to ComfyUI after {COMFYUI_RETRIES} attempts: {last_error}")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON response from ComfyUI: {e}", extra={"request_id": request_id}, exc_info=True)
                raise Exception(f"Invalid JSON response from ComfyUI: {e}")
            except Exception as e:
                logger.error(f"Unexpected error posting to ComfyUI: {e}", extra={"request_id": request_id}, exc_info=True)
                raise

    async def check_if_cached(self, comfyui_job_id: str, request_id: str = None) -> bool:
        """Check if job is already complete (cached result)"""
        await asyncio.sleep(0.5)  # Give ComfyUI a moment to process
        
        timeout = aiohttp.ClientTimeout(total=5)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                url = f"{COMFYUI_API_HISTORY}/{comfyui_job_id}"
                async with session.get(url) as response:
                    if response.status == 200:
                        history_data = await response.json()
                        # If we get non-empty data, the job is complete
                        if history_data and history_data != {}:
                            logger.info(f"Job {comfyui_job_id} found in history (cached)", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                            return True
            return False
        except Exception as e:
            logger.debug(f"Error checking cache status: {e}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
            return False
    
    async def wait_for_completion_websocket(self, comfyui_job_id: str, request_id: str) -> Dict[str, Any]:
        """
        Wait for ComfyUI job completion using WebSocket connection
        Returns execution result details
        """
        execution_result = {
            "prompt_id": comfyui_job_id,
            "nodes_executed": [],
            "progress_updates": [],
            "completed": False,
            "error": None
        }
        
        timeout = aiohttp.ClientTimeout(total=self.max_wait_time)
        
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                logger.info(f"Connecting to ComfyUI WebSocket at {self.ws_url}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                
                async with session.ws_connect(
                    self.ws_url,
                    params={"clientId": self.client_id}
                ) as ws:
                    logger.info(f"WebSocket connected for job {comfyui_job_id}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                    
                    # Start listening for messages
                    start_time = asyncio.get_event_loop().time()
                    last_update_time = start_time
                    last_message_time = start_time
                    last_cancellation_check = start_time
                    
                    # Progressive timeout strategy
                    initial_timeout = 30.0  # 30 seconds to receive first message
                    message_timeout = 60.0  # 60 seconds between messages after first message received
                    max_no_message_retries = 3  # Number of times to retry when no messages received
                    no_message_retry_count = 0
                    
                    while True:
                        try:
                            # Set timeout based on whether we've received any messages
                            timeout_duration = initial_timeout if last_message_time == start_time else message_timeout
                            
                            msg = await asyncio.wait_for(
                                ws.receive(), 
                                timeout=timeout_duration
                            )
                            
                            last_message_time = asyncio.get_event_loop().time()
                            # Reset retry count since we received a message
                            no_message_retry_count = 0

                            current_time = asyncio.get_event_loop().time()
                            if current_time - last_cancellation_check > 5.0:  # Check every 5 seconds
                                if await self._check_if_cancelled(request_id):
                                    logger.info(f"Job {request_id} was cancelled during generation - aborting WebSocket", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                    # Cancel the ComfyUI job
                                    await self.cancel_comfyui_job(comfyui_job_id, request_id)
                                    raise Exception(f"Job {request_id} was cancelled during generation")
                                last_cancellation_check = current_time
                            
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                    message_type = data.get("type")
                                    
                                    logger.debug(f"WebSocket message type: {message_type}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                    
                                    # Check if this message is for our prompt
                                    if data.get("data", {}).get("prompt_id") == comfyui_job_id:
                                        
                                        if message_type == "execution_start":
                                            logger.info(f"Execution started for {comfyui_job_id}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                            await self._update_progress(
                                                request_id, 
                                                "Execution started..."
                                            )
                                        
                                        elif message_type == "execution_cached":
                                            nodes = data.get("data", {}).get("nodes", [])
                                            logger.info(f"Using cached results for nodes: {nodes}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                            execution_result["nodes_executed"].extend(nodes)
                                        
                                        elif message_type == "executing":
                                            node = data.get("data", {}).get("node")
                                            if node:
                                                logger.debug(f"Executing node: {node}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id, "node": node})
                                                execution_result["nodes_executed"].append(node)
                                                await self._update_progress(
                                                    request_id, 
                                                    f"Processing node: {node}"
                                                )
                                            elif data.get("data", {}).get("node") is None:
                                                # node = None means execution is complete
                                                logger.info(f"Execution complete for {comfyui_job_id}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                                execution_result["completed"] = True
                                                return execution_result
                                        
                                        elif message_type == "progress":
                                            progress_data = data.get("data", {})
                                            value = progress_data.get("value", 0)
                                            max_value = progress_data.get("max", 100)
                                            
                                            progress_pct = (value / max_value * 100) if max_value > 0 else 0
                                            progress_msg = f"Progress: {progress_pct:.1f}% ({value}/{max_value})"
                                            
                                            logger.debug(f"Progress update: {progress_msg}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                            execution_result["progress_updates"].append({
                                                "time": asyncio.get_event_loop().time() - start_time,
                                                "value": value,
                                                "max": max_value,
                                                "percentage": progress_pct
                                            })
                                            
                                            # Update status every few seconds to avoid spam
                                            current_time = asyncio.get_event_loop().time()
                                            if current_time - last_update_time > 2:  # Update every 2 seconds
                                                await self._update_progress(request_id, progress_msg)
                                                last_update_time = current_time
                                        
                                        elif message_type == "execution_error":
                                            error_data = data.get("data", {})
                                            error_msg = f"Execution error: {error_data}"
                                            logger.error(error_msg, extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                            execution_result["error"] = error_data
                                            raise Exception(error_msg)
                                        
                                        elif message_type == "executed":
                                            node = data.get("data", {}).get("node")
                                            output = data.get("data", {}).get("output")
                                            logger.info(f"Node {node} executed successfully", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id, "node": node})
                                            logger.debug(f"Node output: {json.dumps(output, indent=2)[:500]}...", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                    
                                except json.JSONDecodeError as e:
                                    logger.warning(f"Failed to parse WebSocket message: {e}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                    logger.debug(f"Raw message: {msg.data}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                        
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error(f"WebSocket error: {ws.exception()}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                raise Exception(f"WebSocket error: {ws.exception()}")
                            
                            elif msg.type == aiohttp.WSMsgType.CLOSED:
                                logger.warning("WebSocket connection closed", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                break
                            
                        except asyncio.TimeoutError:
                            no_message_retry_count += 1
                            elapsed = asyncio.get_event_loop().time() - start_time
                            
                            # If we haven't received any messages, try to check job status before giving up
                            if last_message_time == start_time:
                                logger.warning(f"No WebSocket messages received for {comfyui_job_id} "
                                            f"(attempt {no_message_retry_count}/{max_no_message_retries}) "
                                            f"after {elapsed:.1f}s - checking job status", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                
                                # Check if the job is complete/cached
                                try:
                                    if await self.check_if_cached(comfyui_job_id):
                                        logger.info(f"Job {comfyui_job_id} is complete (cached)", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                        execution_result["completed"] = True
                                        execution_result["cached"] = True
                                        return execution_result
                                except Exception as check_error:
                                    logger.warning(f"Error checking job status: {check_error}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                
                                # If we've exhausted retries, give up
                                if no_message_retry_count >= max_no_message_retries:
                                    logger.error(f"No WebSocket messages received for {comfyui_job_id} "
                                            f"after {max_no_message_retries} attempts and {elapsed:.1f}s", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                    raise Exception(f"No WebSocket messages received for job {comfyui_job_id} "
                                                f"after {max_no_message_retries} retry attempts")
                                
                                # Wait a bit before retrying (exponential backoff)
                                wait_time = min(5 * (2 ** (no_message_retry_count - 1)), 30)  # Cap at 30 seconds
                                logger.info(f"Waiting {wait_time}s before retry {no_message_retry_count + 1}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                await asyncio.sleep(wait_time)
                                
                            else:
                                # We were receiving messages but they stopped
                                logger.warning(f"WebSocket message timeout for job {comfyui_job_id} "
                                            f"(no message for {timeout_duration}s, elapsed: {elapsed:.1f}s)", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                
                                # Try to check job status before giving up completely
                                try:
                                    if await self.check_if_cached(comfyui_job_id):
                                        logger.info(f"Job {comfyui_job_id} completed despite message timeout", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                        execution_result["completed"] = True
                                        return execution_result
                                except Exception as check_error:
                                    logger.warning(f"Error checking job status after timeout: {check_error}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                
                                # If still no completion after timeout, raise error
                                raise Exception(f"WebSocket message timeout for job {comfyui_job_id} "
                                            f"after {timeout_duration} seconds without messages")
                        
                        # Check for overall timeout
                        elapsed = asyncio.get_event_loop().time() - start_time
                        if elapsed > self.max_wait_time:
                            raise Exception(f"Timeout waiting for job {comfyui_job_id} after {elapsed:.1f} seconds")
                    
                    # If we exit the loop without completion, something went wrong
                    if not execution_result["completed"]:
                        # Final check before giving up
                        try:
                            if await self.check_if_cached(comfyui_job_id):
                                logger.info(f"Job {comfyui_job_id} completed (final check)", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                                execution_result["completed"] = True
                                return execution_result
                        except Exception as check_error:
                            logger.warning(f"Error in final job status check: {check_error}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                        
                        raise Exception(f"WebSocket closed without completion for job {comfyui_job_id}")
                    
                    return execution_result
                    
        except asyncio.TimeoutError:
            logger.warning(f"WebSocket overall timeout for job {comfyui_job_id} - attempting to cancel", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
            await self.cancel_comfyui_job(comfyui_job_id, request_id)
            raise Exception(f"WebSocket timeout for job {comfyui_job_id}")
        except aiohttp.ClientError as e:
            # Cancel the job since we can't monitor it anymore
            logger.warning(f"WebSocket connection error for job {comfyui_job_id} - attempting to cancel", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
            await self.cancel_comfyui_job(comfyui_job_id, request_id)
            raise Exception(f"WebSocket connection error: {e}")
        except Exception as e:
            logger.error(f"WebSocket error for job {comfyui_job_id}: {e}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
            # Cancel on other errors to be safe
            await self.cancel_comfyui_job(comfyui_job_id, request_id)
            raise

    async def _update_progress(self, request_id: str, message: str):
        """Helper to update progress in the response store"""
        try:
            result = await self.response_store.get(request_id)
            if result:
                result.message = message
                await self.response_store.set(request_id, result)
        except Exception as e:
            logger.warning(f"Failed to update progress for {request_id}: {e}", extra={"request_id": request_id})

    async def get_result(self, comfyui_job_id: str, request_id: str = None) -> Optional[dict]:
        """Get the final result from ComfyUI history"""
        timeout = aiohttp.ClientTimeout(total=30)
        
        # Wait a moment for history to be updated
        await asyncio.sleep(0.5)
        
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                url = f"{COMFYUI_API_HISTORY}/{comfyui_job_id}"
                logger.debug(f"Fetching result from: {url}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                
                async with session.get(url) as response:
                    response_text = await response.text()
                    logger.debug(f"History API status: {response.status}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                    
                    if response.status == 200:
                        history_data = json.loads(response_text)
                        
                        # Check if we got actual data
                        if not history_data or history_data == {}:
                            logger.warning(f"Empty history response for job {comfyui_job_id}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                            # Try the general history endpoint
                            return await self._get_result_from_general_history(comfyui_job_id, request_id)
                        
                        logger.info(f"Retrieved ComfyUI history for job {comfyui_job_id}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                        return history_data
                    else:
                        raise Exception(f"Failed to get result (status {response.status}): {response_text}")
                        
        except asyncio.TimeoutError:
            raise Exception(f"Timeout getting result for job {comfyui_job_id}")
        except aiohttp.ClientError as e:
            raise Exception(f"Network error getting result: {e}")
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON in result: {e}")

    async def _get_result_from_general_history(self, comfyui_job_id: str, request_id: str = None) -> Optional[dict]:
        """Fallback: Get result from general history endpoint"""
        timeout = aiohttp.ClientTimeout(total=30)
        
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # Try the general history endpoint
                url = COMFYUI_API_HISTORY.rstrip(f"/{comfyui_job_id}")
                logger.debug(f"Trying general history endpoint: {url}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                
                async with session.get(url) as response:
                    if response.status == 200:
                        all_history = await response.json()
                        
                        # Look for our job in the history
                        if comfyui_job_id in all_history:
                            logger.info(f"Found job {comfyui_job_id} in general history", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                            return {comfyui_job_id: all_history[comfyui_job_id]}
                        else:
                            logger.warning(f"Job {comfyui_job_id} not found in general history", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                            return {}
                    else:
                        return {}
                        
        except Exception as e:
            logger.error(f"Failed to get result from general history: {e}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
            return {}

    async def _check_if_cancelled(self, request_id: str) -> bool:
        """Check if the job has been cancelled"""
        try:
            result = await self.response_store.get(request_id)
            return result and getattr(result, 'status', '') == 'cancelled'
        except Exception as e:
            logger.warning(f"Error checking cancellation status for {request_id}: {e}", extra={"request_id": request_id})
            return False

    async def cancel_comfyui_job(self, comfyui_job_id: str, request_id: str = None):
        """Cancel a running job in ComfyUI"""
        try:       
            if not COMFYUI_API_INTERRUPT:
                logger.warning("COMFYUI_API_INTERRUPT not configured, cannot cancel job", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                return False
                
            payload = {
                "prompt_id": comfyui_job_id
            }
            
            headers = {
                'Content-Type': 'application/json'
            }
                
            timeout = aiohttp.ClientTimeout(total=5.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                cancel_url = COMFYUI_API_INTERRUPT
                
                async with session.post(
                    cancel_url,
                    data=json.dumps(payload),
                    headers=headers
                ) as response:
                    
                    if response.status == 200:
                        logger.info(f"Successfully cancelled ComfyUI job {comfyui_job_id}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                        return True
                    else:
                        response_text = await response.text()
                        logger.warning(f"Failed to cancel ComfyUI job {comfyui_job_id}: HTTP {response.status} - {response_text}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
                        return False
                    
        except Exception as e:
            logger.error(f"Error cancelling ComfyUI job {comfyui_job_id}: {e}", extra={"request_id": request_id, "comfyui_job_id": comfyui_job_id})
            return False
