# preprocess_worker
import importlib
import time
from modifiers.basemodifier import BaseModifier
from config.logging_config import get_logger, ErrorMetrics, ENGINE_NAME, extract_endpoint, set_log_endpoint, clear_log_endpoint

logger = get_logger(__name__)


class PreprocessWorker:
    """
    Check for URL's in the payload and download the assets as required
    """
    def __init__(self, worker_id, kwargs):
        self.worker_id = worker_id
        self.preprocess_queue = kwargs["preprocess_queue"]
        self.generation_queue = kwargs["generation_queue"]
        self.postprocess_queue = kwargs["postprocess_queue"]
        self.request_store = kwargs["request_store"]
        self.response_store = kwargs["response_store"]
        self.in_flight_requests = kwargs["in_flight_requests"]

    async def work(self):
        logger.info(
            "Worker starting",
            extra={"worker_id": self.worker_id, "worker_type": "preprocess"}
        )
        while True:
            # Get a task from the job queue
            request_id = await self.preprocess_queue.get()
            if request_id is None:
                break

            self.in_flight_requests["preprocess"] += 1
            start_time = time.time()
            logger.info(
                f"Processing job: {request_id}",
                extra={
                    "worker_id": self.worker_id,
                    "worker_type": "preprocess",
                    "request_id": request_id,
                    "stage": "preprocess_start"
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
                    self.preprocess_queue.task_done()
                    continue
                
                # Get and initialize the workflow modifier
                modifier = await self.get_workflow_modifier(
                    request.input.modifier, 
                    request.input.modifications,
                    request_id
                )
                
                # Load and modify the workflow
                await modifier.load_workflow(request.input.workflow_json)
                request.input.workflow_json = await modifier.get_modified_workflow()
                
                # Update the request store with modified workflow
                await self.request_store.set(request_id, request)
                
                # Update result status to show preprocessing is complete
                result.status = "processing"
                result.message = "Preprocessing complete. Queued for generation."
                await self.response_store.set(request_id, result)
                
                # Send for ComfyUI generation
                await self.generation_queue.put(request_id)
                
                # Log timing
                duration_ms = (time.time() - start_time) * 1000
                ErrorMetrics.log_request_timing(
                    logger, request_id, "preprocess", duration_ms, "success",
                    worker_id=self.worker_id, modifier=request.input.modifier or "BaseModifier"
                )
                
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                ErrorMetrics.log_error(
                    logger, e, "preprocessing",
                    request_id=request_id,
                    worker_id=self.worker_id,
                    duration_ms=duration_ms
                )
                
                try:
                    # Update result to show failure
                    result = await self.response_store.get(request_id)
                    if result:
                        result.status = "failed"
                        result.message = f"Preprocessing failed: {str(e)}"
                        await self.response_store.set(request_id, result)
                    
                    # Send job straight to postprocess for cleanup
                    await self.postprocess_queue.put(request_id)
                    
                except Exception as store_error:
                    ErrorMetrics.log_error(
                        logger, store_error, "store_update",
                        request_id=request_id,
                        worker_id=self.worker_id
                    )
            
            finally:
                self.in_flight_requests["preprocess"] -= 1
                if not ENGINE_NAME:
                    clear_log_endpoint()
                self.preprocess_queue.task_done()
            
        logger.info(
            "Worker finished",
            extra={"worker_id": self.worker_id, "worker_type": "preprocess"}
        )

    async def get_workflow_modifier(self, modifier_name: str, modifications: dict, request_id: str = None) -> BaseModifier:
        """Get the appropriate workflow modifier class"""
        try:
            if modifier_name:
                # Dynamically import the modifier class
                module_name = f'modifiers.{modifier_name.lower()}'
                module = importlib.import_module(module_name)
                modifier_class = getattr(module, modifier_name)
                logger.info(f"Using modifier: {modifier_name}", extra={"request_id": request_id, "modifier": modifier_name})
            else:
                # Use base modifier if no specific modifier specified
                modifier_class = BaseModifier
                logger.info("Using BaseModifier", extra={"request_id": request_id, "modifier": "BaseModifier"})
                
            return modifier_class(modifications)
            
        except ImportError as e:
            logger.error(f"Failed to import modifier '{modifier_name}': {e}", extra={"request_id": request_id, "modifier": modifier_name})
            raise Exception(f"Unknown modifier: {modifier_name}")
        except AttributeError as e:
            logger.error(f"Modifier class '{modifier_name}' not found in module: {e}", extra={"request_id": request_id, "modifier": modifier_name})
            raise Exception(f"Modifier class '{modifier_name}' not found")
        except Exception as e:
            logger.error(f"Failed to create modifier '{modifier_name}': {e}", extra={"request_id": request_id, "modifier": modifier_name})
            raise