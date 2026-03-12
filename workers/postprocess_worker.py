# postprocess_worker
import asyncio
import os  # Still needed for symlink and remove operations
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional
import json

import aiofiles
import aiofiles.os
import aiohttp

# Azure Blob Storage config (async)
from azure.storage.blob.aio import ContainerClient

from config import OUTPUT_DIR, S3_CONFIG, S3_ENABLED, WEBHOOK_CONFIG, WEBHOOK_ENABLED
WEBHOOK_RETRIES: int = WEBHOOK_CONFIG.get("retries", 3)
from config.logging_config import get_logger, ErrorMetrics, ENGINE_NAME, extract_endpoint, set_log_endpoint, clear_log_endpoint

logger = get_logger(__name__)


#map of s3 field names to blob storage field names
S3_FIELD_MAP = {
    "endpoint_url": "connection_string",
    "bucket_name": "container_name",
}

class PostprocessWorker:
    """
    Upload generated assets and fire webhook response
    """
    def __init__(self, worker_id, kwargs):
        self.worker_id = worker_id
        self.preprocess_queue = kwargs["preprocess_queue"]
        self.generation_queue = kwargs["generation_queue"]
        self.postprocess_queue = kwargs["postprocess_queue"]
        self.request_store = kwargs["request_store"]
        self.response_store = kwargs["response_store"]
        
        # Configuration
        self.output_dir = Path(OUTPUT_DIR)
        
        # Shared connector with DNS caching (TTL 300s = 5 min)
        self._connector = aiohttp.TCPConnector(
            use_dns_cache=True,
            ttl_dns_cache=300,
        )

    async def work(self):
        logger.info(
            "Worker starting",
            extra={"worker_id": self.worker_id, "worker_type": "postprocess"}
        )
        while True:
            # Get a task from the job queue
            request_id = await self.postprocess_queue.get()
            if request_id is None:
                # None is a signal that there are no more tasks
                break

            # Process the job
            start_time = time.time()
            s3_uploaded = False
            webhook_sent = False
            output_count = 0
            
            logger.info(
                f"Processing job: {request_id}",
                extra={
                    "worker_id": self.worker_id,
                    "worker_type": "postprocess",
                    "request_id": request_id,
                    "stage": "postprocess_start"
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

                # Only process if we have ComfyUI output (successful generation)
                if hasattr(result, 'comfyui_response') and result.comfyui_response:
                    logger.info(
                        f"Processing outputs for {request_id}",
                        extra={"request_id": request_id, "has_output": True}
                    )
                    logger.info(
                        f"ComfyUI response structure: {json.dumps(result.comfyui_response, indent=2)[:1000]}",
                        extra={"request_id": request_id}
                    )
                    
                    # Move generated assets to organized directory
                    await self.move_assets(request_id, result)
                    output_count = len(getattr(result, 'output', []))
                    
                    # Handle S3 upload - check payload first, then environment variables
                    s3_config = await self.get_s3_config(request.input, request_id)
                    if s3_config:
                        await self.upload_assets(request_id, s3_config, result)
                        s3_uploaded = True
                    else:
                        logger.info(
                            f"No S3 configuration found, skipping upload",
                            extra={"request_id": request_id}
                        )
                else:
                    logger.info(
                        f"No ComfyUI output, likely a failed job",
                        extra={"request_id": request_id, "has_output": False}
                    )
                    if hasattr(result, 'comfyui_response'):
                        logger.info(
                            f"ComfyUI response was: {result.comfyui_response}",
                            extra={"request_id": request_id}
                        )

                # Update final status only if not already failed
                if result.status != "failed":
                    result.status = "completed"
                    result.message = "Processing complete."
                else:
                    logger.info(
                        f"Job already marked as failed, keeping failure status",
                        extra={"request_id": request_id, "status": result.status}
                    )
                
                await self.response_store.set(request_id, result)
                
                # Log timing
                duration_ms = (time.time() - start_time) * 1000
                ErrorMetrics.log_request_timing(
                    logger, request_id, "postprocess", duration_ms, "success",
                    worker_id=self.worker_id,
                    output_count=output_count,
                    s3_uploaded=s3_uploaded
                )
                
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                ErrorMetrics.log_error(
                    logger, e, "postprocessing",
                    request_id=request_id,
                    worker_id=self.worker_id,
                    duration_ms=duration_ms
                )
                
                try:
                    # Update result to show failure
                    result = await self.response_store.get(request_id)
                    if result:
                        result.status = "failed"
                        result.message = f"Post-processing failed: {str(e)}"
                        await self.response_store.set(request_id, result)
                    
                except Exception as store_error:
                    ErrorMetrics.log_error(
                        logger, store_error, "store_update",
                        request_id=request_id,
                        worker_id=self.worker_id
                    )
            
            finally:
                # Handle webhook - check payload first, then environment variables
                webhook_config = await self.get_webhook_config(request.input, request_id)
                if webhook_config:
                    try:
                        # Send regular webhook if URL is provided
                        if webhook_config.get('url'):
                            await self.send_webhook(webhook_config['url'], result, webhook_config.get('extra_params', {}), request_id)
                            webhook_sent = True
                        
                        # Send session-close webhook if URL is provided
                        if webhook_config.get('session_close_url'):
                            session_auth_data = webhook_config.get('session_auth_data')
                            if session_auth_data is None:
                                session_auth_data = {}
                            logger.info(
                                f"Sending session close webhook",
                                extra={
                                    "request_id": request_id,
                                    "session_close_url": webhook_config['session_close_url']
                                }
                            )
                            await self.send_webhook_for_session_close(webhook_config['session_close_url'], result, session_auth_data, request_id)
                    except Exception as webhook_error:
                        # Will not mark a 'completed' job as failed
                        ErrorMetrics.log_error(
                            logger, webhook_error, "webhook",
                            request_id=request_id,
                            worker_id=self.worker_id,
                            webhook_url=webhook_config.get('url', '')
                        )
                else:
                    logger.info(
                        f"No webhook configuration found",
                        extra={"request_id": request_id}
                    )
                # Clean up the request store
                try:
                    await self.request_store.delete(request_id)
                    logger.info(
                        f"Cleaned up request",
                        extra={"request_id": request_id}
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to clean up request: {e}",
                        extra={"request_id": request_id}
                    )
                if not ENGINE_NAME:
                    clear_log_endpoint()
                # Mark the job as complete
                self.postprocess_queue.task_done()
            
        logger.info(
            "Worker finished",
            extra={"worker_id": self.worker_id, "worker_type": "postprocess"}
        )
    
    async def move_assets(self, request_id: str, result) -> None:
        """Move generated assets to organized directory structure"""
        try:
            # Create job-specific output directory
            job_output_dir = self.output_dir / request_id
            await aiofiles.os.makedirs(str(job_output_dir), exist_ok=True)
            
            if not hasattr(result, 'output'):
                result.output = []

            # Parse ComfyUI history response structure
            # The response from history API typically looks like:
            # {
            #   "prompt_id": {
            #     "prompt": [...],
            #     "outputs": {
            #       "node_id": {
            #         "images": [{"filename": "...", "subfolder": "...", "type": "output"}],
            #         "gifs": [...],
            #         "videos": [...]
            #       }
            #     }
            #   }
            # }
            
            # Find the outputs in the response
            outputs = None
            
            # First, check if the response is wrapped with the prompt_id
            if isinstance(result.comfyui_response, dict):
                # Get the first (and usually only) key which is the prompt_id
                for prompt_id, prompt_data in result.comfyui_response.items():
                    if isinstance(prompt_data, dict) and 'outputs' in prompt_data:
                        outputs = prompt_data['outputs']
                        logger.debug(f"Found outputs under prompt_id {prompt_id}", extra={"request_id": request_id})
                        break
                    elif isinstance(prompt_data, dict):
                        # Sometimes outputs might be directly in the prompt_data
                        logger.debug(f"Checking if prompt_data contains output nodes directly", extra={"request_id": request_id})
                        outputs = prompt_data
                        break
            
            if not outputs:
                logger.warning(f"No outputs found in ComfyUI response for {request_id}", extra={"request_id": request_id})
                logger.debug(f"Full response structure: {json.dumps(result.comfyui_response, indent=2)[:2000]}", extra={"request_id": request_id})
                return
            
            # Process each node's outputs
            processed_files = []
            for node_id, node_outputs in outputs.items():
                if not isinstance(node_outputs, dict):
                    logger.debug(f"Skipping non-dict node output: {node_id}", extra={"request_id": request_id})
                    continue
                
                logger.debug(f"Processing node {node_id} outputs: {list(node_outputs.keys())}", extra={"request_id": request_id})
                
                # Look for different output types (images, gifs, videos, etc.)
                for output_type, output_list in node_outputs.items():
                    if not isinstance(output_list, list):
                        logger.debug(f"Skipping non-list output type {output_type} in node {node_id}", extra={"request_id": request_id})
                        continue
                    
                    for item in output_list:
                        if isinstance(item, dict) and 'filename' in item:
                            # Skip preview/temp files
                            file_type = item.get('type', '')
                            if file_type in ['temp', 'preview']:
                                logger.debug(f"Skipping {file_type} file: {item.get('filename')}", extra={"request_id": request_id})
                                continue
                            
                            # Process this output file
                            processed = await self._process_output_file(
                                item, 
                                job_output_dir, 
                                request_id,
                                node_id,
                                output_type
                            )
                            if processed:
                                processed_files.append(processed)
            
            # Add all processed files to the result
            result.output = processed_files
            logger.info(f"Processed {len(processed_files)} output files for {request_id}", extra={"request_id": request_id})
            
        except Exception as e:
            logger.error(f"Error moving assets for {request_id}: {e}", extra={"request_id": request_id}, exc_info=True)
            raise

    async def _process_output_file(self, item: Dict, job_output_dir: Path, request_id: str, node_id: str, output_type: str) -> Optional[Dict]:
        """Process a single output file - copy to job directory and create symlink"""
        try:
            filename = item.get('filename', '')
            subfolder = item.get('subfolder', '')
            file_type = item.get('type', 'output')
            
            if not filename:
                logger.warning(f"No filename in output item: {item}", extra={"request_id": request_id})
                return None
            
            # Construct the original file path
            # ComfyUI typically saves files in OUTPUT_DIR/subfolder/filename
            if subfolder:
                original_path = self.output_dir / subfolder / filename
            else:
                original_path = self.output_dir / filename
            
            # Check if the file exists
            if not original_path.exists():
                logger.warning(f"Original file not found: {original_path}", extra={"request_id": request_id})
                # Try without subfolder as fallback
                if subfolder:
                    fallback_path = self.output_dir / filename
                    if fallback_path.exists():
                        logger.info(f"Found file at fallback location: {fallback_path}", extra={"request_id": request_id})
                        original_path = fallback_path
                    else:
                        return None
                else:
                    return None
            
            # Destination path in job directory
            dest_path = job_output_dir / filename
            
            # Get the real path (in case original_path is a symlink from a cached result)
            real_original_path = original_path.resolve()
            
            logger.info(f"Copying {real_original_path} to {dest_path}", extra={"request_id": request_id})
            
            # Copy the file (using real path to handle symlinks)
            await self._copy_file_async(real_original_path, dest_path)
            
            # Remove original file/symlink and create new symlink pointing to our copy
            if original_path.exists() or original_path.is_symlink():
                await self._remove_file_async(original_path)
            
            # Create symlink from original location to our copy
            await self._create_symlink_async(dest_path, original_path)
            
            logger.debug(f"Created symlink: {original_path} -> {dest_path}", extra={"request_id": request_id})
            
            # Return file info for result
            return {
                "filename": filename,
                "local_path": str(dest_path),
                "type": file_type,
                "subfolder": subfolder,
                "node_id": node_id,
                "output_type": output_type
            }
            
        except Exception as e:
            logger.error(f"Error processing output file {item}: {e}", extra={"request_id": request_id}, exc_info=True)
            return None

    async def _copy_file_async(self, src: Path, dst: Path) -> None:
        """Async file copy"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, shutil.copy2, str(src), str(dst))

    async def _remove_file_async(self, path: Path) -> None:
        """Async file/symlink removal"""
        loop = asyncio.get_running_loop()
        if path.is_symlink():
            await loop.run_in_executor(None, path.unlink)
        else:
            await loop.run_in_executor(None, os.remove, str(path))

    async def _create_symlink_async(self, target: Path, link: Path) -> None:
        """Async symlink creation - link points to target"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, os.symlink, str(target), str(link))

    async def upload_assets(self, request_id: str, s3_config: Dict, result) -> None:
        """Upload assets to Azure Blob Storage"""
        if not hasattr(result, 'output') or not result.output:
            logger.info(f"No assets to upload for {request_id}", extra={"request_id": request_id})
            return
            
        container_client = None
        try:
            # Get Azure Blob Storage configuration
            connection_string = s3_config.get("endpoint_url")
            container_name = s3_config.get("bucket_name")
            
            if not connection_string:
                raise ValueError("Azure connection_string (endpoint_url) is required")
            if not container_name:
                raise ValueError("Azure container_name (bucket_name) is required")
            
            # Create a single ContainerClient for the batch
            # This enables connection pooling and reduces overhead
            container_client = ContainerClient.from_connection_string(
                conn_str=connection_string,
                container_name=container_name
            )
            
            # Upload all files concurrently using the shared client
            tasks = []
            for obj in result.output:
                local_path = obj.get("local_path")
                if local_path and Path(local_path).exists():
                    task = asyncio.create_task(
                        self.upload_file_and_get_url(
                            request_id, container_client, local_path
                        )
                    )
                    tasks.append(task)
                else:
                    logger.warning(f"Local file not found: {local_path}", extra={"request_id": request_id})
                    tasks.append(asyncio.create_task(self._return_none()))
            
            # Wait for all uploads
            if tasks:
                presigned_urls = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Update result objects with URLs
                for obj, url_result in zip(result.output, presigned_urls):
                    if isinstance(url_result, Exception):
                        logger.error(f"Upload failed for {obj.get('local_path')}: {url_result}", extra={"request_id": request_id})
                        obj["upload_error"] = str(url_result)
                    elif url_result:
                        obj["url"] = url_result
                        
                logger.info(f"Uploaded {len([u for u in presigned_urls if u and not isinstance(u, Exception)])} assets for {request_id}", extra={"request_id": request_id})
                    
        except Exception as e:
            logger.error(f"Error uploading assets for {request_id}: {e}", extra={"request_id": request_id})
            raise
        finally:
            # Close the container client to release connections
            if container_client:
                await container_client.close()

    async def _return_none(self):
        """Helper for asyncio.gather with missing files"""
        return None

    async def upload_file_and_get_url(self, request_id: str, container_client: ContainerClient, local_path: str) -> Optional[str]:
        """Upload single file to Azure Blob Storage using streaming and return blob URL"""
        try:
            file_path = Path(local_path)
            blob_name = f"{request_id}_{file_path.name}"
            
            logger.debug(f"Uploading {blob_name} to container (streaming)", extra={"request_id": request_id})

            # Get blob client for this specific blob from the shared container client
            blob_client = container_client.get_blob_client(blob_name)
            
            # Stream file directly without loading into memory
            # The file object is passed directly to upload_blob which reads it in chunks
            async with aiofiles.open(local_path, 'rb') as file:
                await blob_client.upload_blob(file, overwrite=True)

            # Get the blob URL
            blob_url = blob_client.url
            
            logger.debug(f"Uploaded blob URL: {blob_url}", extra={"request_id": request_id})
            return blob_url
            
        except Exception as e:
            logger.error(f"Error uploading {local_path}: {e}", extra={"request_id": request_id})
            raise

    async def send_webhook(self, webhook_url: str, result, extra_params: Dict = None, request_id: str = None) -> None:
        """Send webhook notification with result. Retries on network errors and 5xx responses."""
        timeout = aiohttp.ClientTimeout(total=30)
        
        # Prepare webhook payload
        webhook_data = {
            "id": result.id,
            "status": result.status,
            "message": result.message,
            "output": getattr(result, 'output', [])
        }
        
        # Add extra parameters if provided
        if extra_params:
            webhook_data.update(extra_params)
        
        last_error = None
        for attempt in range(1, WEBHOOK_RETRIES + 1):
            try:
                async with aiohttp.ClientSession(timeout=timeout, connector=self._connector, connector_owner=False) as session:
                    async with session.post(
                        webhook_url,
                        json=webhook_data,
                        headers={'Content-Type': 'application/json'}
                    ) as response:
                        if response.status < 400:
                            logger.info(f"Webhook sent successfully to {webhook_url}", extra={"request_id": request_id})
                            return
                        
                        error_text = await response.text()
                        # 4xx = client error, don't retry
                        if response.status < 500:
                            logger.warning(f"Webhook failed (status {response.status}): {error_text}", extra={"request_id": request_id})
                            return
                        
                        # 5xx = server error, retry
                        last_error = f"status {response.status}: {error_text}"
                        logger.warning(
                            f"Webhook attempt {attempt}/{WEBHOOK_RETRIES} failed ({last_error})",
                            extra={"request_id": request_id}
                        )
            except Exception as e:
                last_error = str(e)
                logger.warning(
                    f"Webhook attempt {attempt}/{WEBHOOK_RETRIES} error: {last_error}",
                    extra={"request_id": request_id}
                )
            
            # Exponential backoff before next retry (10s, 20s, 40s, …)
            if attempt < WEBHOOK_RETRIES:
                delay = 10 * 2 ** (attempt - 1)
                await asyncio.sleep(delay)
        
        logger.error(
            f"Webhook to {webhook_url} failed after {WEBHOOK_RETRIES} attempts. Last error: {last_error}",
            extra={"request_id": request_id}
        )
        # Don't raise - webhook failures shouldn't fail the whole job

    
    async def send_webhook_for_session_close(self, webhook_url: str, result, session_auth_data: Dict = None, request_id: str = None) -> None:
        """Send session-close webhook notification. Retries on network errors and 5xx responses."""
        timeout = aiohttp.ClientTimeout(total=30)
        
        # Prepare webhook payload
        webhook_data = {
            "id": result.id,
            "status": result.status,
            "message": result.message,
            "output": getattr(result, 'output', [])
        }
        
        # Add session auth data if provided
        if session_auth_data:
            webhook_data["session_auth"] = session_auth_data
        
        last_error = None
        for attempt in range(1, WEBHOOK_RETRIES + 1):
            try:
                async with aiohttp.ClientSession(timeout=timeout, connector=self._connector, connector_owner=False) as session:
                    async with session.post(
                        webhook_url,
                        json=webhook_data,
                        headers={'Content-Type': 'application/json'}
                    ) as response:
                        if response.status < 400:
                            logger.info(f"Session close webhook sent successfully to {webhook_url}", extra={"request_id": request_id})
                            return
                        
                        error_text = await response.text()
                        # 4xx = client error, don't retry
                        if response.status < 500:
                            logger.warning(f"Session close webhook failed (status {response.status}): {error_text}", extra={"request_id": request_id})
                            return
                        
                        # 5xx = server error, retry
                        last_error = f"status {response.status}: {error_text}"
                        logger.warning(
                            f"Session close webhook attempt {attempt}/{WEBHOOK_RETRIES} failed ({last_error})",
                            extra={"request_id": request_id}
                        )
            except Exception as e:
                last_error = str(e)
                logger.warning(
                    f"Session close webhook attempt {attempt}/{WEBHOOK_RETRIES} error: {last_error}",
                    extra={"request_id": request_id}
                )
            
            # Exponential backoff before next retry (10s, 20s, 40s, …)
            if attempt < WEBHOOK_RETRIES:
                delay = 10 * 2 ** (attempt - 1)
                await asyncio.sleep(delay)
        
        logger.error(
            f"Session close webhook to {webhook_url} failed after {WEBHOOK_RETRIES} attempts. Last error: {last_error}",
            extra={"request_id": request_id}
        )
        # Don't raise - webhook failures shouldn't fail the whole job

    
    
    async def get_s3_config(self, input_data, request_id: str = None) -> Optional[Dict]:
        """Get S3 configuration from payload or centralized config (from environment)"""
        try:
            # Check if S3 config provided in payload
            if hasattr(input_data, 's3') and input_data.s3:
                if input_data.s3.is_configured():
                    logger.info("Using S3 config from payload", extra={"request_id": request_id})
                    return input_data.s3.get_config()
            
            # Fall back to centralized config (which reads from environment)
            if S3_ENABLED:
                logger.info("Using S3 config from environment variables", extra={"request_id": request_id})
                return S3_CONFIG.copy()  # Return a copy to avoid mutation
            
            # No valid config found
            logger.debug("No S3 configuration available", extra={"request_id": request_id})
            return None
            
        except Exception as e:
            logger.error(f"Error getting S3 config: {e}", extra={"request_id": request_id})
            return None

    async def get_webhook_config(self, input_data, request_id: str = None) -> Optional[Dict]:
        """Get webhook configuration from payload or centralized config (from environment)"""
        try:
            # Check if webhook config provided in payload
            if hasattr(input_data, 'webhook') and input_data.webhook:
                if input_data.webhook.has_valid_url():
                    logger.info("Using webhook config from payload", extra={"request_id": request_id})
                    config_data = {
                        'url': input_data.webhook.url,
                        'session_close_url': input_data.webhook.session_close_url,
                        'extra_params': input_data.webhook.extra_params,
                        'timeout': input_data.webhook.timeout,
                        'session_auth_data': getattr(input_data.webhook, 'session_auth_data', None)
                    }
                    logger.info(f"Webhook config: {config_data}", extra={"request_id": request_id})
                    return config_data
            
            # Fall back to centralized config (which reads from environment)
            if WEBHOOK_ENABLED:
                logger.info("Using webhook config from environment variables", extra={"request_id": request_id})
                return {
                    'url': WEBHOOK_CONFIG['url'],
                    'session_close_url': WEBHOOK_CONFIG.get('session_close_url', ''),
                    'extra_params': {},
                    'timeout': WEBHOOK_CONFIG['timeout'],
                    'session_auth_data': None
                }
            
            # No valid config found
            logger.debug("No webhook configuration available", extra={"request_id": request_id})
            return None
            
        except Exception as e:
            logger.error(f"Error getting webhook config: {e}", extra={"request_id": request_id})
            return None