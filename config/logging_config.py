"""
Logging configuration with Grafana Loki integration.

Supports:
- Console logging (human-readable or JSON)
- Loki log aggregation for Grafana dashboards
- Structured logging with request context
- Error tracking and metrics labels
"""
import logging
import sys
import json
import time
import os
from datetime import datetime
from typing import Optional, Dict, Any
from logging.handlers import QueueHandler, QueueListener
from queue import Queue

# Try to import Loki handler
try:
    import logging_loki
    LOKI_AVAILABLE = True
except ImportError:
    LOKI_AVAILABLE = False


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    Outputs logs in a format that's easy to parse and query in Grafana.
    Prepends [request_id] to messages when available for easy tracking.
    """
    
    def __init__(self, service_name: str = "comfyui-api"):
        super().__init__()
        self.service_name = service_name
    
    def format(self, record: logging.LogRecord) -> str:
        # Get request_id if present in record
        request_id = getattr(record, 'request_id', None)
        
        # Message already has [request_id] prefix from RequestIdPrefixFilter
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": self.service_name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add request_id as top-level field for easy Grafana querying
        if request_id:
            log_data["request_id"] = request_id
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info)
            }
        
        # Add extra fields from record
        extra_fields = {}
        standard_attrs = {
            'name', 'msg', 'args', 'created', 'filename', 'funcName',
            'levelname', 'levelno', 'lineno', 'module', 'msecs',
            'pathname', 'process', 'processName', 'relativeCreated',
            'stack_info', 'exc_info', 'exc_text', 'thread', 'threadName',
            'taskName', 'message', 'request_id'  # exclude request_id from extra since it's top-level
        }
        
        for key, value in record.__dict__.items():
            if key not in standard_attrs:
                try:
                    # Ensure value is JSON serializable
                    json.dumps(value)
                    extra_fields[key] = value
                except (TypeError, ValueError):
                    extra_fields[key] = str(value)
        
        if extra_fields:
            log_data["extra"] = extra_fields
        
        return json.dumps(log_data)


class TextFormatter(logging.Formatter):
    """
    Text formatter for human-readable console output.
    [request_id] prefix is added by RequestIdPrefixFilter.
    """
    
    def __init__(self):
        super().__init__(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )


class RequestIdPrefixFilter(logging.Filter):
    """
    Filter that prepends [request_id] to log messages.
    This modifies the record BEFORE it reaches any handler,
    so all handlers (including Loki) will see the prefix.
    """
    
    def filter(self, record: logging.LogRecord) -> bool:
        request_id = getattr(record, 'request_id', None)
        if request_id:
            # Get the fully formatted message first
            original_message = record.getMessage()
            if not original_message.startswith(f'[{request_id}]'):
                # Replace msg with the complete formatted message + prefix
                # Clear args since the message is now fully formatted
                record.msg = f'[{request_id}] {original_message}'
                record.args = ()
        return True


class RequestContextFilter(logging.Filter):
    """
    Filter that adds request context to log records.
    Useful for tracing logs across a single request.
    """
    
    def __init__(self):
        super().__init__()
        self._context: Dict[str, Any] = {}
    
    def set_context(self, **kwargs):
        """Set context that will be added to all logs."""
        self._context.update(kwargs)
    
    def clear_context(self):
        """Clear the current context."""
        self._context.clear()
    
    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in self._context.items():
            setattr(record, key, value)
        return True


class LokiConfig:
    """Configuration for Loki logging.
    
    Supports two configuration modes:
    1. Direct Loki: LOKI_URL, LOKI_USERNAME, LOKI_PASSWORD
    2. Grafana Cloud: GRAFANA_HOSTNAME, GRAFANA_USERNAME, GRAFANA_APIKEY
    
    Grafana Cloud variables take precedence if GRAFANA_HOSTNAME is set.
    """
    
    def __init__(self):
        # Check for Grafana Cloud configuration first
        grafana_hostname = os.getenv("GRAFANA_HOSTNAME", "")
        grafana_username = os.getenv("GRAFANA_USERNAME", "")
        grafana_apikey = os.getenv("GRAFANA_APIKEY", "")
        
        if grafana_hostname:
            # Grafana Cloud mode
            self.enabled = True  # Auto-enable when Grafana Cloud is configured
            # Construct Loki push URL from Grafana hostname
            # Grafana Cloud Loki endpoints are like: https://logs-prod-036.grafana.net
            hostname = grafana_hostname.rstrip("/")
            self.url = f"{hostname}/loki/api/v1/push"
            self.username = grafana_username
            self.password = grafana_apikey
        else:
            # Direct Loki mode
            self.enabled = os.getenv("LOKI_ENABLED", "false").lower() == "true"
            self.url = os.getenv("LOKI_URL", "http://localhost:3100/loki/api/v1/push")
            self.username = os.getenv("LOKI_USERNAME", "")
            self.password = os.getenv("LOKI_PASSWORD", "")
        
        # Labels for Loki (these help with filtering in Grafana)
        self.labels = {
            "application": os.getenv("LOKI_APP_NAME", "comfyui-api-wrapper"),
            "environment": os.getenv("LOKI_ENVIRONMENT", "development"),
            "host": os.getenv("HOSTNAME", os.getenv("POD_NAME", "unknown")),
        }
        
        # Additional custom labels from environment
        custom_labels = os.getenv("LOKI_CUSTOM_LABELS", "")
        if custom_labels:
            for label in custom_labels.split(","):
                if "=" in label:
                    key, value = label.split("=", 1)
                    self.labels[key.strip()] = value.strip()
    
    def get_auth(self) -> Optional[tuple]:
        """Get authentication tuple if credentials are configured."""
        if self.username and self.password:
            return (self.username, self.password)
        return None


def setup_logging(
    service_name: str = "comfyui-api",
    log_level: str = None,
    json_logs: bool = None,
    loki_config: LokiConfig = None
) -> logging.Logger:
    """
    Configure application logging with optional Loki integration.
    
    Args:
        service_name: Name of the service for log identification
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_logs: Whether to output JSON formatted logs
        loki_config: Loki configuration object
    
    Returns:
        Configured root logger
    """
    # Get configuration from environment if not provided
    if log_level is None:
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    if json_logs is None:
        json_logs = os.getenv("LOG_FORMAT", "text").lower() == "json"
    
    if loki_config is None:
        loki_config = LokiConfig()
    
    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Create the request_id prefix filter (will be added to each handler)
    request_id_filter = RequestIdPrefixFilter()
    
    # Create formatters
    if json_logs:
        formatter = JSONFormatter(service_name=service_name)
    else:
        formatter = TextFormatter()
    
    # Console handler with request_id filter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, log_level, logging.INFO))
    console_handler.addFilter(request_id_filter)
    root_logger.addHandler(console_handler)
    
    # Loki handler
    if loki_config.enabled:
        if not LOKI_AVAILABLE:
            root_logger.warning(
                "Loki logging enabled but 'python-logging-loki' not installed. "
                "Install with: pip install python-logging-loki"
            )
        else:
            try:
                loki_handler = logging_loki.LokiHandler(
                    url=loki_config.url,
                    tags=loki_config.labels,
                    auth=loki_config.get_auth(),
                    version="1",
                )
                loki_handler.setLevel(getattr(logging, log_level, logging.INFO))
                
                # Add filter to Loki handler to prepend [request_id]
                loki_handler.addFilter(request_id_filter)
                
                # Use queue-based handler for async logging to Loki
                log_queue = Queue(-1)  # Unlimited queue size
                queue_handler = QueueHandler(log_queue)
                queue_handler.addFilter(request_id_filter)  # Also filter before queuing
                queue_listener = QueueListener(
                    log_queue, 
                    loki_handler,
                    respect_handler_level=True
                )
                queue_listener.start()
                
                root_logger.addHandler(queue_handler)
                root_logger.info(
                    f"Loki logging enabled: {loki_config.url} [env={loki_config.labels.get('environment', 'unknown')}]",
                    extra={"loki_labels": loki_config.labels}
                )
                
            except Exception as e:
                root_logger.error(f"Failed to initialize Loki handler: {e}")
    
    # Reduce noise from third-party libraries
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    return root_logger


class LoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that adds consistent context to all log messages.
    Useful for adding request_id, worker_id, etc.
    """
    
    def process(self, msg, kwargs):
        # Merge extra from adapter with extra from log call
        extra = self.extra.copy()
        if 'extra' in kwargs:
            extra.update(kwargs['extra'])
        kwargs['extra'] = extra
        return msg, kwargs


def get_logger(name: str, **context) -> logging.LoggerAdapter:
    """
    Get a logger with optional context.
    
    Args:
        name: Logger name (usually __name__)
        **context: Additional context to add to all log messages
    
    Returns:
        LoggerAdapter with context
    
    Example:
        logger = get_logger(__name__, worker_id=1, worker_type="preprocess")
        logger.info("Processing job", extra={"request_id": "abc123"})
    """
    base_logger = logging.getLogger(name)
    return LoggerAdapter(base_logger, context)


# Metrics helper for error tracking
class ErrorMetrics:
    """
    Simple error metrics tracking for logging.
    These can be queried in Grafana from the log labels.
    """
    
    @staticmethod
    def log_error(
        logger: logging.Logger,
        error: Exception,
        category: str,
        request_id: str = None,
        **extra
    ):
        """
        Log an error with consistent structure for Grafana dashboards.
        
        Args:
            logger: Logger instance
            error: The exception that occurred
            category: Error category (e.g., "preprocessing", "generation", "upload")
            request_id: Request ID if available
            **extra: Additional context
        """
        error_data = {
            "error_category": category,
            "error_type": type(error).__name__,
            "error_message": str(error),
        }
        if request_id:
            error_data["request_id"] = request_id
        error_data.update(extra)
        
        logger.error(
            f"[{category}] {type(error).__name__}: {error}",
            extra=error_data,
            exc_info=True
        )
    
    @staticmethod
    def log_request_timing(
        logger: logging.Logger,
        request_id: str,
        stage: str,
        duration_ms: float,
        status: str = "success",
        **extra
    ):
        """
        Log request timing for performance dashboards.
        
        Args:
            logger: Logger instance
            request_id: Request ID
            stage: Processing stage (preprocess, generation, postprocess)
            duration_ms: Duration in milliseconds
            status: Status of the operation
            **extra: Additional context
        """
        timing_data = {
            "request_id": request_id,
            "stage": stage,
            "duration_ms": duration_ms,
            "status": status,
            "metric_type": "timing",
        }
        timing_data.update(extra)
        
        logger.info(
            f"[timing] {stage} completed in {duration_ms:.2f}ms ({status})",
            extra=timing_data
        )

