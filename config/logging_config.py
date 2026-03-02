"""
Logging configuration with Grafana Loki + OpenTelemetry integration.

Supports:
- Console logging (human-readable or JSON)
- Loki log aggregation for Grafana dashboards
- OpenTelemetry OTLP export (traces + structured logs)
- Structured logging with request context and trace correlation
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

# ── Engine / endpoint name (read once from environment) ──
ENGINE_NAME: str = os.getenv("ENGINE_NAME", "")

# Try to import Loki handler
try:
    import logging_loki
    LOKI_AVAILABLE = True
except ImportError:
    LOKI_AVAILABLE = False

# Try to import OTel trace context helpers
try:
    from opentelemetry import trace as otel_trace
    OTEL_TRACE_AVAILABLE = True
except ImportError:
    OTEL_TRACE_AVAILABLE = False


class TraceContextFilter(logging.Filter):
    """
    Injects OpenTelemetry trace context (trace_id, span_id) into every
    log record so that console / JSON logs can be correlated with traces
    in Grafana Tempo.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if OTEL_TRACE_AVAILABLE:
            span = otel_trace.get_current_span()
            ctx = span.get_span_context() if span else None
            if ctx and ctx.trace_id:
                record.otel_trace_id = format(ctx.trace_id, "032x")
                record.otel_span_id = format(ctx.span_id, "016x")
            else:
                record.otel_trace_id = ""
                record.otel_span_id = ""
        else:
            record.otel_trace_id = ""
            record.otel_span_id = ""
        return True


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    Outputs logs in a format that's easy to parse and query in Grafana.
    Prepends [request_id] to messages when available for easy tracking.
    Includes OTel trace_id / span_id when available.
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
        
        # Add endpoint as top-level field for easy Grafana querying
        endpoint = getattr(record, 'endpoint', None)
        if endpoint:
            log_data["endpoint"] = endpoint
        
        # Add OTel trace context for Grafana Tempo correlation
        trace_id = getattr(record, 'otel_trace_id', '')
        span_id = getattr(record, 'otel_span_id', '')
        if trace_id:
            log_data["trace_id"] = trace_id
            log_data["span_id"] = span_id
        
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
            'taskName', 'message', 'request_id',
            'otel_trace_id', 'otel_span_id',  # handled above
            'endpoint',  # handled above
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
    Filter that prepends [request_id] and [ENGINE_NAME] to log messages.
    This modifies the record BEFORE it reaches any handler,
    so all handlers (including Loki) will see the prefix.

    ENGINE_NAME is read once from the environment at import time.
    """
    
    def filter(self, record: logging.LogRecord) -> bool:
        request_id = getattr(record, 'request_id', None)

        if request_id:
            # Get the fully formatted message first
            original_message = record.getMessage()
            if not original_message.startswith(f'[{request_id}]'):
                # Build prefix: [request_id] [ENGINE_NAME] message
                prefix = f'[{request_id}]'
                if ENGINE_NAME:
                    prefix += f' [{ENGINE_NAME}]'
                # Replace msg with the complete formatted message + prefix
                # Clear args since the message is now fully formatted
                record.msg = f'{prefix} {original_message}'
                record.args = ()
        elif ENGINE_NAME:
            # No request_id but we have an engine name
            original_message = record.getMessage()
            if not original_message.startswith(f'[{ENGINE_NAME}]'):
                record.msg = f'[{ENGINE_NAME}] {original_message}'
                record.args = ()

        # Store engine name on the record so JSONFormatter can pick it up
        if ENGINE_NAME:
            record.endpoint = ENGINE_NAME
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
    2. Grafana Cloud (Loki endpoint): GRAFANA_HOSTNAME pointing at a
       Loki-specific host like https://logs-prod-036.grafana.net
    
    NOTE: When GRAFANA_HOSTNAME is an OTLP gateway (contains '/otlp'),
    logs are already sent via the OpenTelemetry OTLP exporter and Grafana
    Cloud routes them to Loki automatically — the legacy Loki handler is
    NOT needed and is disabled to avoid 404 errors.
    """
    
    def __init__(self):
        # Check for Grafana Cloud configuration first
        grafana_hostname = os.getenv("GRAFANA_HOSTNAME", "")
        grafana_username = os.getenv("GRAFANA_USERNAME", "")
        grafana_apikey = os.getenv("GRAFANA_APIKEY", "")
        
        if grafana_hostname:
            # If the hostname is an OTLP gateway, skip legacy Loki —
            # OTel OTLP exporter already handles log delivery.
            if "/otlp" in grafana_hostname.lower():
                self.enabled = False
                self.url = ""
                self.username = ""
                self.password = ""
            else:
                # Grafana Cloud Loki-direct mode
                # Expects a Loki host like: https://logs-prod-036.grafana.net
                self.enabled = True
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
    loki_config: LokiConfig = None,
    otel_handler: logging.Handler = None,
) -> logging.Logger:
    """
    Configure application logging with optional Loki + OpenTelemetry integration.
    
    Args:
        service_name: Name of the service for log identification
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_logs: Whether to output JSON formatted logs
        loki_config: Loki configuration object
        otel_handler: An OpenTelemetry LoggingHandler returned by ``setup_otel()``
    
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
    
    # Create shared filters
    request_id_filter = RequestIdPrefixFilter()
    trace_ctx_filter = TraceContextFilter()
    
    # Create formatters
    if json_logs:
        formatter = JSONFormatter(service_name=service_name)
    else:
        formatter = TextFormatter()
    
    # Console handler with request_id + trace context filters
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, log_level, logging.INFO))
    console_handler.addFilter(trace_ctx_filter)
    console_handler.addFilter(request_id_filter)
    root_logger.addHandler(console_handler)
    
    # ── OpenTelemetry OTLP handler (traces + logs → Grafana) ──────
    if otel_handler is not None:
        otel_handler.setLevel(getattr(logging, log_level, logging.INFO))
        otel_handler.addFilter(trace_ctx_filter)
        otel_handler.addFilter(request_id_filter)
        root_logger.addHandler(otel_handler)
        root_logger.info("OpenTelemetry OTLP log handler attached to root logger")
    
    # ── Loki handler (legacy / direct push) ───────────────────────
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
                
                # Add filters to Loki handler
                loki_handler.addFilter(trace_ctx_filter)
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
    logging.getLogger("opentelemetry").setLevel(logging.WARNING)
    
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

