"""
OpenTelemetry configuration for Grafana Cloud integration.

Sends traces and structured logs via OTLP protocol to Grafana Cloud.
Requires:
    GRAFANA_HOSTNAME  - OTLP gateway endpoint (e.g. https://otlp-gateway-prod-us-east-2.grafana.net/otlp)
    GRAFANA_USERNAME  - Grafana Cloud instance ID
    GRAFANA_APIKEY    - Grafana Cloud API token
    LOKI_APP_NAME     - Service/application name (used as service.name resource attribute)
    LOGGING_ENVIRONMENT - Deployment environment label (beta, production, etc.)
"""

import os
import base64
import logging
from typing import Optional

# ---------- OpenTelemetry imports (guarded) ----------
try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME

    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False


_logger = logging.getLogger(__name__)


def _build_auth_headers(username: str, apikey: str) -> dict:
    """Build Basic-auth header expected by Grafana Cloud OTLP gateway."""
    token = base64.b64encode(f"{username}:{apikey}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def setup_otel(
    app=None,
    service_name: str = "comfyui-api-wrapper",
) -> Optional[logging.Handler]:
    """
    Initialise OpenTelemetry TracerProvider + LoggerProvider and,
    optionally, instrument a FastAPI application.

    Returns an ``opentelemetry.sdk._logs.LoggingHandler`` that can be
    attached to the Python root logger so that every ``logging`` call is
    also forwarded to Grafana via OTLP.  Returns ``None`` when OTel is
    not available or the required env-vars are missing.
    """
    if not OTEL_AVAILABLE:
        _logger.warning(
            "OpenTelemetry packages not installed – skipping OTEL setup. "
            "Install with: pip install opentelemetry-api opentelemetry-sdk "
            "opentelemetry-exporter-otlp-proto-http opentelemetry-instrumentation-fastapi"
        )
        return None

    hostname = os.getenv("GRAFANA_HOSTNAME", "").rstrip("/")
    username = os.getenv("GRAFANA_USERNAME", "")
    apikey = os.getenv("GRAFANA_APIKEY", "")

    if not hostname or not username or not apikey:
        _logger.info(
            "GRAFANA_HOSTNAME / GRAFANA_USERNAME / GRAFANA_APIKEY not fully set – "
            "OTEL export disabled."
        )
        return None

    app_name = os.getenv("LOKI_APP_NAME", service_name)
    environment = os.getenv(
        "LOGGING_ENVIRONMENT",
        os.getenv("LOKI_ENVIRONMENT", "development"),
    )

    headers = _build_auth_headers(username, apikey)

    # ── Resource (shared by traces & logs) ─────────────────────────
    resource = Resource.create(
        {
            SERVICE_NAME: app_name,
            "deployment.environment": environment,
            "service.namespace": "comfyui",
            "service.version": os.getenv("SERVICE_VERSION", "1.0.0"),
            "host.name": os.getenv("HOSTNAME", os.getenv("POD_NAME", "unknown")),
        }
    )

    # ── Traces ─────────────────────────────────────────────────────
    tracer_provider = TracerProvider(resource=resource)
    span_exporter = OTLPSpanExporter(
        endpoint=f"{hostname}/v1/traces",
        headers=headers,
    )
    tracer_provider.add_span_processor(BatchSpanProcessor(span_exporter))
    trace.set_tracer_provider(tracer_provider)

    # ── Logs ───────────────────────────────────────────────────────
    logger_provider = LoggerProvider(resource=resource)
    log_exporter = OTLPLogExporter(
        endpoint=f"{hostname}/v1/logs",
        headers=headers,
    )
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

    # Bridge Python logging → OTel LoggerProvider
    otel_log_handler = LoggingHandler(
        level=logging.NOTSET,
        logger_provider=logger_provider,
    )

    # ── FastAPI instrumentation ────────────────────────────────────
    if app is not None:
        FastAPIInstrumentor.instrument_app(app)
        _logger.info("FastAPI instrumented with OpenTelemetry")

    _logger.info(
        "OpenTelemetry initialised – traces and logs export to %s "
        "[service=%s, env=%s]",
        hostname,
        app_name,
        environment,
    )

    return otel_log_handler


def get_tracer(name: str = __name__):
    """Convenience helper to get an OTel tracer (safe even when OTel is absent)."""
    if OTEL_AVAILABLE:
        return trace.get_tracer(name)
    # Return a no-op object whose methods do nothing
    return None

