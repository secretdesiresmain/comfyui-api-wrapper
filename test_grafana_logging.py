#!/usr/bin/env python3
"""
Test script to verify Grafana Cloud integration via OpenTelemetry OTLP.

Sends test traces AND structured logs to Grafana Cloud so you can verify
that both Tempo (traces) and Loki (logs) are receiving data.

Usage:
    python test_grafana_logging.py

Required environment variables:
    GRAFANA_HOSTNAME=https://otlp-gateway-prod-us-east-2.grafana.net/otlp
    GRAFANA_USERNAME=1141888
    GRAFANA_APIKEY=glc_eyJvIjoiMTMyMT...

Then check Grafana:
    Traces  → Explore → grafanacloud-*-traces  → Search by service.name
    Logs    → Explore → grafanacloud-*-logs    → {service_name="vastai-server"}
"""

import os
import sys
import time
import uuid

# ── Set environment defaults for testing ──────────────────────────────
os.environ.setdefault(
    "GRAFANA_HOSTNAME",
    "https://otlp-gateway-prod-us-east-2.grafana.net/otlp",
)
os.environ.setdefault("GRAFANA_USERNAME", "1141888")
os.environ.setdefault("LOG_FORMAT", "json")
os.environ.setdefault("LOKI_APP_NAME", "comfyui-api-wrapper")
os.environ.setdefault("LOGGING_ENVIRONMENT", "beta")

# ── Gate on API key ───────────────────────────────────────────────────
if not os.environ.get("GRAFANA_APIKEY"):
    print("❌ ERROR: GRAFANA_APIKEY environment variable is not set!")
    print()
    print("Please set it first:")
    print('  export GRAFANA_APIKEY="glc_eyJvIjoiMTMyMTYy..."')
    print()
    sys.exit(1)

# ── Import after env vars are in place ────────────────────────────────
from config.otel_config import setup_otel, get_tracer
from config.logging_config import setup_logging, get_logger, ErrorMetrics

print("=" * 60)
print("🔧 Grafana Cloud OTLP Test (Traces + Logs)")
print("=" * 60)
print()

# 1. Initialise OTel (creates TracerProvider + LoggerProvider)
otel_handler = setup_otel(service_name="vastai-server")

# 2. Setup Python logging with the OTel handler attached
setup_logging(service_name="vastai-server", otel_handler=otel_handler)
logger = get_logger(__name__, test_run=True)

# 3. Grab a tracer for manual span creation
tracer = get_tracer("test_grafana_logging")

print()
print("📤 Sending test traces + logs to Grafana Cloud...")
print()

test_request_id = f"test-{uuid.uuid4().hex[:8]}"
print(f"🔑 Test request_id: {test_request_id}")
print()

# ── Test 1: Log WITHOUT any active span ──────────────────────────────
logger.info("Log without span context – no trace_id expected")
time.sleep(0.3)

# ── Test 2: Log INSIDE a span (trace-correlated) ─────────────────────
if tracer:
    with tracer.start_as_current_span("test-request-processing") as span:
        span.set_attribute("request.id", test_request_id)
        span.set_attribute("test", True)

        logger.info(
            "Request received – inside span, trace_id will appear in log",
            extra={"request_id": test_request_id, "event": "request_start"},
        )
        time.sleep(0.3)

        # Nested span
        with tracer.start_as_current_span("preprocess") as child:
            child.set_attribute("worker.id", 1)
            child.set_attribute("worker.type", "preprocess")
            logger.info(
                "Preprocessing workflow",
                extra={
                    "request_id": test_request_id,
                    "stage": "preprocess_start",
                    "worker_id": 1,
                },
            )
            time.sleep(0.5)
            child.set_attribute("preprocess.duration_ms", 500)

        # Timing metric
        ErrorMetrics.log_request_timing(
            logger,
            request_id=test_request_id,
            stage="preprocess",
            duration_ms=500.0,
            status="success",
            worker_id=1,
            modifier="TestModifier",
        )
        time.sleep(0.3)

        with tracer.start_as_current_span("generation") as child:
            child.set_attribute("worker.id", 1)
            logger.info(
                "Generation started",
                extra={
                    "request_id": test_request_id,
                    "stage": "generation_start",
                },
            )
            time.sleep(0.5)

        # Simulate an error inside the span
        error_request_id = f"test-{uuid.uuid4().hex[:8]}"
        try:
            raise ValueError("Test exception for logging")
        except Exception as e:
            ErrorMetrics.log_error(
                logger, e, "preprocessing",
                request_id=error_request_id,
                worker_id=1,
            )
        time.sleep(0.3)

        # Completion log
        logger.info(
            "Request completed successfully",
            extra={
                "request_id": test_request_id,
                "event": "request_complete",
                "total_duration_ms": 1800,
            },
        )
else:
    logger.info("OTel tracer not available – logging without trace context")

# ── Wait for batched export ───────────────────────────────────────────
print()
print("⏳ Waiting for OTLP batch export flush (5 s)…")
time.sleep(5)

print()
print("=" * 60)
print("✅ Test data sent!")
print("=" * 60)
print()
print("📊 Verify in Grafana Cloud:")
print()
print("  TRACES (Tempo):")
print('    Explore → grafanacloud-*-traces → Service Name = "vastai-server"')
print(f'    Search for span name "test-request-processing"')
print()
print("  LOGS (Loki via OTLP):")
print('    Explore → grafanacloud-*-logs')
print('    {service_name="vastai-server"} | json')
print(f'    Filter: |= "{test_request_id}"')
print()
print("  CORRELATE LOGS ↔ TRACES:")
print("    In a trace detail view, click 'Logs for this span' to see")
print("    logs that share the same trace_id.")
print()
