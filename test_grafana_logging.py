#!/usr/bin/env python3
"""
Test script to verify Grafana Cloud Loki logging integration.

Usage:
    python test_grafana_logging.py

Make sure these environment variables are set:
    GRAFANA_HOSTNAME=https://logs-prod-036.grafana.net
    GRAFANA_USERNAME=1099667
    GRAFANA_APIKEY=your_api_key

Then check Grafana with query:
    {application="comfyui-api-wrapper"}
"""

import os
import sys
import time

# Set environment variables for testing (override if needed)
os.environ.setdefault("GRAFANA_HOSTNAME", "https://logs-prod-036.grafana.net")
os.environ.setdefault("GRAFANA_USERNAME", "1099667")
# GRAFANA_APIKEY must be set externally for security

os.environ.setdefault("LOG_FORMAT", "json")
os.environ.setdefault("LOKI_APP_NAME", "comfyui-api-wrapper")
os.environ.setdefault("LOKI_ENVIRONMENT", "testing")

# Check for API key
if not os.environ.get("GRAFANA_APIKEY"):
    print("❌ ERROR: GRAFANA_APIKEY environment variable is not set!")
    print()
    print("Please set it first:")
    print('  export GRAFANA_APIKEY="glc_eyJvIjoiMTMyMTYy..."')
    print()
    sys.exit(1)

# Now import and setup logging
from config.logging_config import setup_logging, get_logger, ErrorMetrics

print("=" * 60)
print("🔧 Grafana Cloud Loki Logging Test")
print("=" * 60)
print()

# Setup logging
setup_logging(service_name="comfyui-api-wrapper")
logger = get_logger(__name__, test_run=True)

print()
print("📤 Sending test logs to Grafana Cloud...")
print()

# Generate a test request_id to simulate real workflow
import uuid
test_request_id = f"test-{uuid.uuid4().hex[:8]}"
print(f"🔑 Using test request_id: {test_request_id}")
print()

# Log WITHOUT request_id (no prefix)
logger.info("Log without request_id - should have no prefix")
time.sleep(0.3)

# Log WITH request_id (will have [request_id] prefix)
logger.info("Request received - starting processing", extra={
    "request_id": "craig-123",
    "event": "request_start"
})
time.sleep(0.3)

logger.info("Preprocessing workflow", extra={
    "request_id": test_request_id,
    "stage": "preprocess_start",
    "worker_id": 1,
    "worker_type": "preprocess"
})
time.sleep(0.3)

logger.warning("Slow processing detected", extra={
    "request_id": test_request_id,
    "duration_ms": 5000,
    "event": "slow_warning"
})
time.sleep(0.3)

# Simulate timing log (uses request_id)
ErrorMetrics.log_request_timing(
    logger,
    request_id=test_request_id,
    stage="preprocess",
    duration_ms=1234.56,
    status="success",
    worker_id=1,
    modifier="TestModifier"
)
time.sleep(0.3)

logger.info("Generation started", extra={
    "request_id": test_request_id,
    "stage": "generation_start",
    "worker_id": 1,
    "worker_type": "generation"
})
time.sleep(0.3)

# Simulate error log with different request_id
error_request_id = f"test-{uuid.uuid4().hex[:8]}"
try:
    raise ValueError("Test exception for logging")
except Exception as e:
    ErrorMetrics.log_error(
        logger, e, "preprocessing",
        request_id=error_request_id,
        worker_id=1
    )
time.sleep(0.3)

# Final success log
logger.info("Request completed successfully", extra={
    "request_id": test_request_id,
    "event": "request_complete",
    "total_duration_ms": 3500
})

# Wait for logs to be flushed
print("⏳ Waiting for logs to be sent...")
time.sleep(3)

print()
print("=" * 60)
print("✅ Test logs sent!")
print("=" * 60)
print()
print("📊 Now check your Grafana Cloud instance:")
print()
print("1. Go to: https://grafana.com → Sign in → Your Grafana instance")
print("2. Navigate to: Explore → Select 'grafanacloud-*-logs' datasource")
print("3. Run this query:")
print()
print('   {application="comfyui-api-wrapper", environment="testing"}')
print()
print("You should see logs with [request_id] prefix in messages like:")
print(f'   [INFO] [{test_request_id}] Request received - starting processing')
print()
print("🔍 Filter by specific request_id:")
print(f'   {{application="comfyui-api-wrapper"}} |= "[{test_request_id}]"')
print()
print("Other useful queries:")
print('   {application="comfyui-api-wrapper"} | json | request_id != ""')
print('   {application="comfyui-api-wrapper"} | json | error_category != ""')
print('   {application="comfyui-api-wrapper"} |= "metric_type"')

