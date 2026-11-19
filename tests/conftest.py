import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
for key in ('GRAFANA_HOSTNAME','GRAFANA_APIKEY','GRAFANA_API_KEY','GRAFANA_USERNAME','OTEL_EXPORTER_OTLP_ENDPOINT'):
    os.environ.pop(key, None)
os.environ['OUTPUT_DIR']='/tmp/image-observability-test-output'
os.environ['LOGGING_ENVIRONMENT']='test'
