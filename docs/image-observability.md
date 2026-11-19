# Image observability contract

This PR targets `production` and coordinates with server beta-main and yaml-deployment's image dashboards. It does not deploy endpoints or publish dashboards.

Optional `input.telemetry` preserves operation/job/request/trace/user IDs, generation type, selected engine, gateway/provider provenance, dispatch and execution IDs. The private receipt token is returned only in callbacks/results and excluded from telemetry logs. Typed fields are bounded; workflow loader components are allowlisted. Only the Node server emits counted `media_provider_attempt` events. Python observations are diagnostics with zero count weight.

Set `IMAGE_TELEMETRY_REDIS_URL` (or `REDIS_URI`), optional `REDIS_PASSWORD` and `REDIS_TLS=true`, and `LOGGING_ENVIRONMENT=beta`. Use private connectivity to the existing Redis/Valkey deployment. No new Mongo collection. Set SERVICE_VERSION to the build digest/version and ensure Loki/OTLP uses the same environment. Tracked requests require Redis admission and fail before submission when it is unavailable; untracked legacy callers remain compatible.

Operational state uses a per-environment, per-service (`worker`) hashed namespace, seven-day retention, one-hour overdue deadline, 10,000 pending entries, and scans of at most 100. Admission claims suppress duplicate execution IDs; payload conflicts are rejected. A new deliberate execution uses a new ID. Accepted-work records diagnose lost in-memory queues/orphaned GPU work; recovery never blindly submits another generation. Saved terminal callbacks retry with the same receipt/body, at most five recovery rounds; exhausted delivery remains observable.

Delivery state necessarily stores callback URLs, output pointers and the private receipt for redelivery. Restrict Redis access; do not log these records. Generation success and application/storage/delivery outcome remain separate. A callback replay is not another attempt. No verified per-run charged USD is supplied by this contract: SESSION_COST is not an actual charge, and missing cost stays unknown.

Run `python -m pip install -r requirements.txt -r requirements-dev.txt` and `python -m pytest tests -q`; tests require local redis-server and mock network/provider boundaries. They do not launch paid GPU work. The gateway's pinned vastai-sdk 0.4.5 low-level wire adapter deliberately sends once because Session.request can retry exceptions even with retry=False; its contract test must be rechecked on SDK upgrade.

Deploy wrapper before gateway, enable the Node feature gate last, then verify actual endpoint versions and real beta outcomes. The wrapper production, staging and production-dv-realistic branches differ: retain each branch's storage/callback fallback behavior when promoting; do not replace the DaVinci branch wholesale with production. Keep Image Gen stage diagnostics until the new dashboard has passed live acceptance for every engine. Rollback disables the Node gate first; older workers then cannot provide execution receipts and metadata coverage will drop.

See the coordinated server document `docs/observability/image-engine-telemetry.md` for counting semantics, cost evidence, limitations and rollout checks.
