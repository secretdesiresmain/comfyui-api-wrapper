# Image work diagnostics

Generation inputs may include `image_observability` with correlation IDs and
route fields. The wrapper drops fields outside the shared allowlist and values
that are not bounded identifiers. It includes valid context in regular result
callbacks and session-close requests that ask Vast to retry generation.

Preprocessing, submission to ComfyUI, and callback initiation record correlated
stage diagnostics. Existing timing, error, and trace logs continue to use the
configured log handlers. Application submission and outcome counts come from the
server.

`python -m pytest` runs the local regression tests with stubbed ComfyUI and
callback requests.

## Origin context and manual rollout (November 26)

Optional `input.image_observability.request_environment` is the originating server environment, not the worker's deployment environment. The allowlisted context is scoped to each queued job and reset in cleanup. Existing stage timings, errors and callback failures receive job/user/type/engine metadata. Original HTTP request IDs remain `server_request_id`; worker dispatch IDs remain `request_id`. Callback-delivery errors include `error_category=callback_delivery` and never change successful generation into failure.

No environment changes or telemetry storage are required. Requests without metadata continue working. Deploy the reviewed staging commit through the existing worker release process when ready; no release was performed for this PR. Test locally with `python -m pytest -q` after installing `requirements-dev.txt`. Check a beta request's ordinary generation timing and callback logs for `request_environment=beta`, even on a shared worker with another deployment environment. Server counters do not depend on this worker change.
