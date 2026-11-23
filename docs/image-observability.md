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
