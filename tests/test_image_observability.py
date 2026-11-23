from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import Response

import main
from workers import generation_worker, postprocess_worker


class HttpResponse(SimpleNamespace):
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def text(self):
        return self.body


def fake_http(monkeypatch, module, responses):
    calls = []
    responses = iter(responses)

    class Session(HttpResponse):
        def post(self, url, **kwargs):
            calls.append((url, kwargs))
            status, body = next(responses)
            return HttpResponse(status=status, body=body)

    monkeypatch.setattr(module.aiohttp, "ClientSession", Session)
    monkeypatch.setattr(module.asyncio, "sleep", AsyncMock())
    return calls


@pytest.mark.parametrize("metadata", [None, {"job_id": "job-1", "trace_id": "trace-1",
    "prompt": "PRIVATE-PROMPT", "user_id": "https://example.test/?token=secret",
    "attempt_id": "x" * 181}])
async def test_generate_queues_without_telemetry_storage(monkeypatch, metadata):
    input_data = {"request_id": "request", "workflow_json": {"1": {"inputs": {}}}}
    if metadata is not None:
        input_data["image_observability"] = metadata
    payload = main.Payload(input=input_data)
    monkeypatch.setattr(main, "_check_health", AsyncMock(return_value=(True, None)))
    monkeypatch.setattr(main, "FOUND_UNHEALTHY", False)
    monkeypatch.setattr(main, "FORCE_HEALTH_CHECK_FAIL", False)
    monkeypatch.setattr(main, "preprocess_queue", SimpleNamespace(put=AsyncMock()))
    monkeypatch.setattr(main, "request_store", SimpleNamespace(set=AsyncMock()))
    monkeypatch.setattr(main, "response_store", SimpleNamespace(set=AsyncMock()))
    monkeypatch.setattr(main, "in_flight_requests", {"total": 0})
    result = await main.generate(Response(), payload)
    assert result.status == "pending"
    main.preprocess_queue.put.assert_awaited_once_with("request")
    stored = main.request_store.set.await_args.args[1]
    assert getattr(stored.input, "image_observability", None) == (
        {"job_id": "job-1", "trace_id": "trace-1"} if metadata else None
    )
    assert "image_observability" not in result.model_dump()


async def test_comfyui_retries_legacy_requests_without_logging_provider_payload(monkeypatch, caplog):
    calls = fake_http(monkeypatch, generation_worker, [
        (502, "PRIVATE-PROVIDER-BODY"), (200, '{"prompt_id":"run-1"}'),
    ])
    monkeypatch.setattr(generation_worker, "COMFYUI_RETRIES", 2)
    worker = generation_worker.GenerationWorker.__new__(generation_worker.GenerationWorker)
    worker.client_id = "client"
    payload = main.Payload(input={"request_id": "request",
        "workflow_json": {"1": {"inputs": {"text": "PRIVATE-PROMPT"}}}})
    with caplog.at_level("DEBUG"):
        assert await worker.post_workflow(payload, "request") == "run-1"
    assert len(calls) == 2
    assert "PRIVATE-PROMPT" not in caplog.text
    assert "PRIVATE-PROVIDER-BODY" not in caplog.text


async def test_webhook_retries_and_keeps_legacy_payload_without_secret_logs(monkeypatch, caplog):
    calls = fake_http(monkeypatch, postprocess_worker, [
        (503, "PRIVATE-CALLBACK-BODY"), (200, ""),
    ])
    monkeypatch.setattr(postprocess_worker, "WEBHOOK_RETRIES", 2)
    worker = postprocess_worker.PostprocessWorker.__new__(postprocess_worker.PostprocessWorker)
    worker._connector = None
    callback_url = "https://callback.invalid/result?key=PRIVATE-SIGNED-URL"
    result = SimpleNamespace(id="r", status="completed", message="done", output=[])
    await worker.send_webhook(callback_url, result, {"custom": "PRIVATE-PAYLOAD"}, "r")
    assert [call[1]["json"] for call in calls] == [
        {"id": "r", "status": "completed", "message": "done",
         "output": [], "custom": "PRIVATE-PAYLOAD"}
    ] * 2
    assert "PRIVATE-" not in caplog.text


async def test_callback_is_attempted_when_diagnostic_logging_fails(monkeypatch):
    calls = fake_http(monkeypatch, postprocess_worker, [(200, "")])
    worker = postprocess_worker.PostprocessWorker.__new__(postprocess_worker.PostprocessWorker)
    worker._connector = None
    result = SimpleNamespace(id="r", status="completed", message="done", output=[])

    original_info = postprocess_worker.logger.info

    def fail_diagnostic(message, *args, **kwargs):
        if kwargs.get("extra", {}).get("stage") == "callback":
            raise RuntimeError("log transport unavailable")
        return original_info(message, *args, **kwargs)

    monkeypatch.setattr(postprocess_worker.logger, "info", fail_diagnostic)
    await worker.send_webhook("https://callback.invalid", result, {}, "r",
                              image_observability={"job_id": "job-1"})
    assert calls[0][1]["json"]["image_observability"] == {"job_id": "job-1"}


async def test_workflow_asset_lookup_does_not_log_signed_url(monkeypatch, caplog):
    from modifiers.basemodifier import BaseModifier

    modifier = BaseModifier.__new__(BaseModifier)
    modifier.input_dir = "/unused"
    monkeypatch.setattr(modifier, "find_input_file", AsyncMock(
        return_value=SimpleNamespace(name="cached.png")))
    with caplog.at_level("INFO"):
        assert await modifier.get_url_content(
            "https://example.test/image.png?signature=PRIVATE-SIGNED-URL") == "cached.png"
    assert "PRIVATE-SIGNED-URL" not in caplog.text
