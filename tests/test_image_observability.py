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


def test_worker_diagnostics_keep_original_request_and_dispatch_ids(caplog):
    import logging
    from image_observability import log_stage
    logger = logging.getLogger("image-observability-correlation-test")
    caplog.set_level(logging.INFO, logger=logger.name)
    log_stage(logger, {"request_id": "server-request", "trace_id": "trace", "job_id": "job"},
              "dispatch-task", "dispatch", "accepted")
    record = caplog.records[-1]
    assert record.request_id == "dispatch-task"
    assert record.server_request_id == "server-request"
    assert record.trace_id == "trace"

async def test_concurrent_context_reaches_ordinary_logs_and_resets(caplog):
    import asyncio
    import logging
    from image_observability import bind_context, reset_context, ContextFilter
    log = logging.getLogger('isolated-image-context')
    log.addFilter(ContextFilter())
    caplog.set_level(logging.INFO, logger=log.name)
    async def job(env, user):
        token = bind_context({'request_environment':env,'user_id':user,'request_id':'server-'+user,'job_id':'job-'+user}, 'worker-'+user)
        try:
            await asyncio.sleep(0)
            log.info('ordinary timing', extra={'metric_type':'timing','duration_ms':3})
        finally:
            reset_context(token)
        log.info('after scope')
    await asyncio.gather(job('beta','u1'),job('production','u2'))
    records = [r for r in caplog.records if getattr(r,'metric_type',None) == 'timing']
    assert {(r.request_environment,r.user_id,r.server_request_id,r.request_id) for r in records} == {
        ('beta','u1','server-u1','worker-u1'),('production','u2','server-u2','worker-u2')}
    assert all(not hasattr(r,'user_id') for r in caplog.records if r.message == 'after scope')


def test_context_sanitization_preserves_worker_environment_and_explicit_ids():
    import logging
    from image_observability import bind_context, reset_context, ContextFilter
    token = bind_context({'request_environment':'beta','environment':'beta','prompt':'PRIVATE','request_id':'original'}, 'dispatch')
    try:
        record = logging.makeLogRecord({'environment':'production','request_id':'explicit'})
        assert ContextFilter().filter(record)
        assert record.request_environment == 'beta'
        assert record.environment == 'production'
        assert record.request_id == 'explicit'
        assert record.server_request_id == 'original'
        assert not hasattr(record,'prompt')
    finally:
        reset_context(token)

async def test_callback_failure_has_request_environment_and_category(monkeypatch, caplog):
    fake_http(monkeypatch, postprocess_worker, [(503, 'PRIVATE')])
    monkeypatch.setattr(postprocess_worker, 'WEBHOOK_RETRIES', 1)
    worker = postprocess_worker.PostprocessWorker.__new__(postprocess_worker.PostprocessWorker)
    worker._connector = None
    result = SimpleNamespace(id='r',status='completed',message='done',output=[])
    await worker.send_webhook('https://callback.invalid',result,{},'r',image_observability={'request_environment':'beta','job_id':'job','user_id':'user'})
    error = next(r for r in caplog.records if r.levelname == 'ERROR')
    assert error.error_category == 'callback_delivery'
    assert error.request_environment == 'beta'
    assert error.user_id == 'user'
    assert result.status == 'completed'

async def test_generation_worker_attaches_context_to_existing_timings_and_clears_it(monkeypatch, caplog):
    import asyncio
    from config.logging_config import get_logger
    queue = asyncio.Queue()
    for request_id in ('worker-1','worker-2',None):
        await queue.put(request_id)
    requests = {
        'worker-1': main.Payload(input={'request_id':'worker-1','workflow_json':{'1':{'inputs':{}}},'image_observability':{
            'request_environment':'beta','user_id':'user-1','job_id':'job-1','requested_engine':'monet','generation_type':'generate'}}),
        'worker-2': main.Payload(input={'request_id':'worker-2','workflow_json':{'1':{'inputs':{}}}}),
    }
    worker = generation_worker.GenerationWorker.__new__(generation_worker.GenerationWorker)
    worker.worker_id = 'scope-check'
    worker.generation_queue = queue
    worker.postprocess_queue = SimpleNamespace(put=AsyncMock())
    worker.request_store = SimpleNamespace(get=AsyncMock(side_effect=lambda rid: requests[rid]))
    worker.response_store = SimpleNamespace(get=AsyncMock(side_effect=lambda rid: SimpleNamespace(status='pending')),set=AsyncMock())
    worker.in_flight_requests = {'generation':0}
    worker.post_workflow = AsyncMock(return_value='comfy-run')
    worker.check_if_cached = AsyncMock(return_value=True)
    worker.get_result = AsyncMock(return_value={})
    caplog.set_level('INFO')
    await worker.work()
    timings = [r for r in caplog.records if getattr(r,'metric_type',None)=='timing' and getattr(r,'stage',None)=='generation']
    assert len(timings)==2
    assert (timings[0].request_environment,timings[0].user_id)==('beta','user-1')
    assert not hasattr(timings[1],'user_id')
    get_logger('after-worker').info('scope ended')
    assert not hasattr(caplog.records[-1],'job_id')

async def test_admission_failure_keeps_origin_context_and_resets(monkeypatch,caplog):
    from config.logging_config import get_logger
    monkeypatch.setattr(main,'FOUND_UNHEALTHY',True)
    payload=main.Payload(input={'request_id':'admission','workflow_json':{'1':{'inputs':{}}},'image_observability':{'job_id':'j','request_environment':'beta','user_id':'u'}})
    response=Response()
    result=await main.generate(response,payload)
    assert result.status=='failed'
    error=next(r for r in caplog.records if r.levelname=='ERROR')
    assert error.error_category=='admission_health'
    assert error.request_environment=='beta'
    assert error.job_id=='j'
    caplog.set_level('INFO')
    get_logger('outside-admission').info('outside')
    assert not hasattr(caplog.records[-1],'job_id')

@pytest.mark.parametrize('status',[400,503])
async def test_terminal_callback_failures_are_categorized_errors(monkeypatch,caplog,status):
    fake_http(monkeypatch,postprocess_worker,[(status,''),(status,'')])
    monkeypatch.setattr(postprocess_worker,'WEBHOOK_RETRIES',1)
    worker=postprocess_worker.PostprocessWorker.__new__(postprocess_worker.PostprocessWorker)
    worker._connector=None
    result=SimpleNamespace(id='r',status='completed',message='done',output=[])
    await worker.send_webhook('https://callback.invalid',result,{},'r')
    await worker.send_webhook_for_session_close('https://callback.invalid',result,{},'r')
    errors=[r for r in caplog.records if r.levelname=='ERROR']
    assert {r.error_category for r in errors}=={'callback_delivery','session_close_delivery'}
    assert result.status=='completed'
