import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock
import pytest
from workers.postprocess_worker import PostprocessWorker
from workers.generation_worker import GenerationWorker

@pytest.mark.asyncio
@pytest.mark.parametrize('initial,expected', [('cancelled','cancelled'),('timeout','timeout'),('generated','failed'),('failed','failed')])
async def test_empty_outputs_never_upgrade_terminal_state(initial, expected):
    result=NS(status=initial,comfyui_response={},output=[],telemetry={})
    queue=asyncio.Queue();await queue.put('job');await queue.put(None)
    worker=PostprocessWorker.__new__(PostprocessWorker)
    worker.worker_id=1;worker.postprocess_queue=queue
    worker.in_flight_requests={'postprocess':0,'total':1}
    worker.request_store=NS(get=AsyncMock(return_value=NS(input=NS(telemetry=None))),delete=AsyncMock())
    worker.response_store=NS(get=AsyncMock(return_value=result),set=AsyncMock())
    worker.get_webhook_config=AsyncMock(return_value=None)
    await worker.work()
    assert result.status==expected

@pytest.mark.asyncio
async def test_failed_history_is_not_a_successful_cache_hit(monkeypatch):
    import workers.generation_worker as module
    class Response:
        status=200
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        async def json(self):return {'prompt':{'status':{'status_str':'error','completed':False},'outputs':{}}}
    class Session(Response):
        def __init__(self,**kwargs):pass
        def get(self,*args):return Response()
    monkeypatch.setattr(module.aiohttp,'ClientSession',Session)
    with pytest.raises(Exception,match='execution_failed'):
        await GenerationWorker.check_if_cached(NS(),'prompt','job')

@pytest.mark.asyncio
async def test_cancelled_job_does_not_kill_generation_worker():
    worker=GenerationWorker.__new__(GenerationWorker);worker.worker_id=1
    worker.generation_queue=asyncio.Queue();await worker.generation_queue.put('job')
    worker.postprocess_queue=asyncio.Queue();worker.in_flight_requests={'generation':0}
    worker.request_store=NS(get=AsyncMock(return_value=NS(input=NS(telemetry=None))))
    worker.response_store=NS(get=AsyncMock(return_value=NS(status='cancelled',telemetry={})),set=AsyncMock())
    running=asyncio.create_task(worker.work())
    await asyncio.wait_for(worker.postprocess_queue.get(),1)
    await asyncio.sleep(0.01)
    assert not running.done(), str(running.exception()) if running.done() else ''
    running.cancel()
    with pytest.raises(asyncio.CancelledError):await running

@pytest.mark.asyncio
async def test_upload_failure_preserves_provider_success_but_fails_delivery_result():
    result=NS(id='job',status='generated',message='',comfyui_response={},output=[{'local_path':'file.png'}],telemetry={'provider_status':'completed'})
    queue=asyncio.Queue();await queue.put('job');await queue.put(None)
    worker=PostprocessWorker.__new__(PostprocessWorker);worker.worker_id=1;worker.postprocess_queue=queue
    worker.in_flight_requests={'postprocess':0,'total':1}
    worker.request_store=NS(get=AsyncMock(return_value=NS(input=NS(telemetry=None))),delete=AsyncMock())
    worker.response_store=NS(get=AsyncMock(return_value=result),set=AsyncMock())
    worker.get_webhook_config=AsyncMock(return_value={'url':'https://callback.invalid'})
    worker.send_webhook=AsyncMock(return_value={'delivered':True})
    await worker.work()
    assert result.status=='failed'
    assert result.telemetry['provider_status']=='completed'

@pytest.mark.asyncio
async def test_ambiguous_prompt_post_is_never_repeated_or_logged_with_private_input(monkeypatch,caplog):
    import workers.generation_worker as module
    import logging
    calls=[]
    class Session:
        def __init__(self,**kw):pass
        async def __aenter__(self):return self
        async def __aexit__(self,*a):pass
        def post(self,*a,**kw):calls.append(kw);raise asyncio.TimeoutError()
    monkeypatch.setattr(module.aiohttp,'ClientSession',Session)
    monkeypatch.setattr(module.asyncio,'sleep',AsyncMock())
    worker=GenerationWorker.__new__(GenerationWorker);worker.client_id='client'
    request=NS(input=NS(workflow_json={'1':{'inputs':{'text':'PRIVATE_PROMPT'}}},telemetry={'receipt_token':'PRIVATE_TOKEN'}))
    with caplog.at_level(logging.INFO):
        with pytest.raises(Exception):await worker.post_workflow(request,'r')
    assert len(calls)==1
    assert 'PRIVATE_PROMPT' not in caplog.text and 'PRIVATE_TOKEN' not in caplog.text
