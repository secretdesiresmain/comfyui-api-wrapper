from types import SimpleNamespace as NS
from unittest.mock import AsyncMock
import pytest
from workers.postprocess_worker import PostprocessWorker

@pytest.mark.asyncio
@pytest.mark.parametrize('statuses,expected', [([200],True),([400],False),([503,503],False),([503,200],True)])
async def test_callback_result_is_distinct_from_generation(monkeypatch,statuses,expected):
    import workers.postprocess_worker as module
    seen=[];codes=iter(statuses)
    class Response:
        async def __aenter__(self):return self
        async def __aexit__(self,*a):pass
        async def text(self):return 'upstream'
    class Session(Response):
        def __init__(self,**kw):pass
        def post(self,url,**kw):
            seen.append(kw['json']);r=Response();r.status=next(codes);return r
    monkeypatch.setattr(module.aiohttp,'ClientSession',Session)
    monkeypatch.setattr(module,'WEBHOOK_RETRIES',2)
    monkeypatch.setattr(module.asyncio,'sleep',AsyncMock())
    worker=PostprocessWorker.__new__(PostprocessWorker);worker._connector=None
    result=NS(id='r',status='completed',message='done',output=[{'url':'image'}],telemetry={})
    delivery=await worker.send_webhook('https://callback.invalid',result,{'status':'failed'},'r')
    assert delivery['delivered'] is expected
    assert result.status=='completed'
    assert seen[0]['status']=='completed'
