from unittest.mock import AsyncMock
from types import SimpleNamespace as NS
import pytest
from telemetry import recovery

@pytest.mark.asyncio
async def test_recovery_uses_explicit_service_and_never_resubmits_execution(monkeypatch):
    state=NS(overdue=AsyncMock(return_value=[{'job_id':'j','status':'generating','comfyui_prompt_id':'p'}]))
    scopes=[]
    monkeypatch.setattr(recovery,'configured_store',lambda service:scopes.append(service) or state)
    await recovery.reconcile_once('gateway')
    assert scopes==['gateway']

@pytest.mark.asyncio
async def test_lost_callback_response_replays_saved_receipt_without_new_execution(monkeypatch):
    body={'id':'job','status':'completed','telemetry':{'execution_attempt_id':'execution'}}
    state=NS(overdue=AsyncMock(return_value=[{'delivery_id':'d','url':'https://callback.invalid','body':body}]),update=AsyncMock())
    monkeypatch.setattr(recovery,'configured_store',lambda *args:state)
    sent=[]
    class Response:
        status=200
        async def __aenter__(self):return self
        async def __aexit__(self,*a):pass
    class Session(Response):
        def __init__(self,**kw):pass
        def post(self,url,**kw):sent.append(kw['json']);return Response()
    monkeypatch.setattr(recovery.aiohttp,'ClientSession',Session)
    await recovery.reconcile_once()
    assert sent==[body]
    state.update.assert_any_await('delivery','d',{'delivered':True})
