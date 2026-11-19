from types import SimpleNamespace as NS
from unittest.mock import AsyncMock
import pytest
import sys
# File-type sniffing is unrelated to admission; native libmagic is optional on test hosts.
try:
    import magic
except ImportError:
    sys.modules['magic'] = NS()
import main
from fastapi import Response, HTTPException
from telemetry.context import Telemetry

def payload(workflow=None):
    return main.Payload(input=main.Payload.model_fields['input'].annotation(request_id='request',workflow_json=workflow or {'1':{'inputs':{}}},telemetry={'job_id':'job','dispatch_id':'dispatch','execution_attempt_id':'execution','receipt_token':'a'*64}))

@pytest.mark.asyncio
async def test_duplicate_execution_does_not_enqueue_again(monkeypatch):
    state=NS(claim=AsyncMock(side_effect=[True,False]),read=AsyncMock())
    monkeypatch.setattr(main,'configured_store',lambda:state)
    monkeypatch.setattr(main,'_check_health',AsyncMock(return_value=(True,None)))
    monkeypatch.setattr(main,'FOUND_UNHEALTHY',False)
    monkeypatch.setattr(main,'FORCE_HEALTH_CHECK_FAIL',False)
    monkeypatch.setattr(main,'request_store',NS(set=AsyncMock()))
    responses={}
    async def put(k,v):responses[k]=v
    monkeypatch.setattr(main,'response_store',NS(set=put,get=AsyncMock(side_effect=lambda k:responses.get(k))))
    queue=NS(put=AsyncMock());monkeypatch.setattr(main,'preprocess_queue',queue)
    await main.generate(Response(),payload())
    state.read.return_value=state.claim.call_args.args[2]
    await main.generate(Response(),payload())
    assert queue.put.await_count==1

@pytest.mark.asyncio
async def test_same_execution_id_with_changed_owner_is_rejected(monkeypatch):
    state=NS(claim=AsyncMock(return_value=False),read=AsyncMock(return_value={'job_id':'different','workflow_version':main.execution_models({'1':{'inputs':{}}})['workflow_version']}))
    monkeypatch.setattr(main,'configured_store',lambda:state)
    monkeypatch.setattr(main,'response_store',NS(get=AsyncMock(return_value=None)))
    with pytest.raises(HTTPException) as error:await main.generate(Response(),payload())
    assert error.value.status_code==409
