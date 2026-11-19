import asyncio
import pytest
from telemetry.context import Telemetry, bind, reset, current, execution_models

def test_contract_drops_secrets_and_validates_ids():
    model=Telemetry(job_id='job',execution_attempt_id='attempt',prompt='SECRET',session_auth={'key':'SECRET'},receipt_token='a'*64)
    assert 'SECRET' not in str(model.model_dump())
    assert 'receipt_token' not in model.log_fields()

@pytest.mark.asyncio
async def test_interleaved_workers_restore_original_context():
    async def work(user):
        token=bind(Telemetry(user_id=user,trace_id='a'*32));await asyncio.sleep(0)
        result=current()['user_id'];reset(token);return result
    assert await asyncio.gather(work('one'),work('two'))==['one','two']
    assert current()=={}

def test_model_identity_comes_from_effective_workflow_not_prompt():
    result=execution_models({'1':{'class_type':'UNETLoader','inputs':{'unet_name':'chroma/model.safetensors'}},'2':{'inputs':{'text':'SECRET'}}})
    assert result['model_executed']=='chroma/model.safetensors'
    assert 'SECRET' not in str(result)

def test_logger_filter_carries_worker_context_without_receipt_secret():
    import logging
    from config.logging_config import TraceContextFilter
    token=bind(Telemetry(job_id='j',user_id='u',receipt_token='a'*64))
    try:
        record=logging.LogRecord('worker',logging.INFO,'',1,'stage',(),None)
        TraceContextFilter().filter(record)
        assert record.user_id=='u'
        assert not hasattr(record,'receipt_token')
    finally:reset(token)

def test_public_result_serialization_never_exposes_callback_receipt_secret():
    from responses.result import Result
    token='a'*64
    result=Result(id='r',telemetry={'execution_attempt_id':'e','receipt_token':token})
    assert token not in result.model_dump_json()
    assert token not in str(result.model_dump())
    assert result.telemetry['receipt_token']==token # internal callback still has proof
