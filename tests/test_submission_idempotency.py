import asyncio
import socket
import subprocess
import pytest
import pytest_asyncio
from redis.asyncio import Redis
from telemetry.store import StateStore

@pytest_asyncio.fixture
async def state():
    sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close()
    proc=subprocess.Popen(['redis-server','--bind','127.0.0.1','--port',str(port),'--save','','--appendonly','no','--requirepass','media-test'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    redis=Redis(host='127.0.0.1',port=port,password='media-test',decode_responses=True)
    for _ in range(100):
        try:await redis.ping();break
        except Exception:await asyncio.sleep(.01)
    yield StateStore(redis,'test')
    await redis.aclose();proc.terminate();proc.wait()

@pytest.mark.asyncio
async def test_duplicate_dispatch_and_recreation_do_not_enqueue_twice(state):
    claims=await asyncio.gather(*(state.claim('execution','a',{'job_id':'j'}) for _ in range(10)))
    assert sum(claims)==1
    recreated=StateStore(state.redis,'test')
    assert not await recreated.claim('execution','a',{'job_id':'j'})
    assert await recreated.claim('execution','b',{'job_id':'j'})

@pytest.mark.asyncio
async def test_result_and_undelivered_callback_survive_restart(state):
    await state.claim('execution','a',{'job_id':'j'})
    await state.update('execution','a',{'status':'completed','provider_status':'completed'})
    await state.claim('delivery','a',{'url':'https://callback.invalid','body':{'id':'a','status':'completed'}})
    recreated=StateStore(state.redis,'test')
    assert (await recreated.read('execution','a'))['status']=='completed'
    assert (await recreated.read('delivery','a'))['body']['status']=='completed'
    await recreated.update('delivery','a',{'delivered':True})
    assert (await state.read('delivery','a'))['delivered'] is True
