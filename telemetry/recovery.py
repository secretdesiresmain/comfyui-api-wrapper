"""Bounded retry of saved callback deliveries; never blindly resubmit GPU work."""
import asyncio
import logging
import aiohttp
from telemetry.store import configured_store
logger=logging.getLogger(__name__)
async def reconcile_once(service="worker"):
    state=configured_store(service)
    if not state:return
    for row in await state.overdue():
        delivery_id=row.get('delivery_id')
        if delivery_id and not row.get('delivered') and not row.get('resolved'):
            retries=int(row.get('recovery_attempts',0))+1
            await state.update('delivery',delivery_id,{'recovery_attempts':retries})
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                    async with session.post(row['url'],json=row['body']) as response:
                        if 200<=response.status<300:
                            await state.update('delivery',delivery_id,{'delivered':True})
                            continue
            except Exception:pass
            if retries>=5:await state.update('delivery',delivery_id,{'resolved':True,'failure_code':'delivery_exhausted'})
            logger.error('media_stage_observation',extra={'stage':'callback','failure_code':'delivery_unresolved','request_id':row.get('request_id',''),'attempt_weight':0})
        else:
            logger.warning('media_stage_observation',extra={'stage':'recovery','failure_code':'terminal_missing','request_id':row.get('request_id',''),'attempt_weight':0})
async def recovery_loop(service="worker"):
    while True:
        try:await reconcile_once(service)
        except Exception:logger.warning('media_stage_observation',extra={'stage':'recovery','failure_code':'storage_unavailable','attempt_weight':0})
        await asyncio.sleep(30)
