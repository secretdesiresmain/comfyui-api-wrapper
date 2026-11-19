"""Bounded operational correlation and delivery state. Never a billing ledger."""
import hashlib
import json
import os
import time
from urllib.parse import urlsplit, unquote
from redis.asyncio import Redis

class StateStore:
    def __init__(self, redis, environment, service='worker'):
        self.redis=redis
        scope=hashlib.sha256((environment+':'+service).encode()).hexdigest()[:24]
        self.prefix='image:{'+scope+'}:'
    def key(self, kind, identity):
        return self.prefix+kind+':'+hashlib.sha256(identity.encode()).hexdigest()
    async def claim(self, kind, identity, fields):
        key=self.key(kind,identity)
        return bool(await self.redis.eval('''
        if redis.call('EXISTS',KEYS[1])==1 then return 0 end
        if redis.call('ZCARD',KEYS[2])>=10000 then return redis.error_reply('media_capacity') end
        redis.call('SET',KEYS[1],ARGV[1],'EX',604800)
        redis.call('ZADD',KEYS[2],ARGV[2],KEYS[1])
        return 1''',2,key,self.prefix+'pending',json.dumps(fields),time.time()+3600))
    async def read(self, kind, identity):
        raw=await self.redis.get(self.key(kind,identity))
        return json.loads(raw) if raw else None
    async def update(self, kind, identity, fields):
        return await self.redis.eval('''
        local raw=redis.call('GET',KEYS[1]);if not raw then return 0 end
        local obj=cjson.decode(raw);local patch=cjson.decode(ARGV[1])
        for k,v in pairs(patch) do obj[k]=v end
        redis.call('SET',KEYS[1],cjson.encode(obj),'EX',604800)
        if obj.delivered or obj.resolved then redis.call('ZREM',KEYS[2],KEYS[1]) end
        return 1''',2,self.key(kind,identity),self.prefix+'pending',json.dumps(fields))
    async def overdue(self):
        keys=await self.redis.eval("""
        local keys=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[1],'LIMIT',0,100)
        for _,key in ipairs(keys) do redis.call('ZADD',KEYS[1],ARGV[2],key) end
        return keys""",1,self.prefix+'pending',time.time(),time.time()+60)
        rows=[]
        for key in keys:
            raw=await self.redis.get(key)
            row=json.loads(raw) if raw else {'status':'correlation_expired'}
            if row.get('resolved') or row.get('delivered'):
                await self.redis.zrem(self.prefix+'pending',key)
                continue
            if not row.get('delivery_id'):await self.redis.zrem(self.prefix+'pending',key)
            rows.append(row)
        return rows

_runtime={}
def configured_store(service='worker'):
    global _runtime
    if service in _runtime:return _runtime[service]
    uri=os.getenv('IMAGE_TELEMETRY_REDIS_URL') or os.getenv('REDIS_URI')
    if not uri:return None
    parsed=urlsplit(uri if '://' in uri else 'redis://'+uri)
    if parsed.scheme not in ('redis','rediss') or not parsed.hostname:raise ValueError('invalid_media_redis_config')
    redis=Redis(host=parsed.hostname,port=parsed.port or 6379,username=unquote(parsed.username) if parsed.username else None,password=os.getenv('REDIS_PASSWORD') or (unquote(parsed.password) if parsed.password else None),ssl=parsed.scheme=='rediss' or os.getenv('REDIS_TLS')=='true',decode_responses=True,socket_connect_timeout=2,socket_timeout=2)
    _runtime[service]=StateStore(redis,os.getenv('LOGGING_ENVIRONMENT',os.getenv('ENVIRONMENT','development')),service)
    return _runtime[service]
