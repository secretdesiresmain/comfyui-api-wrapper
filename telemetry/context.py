from contextvars import ContextVar
from typing import Annotated
from pydantic import BaseModel, ConfigDict, Field
import hashlib
import json
import re

Safe = Annotated[str, Field(max_length=180, pattern=r'^[\w .:/+\-]*$')]
class Telemetry(BaseModel):
    model_config = ConfigDict(extra='ignore')
    schema_version: int = 1
    job_id: Safe = ''
    operation_id: Safe = ''
    parent_job_id: Safe = ''
    request_id: Safe = ''
    trace_id: Safe = ''
    user_id: Safe = ''
    generation_type: Safe = 'unknown'
    surface: Safe = 'unknown'
    request_kind: Safe = 'unknown'
    requested_engine: Safe = 'unknown'
    executed_engine: Safe = 'unknown'
    gateway: Safe = 'vastai-server'
    provider_name: Safe = 'vast.ai'
    provider_identity_source: Safe = 'configured_route'
    model_requested: Safe = 'unknown'
    model_executed: Safe = 'unknown'
    model_identity_source: Safe = 'unknown'
    model_components: list[Safe] = Field(default_factory=list, max_length=16)
    workflow_version: Safe = ''
    attempt_role: Safe = 'visible'
    dispatch_id: Safe = ''
    execution_attempt_id: Safe = ''
    comfyui_prompt_id: Safe = ''
    session_id: Safe = ''
    receipt_token: str = Field(default='', pattern=r'^([a-f0-9]{64})?$')
    submission_attempted: bool = False
    provider_status: Safe = 'unknown'
    failure_code: Safe = ''
    failure_stage: Safe = ''
    duration_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)

    def log_fields(self):
        return self.model_dump(exclude={'receipt_token'}, exclude_none=True)

_context = ContextVar('media_telemetry', default={})
def bind(value):
    if isinstance(value, dict): value=Telemetry.model_validate(value)
    return _context.set(value.log_fields() if value else {})
def reset(token): _context.reset(token)
def current(): return dict(_context.get())

def execution_models(workflow):
    keys=('ckpt_name','unet_name','clip_name','clip_name1','clip_name2','vae_name','lora_name')
    components=[]
    for node in workflow.values():
        if not isinstance(node,dict):continue
        for key in keys:
            value=node.get('inputs',{}).get(key)
            if isinstance(value,str) and re.fullmatch(r'[\w .()/+\-]{1,180}',value) and value not in components:components.append(value)
    return {'model_executed':components[0] if components else 'unknown','model_components':components[:16], 'model_identity_source':'executed_workflow' if components else 'unknown','workflow_version':hashlib.sha256(json.dumps(workflow,sort_keys=True).encode()).hexdigest()}
