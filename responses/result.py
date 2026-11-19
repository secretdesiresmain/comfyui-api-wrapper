from pydantic import BaseModel, Field, field_serializer
from typing import Dict

class Result(BaseModel):
    id: str
    message: str = Field(default='Request accepted')
    status: str = Field(default='pending')
    comfyui_response: Dict = Field(default={})
    output: list = Field(default=[])
    timings: Dict = Field(default_factory=dict)
    telemetry: Dict = Field(default_factory=dict)
    retry_scheduled: bool = False
    delivery: Dict = Field(default_factory=dict)


    @field_serializer('telemetry')
    def public_telemetry(self, value):
        # Callback construction accesses the in-memory dict explicitly. Public HTTP
        # responses, JSON dumps and SSE serialization must never expose its proof.
        return {key:item for key,item in value.items() if key != 'receipt_token'}
