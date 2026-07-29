import os
import json
from typing import List, Union, Dict, Annotated, Optional
from urllib.parse import urlparse, parse_qs
from pydantic import BaseModel, Field, validator, model_validator


class S3Config(BaseModel):
    access_key_id: str = Field(default="")
    secret_access_key: str = Field(default="")
    endpoint_url: str = Field(default="")
    bucket_name: str = Field(default="")
    region: str = Field(default="")
    connect_timeout: int = Field(default=60)
    connect_attempts: int = Field(default=3)
    
    @staticmethod
    def get_defaults():
        return {
            "access_key_id": "",
            "secret_access_key": "",
            "endpoint_url": "",
            "bucket_name": "",
            "region": "",
            "connect_timeout": 60,
            "connect_attempts": 3
        }
    
    def get_config(self) -> Dict:
        """Get S3 configuration with environment variable fallbacks"""
        return {
            "access_key_id": self.access_key_id or os.environ.get("S3_ACCESS_KEY_ID", ""),
            "secret_access_key": self.secret_access_key or os.environ.get("S3_SECRET_ACCESS_KEY", ""),
            "endpoint_url": self.endpoint_url or os.environ.get("S3_ENDPOINT_URL", ""),
            "bucket_name": self.bucket_name or os.environ.get("S3_BUCKET_NAME", ""),
            "region": self.region or os.environ.get("S3_REGION", ""),
            "connect_timeout": self.connect_timeout,
            "connect_attempts": self.connect_attempts
        }
    
    def is_configured(self) -> bool:
        """Check if S3 is properly configured"""
        config = self.get_config()
        return bool(
            config["access_key_id"] and 
            config["secret_access_key"] and 
            config["bucket_name"]
        )


class WebHook(BaseModel):
    url: str = Field(default="")
    session_close_url: Optional[str] = Field(default="")
    extra_params: Dict = Field(default_factory=dict)
    timeout: int = Field(default=30)
    session_auth_data: Optional[Dict] = Field(default=None)
    retries: int = Field(default=0, description="Number of retries (passed through for orchestrator)")
    
    @staticmethod
    def get_defaults():
        return {
            "url": "",
            "session_close_url": "",
            "extra_params": {},
            "timeout": 30,
            "retries": 0,
        }
    
    def has_valid_url(self) -> bool:
        """Check if webhook has a valid URL"""
        return self.is_url(self.url)

    def query_param(self, name: str, default: str = "") -> str:
        """Read a single query param off the webhook URL. The caller (this repo's
        server) encodes everything it needs us to know per-job here (`source`,
        `user_id`, `char_id`, `isPublic`, `taskId`, ...) rather than adding
        separate payload fields, so there's only one URL to manage end to end."""
        try:
            query = parse_qs(urlparse(self.url).query)
            return query.get(name, [default])[0]
        except Exception:
            return default

    def is_public(self) -> bool:
        """Whether this job's assets should go to the public OVH bucket.

        Read directly off the `isPublic` query param on the webhook URL. Defaults to
        False (private) if the param is missing or not exactly "true", so existing
        callers that don't send it yet keep going to the private bucket unchanged.
        """
        return self.query_param("isPublic", "false").strip().lower() == "true"

    def ovh_category(self) -> str:
        """Storage category for this job's OVH key layout, derived from the `source`
        query param (`profile` | `chat` | `generate`, same values the server's own
        `_prepareQueryData`/`_generateWebhookUrl` use). Maps onto the exact same
        category strings as `storageService.CATEGORIES` in the server repo (see also
        ArkImage.provider.js's identical `ovhCategory` mapping), so VastAI-generated
        assets land in the same `${category}/${userId}/${uuid}.${ext}` folder layout
        as Ark/Raphael's own dual-write instead of a wrapper-specific flat naming
        scheme. Defaults to "generated-page-image" (private, generate-page route) for
        any unrecognised/missing source.
        """
        return {
            "profile": "character-profile-image",
            "chat": "generated-chat-image",
        }.get(self.query_param("source"), "generated-page-image")

    def user_id(self) -> str:
        """The owning user's id, read off the `user_id` query param on the webhook
        URL - forms the folder segment in the OVH key, mirroring
        storageService.writeUserUpload's `${category}/${userId}/${uuid}.${ext}`."""
        return self.query_param("user_id")
    
    @staticmethod
    def is_url(value: str) -> bool:
        """Check if a string is a valid URL"""
        try:
            parsed = urlparse(value)
            return bool(parsed.scheme and parsed.netloc)
        except Exception:
            return False


class Input(BaseModel):
    request_id: str = Field(default="")
    modifier: str = Field(default="")
    modifications: Dict = Field(default_factory=dict)
    workflow_json: Dict = Field(default_factory=dict)
    s3: Optional[S3Config] = Field(default=None)
    webhook: Optional[WebHook] = Field(default=None)
    
    @model_validator(mode='after')
    def validate_workflow_mode(self):
        """Ensure workflow_json and modifier are mutually exclusive, and one is provided"""
        modifier = self.modifier
        workflow_json = self.workflow_json
        modifications = self.modifications
        
        # Check if both are provided
        if workflow_json and modifier:
            raise ValueError("Cannot provide both workflow_json and modifier - they are mutually exclusive")
        
        # Check if neither are provided
        if not workflow_json and not modifier:
            raise ValueError("Must provide either workflow_json OR modifier")
        
        # Check if modifications provided without modifier
        if modifications and not modifier:
            raise ValueError("modifications can only be provided when modifier is specified")
            
        return self
    
    class Config:
        # Allow extra fields for forward compatibility
        extra = "allow"


class Payload(BaseModel):
    input: Input
    
    class Config:
        # Generate schema with examples
        json_schema_extra = {
            "example": {
                "input": {
                    "request_id": "123e4567-e89b-12d3-a456-426614174000",
                    "modifier": "RawWorkflow",
                    "modifications": {
                        "param1": "value1",
                        "param2": 42
                    },
                    "workflow_json": {
                        "9": {
                            "inputs": {
                                "filename_prefix": "ComfyUI",
                                "images": ["10", 0]
                            },
                            "class_type": "SaveImage",
                            "_meta": {
                                "title": "Save Image"
                            }
                        },
                        "10": {
                            "inputs": {
                                "image": "https://raw.githubusercontent.com/comfyanonymous/ComfyUI/master/input/example.png",
                                "upload": "image"
                            },
                            "class_type": "LoadImage",
                            "_meta": {
                                "title": "Load Image"
                            }
                        }
                    },
                    "s3": {
                        "access_key_id": "your-s3-access-key",
                        "secret_access_key": "your-s3-secret-access-key",
                        "endpoint_url": "https://my-endpoint.backblaze.com",
                        "bucket_name": "your-bucket",
                        "region": "us-east-1"
                    },
                    "webhook": {
                        "url": "your-webhook-url",           # Changed from "webhook_url"
                        "extra_params": {                    # Changed from "webhook_extra_params"
                            "custom_field": "value"
                        }
                    }
                }
            }
        }
    
    @staticmethod
    def get_openapi_examples():
        """Load examples from JSON files in payloads directory"""
        directory = './payloads'
        result = {}
        
        # Check if directory exists
        if not os.path.exists(directory):
            return result
    
        try:
            for filename in os.listdir(directory):
                if filename.endswith('.json'):
                    filepath = os.path.join(directory, filename)
                    try:
                        with open(filepath, 'r', encoding='utf-8') as file:
                            file_content = json.load(file)
                        
                        # Remove the file extension and convert to natural language
                        key = Payload.snake_to_natural(os.path.splitext(filename)[0])
                        
                        # Add the content to the result dictionary
                        result[key] = {"value": file_content}
                        
                    except (json.JSONDecodeError, IOError) as e:
                        print(f"Warning: Could not load example file {filename}: {e}")
                        continue
                        
        except OSError as e:
            print(f"Warning: Could not read payloads directory: {e}")
        
        return result
    
    @staticmethod
    def snake_to_natural(snake_str: str) -> str:
        """Convert snake_case to Natural Language"""
        return ' '.join(word.capitalize() for word in snake_str.split('_'))