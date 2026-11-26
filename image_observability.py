"""Bounded, optional image request context for operational diagnostics."""
from contextvars import ContextVar
import logging
import re
from typing import Annotated

from pydantic import BeforeValidator


_FIELDS = (
    "request_id", "trace_id", "job_id", "parent_job_id", "operation_id",
    "attempt_id", "user_id", "requested_engine", "executed_engine",
    "generation_type", "surface", "request_environment",
)
_IDENTIFIER = re.compile(r"[A-Za-z0-9._:-]{1,180}")


def project(value: object) -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    projected = {}
    for key in _FIELDS:
        identifier = value.get(key)
        if isinstance(identifier, str) and _IDENTIFIER.fullmatch(identifier):
            projected[key] = identifier
    return projected or None


ImageObservability = Annotated[dict[str, str] | None, BeforeValidator(project)]


_context: ContextVar[dict] = ContextVar("image_log_context", default={})


def context_fields(metadata, request_id=None):
    context = project(metadata) or {}
    origin = context.pop("request_id", None)
    if origin:
        context["server_request_id"] = origin
    if request_id:
        context["request_id"] = request_id
    return context


def bind_context(metadata, request_id=None):
    return _context.set(context_fields(metadata, request_id))


def reset_context(token):
    _context.reset(token)


class ContextFilter(logging.Filter):
    """Attach context before logs leave the originating async task."""
    def filter(self, record):
        try:
            for key, value in _context.get().items():
                if not hasattr(record, key):
                    setattr(record, key, value)
        except Exception:
            pass
        return True


def log_stage(logger: logging.Logger, metadata: ImageObservability, request_id: str,
              stage: str, status: str, **details) -> None:
    """An observation failure must never affect the observed operation."""
    try:
        logger.info("Image worker stage", extra={
            **context_fields(metadata, request_id),
            "stage": stage,
            "status": status,
            **details,
        })
    except Exception:
        pass
