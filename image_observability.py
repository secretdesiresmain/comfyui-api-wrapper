"""Bounded, optional image request context for operational diagnostics."""
import logging
import re
from typing import Annotated

from pydantic import BeforeValidator


_FIELDS = (
    "request_id", "trace_id", "job_id", "parent_job_id", "operation_id",
    "attempt_id", "user_id", "requested_engine", "executed_engine",
    "generation_type", "surface",
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


def log_stage(logger: logging.Logger, metadata: ImageObservability, request_id: str,
              stage: str, status: str, **details) -> None:
    """An observation failure must never affect the observed operation."""
    try:
        context = project(metadata) or {}
        # request_id belongs to the existing worker dispatch/logging context.
        # Retain the originating HTTP request separately when task IDs differ.
        origin = context.pop("request_id", None)
        logger.info("Image worker stage", extra={
            **context,
            **({"server_request_id": origin} if origin else {}),
            "request_id": request_id,
            "stage": stage,
            "status": status,
            **details,
        })
    except Exception:
        pass
