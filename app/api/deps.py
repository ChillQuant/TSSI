"""Shared FastAPI dependencies.

Only cross-cutting concerns live here — the per-endpoint DB session dep
lives in :mod:`app.db.session` so the scraper / calc modules can re-use it
without taking a FastAPI import dependency.
"""

from __future__ import annotations

import hmac

from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader

from app.core.config import Settings, get_settings

# ``auto_error=False`` lets us return a richer 401 body instead of the stock
# FastAPI 403 that ``APIKeyHeader`` would otherwise produce when the header
# is missing.
_api_key_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(
    provided: str | None = Depends(_api_key_scheme),
    settings: Settings = Depends(get_settings),
) -> None:
    """Guard write endpoints with an ``X-API-Key`` header.

    The comparison is constant-time (``hmac.compare_digest``) to keep the
    endpoint safe from header-length / byte-timing side channels even though
    a single shared key is a coarse auth model — production traffic should
    layer an edge proxy / mTLS on top of this.
    """
    expected = settings.api_key.get_secret_value()
    if not provided or not hmac.compare_digest(provided, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key header.",
            headers={"WWW-Authenticate": "APIKey"},
        )
