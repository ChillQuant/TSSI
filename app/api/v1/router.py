"""Aggregate v1 router."""

from __future__ import annotations

from fastapi import APIRouter

from app.api.v1 import scraper, tssi

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(tssi.router)
api_router.include_router(scraper.router)

__all__ = ["api_router"]
