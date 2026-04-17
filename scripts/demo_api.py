#!/usr/bin/env python3
"""Print sample API responses (stub DB + fake scraper). Run: python scripts/demo_api.py"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

os.environ.setdefault("API_KEY", "demo-secret")
os.environ.setdefault("TSSI_BASELINE_DATE", "2020-01-01")

from app.core import config as _cfg

_cfg.get_settings.cache_clear()

from fastapi.testclient import TestClient

from app.main import app
from app.db.session import get_session
from app.api.v1 import scraper as scraper_module
from app.scraper.schemas import AssetObservation, IngestionReport, IngestionResult

ROWS = [
    {"day": date(2020, 1, 1), "asset_name": "mahboonkrong_rice_5kg", "ppu": 0.036, "nominal_price": 180.0, "net_weight": 5000.0},
    {"day": date(2020, 1, 1), "asset_name": "mama_tomyum_pack", "ppu": 0.1091, "nominal_price": 6.0, "net_weight": 55.0},
    {"day": date(2020, 1, 1), "asset_name": "ezygo_kaphrao_box", "ppu": 0.2857, "nominal_price": 60.0, "net_weight": 210.0},
    {"day": date(2020, 1, 1), "asset_name": "crystal_water_600ml", "ppu": 0.01, "nominal_price": 6.0, "net_weight": 600.0},
    {"day": date(2020, 1, 1), "asset_name": "m150_bottle", "ppu": 0.0667, "nominal_price": 10.0, "net_weight": 150.0},
    {"day": date(2020, 1, 2), "asset_name": "mama_tomyum_pack", "ppu": 0.11, "nominal_price": 6.05, "net_weight": 55.0},
    {"day": date(2020, 1, 3), "asset_name": "mama_tomyum_pack", "ppu": 0.12, "nominal_price": 6.6, "net_weight": 55.0},
    {"day": date(2020, 1, 3), "asset_name": "ezygo_kaphrao_box", "ppu": 0.3, "nominal_price": 63.0, "net_weight": 210.0},
]


class FakeMappings:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return FakeMappings(self._rows)


class FakeSession:
    def __init__(self, rows):
        self._rows = rows

    async def execute(self, stmt, params=None):
        return FakeResult(self._rows)


async def _session_dep():
    yield FakeSession(ROWS)


app.dependency_overrides[get_session] = _session_dep


async def _fake_run_ingestion():
    obs = AssetObservation(
        time=datetime(2020, 1, 3, 12, 0, tzinfo=timezone.utc),
        asset_name="mama_tomyum_pack",
        platform_source="lazada",
        nominal_price=Decimal("6.60"),
        net_weight=Decimal("55.0"),
        unit_type="g",
    )
    return IngestionReport(
        started_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        results=[
            IngestionResult(
                asset_id="mama_tomyum_pack",
                platform_source="lazada",
                success=True,
                observation=obs,
                duration_ms=123,
            ),
        ],
    )


scraper_module.run_ingestion = _fake_run_ingestion

client = TestClient(app)


def show(title: str, r):
    print(f"\n{'=' * 60}\n{title}\n{'=' * 60}")
    print(f"HTTP {r.status_code}")
    try:
        print(json.dumps(r.json(), indent=2, ensure_ascii=False))
    except Exception:
        print(r.text)


if __name__ == "__main__":
    show("GET /health", client.get("/health"))

    show(
        "GET /api/v1/tssi/historical?start_date=2020-01-01&end_date=2020-01-03",
        client.get(
            "/api/v1/tssi/historical",
            params={"start_date": "2020-01-01", "end_date": "2020-01-03"},
        ),
    )

    show("GET /api/v1/tssi/current", client.get("/api/v1/tssi/current"))

    rppu = client.get(
        "/api/v1/tssi/assets/ppu",
        params={"start_date": "2020-01-01", "end_date": "2020-01-03"},
    )
    body = rppu.json()
    if isinstance(body, dict) and "series" in body:
        total = len(body["series"])
        body["series"] = body["series"][:6]
        body["_truncated_note"] = f"showing 6 of {total} rows"
    print(f"\n{'=' * 60}\nGET /api/v1/tssi/assets/ppu (truncated)\n{'=' * 60}")
    print(f"HTTP {rppu.status_code}")
    print(json.dumps(body, indent=2, ensure_ascii=False))

    show(
        "POST /api/v1/scraper/trigger (X-API-Key: demo-secret)",
        client.post("/api/v1/scraper/trigger", headers={"X-API-Key": "demo-secret"}),
    )
