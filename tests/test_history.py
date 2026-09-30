from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import history, sync
from app.database import Base
from app.models import Order, SyncRun, SyncState


@pytest.fixture
def db_factory(monkeypatch):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr(sync, "SessionLocal", factory)
    monkeypatch.setattr(history, "REQUEST_PAUSE", 0)
    return factory


@pytest.mark.asyncio
async def test_recovers_missing_skips_existing_and_advances_past_404(db_factory, monkeypatch):
    with db_factory() as db:
        db.add(Order(mercos_id="3", number="3", status="order", total=90, issued_at=datetime(2025, 1, 1, tzinfo=timezone.utc)))
        db.commit()
    calls = []
    async def fetch(self, path, *, retries):
        calls.append(path)
        assert retries == 1
        if "/1/" in path:
            raise HTTPException(404, "Not found")
        return {"order": {"id": 2, "date": "2020-01-01", "total": 20}, "products": []}
    monkeypatch.setattr(type(history.adaptor), "_get", fetch)
    history.prepare(1, 3)
    result = await history.resume()
    assert result["status"] == "success"
    assert calls == ["/internal/orders/2/complete", "/internal/orders/1/complete"]
    with db_factory() as db:
        state = db.get(SyncState, sync.ORDER_HISTORY_RESOURCE)
        assert state.cursor == history.PREFIX + "0:1"
        orders = db.scalars(select(Order).order_by(Order.mercos_id)).all()
        assert [order.mercos_id for order in orders] == ["2", "3"]
        assert orders[1].total == 90
        run = db.scalar(select(SyncRun))
        assert run.persisted == 1
        assert run.details["detailsConsulted"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("code,status", [(429, "partial"), (403, "interrupted"), (500, "interrupted")])
async def test_errors_never_skip_identifier(db_factory, monkeypatch, code, status):
    async def fetch(self, path, *, retries):
        raise HTTPException(code, "Error")
    monkeypatch.setattr(type(history.adaptor), "_get", fetch)
    initial = history.prepare(1, 2)
    result = await history.resume()
    assert result["status"] == status
    assert result["cursor"] == initial
    assert history.daily_used() == 1


@pytest.mark.asyncio
async def test_contract_error_does_not_create_empty_order(db_factory, monkeypatch):
    async def fetch(self, path, *, retries):
        return {"order": {}, "products": []}
    monkeypatch.setattr(type(history.adaptor), "_get", fetch)
    initial = history.prepare(1, 1)
    result = await history.resume()
    assert result["status"] == "interrupted"
    assert result["cursor"] == initial
    with db_factory() as db:
        assert db.scalar(select(Order)) is None


@pytest.mark.asyncio
async def test_daily_budget_and_operator_cancellation(db_factory, monkeypatch):
    history.prepare(1, 1)
    monkeypatch.setattr(history, "DAILY_ATTEMPTS", 0)
    assert (await history.resume())["status"] == "quota_wait"
    with db_factory() as db:
        state = db.get(SyncState, sync.ORDER_HISTORY_RESOURCE)
        state.status = "interrupted"
        state.error = "Interrompida pelo operador"
        db.commit()
    assert (await history.resume())["status"] == "idle"


def test_stale_worker_can_resume_but_live_worker_cannot(db_factory):
    history.prepare(1, 1)
    with db_factory() as db:
        state = db.get(SyncState, sync.ORDER_HISTORY_RESOURCE)
        state.status = "running"
        state.heartbeat_at = datetime.now(timezone.utc)
        state.lease_token = "live"
        db.commit()
    assert not history.pending()
    with db_factory() as db:
        state = db.get(SyncState, sync.ORDER_HISTORY_RESOURCE)
        state.heartbeat_at = datetime.now(timezone.utc) - timedelta(minutes=5)
        db.commit()
    assert history.pending()
