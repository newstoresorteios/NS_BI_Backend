"""Resumable, quota-bounded recovery when Tray listing omits old orders.

All extraction remains behind TRAYadaptor. Existing dated orders are skipped;
only an upstream 404 counts as an absent ID. Other errors retain the checkpoint.
"""
import asyncio
import time
from contextlib import suppress
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException
from sqlalchemy import select

from app import sync
from app.adaptor import _order_detail, adaptor
from app.models import Order, SyncRun, SyncState

PREFIX = "tray-order-id-history:"
BATCH_ATTEMPTS = 200
DAILY_ATTEMPTS = 3000
REQUEST_PAUSE = 1.1
MAX_BATCH_SECONDS = 90
RETRY_COOLDOWN = timedelta(minutes=5)


def schedule(scheduler):
    """Resume durable work soon after boot, without creating a new import."""
    scheduler.add_job(
        resume, "interval", seconds=60,
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=5),
        id="recover_order_history", replace_existing=True,
        max_instances=1, coalesce=True,
    )


def decode(cursor):
    if not cursor or not cursor.startswith(PREFIX):
        raise ValueError("Checkpoint de recuperação por ID inválido")
    current, first = map(int, cursor[len(PREFIX):].split(":"))
    return current, first


def prepare(first_id: int, last_id: int, reset: bool = False):
    with sync.SessionLocal() as db:
        sync._acquire_sync_coordination_lock(db)
        state = db.get(SyncState, sync.ORDER_HISTORY_RESOURCE)
        if state and sync._lease_is_active(state, datetime.now(timezone.utc)):
            raise HTTPException(409, "Carga histórica em andamento")
        if state is None:
            state = SyncState(resource=sync.ORDER_HISTORY_RESOURCE)
        if reset or not (state.cursor or "").startswith(PREFIX):
            state.cursor = f"{PREFIX}{last_id}:{first_id}"
        state.status = "partial"
        state.error = None
        state.lease_token = None
        db.add(state)
        db.commit()
        return state.cursor


def pending():
    with sync.SessionLocal() as db:
        state = db.get(SyncState, sync.ORDER_HISTORY_RESOURCE)
        if not state or not (state.cursor or "").startswith(PREFIX):
            return False
        if state.status == "running":
            return not sync._lease_is_active(state, datetime.now(timezone.utc))
        if state.status == "partial" and state.error and state.heartbeat_at:
            heartbeat = state.heartbeat_at
            if heartbeat.tzinfo is None:
                heartbeat = heartbeat.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - heartbeat < RETRY_COOLDOWN:
                return False
        # Explicit operator cancellation is never auto-resumed.
        return state.status in {"partial", "never"} or (
            state.status == "interrupted"
            and (state.error or "").startswith(("Serviço reiniciou", "Lease de sincronização"))
        )


def daily_used():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    with sync.SessionLocal() as db:
        runs = db.scalars(select(SyncRun).where(
            SyncRun.resource == sync.ORDER_HISTORY_RESOURCE,
            SyncRun.started_at >= cutoff,
        )).all()
        return sum(int((run.details or {}).get("detailsConsulted", 0)) for run in runs)


def existing(first, last):
    ids = [str(value) for value in range(first, last + 1)]
    with sync.SessionLocal() as db:
        return set(db.scalars(select(Order.mercos_id).where(
            Order.mercos_id.in_(ids), Order.issued_at.is_not(None),
        )).all())


async def resume():
    if not await asyncio.to_thread(pending):
        return {"status": "idle"}
    remaining = DAILY_ATTEMPTS - await asyncio.to_thread(daily_used)
    if remaining <= 0:
        return {"status": "quota_wait", "dailyLimit": DAILY_ATTEMPTS}
    started = datetime.now(timezone.utc)
    claim = await asyncio.to_thread(sync._claim_sync, sync.ORDER_HISTORY_RESOURCE, False, started)
    if claim is None:
        return {"status": "running"}
    run_id, lease, cursor = claim
    heartbeat = asyncio.create_task(sync._keep_sync_lease_alive(sync.ORDER_HISTORY_RESOURCE, lease))
    pages = received = persisted = failed = attempts = items = 0
    status, error = "partial", None
    cancelled = False
    deadline = time.monotonic() + MAX_BATCH_SECONDS
    try:
        current, first = decode(cursor)
        allowance = min(BATCH_ATTEMPTS, remaining)
        while current >= first and attempts < allowance and time.monotonic() < deadline:
            bottom = max(first, current - 9)
            known = await asyncio.to_thread(existing, bottom, current)
            rows = []
            inspected = 0
            for source_id in range(current, bottom - 1, -1):
                if attempts >= allowance or time.monotonic() >= deadline:
                    break
                if str(source_id) not in known:
                    attempts += 1
                    request_started = time.monotonic()
                    try:
                        payload = await adaptor._get(f"/internal/orders/{source_id}/complete", retries=1)
                    except HTTPException as exc:
                        if exc.status_code != 404:
                            raise
                    else:
                        source = payload.get("order", {})
                        if str(source.get("id")) != str(source_id) or not source.get("date") or "total" not in source:
                            raise ValueError(f"Pedido {source_id} incompleto; checkpoint preservado")
                        rows.append(_order_detail(payload, str(source_id)))
                        received += 1
                    await asyncio.sleep(max(0, REQUEST_PAUSE - (time.monotonic() - request_started)))
                inspected += 1
                next_cursor = f"{PREFIX}{source_id - 1}:{first}"
            # Data and cursor commit together. Errors replay the uncommitted
            # chunk (at most ten IDs), never skipping an unresolved identifier.
            if inspected:
                persisted, items = await asyncio.to_thread(
                    sync._persist_sync_page, run_id, lease, sync.ORDER_HISTORY_RESOURCE, rows,
                    page_cursor=next_cursor, next_pages=pages + inspected,
                    received=received, persisted=persisted, failed=failed,
                    details_consulted=attempts, items_persisted=items,
                )
                pages += inspected
                cursor = next_cursor
                current -= inspected
        if current < first:
            status = "success"
    except asyncio.CancelledError:
        # A platform shutdown is not an operator cancellation. Release our lease
        # and preserve the last committed ID so the next process can resume.
        cancelled = True
    except Exception as exc:
        failed += 1
        code = exc.status_code if isinstance(exc, HTTPException) else None
        # Transient/quota errors resume on the next hourly pass; contract errors stop.
        status = "partial" if code in {429, 502, 503, 504} else "interrupted"
        error = f"Recuperação por ID: {exc.detail if isinstance(exc, HTTPException) else str(exc)}"
    finally:
        heartbeat.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat
    result = await asyncio.to_thread(
        sync._finish_sync_run, run_id, lease, sync.ORDER_HISTORY_RESOURCE,
        status=status, pages=pages, received=received, persisted=persisted, failed=failed,
        cursor_after=cursor, details_consulted=attempts, items_persisted=items,
        started_at=started, error=error,
    )
    if cancelled:
        raise asyncio.CancelledError
    return result
