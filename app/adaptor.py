"""HTTP client and normalization layer for the NS Tray Adapter.

The analytical core uses a stable internal commerce payload. This module
translates the normalized responses exposed by ``TRAYadaptor`` into it.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import httpx
from fastapi import HTTPException

from app.config import settings

log = logging.getLogger("uvicorn.error")

TRANSIENT = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.RemoteProtocolError,
)
DEFAULT_RETRIES = 8
WARMUP_RETRIES = 6
RETRYABLE_STATUS = {429, 502, 503, 504}
MAX_RETRY_WAIT = 180.0
DEFAULT_429_WAIT = 20.0
RATE_LIMIT_BUDGET = 240.0
PAGE_SIZE = 50

_request_lock = asyncio.Lock()
_not_before = 0.0
_cancel = asyncio.Event()


def request_cancel() -> None:
    _cancel.set()


def clear_cancel() -> None:
    _cancel.clear()


def _response_detail(response: httpx.Response) -> str:
    text = response.text[:500].strip()
    content_type = response.headers.get("content-type", "").lower()
    if "text/html" in content_type or text.lower().startswith(("<!doctype", "<html")):
        return "resposta HTML temporária do provedor"
    return text or response.reason_phrase


def _retry_wait(response: httpx.Response, attempt: int) -> float:
    header = response.headers.get("Retry-After")
    if header:
        try:
            return min(max(float(header), 1.0), MAX_RETRY_WAIT)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(header)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                return min(
                    max((retry_at - datetime.now(timezone.utc)).total_seconds(), 1.0),
                    MAX_RETRY_WAIT,
                )
            except (TypeError, ValueError, OverflowError):
                pass
    if response.status_code == 429:
        return min(DEFAULT_429_WAIT * (2**attempt), MAX_RETRY_WAIT)
    return min(2**attempt, 30.0)


def _extend_cooldown(wait: float) -> None:
    global _not_before
    _not_before = max(_not_before, time.monotonic() + max(wait, 0))


async def _respect_cooldown() -> None:
    if _cancel.is_set():
        raise HTTPException(409, "Sincronização interrompida pelo operador")
    delay = _not_before - time.monotonic()
    if delay <= 0.05:
        return
    waiter = asyncio.create_task(_cancel.wait())
    sleeper = asyncio.create_task(asyncio.sleep(delay))
    done, pending = await asyncio.wait(
        {waiter, sleeper}, return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
    if waiter in done and _cancel.is_set():
        raise HTTPException(409, "Sincronização interrompida pelo operador")


def _decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value or "0").replace(",", "."))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _bool(value: object, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return default
    return str(value).strip().lower() in {
        "1", "true", "yes", "sim", "active", "ativo"
    }


def _watermark(rows: list[dict], previous: str | None) -> str | None:
    candidates = [previous] if previous else []
    for row in rows:
        for key in ("modified", "created", "date", "payment_date"):
            value = row.get(key)
            if value:
                candidates.append(str(value))
    return max(candidates) if candidates else previous


def _decode_cursor(cursor: str | None) -> tuple[int, str | None, str | None]:
    if cursor and cursor.startswith("tray-page:"):
        _, page, state = cursor.split(":", 2)
        base_since, _, watermark = state.partition("|")
        return max(int(page), 1), base_since or None, watermark or None
    return 1, cursor or None, cursor or None


def _encode_cursor(
    page: int, base_since: str | None, watermark: str | None
) -> str:
    return f"tray-page:{page}:{base_since or ''}|{watermark or ''}"


def _sale_status(order: dict) -> str:
    group = str(order.get("status_group") or "").strip().lower()
    raw = str(order.get("status") or "").strip().lower()
    if group == "cancelled" or "cancel" in raw:
        return "cancelled"
    if group == "awaiting_payment" and not _bool(order.get("has_payment"), False):
        return "quote"
    if group in {"awaiting_shipment", "shipped", "completed"}:
        return "order"
    return "order" if _bool(order.get("has_payment"), False) else "quote"


def _first(mapping: dict, *keys: str):
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _shipment_status(order: dict) -> str:
    raw = str(
        _first(
            order,
            "shipment_status",
            "shipping_status",
            "status_frete_ml",
            "status_group",
            "status",
        )
        or ""
    ).strip().lower()
    delivered = _first(order, "delivery_date", "date_delivered", "delivered_at")
    delivered_flag = str(order.get("delivered") or "").strip().lower()
    if delivered or delivered_flag in {"1", "true", "yes", "sim"} or raw in {
        "completed",
        "delivered",
        "entregue",
        "finalizado",
    }:
        return "delivered"
    if (
        _first(order, "shipment_date", "sending_date", "shipped_at", "sending_code")
        or raw in {"shipped", "sent", "enviado"}
    ):
        return "shipped"
    if _first(order, "shipment", "shipping_method", "shipping_id") or raw in {
        "awaiting_shipment",
        "a enviar",
    }:
        return "awaiting_shipment"
    return "not_informed"


def _customer(row: dict) -> dict:
    email = row.get("email")
    return {
        "id": row.get("id"),
        "nome": row.get("name") or row.get("company_name") or "Sem nome",
        "razao_social": row.get("company_name"),
        "cpf": row.get("cpf"),
        "cnpj": row.get("cnpj"),
        "cidade": row.get("city"),
        "estado": row.get("state"),
        "emails": [{"email": email}] if email else [],
        "celular": row.get("cellphone"),
        "telefone": row.get("phone"),
        "data_criacao": row.get("created") or row.get("registration_date"),
        "ultima_alteracao": row.get("modified"),
        "ativo": True,
        "tray": row,
    }


def _product(row: dict) -> dict:
    price = (
        row.get("current_price")
        or row.get("promotional_price")
        or row.get("price")
        or 0
    )
    return {
        "id": row.get("id"),
        "codigo": row.get("reference") or row.get("ean") or row.get("id"),
        "nome": row.get("name") or row.get("title") or "Sem nome",
        "categoria_id": row.get("category_id"),
        "unidade": "UN",
        "preco_tabela": price,
        "preco_minimo": row.get("promotional_price") or price,
        "saldo_estoque": row.get("stock") or 0,
        "ativo": _bool(row.get("available_in_store", row.get("available")), True),
        "data_criacao": row.get("created"),
        "ultima_alteracao": row.get("modified"),
        "tray": row,
    }


def _user(row: dict) -> dict:
    return {
        "id": row.get("id"),
        "nome": row.get("full_name") or row.get("name") or row.get("email") or "Sem nome",
        "email": row.get("email"),
        "ativo": _bool(row.get("active"), True),
        "tray": row,
    }


def _category(row: dict) -> dict:
    return {
        "id": row.get("id"),
        "nome": row.get("name") or row.get("title") or "Sem nome",
        "categoria_pai_id": row.get("parent_id"),
        "ativo": True,
        "tray": row,
    }


def _raw_entity(row: dict) -> dict:
    """Preserve every normalized field exposed by TRAYadaptor."""
    return dict(row)


def _order_header(row: dict) -> dict:
    address = row.get("customer_address") or row.get("CustomerAddress") or {}
    if isinstance(address, list):
        address = address[0] if address else {}
    if not isinstance(address, dict):
        address = {}
    return {
        "id": row.get("id"),
        "numero": row.get("id"),
        "cliente_id": row.get("customer_id"),
        "status": _sale_status(row),
        "status_tray": row.get("status"),
        "status_group": row.get("status_group"),
        "data_emissao": row.get("payment_date") or row.get("date") or row.get("created"),
        "data_criacao": row.get("created") or row.get("date"),
        "ultima_alteracao": row.get("modified") or row.get("date"),
        "total": row.get("total") or 0,
        "total_liquido": row.get("total") or 0,
        "condicao_pagamento_id": row.get("payment_method_id"),
        "metodo_envio_id": _first(row, "shipping_id", "shipping_method_id"),
        "metodo_envio": _first(row, "shipment", "shipping_method"),
        "valor_frete": _first(row, "shipment_value", "shipping_value", "shipping_cost"),
        "status_envio": _shipment_status(row),
        "codigo_rastreio": _first(row, "sending_code", "tracking_code"),
        "url_rastreio": _first(row, "tracking_url", "shipment_tracking_url"),
        "data_envio": _first(row, "shipment_date", "sending_date", "shipped_at"),
        "data_entrega": _first(row, "delivery_date", "date_delivered", "delivered_at"),
        "previsao_entrega": _first(
            row,
            "estimated_delivery_date",
            "estimated_delivery_time",
            "estimated_delivery",
        ),
        "integrador_envio": _first(row, "shipment_integrator", "shipping_integrator"),
        "centro_distribuicao_id": _first(row, "dc_id", "distribution_center_id"),
        "cidade_entrega": _first(row, "shipping_city", "delivery_city")
        or _first(address, "city"),
        "estado_entrega": _first(row, "shipping_state", "delivery_state")
        or _first(address, "state"),
        "tray": row,
    }


def _order_detail(payload: dict, order_id: str) -> dict:
    order = payload.get("order") if isinstance(payload.get("order"), dict) else {}
    shipping = payload.get("shipping") if isinstance(payload.get("shipping"), dict) else {}
    address = next(
        (
            payload.get(key)
            for key in ("customer_address", "customerAddress", "CustomerAddress", "address")
            if isinstance(payload.get(key), (dict, list))
        ),
        order.get("customer_address") or order.get("CustomerAddress"),
    )
    combined_order = {**order, **shipping}
    if address:
        combined_order["customer_address"] = address
    products = payload.get("products") if isinstance(payload.get("products"), list) else []
    header = {
        key: value
        for key, value in _order_header({"id": order_id, **combined_order}).items()
        if value is not None
    }
    header["tray"] = payload
    header["itens"] = []
    for position, product in enumerate(products):
        quantity = _decimal(product.get("quantity") or 0)
        unit = _decimal(product.get("price") or 0)
        original = _decimal(product.get("original_price") or product.get("price") or 0)
        header["itens"].append(
            {
                "id": (
                    f"{order_id}:{product.get('product_id')}:"
                    f"{product.get('variant_id') or 0}:{position}"
                ),
                "produto_id": product.get("product_id"),
                "produto_codigo": product.get("variant_id") or product.get("product_id"),
                "produto_nome": product.get("name") or "Produto",
                "quantidade": str(quantity),
                "preco_tabela": str(original),
                "preco_liquido": str(unit),
                "desconto": str(max((original - unit) * quantity, Decimal("0"))),
                "subtotal": str(unit * quantity),
                "excluido": False,
                "tray": product,
            }
        )
    return header


RESOURCE_MAP = {
    "customers": ("/internal/customers", "customers", _customer),
    "products": ("/internal/products", "products", _product),
    "users": ("/internal/users", "users", _user),
    "categories": ("/internal/categories", "categories", _category),
    "orders": ("/internal/orders", "orders", _order_header),
    "product-properties": ("/internal/products/properties", "properties", _raw_entity),
    "variants": ("/internal/products/variants", "variants", _raw_entity),
    "brands": ("/internal/brands", "brands", _raw_entity),
    "kits": ("/internal/kits", "kits", _raw_entity),
    "customer-addresses": ("/internal/customer-addresses", "addresses", _raw_entity),
    "coupons": ("/internal/coupons", "coupons", _raw_entity),
    "distribution-centers": (
        "/internal/inventory/distribution-centers",
        "distribution_centers",
        _raw_entity,
    ),
    "shipping-methods": (
        "/internal/shippings/methods",
        ("shipping_methods", "methods", "shippings"),
        _raw_entity,
    ),
}


class Adaptor:
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {settings().tray_adaptor_token}"}

    async def wake(self, *, retries: int = WARMUP_RETRIES) -> None:
        cfg = settings()
        if not cfg.tray_adaptor_url:
            return
        url = f"{cfg.tray_adaptor_url.rstrip('/')}/health"
        for attempt in range(retries):
            try:
                async with httpx.AsyncClient(timeout=20) as client:
                    response = await client.get(url)
                if not response.is_error:
                    return
            except TRANSIENT:
                pass
            if attempt + 1 < retries:
                await asyncio.sleep(min(2**attempt, 15))

    async def _get(
        self,
        path: str,
        *,
        params: dict | None = None,
        retries: int = DEFAULT_RETRIES,
    ) -> dict:
        cfg = settings()
        if not cfg.tray_adaptor_url or not cfg.tray_adaptor_token:
            raise HTTPException(
                503, "TRAY_ADAPTOR_URL/TRAY_ADAPTOR_TOKEN não configurados"
            )
        await self.wake()
        url = f"{cfg.tray_adaptor_url.rstrip('/')}{path}"
        waited = 0.0
        last_exc: Exception | None = None
        async with _request_lock:
            for attempt in range(retries):
                try:
                    await _respect_cooldown()
                    async with httpx.AsyncClient(
                        timeout=httpx.Timeout(90, connect=15)
                    ) as client:
                        response = await client.get(
                            url, params=params, headers=self._headers()
                        )
                except TRANSIENT as exc:
                    last_exc = exc
                    if attempt + 1 >= retries:
                        break
                    _extend_cooldown(min(2**attempt, 30.0))
                    continue
                except httpx.RequestError as exc:
                    raise HTTPException(
                        502, f"TrayAdaptor inacessível: {type(exc).__name__}"
                    ) from exc

                if response.status_code in RETRYABLE_STATUS and attempt + 1 < retries:
                    wait = _retry_wait(response, attempt)
                    if waited + wait <= RATE_LIMIT_BUDGET:
                        waited += wait
                        _extend_cooldown(wait)
                        continue
                if response.is_error:
                    raise HTTPException(
                        502 if response.status_code >= 500 else response.status_code,
                        f"TrayAdaptor {response.status_code} em {path}: "
                        f"{_response_detail(response)}",
                    )
                payload = response.json()
                if not isinstance(payload, dict):
                    raise HTTPException(
                        502, f"TrayAdaptor retornou formato inválido em {path}"
                    )
                return payload
        raise HTTPException(
            502,
            f"TrayAdaptor inacessível após {retries} tentativas: "
            f"{type(last_exc).__name__ if last_exc else 'erro'}",
        )

    async def list(
        self,
        resource: str,
        cursor: str | None = None,
        *,
        retries: int = DEFAULT_RETRIES,
    ):
        if resource not in RESOURCE_MAP:
            raise HTTPException(404, f"Recurso Tray não suportado: {resource}")
        path, key, normalizer = RESOURCE_MAP[resource]
        page, base_since, accumulated_watermark = _decode_cursor(cursor)
        params: dict[str, object] = {"page": page, "limit": PAGE_SIZE}
        if base_since and resource in {"orders", "customers"}:
            params["lastModifiedStart"] = base_since
        payload = await self._get(path, params=params, retries=retries)
        keys = (key,) if isinstance(key, str) else key
        source_rows = next(
            (payload.get(candidate) for candidate in keys if isinstance(payload.get(candidate), list)),
            [],
        )
        rows = [
            normalizer(row)
            for row in source_rows
            if isinstance(row, dict) and row.get("id") is not None
        ]
        paging = payload.get("paging") if isinstance(payload.get("paging"), dict) else {}
        total = int(paging.get("total") or len(rows))
        limit = int(paging.get("limit") or PAGE_SIZE)
        current_page = int(paging.get("page") or page)
        current_watermark = _watermark(source_rows, accumulated_watermark)
        has_next = current_page * limit < total and bool(rows)
        return {
            "data": rows,
            "nextCursor": (
                _encode_cursor(current_page + 1, base_since, current_watermark)
                if has_next
                else None
            ),
            "pageCursor": current_watermark,
        }

    async def detail(
        self,
        resource: str,
        source_id: str,
        *,
        retries: int = DEFAULT_RETRIES,
    ):
        if resource != "orders":
            raise HTTPException(404, f"Detalhe Tray não suportado: {resource}")
        safe_id = quote(str(source_id), safe="")
        payload = await self._get(
            f"/internal/orders/{safe_id}/complete", retries=retries
        )
        return _order_detail(payload, str(source_id))

    async def health(self):
        cfg = settings()
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(
                f"{cfg.tray_adaptor_url.rstrip('/')}/health"
            )
            response.raise_for_status()
            return response.json()


adaptor = Adaptor()


async def keep_adaptor_warm() -> None:
    try:
        await adaptor.wake(retries=2)
    except Exception as exc:
        log.warning("TrayAdaptor keep-warm failed: %s", type(exc).__name__)
