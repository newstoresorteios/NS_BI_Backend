from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain.order_status import VALID_SALE_STATUSES, status_sql_in
from app.models import Customer, Order
from app.schemas.analytics import AnalyticsFilters
from app.services.analytics_filters import applied_filters, order_conditions
from app.services.analytics_v2 import analytics_metadata


ZERO = Decimal("0")


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _shipment_status(order: Order) -> str:
    raw = (order.shipment_status or "").strip().lower()
    if order.delivered_at is not None or raw in {"delivered", "entregue", "completed"}:
        return "delivered"
    if order.shipped_at is not None or order.tracking_code or raw in {"shipped", "enviado", "sent"}:
        return "shipped"
    if order.shipping_method or order.shipping_method_id or raw == "awaiting_shipment":
        return "awaiting_shipment"
    return "not_informed"


def _pct(numerator: int, denominator: int) -> float:
    return round((numerator / denominator) * 100, 2) if denominator else 0.0


def _average(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 2) if values else None


def _row(order: Order, customer_name: str | None) -> dict[str, Any]:
    return {
        "id": order.mercos_id,
        "number": order.number,
        "issuedAt": order.issued_at,
        "customerName": customer_name,
        "orderStatus": order.status,
        "shipmentStatus": _shipment_status(order),
        "shippingMethod": order.shipping_method,
        "shippingCost": order.shipping_cost,
        "trackingCode": order.tracking_code,
        "trackingUrl": order.tracking_url,
        "shippedAt": order.shipped_at,
        "deliveredAt": order.delivered_at,
        "estimatedDelivery": order.estimated_delivery,
        "integrator": order.shipment_integrator,
        "distributionCenterId": order.distribution_center_id,
        "city": order.shipping_city,
        "state": order.shipping_state,
    }


def logistics_overview(
    db: Session,
    filters: AnalyticsFilters,
    *,
    page: int,
    page_size: int,
    search: str | None,
    status: str | None,
    sort: str,
    order: str,
) -> dict[str, Any]:
    conditions = [
        *order_conditions(filters),
        status_sql_in(Order.status, VALID_SALE_STATUSES),
    ]
    all_records = [
        _row(order_row, customer_name)
        for order_row, customer_name in db.execute(
            select(Order, Customer.name)
            .outerjoin(Customer, Customer.mercos_id == Order.customer_mercos_id)
            .where(*conditions)
        )
    ]
    records = all_records
    if status:
        records = [item for item in records if item["shipmentStatus"] == status]
    if search:
        needle = search.strip().casefold()
        records = [
            item
            for item in records
            if any(
                needle in str(item[field] or "").casefold()
                for field in (
                    "number",
                    "customerName",
                    "trackingCode",
                    "shippingMethod",
                    "integrator",
                )
            )
        ]

    total_orders = len(all_records)
    with_shipping = sum(
        1
        for item in all_records
        if item["shippingMethod"]
        or item["shippingCost"] is not None
        or item["trackingCode"]
        or item["shippedAt"]
        or item["deliveredAt"]
    )
    shipped = sum(item["shipmentStatus"] == "shipped" for item in all_records)
    delivered = sum(item["shipmentStatus"] == "delivered" for item in all_records)
    awaiting = sum(item["shipmentStatus"] == "awaiting_shipment" for item in all_records)
    not_informed = sum(item["shipmentStatus"] == "not_informed" for item in all_records)
    trackable = sum(
        bool(item["trackingCode"] or item["trackingUrl"])
        for item in all_records
        if item["shipmentStatus"] in {"shipped", "delivered"}
    )
    shipping_costs = [
        Decimal(item["shippingCost"])
        for item in all_records
        if item["shippingCost"] is not None
    ]

    fulfillment_days: list[float] = []
    delivery_days: list[float] = []
    for item in all_records:
        issued = _aware(item["issuedAt"])
        shipped_at = _aware(item["shippedAt"])
        delivered_at = _aware(item["deliveredAt"])
        if issued and shipped_at and shipped_at >= issued:
            fulfillment_days.append((shipped_at - issued).total_seconds() / 86400)
        if shipped_at and delivered_at and delivered_at >= shipped_at:
            delivery_days.append((delivered_at - shipped_at).total_seconds() / 86400)

    method_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "orders": 0,
            "delivered": 0,
            "shippingCost": ZERO,
            "costRecords": 0,
        }
    )
    for item in all_records:
        name = item["shippingMethod"] or "Não informado"
        method_stats[name]["orders"] += 1
        method_stats[name]["delivered"] += int(item["shipmentStatus"] == "delivered")
        if item["shippingCost"] is not None:
            method_stats[name]["shippingCost"] += Decimal(item["shippingCost"])
            method_stats[name]["costRecords"] += 1

    by_method = [
        {
            "method": name,
            "orders": values["orders"],
            "sharePct": _pct(values["orders"], total_orders),
            "delivered": values["delivered"],
            "deliveryRatePct": _pct(values["delivered"], values["orders"]),
            "shippingCost": values["shippingCost"],
            "averageShippingCost": (
                values["shippingCost"] / values["costRecords"]
                if values["costRecords"]
                else ZERO
            ),
        }
        for name, values in method_stats.items()
    ]
    by_method.sort(key=lambda item: (-item["orders"], item["method"]))

    sort_keys = {
        "issued_at": lambda item: item["issuedAt"] or datetime.min.replace(tzinfo=timezone.utc),
        "shipped_at": lambda item: item["shippedAt"] or datetime.min.replace(tzinfo=timezone.utc),
        "delivered_at": lambda item: item["deliveredAt"] or datetime.min.replace(tzinfo=timezone.utc),
        "shipping_cost": lambda item: Decimal(item["shippingCost"] or 0),
        "number": lambda item: item["number"],
        "shipping_method": lambda item: item["shippingMethod"] or "",
        "shipment_status": lambda item: item["shipmentStatus"],
    }
    records.sort(key=sort_keys[sort], reverse=order == "desc")
    total_items = len(records)
    start = (page - 1) * page_size

    shipped_or_delivered = shipped + delivered
    return {
        "summary": {
            "orders": total_orders,
            "withShipping": with_shipping,
            "awaitingShipment": awaiting,
            "shipped": shipped,
            "delivered": delivered,
            "notInformed": not_informed,
            "shippingCoveragePct": _pct(with_shipping, total_orders),
            "trackingCoveragePct": _pct(trackable, shipped_or_delivered),
            "deliveryRatePct": _pct(delivered, shipped_or_delivered),
            "totalShippingCost": sum(shipping_costs, ZERO),
            "averageShippingCost": (
                sum(shipping_costs, ZERO) / len(shipping_costs)
                if shipping_costs
                else ZERO
            ),
            "averageFulfillmentDays": _average(fulfillment_days),
            "averageDeliveryDays": _average(delivery_days),
        },
        "byStatus": [
            {"status": "awaiting_shipment", "orders": awaiting, "sharePct": _pct(awaiting, total_orders)},
            {"status": "shipped", "orders": shipped, "sharePct": _pct(shipped, total_orders)},
            {"status": "delivered", "orders": delivered, "sharePct": _pct(delivered, total_orders)},
            {"status": "not_informed", "orders": not_informed, "sharePct": _pct(not_informed, total_orders)},
        ],
        "byMethod": by_method,
        "items": records[start : start + page_size],
        "page": page,
        "pageSize": page_size,
        "totalItems": total_items,
        "totalPages": max(1, (total_items + page_size - 1) // page_size),
        "sort": sort,
        "order": order,
        "appliedFilters": applied_filters(filters),
        "metadata": analytics_metadata(db),
    }
