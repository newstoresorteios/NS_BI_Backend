from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import Customer, Order
from app.schemas.analytics import AnalyticsFilters
from app.services.logistics import logistics_overview


def make_session() -> Session:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return Session(engine)


def test_logistics_statistics_and_percentages_use_the_same_filtered_orders() -> None:
    with make_session() as db:
        db.add(Customer(mercos_id="c1", name="Cliente", active=True))
        db.add_all(
            [
                Order(
                    mercos_id="o1",
                    number="1",
                    customer_mercos_id="c1",
                    status="order",
                    issued_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
                    total=Decimal("100"),
                    shipping_method="SEDEX",
                    shipping_cost=Decimal("10"),
                    shipment_status="awaiting_shipment",
                ),
                Order(
                    mercos_id="o2",
                    number="2",
                    customer_mercos_id="c1",
                    status="order",
                    issued_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
                    total=Decimal("100"),
                    shipping_method="SEDEX",
                    shipping_cost=Decimal("20"),
                    shipment_status="shipped",
                    tracking_code="BR2",
                    shipped_at=datetime(2026, 9, 3, tzinfo=timezone.utc),
                ),
                Order(
                    mercos_id="o3",
                    number="3",
                    customer_mercos_id="c1",
                    status="order",
                    issued_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
                    total=Decimal("100"),
                    shipping_method="PAC",
                    shipping_cost=Decimal("30"),
                    shipment_status="delivered",
                    tracking_code="BR3",
                    shipped_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
                    delivered_at=datetime(2026, 9, 5, tzinfo=timezone.utc),
                ),
                Order(
                    mercos_id="cancelled",
                    number="4",
                    status="cancelled",
                    issued_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
                    total=Decimal("999"),
                    shipping_method="Ignorado",
                ),
            ]
        )
        db.commit()

        result = logistics_overview(
            db,
            AnalyticsFilters(period="all"),
            page=1,
            page_size=50,
            search=None,
            status=None,
            sort="issued_at",
            order="desc",
        )

        assert result["summary"] == {
            "orders": 3,
            "withShipping": 3,
            "awaitingShipment": 1,
            "shipped": 1,
            "delivered": 1,
            "notInformed": 0,
            "shippingCoveragePct": 100.0,
            "trackingCoveragePct": 100.0,
            "deliveryRatePct": 50.0,
            "totalShippingCost": Decimal("60"),
            "averageShippingCost": Decimal("20"),
            "averageFulfillmentDays": 1.5,
            "averageDeliveryDays": 3.0,
        }
        assert result["byMethod"][0]["method"] == "SEDEX"
        assert result["byMethod"][0]["sharePct"] == 66.67
        assert result["totalItems"] == 3


def test_logistics_status_and_search_are_applied_before_pagination() -> None:
    with make_session() as db:
        db.add(Customer(mercos_id="c1", name="Maria", active=True))
        db.add(
            Order(
                mercos_id="o1",
                number="PED-1",
                customer_mercos_id="c1",
                status="order",
                total=Decimal("100"),
                shipment_status="shipped",
                tracking_code="ABC123",
            )
        )
        db.commit()

        result = logistics_overview(
            db,
            AnalyticsFilters(period="all"),
            page=1,
            page_size=1,
            search="ABC123",
            status="shipped",
            sort="number",
            order="asc",
        )

        assert result["totalItems"] == 1
        assert result["items"][0]["number"] == "PED-1"
