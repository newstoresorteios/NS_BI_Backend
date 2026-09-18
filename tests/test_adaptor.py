from decimal import Decimal

import pytest

import app.adaptor as adaptor_module
from app.adaptor import Adaptor, _order_detail, _sale_status


@pytest.fixture(autouse=True)
def tray_settings(monkeypatch):
    monkeypatch.setattr(
        adaptor_module,
        "settings",
        lambda: type(
            "Config",
            (),
            {
                "tray_adaptor_url": "https://tray.example",
                "tray_adaptor_token": "internal-token",
            },
        )(),
    )


@pytest.mark.asyncio
async def test_list_translates_tray_products_and_pagination(monkeypatch):
    async def fake_get(self, path, *, params=None, retries=8):
        assert path == "/internal/products"
        assert params == {"page": 1, "limit": 50}
        return {
            "success": True,
            "paging": {"total": 75, "page": 1, "limit": 50},
            "products": [
                {
                    "id": 10,
                    "name": "Relógio",
                    "reference": "NS-10",
                    "current_price": 299.9,
                    "stock": 7,
                    "available": True,
                    "modified": "2026-09-17T12:00:00-03:00",
                }
            ],
        }

    monkeypatch.setattr(Adaptor, "_get", fake_get)
    client = Adaptor()
    result = await client.list("products")

    assert result["data"][0]["codigo"] == "NS-10"
    assert result["data"][0]["preco_tabela"] == 299.9
    assert result["nextCursor"].startswith("tray-page:2:")
    assert result["checkpointCursor"].startswith("tray-page:2:")
    assert client._headers() == {"Authorization": "Bearer internal-token"}


@pytest.mark.asyncio
async def test_orders_use_small_pages_for_frequent_checkpoints(monkeypatch):
    async def fake_get(self, path, *, params=None, retries=8):
        assert path == "/internal/orders"
        assert params == {"page": 1, "limit": 10}
        return {
            "paging": {"total": 0, "page": 1, "limit": 10},
            "orders": [],
        }

    monkeypatch.setattr(Adaptor, "_get", fake_get)

    result = await Adaptor().list("orders")

    assert result["data"] == []
    assert result["nextCursor"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("resource", "path", "key"),
    [
        ("product-properties", "/internal/products/properties", "properties"),
        ("variants", "/internal/products/variants", "variants"),
        ("brands", "/internal/brands", "brands"),
        ("kits", "/internal/kits", "kits"),
        ("customer-addresses", "/internal/customer-addresses", "addresses"),
        ("coupons", "/internal/coupons", "coupons"),
        (
            "distribution-centers",
            "/internal/inventory/distribution-centers",
            "distribution_centers",
        ),
    ],
)
async def test_extended_resources_preserve_normalized_payload(
    monkeypatch,
    resource,
    path,
    key,
):
    async def fake_get(self, requested_path, *, params=None, retries=8):
        assert requested_path == path
        assert params == {"page": 1, "limit": 50}
        return {
            "paging": {"total": 1, "page": 1, "limit": 50},
            key: [{"id": "source-1", "name": "Preservado", "extra": {"x": 1}}],
        }

    monkeypatch.setattr(Adaptor, "_get", fake_get)

    result = await Adaptor().list(resource)

    assert result["data"] == [
        {"id": "source-1", "name": "Preservado", "extra": {"x": 1}}
    ]


def test_order_complete_preserves_every_item_and_discount():
    result = _order_detail(
        {
            "order": {
                "id": 123,
                "status": "FINALIZADO",
                "status_group": "completed",
                "total": "180.00",
                "shipment": "SEDEX",
                "shipment_value": "24.90",
                "sending_code": "BR123",
                "sending_date": "2026-09-10T12:00:00Z",
                "tracking_url": "https://tracking.example/BR123",
                "dc_id": "4",
            },
            "customer_address": {"city": "São Paulo", "state": "SP"},
            "products": [
                {
                    "product_id": 7,
                    "name": "Produto A",
                    "quantity": 2,
                    "price": "90.00",
                    "original_price": "100.00",
                },
                {
                    "product_id": 8,
                    "name": "Produto B",
                    "quantity": 1,
                    "price": "30.00",
                    "original_price": "30.00",
                },
            ],
        },
        "123",
    )

    assert result["status"] == "order"
    assert len(result["itens"]) == 2
    assert Decimal(result["itens"][0]["desconto"]) == Decimal("20.00")
    assert result["itens"][1]["produto_id"] == 8
    assert result["metodo_envio"] == "SEDEX"
    assert result["valor_frete"] == "24.90"
    assert result["status_envio"] == "delivered"
    assert result["codigo_rastreio"] == "BR123"
    assert result["centro_distribuicao_id"] == "4"
    assert result["cidade_entrega"] == "São Paulo"
    assert result["tray"]["order"]["id"] == 123


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"status_group": "cancelled"}, "cancelled"),
        ({"status_group": "awaiting_payment", "has_payment": False}, "quote"),
        ({"status_group": "awaiting_shipment"}, "order"),
        ({"status_group": "completed"}, "order"),
    ],
)
def test_tray_status_is_mapped_to_analytics_status(payload, expected):
    assert _sale_status(payload) == expected
