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
    assert client._headers() == {"Authorization": "Bearer internal-token"}


def test_order_complete_preserves_every_item_and_discount():
    result = _order_detail(
        {
            "order": {
                "id": 123,
                "status": "FINALIZADO",
                "status_group": "completed",
                "total": "180.00",
            },
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
