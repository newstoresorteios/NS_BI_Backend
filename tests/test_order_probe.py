import httpx
import pytest

from app.adaptor import Adaptor
from app.main import app


@pytest.mark.asyncio
async def test_probe_is_bounded_and_redacts_customer_data(monkeypatch):
    async def fake_get(self, path, *, params=None, retries=8):
        assert path == "/internal/orders"
        assert params == {
            "page": 1, "limit": 10, "sort": "id_asc",
            "date": "2020-01-01,2020-01-31 23:59:59",
        }
        return {"paging": {"total": 1}, "orders": [
            {"id": 7, "date": "2020-01-02", "customer_id": 9, "email": "private"}
        ]}
    monkeypatch.setattr(Adaptor, "_get", fake_get)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        result = await client.get("/api/v1/sync/orders-probe?start=2020-01-01&end=2020-01-31")
    assert result.status_code == 200
    assert "private" not in result.text
    assert "customer_id" not in result.text
    assert result.json()["orders"][0]["id"] == 7
