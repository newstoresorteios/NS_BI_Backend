import pytest

from app import sync


def test_second_store_ids_do_not_replace_the_primary_order():
    row = sync.namespace_store_row(
        {
            "id": "1523",
            "numero": "1523",
            "cliente_id": "9",
            "itens": [{"id": "1523:4:0:0", "produto_id": "4", "produto_codigo": "SKU"}],
        },
        "1490060",
    )
    assert row["id"] == "1490060:1523"
    assert row["numero"] == "1523"
    assert row["cliente_id"] == "1490060:9"
    assert row["loja_id"] == "1490060"
    assert row["tray_id"] == "1523"
    assert row["itens"][0]["id"] == "1490060:1523:4:0:0"
    assert row["itens"][0]["produto_id"] == "1490060:4"
    assert row["itens"][0]["produto_codigo"] == "SKU"


def test_primary_order_id_is_not_split():
    assert sync._store_of_mercos_id("1523") == (None, "1523")


def test_adaptor_health_lists_only_the_extra_store():
    assert sync.stores_from_adaptor_health(
        {"store_id": "687890", "configured_stores": ["1490060", "687890"]}
    ) == ["1490060"]


@pytest.mark.asyncio
async def test_orders_job_also_syncs_the_extra_store(monkeypatch):
    seen: list[tuple[tuple[str, ...], str | None]] = []

    async def fake_sequence(resources, full, store=None):
        seen.append((resources, store))
        return []

    monkeypatch.setattr(sync, "_run_resource_sequence", fake_sequence)
    monkeypatch.setattr(sync, "extra_store_codes", lambda: ["1490060"])

    await sync.sync_orders_job()

    assert seen == [
        (sync.EXTRA_STORE_ORDER_RESOURCES, "1490060"),
        (sync.ORDER_SYNC_RESOURCES, None),
    ]
