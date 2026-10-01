from types import SimpleNamespace

import httpx
import pytest
from fastapi import HTTPException

from app import adaptor as module


@pytest.mark.asyncio
async def test_reuses_client_and_caches_health_but_invalidates_on_failure(monkeypatch):
    monkeypatch.setattr(module, "settings", lambda: SimpleNamespace(
        tray_adaptor_url="https://adaptor.test", tray_adaptor_token="test-token"))
    monkeypatch.setattr(module, "_not_before", 0)
    module.clear_cancel()
    paths = []
    def respond(request):
        paths.append(request.url.path)
        return httpx.Response(503 if request.url.path == "/fail" else 200, json={})
    instance = module.Adaptor()
    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    instance._client = client
    try:
        await instance._get("/one", retries=1)
        await instance._get("/two", retries=1)
        assert instance._http_client() is client
        assert paths == ["/health", "/one", "/two"]
        with pytest.raises(HTTPException):
            await instance._get("/fail", retries=1)
        await instance._get("/three", retries=1)
        assert paths[-2:] == ["/health", "/three"]
    finally:
        await instance.aclose()
    assert client.is_closed
