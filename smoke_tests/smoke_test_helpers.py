from __future__ import annotations


class FakePreflightClient:
    def __init__(self, base_url: str, token: str, timeout: int = 30, auth_scheme: str = "Token") -> None:
        self.base_url = base_url
        self.token = token
        self.timeout = timeout
        self.auth_scheme = auth_scheme

    def request_json(self, method: str, endpoint_or_url: str, data=None, absolute_url: bool = False):
        if method == "GET" and endpoint_or_url == "/api/status/":
            return {"status": "ok"}
        if method == "OPTIONS" and endpoint_or_url == "/api/dcim/regions/":
            return {"actions": {"POST": {"name": {"type": "string"}}}}
        raise AssertionError(f"Unexpected preflight probe: method={method} endpoint={endpoint_or_url}")


class FakeImportClient:
    def __init__(self, base_url: str, token: str, timeout: int = 30, auth_scheme: str = "Token") -> None:
        self.base_url = base_url
        self.token = token
        self.timeout = timeout
        self.auth_scheme = auth_scheme
        self.endpoint_allowlist = None

    def get_paginated(self, endpoint: str, query=None):
        return []
