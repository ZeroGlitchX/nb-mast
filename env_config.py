#!/usr/bin/env python3
"""
Shared environment configuration helpers.
"""

from __future__ import annotations

import os
from pathlib import Path


def _strip_wrapping_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def load_env_file(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = _strip_wrapping_quotes(value.strip())
        os.environ.setdefault(key, value)
    return True


def load_default_env(base_dir: Path) -> list[Path]:
    loaded: list[Path] = []
    candidates = [Path.cwd() / ".env", base_dir / ".env"]
    seen: set[Path] = set()

    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if load_env_file(resolved):
            loaded.append(resolved)
    return loaded


def normalize_auth_scheme(value: str | None) -> str:
    if value is None:
        return "Token"
    cleaned = value.strip()
    if not cleaned:
        return "Token"
    lowered = cleaned.lower()
    if lowered == "token":
        return "Token"
    if lowered == "bearer":
        return "Bearer"
    return cleaned
