#!/usr/bin/env python3
"""
Export ordered NetBox resources into grouped JSON/CSV files.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from env_config import load_default_env, normalize_auth_scheme
from generate_export_templates import SECTION_ENDPOINTS


load_default_env(Path(__file__).resolve().parent)


@dataclass(frozen=True)
class ResourceSpec:
    group: str
    name: str
    endpoint: str
    description: str = ""


def sanitize_name(value: str) -> str:
    return value.replace("-", "_")


def build_resource_order() -> list[ResourceSpec]:
    resources: list[ResourceSpec] = []
    for section, specs in SECTION_ENDPOINTS.items():
        for spec in specs:
            app_name = spec[0]
            endpoint_name = spec[1]
            resource_name = spec[2] if len(spec) == 3 else endpoint_name
            endpoint_path = f"/api/{app_name}/{endpoint_name}/"
            resources.append(
                ResourceSpec(
                    group=section,
                    name=sanitize_name(resource_name),
                    endpoint=endpoint_path,
                    description=f"{app_name}/{endpoint_name}",
                )
            )
    return resources


RESOURCE_ORDER: list[ResourceSpec] = build_resource_order()


class NetBoxClient:
    def __init__(
        self, base_url: str, token: str, timeout: int = 30, auth_scheme: str = "Token", verify_ssl: bool = True
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.auth_scheme = normalize_auth_scheme(auth_scheme)
        self.ssl_context = None if verify_ssl else ssl.create_default_context()
        if self.ssl_context:
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE

    def _request_json(self, url: str) -> Any:
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"{self.auth_scheme} {self.token}",
                "Accept": "application/json",
            },
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=self.timeout, context=self.ssl_context) as response:
            return json.loads(response.read().decode("utf-8"))

    def fetch_all(self, endpoint: str) -> list[dict[str, Any]]:
        next_url = urllib.parse.urljoin(f"{self.base_url}/", endpoint.lstrip("/"))
        results: list[dict[str, Any]] = []

        while next_url:
            payload = self._request_json(next_url)
            if isinstance(payload, dict) and "results" in payload:
                page_items = payload.get("results") or []
                if not isinstance(page_items, list):
                    raise ValueError(f"Unexpected 'results' type for URL: {next_url}")
                results.extend(page_items)
                next_url = payload.get("next")
            elif isinstance(payload, list):
                results.extend(payload)
                next_url = None
            else:
                raise ValueError(f"Unexpected payload format from URL: {next_url}")

        return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export NetBox resources to JSON and CSV.")
    parser.add_argument(
        "--url",
        default=os.getenv("NETBOX_URL"),
        help="NetBox base URL, defaults to NETBOX_URL env var.",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("NETBOX_API_TOKEN"),
        help="NetBox API token. Defaults to NETBOX_API_TOKEN env var.",
    )
    parser.add_argument(
        "--auth-scheme",
        default=os.getenv("NETBOX_AUTH_SCHEME", "Token"),
        help="Authorization scheme (Token or Bearer). Defaults to NETBOX_AUTH_SCHEME or Token.",
    )
    parser.add_argument(
        "--output-dir",
        default="exports",
        help="Output directory root (default: exports).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--csv-flat",
        action="store_true",
        help="Write CSV files with nested/list fields excluded.",
    )
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="Disable SSL certificate verification (use for self-signed certs).",
    )
    return parser.parse_args()


def is_nested_csv_value(value: Any) -> bool:
    return isinstance(value, (dict, list))


def safe_csv_value(value: Any, flat_only: bool = False) -> str:
    if value is None:
        return ""
    if is_nested_csv_value(value):
        if flat_only:
            return ""
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    return str(value)


def collect_csv_columns(records: list[dict[str, Any]], flat_only: bool = False) -> list[str]:
    columns: set[str] = set()
    nested_columns: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        columns.update(record.keys())
        if flat_only:
            for key, value in record.items():
                if value is not None and is_nested_csv_value(value):
                    nested_columns.add(key)
    if flat_only:
        columns -= nested_columns
    return sorted(columns)


def write_json(path: Path, data: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=True, sort_keys=True)
        handle.write("\n")


def write_csv(path: Path, data: list[dict[str, Any]], flat_only: bool = False) -> None:
    columns = collect_csv_columns(data, flat_only=flat_only)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        if columns:
            writer.writeheader()
            for row in data:
                writer.writerow({k: safe_csv_value(row.get(k), flat_only=flat_only) for k in columns})


def ensure_group_dirs(root: Path, groups: set[str]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    for group in groups:
        (root / group).mkdir(parents=True, exist_ok=True)


def main() -> int:
    args = parse_args()
    if not args.url:
        print("Error: NetBox URL is required via --url or NETBOX_URL.", file=sys.stderr)
        return 2
    if not args.token:
        print("Error: API token is required via --token or NETBOX_API_TOKEN.", file=sys.stderr)
        return 2

    client = NetBoxClient(
        args.url,
        args.token,
        timeout=args.timeout,
        auth_scheme=args.auth_scheme,
        verify_ssl=not args.no_verify_ssl,
    )
    output_root = Path(args.output_dir).resolve()
    ensure_group_dirs(output_root, {spec.group for spec in RESOURCE_ORDER})

    manifest: dict[str, Any] = {
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
        "netbox_url": args.url,
        "csv_mode": "flat" if args.csv_flat else "full",
        "resource_order": [],
    }

    for spec in RESOURCE_ORDER:
        print(f"[export] {spec.group}/{spec.name} -> {spec.endpoint}")
        try:
            records = client.fetch_all(spec.endpoint)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                print(f"  [skip] HTTP 404 — endpoint not found in this NetBox version, writing empty export.")
                records = []
            print(
                f"Error: HTTP {exc.code} while fetching {spec.endpoint}: {exc.reason}",
                file=sys.stderr,
            )
            return 1
        except urllib.error.URLError as exc:
            print(f"Error: Connection error for {spec.endpoint}: {exc.reason}", file=sys.stderr)
            return 1

        group_dir = output_root / spec.group
        json_path = group_dir / f"{spec.name}.json"
        csv_path = group_dir / f"{spec.name}.csv"

        write_json(json_path, records)
        write_csv(csv_path, records, flat_only=args.csv_flat)

        manifest["resource_order"].append(
            {
                "group": spec.group,
                "name": spec.name,
                "endpoint": spec.endpoint,
                "record_count": len(records),
                "json_file": str(json_path.relative_to(output_root)),
                "csv_file": str(csv_path.relative_to(output_root)),
            }
        )
        print(f"  wrote {len(records)} records")

    manifest_path = output_root / "export_manifest.json"
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, ensure_ascii=True, sort_keys=True)
        handle.write("\n")

    print(f"[done] Export completed at: {output_root}")
    print(f"[done] Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())