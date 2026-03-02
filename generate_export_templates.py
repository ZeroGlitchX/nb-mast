#!/usr/bin/env python3
"""
Generate header-only CSV templates for importable NetBox resources.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from env_config import load_default_env, normalize_auth_scheme


load_default_env(Path(__file__).resolve().parent)


EndpointSpec = tuple[str, str] | tuple[str, str, str]


SECTION_ENDPOINTS: dict[str, list[EndpointSpec]] = {
    "organization": [
        ("dcim", "regions"),
        ("dcim", "site-groups"),
        ("dcim", "sites"),
        ("dcim", "locations"),
        ("tenancy", "tenants"),
        ("tenancy", "tenant-groups"),
        ("tenancy", "contacts"),
        ("tenancy", "contact-groups"),
        ("tenancy", "contact-roles"),
        ("tenancy", "contact-assignments"),
    ],
    "racks": [
        ("dcim", "racks"),
        ("dcim", "rack-roles"),
        ("dcim", "rack-reservations"),
        ("dcim", "rack-types"),
    ],
    "devices": [
        ("dcim", "devices"),
        ("dcim", "modules"),
        ("dcim", "device-roles"),
        ("dcim", "platforms"),
        ("dcim", "virtual-chassis"),
        ("dcim", "virtual-device-contexts"),
        ("dcim", "device-types"),
        ("dcim", "module-types"),
        ("dcim", "module-type-profiles"),
        ("dcim", "manufacturers"),
        ("dcim", "interfaces"),
        ("dcim", "front-ports"),
        ("dcim", "rear-ports"),
        ("dcim", "console-ports"),
        ("dcim", "console-server-ports"),
        ("dcim", "power-ports"),
        ("dcim", "power-outlets"),
        ("dcim", "device-bays"),
        ("dcim", "module-bays"),
        ("dcim", "inventory-items"),
        ("dcim", "mac-addresses"),
    ],
    "connections": [
        ("dcim", "cables"),
    ],
    "wireless": [
        ("wireless", "wireless-lan-groups"),
        ("wireless", "wireless-lans"),
        ("wireless", "wireless-links"),
    ],
    "ipam": [
        ("ipam", "aggregates"),
        ("ipam", "asn-ranges"),
        ("ipam", "asns"),
        ("ipam", "fhrp-group-assignments"),
        ("ipam", "fhrp-groups"),
        ("ipam", "ip-addresses"),
        ("ipam", "ip-ranges"),
        ("ipam", "prefixes"),
        ("ipam", "rirs"),
        ("ipam", "roles", "prefix_vlan_roles"),
        ("ipam", "route-targets"),
        ("ipam", "service-templates"),
        ("ipam", "services"),
        ("ipam", "vlan-groups"),
        ("ipam", "vlan-translation-policies"),
        ("ipam", "vlan-translation-rules"),
        ("ipam", "vlans"),
        ("ipam", "vrfs"),
    ],
    "vpn": [
        ("vpn", "ike-policies"),
        ("vpn", "ike-proposals"),
        ("vpn", "ipsec-policies"),
        ("vpn", "ipsec-profiles"),
        ("vpn", "ipsec-proposals"),
        ("vpn", "l2vpn-terminations"),
        ("vpn", "l2vpns"),
        ("vpn", "tunnel-groups"),
        ("vpn", "tunnel-terminations"),
        ("vpn", "tunnels"),
    ],
    "virtualization": [
        ("virtualization", "cluster-groups"),
        ("virtualization", "cluster-types"),
        ("virtualization", "clusters"),
        ("virtualization", "interfaces"),
        ("virtualization", "virtual-disks"),
        ("virtualization", "virtual-machines"),
    ],
    "circuits": [
        ("circuits", "circuit-group-assignments"),
        ("circuits", "circuit-groups"),
        ("circuits", "circuit-terminations"),
        ("circuits", "circuit-types"),
        ("circuits", "circuits"),
        ("circuits", "provider-accounts"),
        ("circuits", "provider-networks"),
        ("circuits", "providers"),
        ("circuits", "virtual-circuit-terminations"),
        ("circuits", "virtual-circuit-types"),
        ("circuits", "virtual-circuits"),
    ],
    "power": [
        ("dcim", "power-panels"),
        ("dcim", "power-feeds"),
    ],
    "provisioning": [
        ("extras", "config-contexts"),
        ("extras", "config-context-profiles"),
        ("extras", "config-templates"),
    ],
    "customization": [
        ("extras", "custom-field-choice-sets"),
        ("extras", "custom-fields"),
        ("extras", "custom-links"),
        ("extras", "export-templates"),
        ("extras", "saved-filters"),
        ("extras", "tags"),
    ],
    "operations": [
        ("core", "data-sources"),
        ("extras", "event-rules"),
        ("extras", "notification-groups"),
        ("extras", "journal-entries"),
        ("extras", "webhooks"),
    ],
    "admin": [
        ("users", "groups"),
        ("users", "owner-groups", "owner_groups"),
        ("users", "owners"),
        ("users", "users"),
        ("users", "permissions"),
        ("users", "tokens"),
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate header-only CSV templates for NetBox importable resources."
    )
    parser.add_argument(
        "--url",
        default=os.getenv("NETBOX_URL"),
        help="NetBox URL, defaults to NETBOX_URL",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("NETBOX_API_TOKEN"),
        help="NetBox API token, defaults to NETBOX_API_TOKEN",
    )
    parser.add_argument(
        "--auth-scheme",
        default=os.getenv("NETBOX_AUTH_SCHEME", "Token"),
        help="Authorization scheme (Token or Bearer), defaults to NETBOX_AUTH_SCHEME",
    )
    parser.add_argument(
        "--output-dir",
        default="export_templates",
        help="Directory where template CSV files are written",
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    parser.add_argument(
        "--csv-flat",
        action="store_true",
        help="Exclude clearly nested/multi-value fields from generated CSV headers.",
    )
    return parser.parse_args()


class ApiClient:
    def __init__(
        self, base_url: str, token: str, timeout: int = 30, auth_scheme: str = "Token"
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.auth_scheme = normalize_auth_scheme(auth_scheme)

    def request_json(self, method: str, url: str) -> Any:
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"{self.auth_scheme} {self.token}",
                "Accept": "application/json",
            },
            method=method.upper(),
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}


def sanitize_name(value: str) -> str:
    return value.replace("-", "_")


def sanitize_filename(section: str, endpoint_or_alias: str) -> str:
    return f"{sanitize_name(section)}_{sanitize_name(endpoint_or_alias)}.csv"


def is_flat_template_field(meta: Any) -> bool:
    if not isinstance(meta, dict):
        return True
    if bool(meta.get("many")):
        return False
    field_type = str(meta.get("type") or "").lower().strip()
    if field_type in {"object", "nested object", "json", "array", "list"}:
        return False
    if "child" in meta or "children" in meta:
        return False
    return True


def extract_writable_fields(post_schema: dict[str, Any], flat_only: bool = False) -> list[str]:
    fields: list[str] = []
    for field_name, meta in post_schema.items():
        if isinstance(meta, dict) and meta.get("read_only"):
            continue
        if flat_only and not is_flat_template_field(meta):
            continue
        fields.append(field_name)
    return fields


def write_header_csv(path: Path, headers: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)


def main() -> int:
    args = parse_args()
    if not args.url:
        print("Error: NetBox URL is required via --url or NETBOX_URL", file=sys.stderr)
        return 2
    if not args.token:
        print("Error: API token is required via --token or NETBOX_API_TOKEN", file=sys.stderr)
        return 2

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    client = ApiClient(
        args.url,
        args.token,
        timeout=args.timeout,
        auth_scheme=args.auth_scheme,
    )

    try:
        api_root = client.request_json("GET", f"{args.url.rstrip('/')}/api/")
    except urllib.error.URLError as exc:
        print(f"Error: could not reach API root: {exc.reason}", file=sys.stderr)
        return 1

    app_urls: dict[str, str] = {}
    for app_name, app_url in api_root.items():
        if isinstance(app_name, str) and isinstance(app_url, str):
            app_urls[app_name] = app_url

    created_files: list[str] = []
    skipped_files: list[str] = []

    for section, app_endpoints in SECTION_ENDPOINTS.items():
        for spec in app_endpoints:
            app_name = spec[0]
            endpoint_name = spec[1]
            template_name = spec[2] if len(spec) == 3 else endpoint_name
            app_url = app_urls.get(app_name)
            if not app_url:
                skipped_files.append(f"{section}/{endpoint_name} (missing app: {app_name})")
                continue

            try:
                app_index = client.request_json("GET", app_url)
            except urllib.error.URLError as exc:
                skipped_files.append(f"{section}/{endpoint_name} ({exc.reason})")
                continue

            endpoint_url = app_index.get(endpoint_name)
            if not isinstance(endpoint_url, str):
                skipped_files.append(f"{section}/{endpoint_name} (missing endpoint)")
                continue

            try:
                options = client.request_json("OPTIONS", endpoint_url)
            except urllib.error.URLError as exc:
                skipped_files.append(f"{section}/{endpoint_name} ({exc.reason})")
                continue

            actions = options.get("actions") if isinstance(options, dict) else None
            post_schema = actions.get("POST") if isinstance(actions, dict) else None
            if not isinstance(post_schema, dict) or not post_schema:
                skipped_files.append(f"{section}/{endpoint_name} (no POST schema)")
                continue

            headers = extract_writable_fields(post_schema, flat_only=args.csv_flat)
            if not headers:
                skipped_files.append(f"{section}/{endpoint_name} (no writable fields)")
                continue

            file_name = sanitize_filename(section, template_name)
            file_path = output_dir / file_name
            write_header_csv(file_path, headers)
            created_files.append(file_name)

    created_set = set(created_files)

    # Remove previously created manual/non-importable templates and stale legacy names.
    removed_files: list[str] = []
    for existing in output_dir.glob("*.csv"):
        if existing.name not in created_set:
            existing.unlink()
            removed_files.append(existing.name)

    print(f"Templates generated: {len(created_files)}")
    for name in sorted(created_files):
        print(f"  + {name}")

    if skipped_files:
        print(f"Skipped: {len(skipped_files)}")
        for item in sorted(skipped_files):
            print(f"  - {item}")

    if removed_files:
        print(f"Removed stale templates: {len(removed_files)}")
        for item in sorted(removed_files):
            print(f"  x {item}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
