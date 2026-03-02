#!/usr/bin/env python3
"""
Import ordered NetBox resources from exported JSON files.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from env_config import load_default_env, normalize_auth_scheme
from generate_export_templates import SECTION_ENDPOINTS


load_default_env(Path(__file__).resolve().parent)


def sanitize_name(value: str) -> str:
    return value.replace("-", "_")


def build_catalog_resource_keys() -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for section, specs in SECTION_ENDPOINTS.items():
        section_name = sanitize_name(section)
        for spec in specs:
            endpoint_name = spec[1]
            alias = spec[2] if len(spec) == 3 else endpoint_name
            alias_name = sanitize_name(alias)
            keys.add((section_name, alias_name))
            # Allow section-prefixed internal resource keys to avoid collisions
            # (for example: virtualization/interfaces vs devices/interfaces).
            keys.add((section_name, f"{section_name}_{alias_name}"))
    return keys


SUPPORTED_IMPORT_RESOURCES: list[tuple[str, str]] = [
    ("organization", "regions"),
    ("organization", "site_groups"),
    ("organization", "sites"),
    ("organization", "locations"),
    ("organization", "tenant_groups"),
    ("organization", "tenants"),
    ("organization", "contact_groups"),
    ("organization", "contact_roles"),
    ("organization", "contacts"),
    ("organization", "contact_assignments"),
    ("racks", "rack_roles"),
    ("racks", "rack_types"),
    ("racks", "racks"),
    ("admin", "groups"),
    ("admin", "owner_groups"),
    ("admin", "owners"),
    ("admin", "users"),
    ("admin", "permissions"),
    ("admin", "tokens"),
    ("racks", "rack_reservations"),
    ("devices", "device_roles"),
    ("devices", "manufacturers"),
    ("devices", "module_type_profiles"),
    ("devices", "module_types"),
    ("devices", "platforms"),
    ("devices", "device_types"),
    ("devices", "devices"),
    ("devices", "virtual_chassis"),
    ("devices", "virtual_device_contexts"),
    ("devices", "module_bays"),
    ("devices", "modules"),
    ("devices", "interfaces"),
    ("devices", "front_ports"),
    ("devices", "rear_ports"),
    ("devices", "console_ports"),
    ("devices", "console_server_ports"),
    ("devices", "power_ports"),
    ("devices", "power_outlets"),
    ("devices", "device_bays"),
    ("devices", "inventory_items"),
    ("devices", "mac_addresses"),
    ("connections", "cables"),
    ("wireless", "wireless_lan_groups"),
    ("wireless", "wireless_lans"),
    ("wireless", "wireless_links"),
    ("ipam", "rirs"),
    ("ipam", "prefix_vlan_roles"),
    ("ipam", "route_targets"),
    ("ipam", "vrfs"),
    ("ipam", "vlan_groups"),
    ("ipam", "vlans"),
    ("ipam", "vlan_translation_policies"),
    ("ipam", "vlan_translation_rules"),
    ("ipam", "aggregates"),
    ("ipam", "asns"),
    ("ipam", "asn_ranges"),
    ("ipam", "prefixes"),
    ("ipam", "ip_ranges"),
    ("ipam", "fhrp_groups"),
    ("ipam", "ip_addresses"),
    ("ipam", "fhrp_group_assignments"),
    ("ipam", "service_templates"),
    ("ipam", "services"),
    ("vpn", "ike_policies"),
    ("vpn", "ike_proposals"),
    ("vpn", "ipsec_policies"),
    ("vpn", "ipsec_profiles"),
    ("vpn", "ipsec_proposals"),
    ("vpn", "l2vpns"),
    ("vpn", "l2vpn_terminations"),
    ("vpn", "tunnel_groups"),
    ("vpn", "tunnels"),
    ("vpn", "tunnel_terminations"),
    ("virtualization", "cluster_groups"),
    ("virtualization", "cluster_types"),
    ("virtualization", "clusters"),
    ("virtualization", "virtual_machines"),
    ("virtualization", "virtual_disks"),
    ("virtualization", "virtualization_interfaces"),
    ("circuits", "providers"),
    ("circuits", "provider_accounts"),
    ("circuits", "provider_networks"),
    ("circuits", "circuit_groups"),
    ("circuits", "circuit_types"),
    ("circuits", "circuits"),
    ("circuits", "circuit_terminations"),
    ("circuits", "circuit_group_assignments"),
    ("circuits", "virtual_circuit_types"),
    ("circuits", "virtual_circuits"),
    ("circuits", "virtual_circuit_terminations"),
    ("power", "power_panels"),
    ("power", "power_feeds"),
    ("provisioning", "config_context_profiles"),
    ("provisioning", "config_contexts"),
    ("provisioning", "config_templates"),
    ("customization", "custom_field_choice_sets"),
    ("customization", "custom_fields"),
    ("customization", "tags"),
    ("customization", "custom_links"),
    ("customization", "export_templates"),
    ("customization", "saved_filters"),
    ("operations", "data_sources"),
    ("operations", "event_rules"),
    ("operations", "notification_groups"),
    ("operations", "journal_entries"),
    ("operations", "webhooks"),
]

CATALOG_RESOURCE_KEYS = build_catalog_resource_keys()

MISSING_CATALOG_RESOURCES = [
    f"{section}/{name}"
    for section, name in SUPPORTED_IMPORT_RESOURCES
    if (section, name) not in CATALOG_RESOURCE_KEYS
]

if MISSING_CATALOG_RESOURCES:
    missing_text = ", ".join(MISSING_CATALOG_RESOURCES)
    raise RuntimeError(
        "Importer resource map is out of sync with shared SECTION_ENDPOINTS. "
        f"Missing: {missing_text}"
    )

IMPORT_STATS_ORDER: list[str] = [name for _, name in SUPPORTED_IMPORT_RESOURCES]
SECTION_RESOURCE_ORDER: dict[str, list[str]] = {}
RESOURCE_TO_SECTION: dict[str, str] = {}
for _section, _name in SUPPORTED_IMPORT_RESOURCES:
    SECTION_RESOURCE_ORDER.setdefault(_section, []).append(_name)
    RESOURCE_TO_SECTION[_name] = _section

SECTION_INDEX_DEPENDENCIES: dict[str, set[str]] = {
    "organization": {
        "regions",
        "site_groups",
        "sites",
        "locations",
        "tenant_groups",
        "tenants",
        "contact_groups",
        "contact_roles",
        "contacts",
        "contact_assignments",
    },
    "racks": {"sites", "locations", "tenants", "users", "rack_roles", "rack_types", "racks", "rack_reservations"},
    "admin": {"groups", "owner_groups", "owners", "users", "permissions", "tokens"},
    "devices": {
        "sites",
        "locations",
        "tenants",
        "rack_roles",
        "racks",
        "device_roles",
        "manufacturers",
        "module_type_profiles",
        "module_types",
        "platforms",
        "device_types",
        "devices",
        "virtual_chassis",
        "virtual_device_contexts",
        "module_bays",
        "modules",
        "interfaces",
        "front_ports",
        "rear_ports",
        "console_ports",
        "console_server_ports",
        "power_ports",
        "power_outlets",
        "device_bays",
        "inventory_items",
        "mac_addresses",
    },
    "connections": {
        "tenants",
        "devices",
        "interfaces",
        "front_ports",
        "rear_ports",
        "console_ports",
        "console_server_ports",
        "power_ports",
        "power_outlets",
        "cables",
    },
    "wireless": {"interfaces", "wireless_lan_groups", "wireless_lans", "wireless_links"},
    "ipam": {
        "regions",
        "site_groups",
        "sites",
        "locations",
        "tenants",
        "rirs",
        "prefix_vlan_roles",
        "route_targets",
        "vrfs",
        "vlan_groups",
        "vlans",
        "vlan_translation_policies",
        "vlan_translation_rules",
        "aggregates",
        "asns",
        "asn_ranges",
        "prefixes",
        "ip_ranges",
        "fhrp_groups",
        "ip_addresses",
        "fhrp_group_assignments",
        "service_templates",
        "services",
    },
    "vpn": {
        "interfaces",
        "virtualization_interfaces",
        "ike_policies",
        "ike_proposals",
        "ipsec_policies",
        "ipsec_profiles",
        "ipsec_proposals",
        "l2vpns",
        "l2vpn_terminations",
        "tunnel_groups",
        "tunnels",
        "tunnel_terminations",
    },
    "virtualization": {
        "regions",
        "site_groups",
        "sites",
        "locations",
        "tenants",
        "device_roles",
        "platforms",
        "devices",
        "cluster_groups",
        "cluster_types",
        "clusters",
        "virtual_machines",
        "virtual_disks",
        "virtualization_interfaces",
    },
    "circuits": {
        "regions",
        "site_groups",
        "sites",
        "locations",
        "tenants",
        "asns",
        "providers",
        "provider_accounts",
        "provider_networks",
        "circuit_groups",
        "circuit_types",
        "circuits",
        "circuit_terminations",
        "circuit_group_assignments",
        "virtual_circuit_types",
        "virtual_circuits",
        "virtual_circuit_terminations",
    },
    "power": {"sites", "locations", "tenants", "racks", "power_panels", "power_feeds"},
    "provisioning": {
        "regions",
        "site_groups",
        "sites",
        "locations",
        "tenant_groups",
        "tenants",
        "device_roles",
        "platforms",
        "device_types",
        "cluster_types",
        "cluster_groups",
        "clusters",
        "tags",
        "data_sources",
        "config_context_profiles",
        "config_contexts",
        "config_templates",
    },
    "customization": {
        "custom_field_choice_sets",
        "custom_fields",
        "tags",
        "custom_links",
        "export_templates",
        "saved_filters",
    },
    "operations": {"data_sources", "event_rules", "notification_groups", "journal_entries", "webhooks"},
}


def build_required_index_resources(selected_resources: set[str] | None) -> set[str] | None:
    if selected_resources is None:
        return None
    required = set(selected_resources)
    selected_sections = {
        RESOURCE_TO_SECTION[name] for name in selected_resources if name in RESOURCE_TO_SECTION
    }
    for section in selected_sections:
        required.update(SECTION_INDEX_DEPENDENCIES.get(section, set()))
    return required


def build_index_endpoint_allowlist(required_resources: set[str] | None) -> set[str] | None:
    if required_resources is None:
        return None
    allowlist: set[str] = set()
    for section, name in SUPPORTED_IMPORT_RESOURCES:
        if name not in required_resources:
            continue
        endpoint = RESOURCE_ENDPOINT_LOOKUP.get((section, name))
        if endpoint:
            allowlist.add(endpoint)
    return allowlist


def build_app_endpoint_resource_map() -> dict[tuple[str, str], str]:
    mapping: dict[tuple[str, str], str] = {}
    for _section, specs in SECTION_ENDPOINTS.items():
        for spec in specs:
            app = spec[0]
            endpoint = spec[1]
            alias = spec[2] if len(spec) == 3 else endpoint
            mapping[(app, endpoint)] = sanitize_name(alias)
    return mapping


APP_ENDPOINT_RESOURCE_MAP = build_app_endpoint_resource_map()


def build_resource_endpoint_lookup() -> dict[tuple[str, str], str]:
    lookup: dict[tuple[str, str], str] = {}
    for section, specs in SECTION_ENDPOINTS.items():
        section_name = sanitize_name(section)
        for spec in specs:
            app = spec[0]
            endpoint = spec[1]
            alias = spec[2] if len(spec) == 3 else endpoint
            alias_name = sanitize_name(alias)
            endpoint_path = f"/api/{app}/{endpoint}/"
            lookup[(section_name, alias_name)] = endpoint_path
            lookup[(section_name, f"{section_name}_{alias_name}")] = endpoint_path
    return lookup


def build_source_resource_lookup() -> dict[tuple[str, str], str]:
    lookup: dict[tuple[str, str], str] = {}
    for section, specs in SECTION_ENDPOINTS.items():
        section_name = sanitize_name(section)
        for spec in specs:
            endpoint = spec[1]
            alias = spec[2] if len(spec) == 3 else endpoint
            alias_name = sanitize_name(alias)
            lookup[(section_name, alias_name)] = alias_name
            lookup[(section_name, f"{section_name}_{alias_name}")] = alias_name
    return lookup


RESOURCE_ENDPOINT_LOOKUP = build_resource_endpoint_lookup()
SOURCE_RESOURCE_LOOKUP = build_source_resource_lookup()

OBJECT_TYPE_RESOURCE_KEY_MAP: dict[str, str] = {
    "circuits.circuitgroup": "circuit_groups",
    "circuits.provideraccount": "provider_accounts",
    "circuits.circuittermination": "circuit_terminations",
    "circuits.circuit": "circuits",
    "circuits.providernetwork": "provider_networks",
    "circuits.virtualcircuit": "virtual_circuits",
    "circuits.virtualcircuittype": "virtual_circuit_types",
    "circuits.virtualcircuittermination": "virtual_circuit_terminations",
    "dcim.device": "devices",
    "dcim.devicerole": "device_roles",
    "dcim.devicetype": "device_types",
    "dcim.module": "modules",
    "dcim.modulebay": "module_bays",
    "dcim.moduletype": "module_types",
    "dcim.rack": "racks",
    "dcim.site": "sites",
    "dcim.location": "locations",
    "dcim.region": "regions",
    "dcim.sitegroup": "site_groups",
    "dcim.interface": "interfaces",
    "dcim.frontport": "front_ports",
    "dcim.rearport": "rear_ports",
    "dcim.consoleport": "console_ports",
    "dcim.consoleserverport": "console_server_ports",
    "dcim.powerport": "power_ports",
    "dcim.poweroutlet": "power_outlets",
    "dcim.virtualchassis": "virtual_chassis",
    "dcim.virtualdevicecontext": "virtual_device_contexts",
    "extras.tag": "tags",
    "ipam.fhrpgroup": "fhrp_groups",
    "ipam.ipaddress": "ip_addresses",
    "ipam.vlantranslationpolicy": "vlan_translation_policies",
    "tenancy.contact": "contacts",
    "tenancy.contactgroup": "contact_groups",
    "tenancy.contactrole": "contact_roles",
    "tenancy.tenant": "tenants",
    "tenancy.tenantgroup": "tenant_groups",
    "users.group": "groups",
    "users.ownergroup": "owner_groups",
    "users.owner": "owners",
    "users.permission": "permissions",
    "users.token": "tokens",
    "users.user": "users",
    "virtualization.cluster": "clusters",
    "virtualization.virtualmachine": "virtual_machines",
    "virtualization.vminterface": "virtualization_interfaces",
}

class NetBoxApiError(Exception):
    def __init__(self, status: int, reason: str, body: str) -> None:
        super().__init__(f"HTTP {status} {reason}: {body}")
        self.status = status
        self.reason = reason
        self.body = body


class NetBoxClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        timeout: int = 30,
        auth_scheme: str = "Token",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.auth_scheme = normalize_auth_scheme(auth_scheme)
        self.endpoint_allowlist: set[str] | None = None

    def request_json(
        self,
        method: str,
        endpoint_or_url: str,
        data: dict[str, Any] | None = None,
        absolute_url: bool = False,
    ) -> Any:
        if absolute_url:
            url = endpoint_or_url
        else:
            url = urllib.parse.urljoin(f"{self.base_url}/", endpoint_or_url.lstrip("/"))

        payload = None
        headers = {"Authorization": f"{self.auth_scheme} {self.token}", "Accept": "application/json"}
        if data is not None:
            payload = json.dumps(data).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, data=payload, headers=headers, method=method.upper())
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise NetBoxApiError(exc.code, exc.reason, body) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Connection error for {url}: {exc.reason}") from exc

    def get_paginated(
        self, endpoint: str, query: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        endpoint_path = endpoint
        if "//" in endpoint:
            endpoint_path = urllib.parse.urlparse(endpoint).path
        endpoint_path = endpoint_path.split("?", 1)[0]
        if not endpoint_path.startswith("/"):
            endpoint_path = "/" + endpoint_path.lstrip("/")
        if self.endpoint_allowlist is not None and endpoint_path not in self.endpoint_allowlist:
            return []

        base_url = urllib.parse.urljoin(f"{self.base_url}/", endpoint.lstrip("/"))
        if query:
            base_url = f"{base_url}?{urllib.parse.urlencode(query, doseq=True)}"

        results: list[dict[str, Any]] = []
        next_url: str | None = base_url
        while next_url:
            payload = self.request_json("GET", next_url, absolute_url=True)
            if isinstance(payload, dict) and "results" in payload:
                page_items = payload.get("results") or []
                if not isinstance(page_items, list):
                    raise ValueError(f"Unexpected 'results' format for {next_url}")
                results.extend(page_items)
                next_url = payload.get("next")
            elif isinstance(payload, list):
                results.extend(payload)
                next_url = None
            else:
                raise ValueError(f"Unexpected list response for {next_url}")
        return results

    def create(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = self.request_json("POST", endpoint, data=payload)
        if not isinstance(response, dict):
            raise ValueError(f"Unexpected create response for {endpoint}")
        return response

    def update(self, endpoint: str, object_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        obj_endpoint = f"{endpoint.rstrip('/')}/{object_id}/"
        response = self.request_json("PATCH", obj_endpoint, data=payload)
        if not isinstance(response, dict):
            raise ValueError(f"Unexpected update response for {obj_endpoint}")
        return response


@dataclass
class ResourceStats:
    name: str
    created: int = 0
    updated: int = 0
    existing: int = 0
    failed: int = 0
    deferred: int = 0


@dataclass
class ImportContext:
    client: NetBoxClient
    dry_run: bool
    update_existing: bool
    fail_fast: bool
    default_user_password: str | None = None
    maps: dict[str, dict[int, int]] = field(
        default_factory=lambda: {
            **{name: {} for _, name in SUPPORTED_IMPORT_RESOURCES},
            "ipam_roles": {},
        }
    )
    fake_id_seq: int = -1
    errors: list[str] = field(default_factory=list)
    stats: dict[str, ResourceStats] = field(default_factory=dict)
    rollback_created: list[dict[str, Any]] = field(default_factory=list)

    def add_stat(self, resource: str, action: str) -> None:
        if resource not in self.stats:
            self.stats[resource] = ResourceStats(name=resource)
        stat = self.stats[resource]
        if action == "created":
            stat.created += 1
        elif action == "updated":
            stat.updated += 1
        elif action == "existing":
            stat.existing += 1
        elif action == "failed":
            stat.failed += 1
        elif action == "deferred":
            stat.deferred += 1

    def next_fake_id(self) -> int:
        current = self.fake_id_seq
        self.fake_id_seq -= 1
        return current


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import NetBox resources from JSON exports (with CSV-to-JSON fallback)."
    )
    parser.add_argument(
        "--url",
        default=os.getenv("NETBOX_URL"),
        help="Target NetBox URL, defaults to NETBOX_URL",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("NETBOX_API_TOKEN"),
        help="Target API token, defaults to NETBOX_API_TOKEN",
    )
    parser.add_argument(
        "--auth-scheme",
        default=os.getenv("NETBOX_AUTH_SCHEME", "Token"),
        help="Authorization scheme (Token or Bearer), defaults to NETBOX_AUTH_SCHEME",
    )
    parser.add_argument(
        "--input-dir",
        default="exports",
        help="Export directory containing section folders from netbox_export.py (default: exports)",
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan import actions without creating/updating records",
    )
    parser.add_argument(
        "--update-existing",
        action="store_true",
        help="Patch existing matched records with imported values",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately on first API error",
    )
    parser.add_argument(
        "--only",
        help=(
            "Comma-separated sections/resources to import. Examples: "
            "'ipam,organization' or 'ipam/prefixes,devices'"
        ),
    )
    parser.add_argument(
        "--rollback-manifest",
        help=(
            "Optional path for rollback manifest JSON. Defaults to "
            "<input-dir>/rollback_manifest_<timestamp>.json during apply."
        ),
    )
    parser.add_argument(
        "--default-user-password",
        default=os.getenv("NETBOX_DEFAULT_USER_PASSWORD"),
        help=(
            "Default password used when importing admin/users records that do not include a password. "
            "Defaults to NETBOX_DEFAULT_USER_PASSWORD."
        ),
    )
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="Run readiness checks only (no import execution).",
    )
    parser.add_argument(
        "--preflight-strict",
        action="store_true",
        help="With --preflight, treat missing expected section/resource files as blockers.",
    )
    return parser.parse_args()


def parse_only_resources(raw_only: str | None) -> set[str] | None:
    if not raw_only:
        return None

    selected: set[str] = set()
    unknown_tokens: list[str] = []
    for token in raw_only.split(","):
        cleaned = sanitize_name(token.strip().lower())
        if not cleaned:
            continue

        section_part: str | None = None
        resource_part: str | None = None
        if "/" in cleaned:
            section_part, resource_part = cleaned.split("/", 1)
        elif "." in cleaned:
            section_part, resource_part = cleaned.split(".", 1)

        if section_part is not None and resource_part is not None:
            section_resources = SECTION_RESOURCE_ORDER.get(section_part)
            if section_resources and resource_part in section_resources:
                selected.add(resource_part)
                continue
            unknown_tokens.append(token.strip())
            continue

        section_resources = SECTION_RESOURCE_ORDER.get(cleaned)
        if section_resources:
            selected.update(section_resources)
            continue

        if cleaned in RESOURCE_TO_SECTION:
            selected.add(cleaned)
            continue

        unknown_tokens.append(token.strip())

    if unknown_tokens:
        valid_sections = ", ".join(sorted(SECTION_RESOURCE_ORDER.keys()))
        raise ValueError(
            "Unknown --only selectors: "
            f"{', '.join(unknown_tokens)}. "
            "Use section names or section/resource names. "
            f"Valid sections: {valid_sections}"
        )

    if not selected:
        raise ValueError("--only resolved to an empty selection")

    return selected


def load_json_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        raise ValueError(f"Expected JSON list in {path}")
    return payload


def parse_csv_cell(raw_value: str) -> Any:
    value = raw_value.strip()
    if value == "":
        return None

    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none"}:
        return None

    if re.fullmatch(r"[-+]?\d+", value):
        try:
            return int(value)
        except ValueError:
            pass

    if re.fullmatch(r"[-+]?\d*\.\d+", value):
        try:
            return float(value)
        except ValueError:
            pass

    if (value.startswith("{") and value.endswith("}")) or (
        value.startswith("[") and value.endswith("]")
    ):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    return value


def load_csv_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            return []

        rows: list[dict[str, Any]] = []
        for row in reader:
            parsed: dict[str, Any] = {}
            for key, raw in row.items():
                if key is None:
                    continue
                parsed_value = parse_csv_cell(raw or "")
                if parsed_value is not None:
                    parsed[key] = parsed_value
            rows.append(parsed)
        return rows


def write_json_list(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, indent=2, ensure_ascii=True, sort_keys=True)


def materialize_missing_json_from_csv(
    input_root: Path, include_resources: set[str] | None = None
) -> tuple[int, int]:
    created_files = 0
    converted_records = 0

    for section, name in SUPPORTED_IMPORT_RESOURCES:
        if include_resources is not None and name not in include_resources:
            continue

        file_resource = source_resource_name(section, name)
        json_path = input_root / section / f"{file_resource}.json"
        if json_path.exists():
            continue

        csv_path = input_root / section / f"{file_resource}.csv"
        if not csv_path.exists():
            continue

        rows = load_csv_list(csv_path)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        write_json_list(json_path, rows)
        created_files += 1
        converted_records += len(rows)

    return created_files, converted_records


def load_resource_records(input_root: Path, section: str, file_resource: str) -> list[dict[str, Any]]:
    json_path = input_root / section / f"{file_resource}.json"
    return load_json_list(json_path)


def load_import_records(
    input_root: Path, include_resources: set[str] | None = None
) -> dict[str, list[dict[str, Any]]]:
    records: dict[str, list[dict[str, Any]]] = {}
    for section, name in SUPPORTED_IMPORT_RESOURCES:
        if include_resources is not None and name not in include_resources:
            records[name] = []
            continue
        file_resource = source_resource_name(section, name)
        records[name] = load_resource_records(input_root, section, file_resource)
    return records


def selected_resource_pairs(
    include_resources: set[str] | None = None,
) -> list[tuple[str, str]]:
    return [
        (section, name)
        for section, name in SUPPORTED_IMPORT_RESOURCES
        if include_resources is None or name in include_resources
    ]


def run_preflight(
    *,
    url: str,
    token: str,
    auth_scheme: str,
    timeout: int,
    input_root: Path,
    include_resources: set[str] | None,
    strict_missing_files: bool,
) -> int:
    print("Running preflight checks...")
    blockers: list[str] = []
    warnings: list[str] = []
    passes: list[str] = []

    selected_pairs = selected_resource_pairs(include_resources)
    selected_sections = sorted({section for section, _ in selected_pairs})
    passes.append(
        "Scope resolved: "
        f"{len(selected_pairs)} resources across {len(selected_sections)} sections"
    )

    if input_root.exists() and input_root.is_dir():
        passes.append(f"Input directory exists: {input_root}")
    elif input_root.exists() and not input_root.is_dir():
        blockers.append(f"Input path is not a directory: {input_root}")
    else:
        blockers.append(f"Input directory does not exist: {input_root}")

    missing_sections: set[str] = set()
    if input_root.exists() and input_root.is_dir():
        for section in selected_sections:
            section_dir = input_root / section
            if section_dir.exists() and section_dir.is_dir():
                continue
            message = f"Missing section directory: {section}/"
            if strict_missing_files:
                blockers.append(message)
            else:
                warnings.append(message)
            missing_sections.add(section)

        checked_files = 0
        checked_json_files = 0
        checked_csv_files = 0
        total_records = 0
        for section, resource in selected_pairs:
            if section in missing_sections:
                continue
            file_resource = source_resource_name(section, resource)
            json_path = input_root / section / f"{file_resource}.json"
            csv_path = input_root / section / f"{file_resource}.csv"

            chosen_path: Path | None = None
            chosen_format: str | None = None
            if json_path.exists():
                chosen_path = json_path
                chosen_format = "json"
            elif csv_path.exists():
                chosen_path = csv_path
                chosen_format = "csv"

            if chosen_path is None:
                message = (
                    f"Missing resource file: {section}/{file_resource}.json "
                    f"(or {section}/{file_resource}.csv)"
                )
                if strict_missing_files:
                    blockers.append(message)
                else:
                    warnings.append(message)
                continue

            checked_files += 1
            if chosen_format == "json":
                checked_json_files += 1
            else:
                checked_csv_files += 1

            try:
                if chosen_format == "json":
                    rows = load_json_list(chosen_path)
                else:
                    rows = load_csv_list(chosen_path)
            except (OSError, ValueError, json.JSONDecodeError, csv.Error) as exc:
                blockers.append(f"Invalid {chosen_format.upper()} list in {chosen_path}: {exc}")
                continue

            total_records += len(rows)

        passes.append(
            "Input data checks: "
            f"files={checked_files} json={checked_json_files} csv={checked_csv_files} "
            f"records={total_records}"
        )

        manifest_path = input_root / "export_manifest.json"
        if manifest_path.exists():
            try:
                with manifest_path.open("r", encoding="utf-8") as handle:
                    manifest_payload = json.load(handle)
                if isinstance(manifest_payload, dict):
                    passes.append("Manifest detected: export_manifest.json")
                else:
                    warnings.append("export_manifest.json is not a JSON object")
            except (OSError, json.JSONDecodeError) as exc:
                warnings.append(f"Unable to parse export_manifest.json: {exc}")
        else:
            warnings.append("Missing export_manifest.json")

    client = NetBoxClient(url, token, timeout=timeout, auth_scheme=auth_scheme)
    try:
        status_payload = client.request_json("GET", "/api/status/")
        if isinstance(status_payload, dict):
            passes.append("Target reachable: GET /api/status/")
        else:
            blockers.append("Unexpected response format from /api/status/")
    except (RuntimeError, NetBoxApiError, ValueError) as exc:
        blockers.append(f"Target reachability failed (/api/status/): {exc}")

    try:
        auth_payload = client.request_json("OPTIONS", "/api/dcim/regions/")
        if not isinstance(auth_payload, dict):
            blockers.append("Unexpected response format from OPTIONS /api/dcim/regions/")
        else:
            actions = auth_payload.get("actions")
            if isinstance(actions, dict) and "POST" in actions:
                passes.append(
                    "Token auth/write probe passed: OPTIONS /api/dcim/regions/ includes POST action"
                )
            else:
                blockers.append(
                    "Token auth/write probe failed: POST action not available on "
                    "OPTIONS /api/dcim/regions/ (check token validity/permissions)"
                )
    except (RuntimeError, NetBoxApiError, ValueError) as exc:
        blockers.append(f"Token auth/write probe failed: {exc}")

    print("")
    print("Preflight results:")
    for message in passes:
        print(f"- PASS: {message}")
    for message in warnings:
        print(f"- WARN: {message}")
    for message in blockers:
        print(f"- BLOCKER: {message}")

    if blockers:
        print("")
        print(f"Preflight FAILED with {len(blockers)} blocker(s).")
        return 1

    print("")
    print("Preflight PASSED.")
    return 0


def source_resource_name(section: str, resource: str) -> str:
    return SOURCE_RESOURCE_LOOKUP.get((section, resource), resource)


def load_template_fields(section: str, resource: str) -> list[str]:
    template_resource = source_resource_name(section, resource)
    template_path = Path("export_templates") / f"{section}_{template_resource}.csv"
    if not template_path.exists():
        return []
    with template_path.open("r", encoding="utf-8") as handle:
        header_line = handle.readline().strip()
    if not header_line:
        return []
    return [field.strip() for field in header_line.split(",") if field.strip()]


def get_related_id(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, dict):
        raw = value.get("id")
        return raw if isinstance(raw, int) else None
    return None


def get_choice_value(value: Any) -> Any:
    if isinstance(value, dict) and "value" in value:
        return value.get("value")
    return value


def slugify(text: str) -> str:
    lowered = text.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", lowered)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug or "item"


def non_empty(value: Any) -> bool:
    return value is not None and value != ""


def copy_fields(src: dict[str, Any], dst: dict[str, Any], fields: list[str]) -> None:
    for field in fields:
        if field in src and non_empty(src[field]):
            dst[field] = src[field]


