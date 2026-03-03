#!/usr/bin/env python3
"""
Resource keying, resolution, and import handlers for NetBox import.
"""

from __future__ import annotations

from import_core import *


def key_region(obj: dict[str, Any]) -> str | None:
    slug = obj.get("slug")
    if isinstance(slug, str) and slug:
        return slug
    name = obj.get("name")
    return name if isinstance(name, str) and name else None


key_site_group = key_region


key_site = key_region


def key_location(obj: dict[str, Any]) -> tuple[int | None, str] | None:
    slug_or_name = obj.get("slug") or obj.get("name")
    if not isinstance(slug_or_name, str) or not slug_or_name:
        return None
    site_id = get_related_id(obj.get("site"))
    return (site_id, slug_or_name)


key_device_role = key_region


key_manufacturer = key_region


key_tenant_group = key_region


key_tenant = key_region


key_platform = key_region


key_rack_role = key_region


def key_device_type(obj: dict[str, Any]) -> tuple[int | None, str] | None:
    slug_or_model = obj.get("slug") or obj.get("model")
    if not isinstance(slug_or_model, str) or not slug_or_model:
        return None
    manufacturer_id = get_related_id(obj.get("manufacturer"))
    return (manufacturer_id, slug_or_model)


def key_device(obj: dict[str, Any]) -> Any:
    site_id = get_related_id(obj.get("site"))
    name = obj.get("name")
    if isinstance(name, str) and name:
        return ("name", site_id, name)

    serial = obj.get("serial")
    if isinstance(serial, str) and serial:
        return ("serial", serial)

    asset_tag = obj.get("asset_tag")
    if isinstance(asset_tag, str) and asset_tag:
        return ("asset_tag", asset_tag)

    rack_id = get_related_id(obj.get("rack"))
    position = obj.get("position")
    device_type_id = get_related_id(obj.get("device_type"))
    face = get_choice_value(obj.get("face"))
    if (
        site_id is not None
        and rack_id is not None
        and position is not None
        and device_type_id is not None
    ):
        return (
            "rackpos",
            site_id,
            rack_id,
            str(position),
            str(face or ""),
            device_type_id,
        )

    display = obj.get("display")
    if isinstance(display, str) and display:
        return ("display", site_id, display)
    return None


def key_rack(obj: dict[str, Any]) -> tuple[int | None, str] | None:
    site_id = get_related_id(obj.get("site"))
    rack_name = obj.get("name") or obj.get("slug")
    if not isinstance(rack_name, str) or not rack_name:
        return None
    return (site_id, rack_name)


key_rir = key_region


key_ipam_role = key_region


def key_route_target(obj: dict[str, Any]) -> str | None:
    name = obj.get("name")
    return name if isinstance(name, str) and name else None


def key_vrf(obj: dict[str, Any]) -> tuple[str, str] | None:
    rd = obj.get("rd")
    if isinstance(rd, str) and rd:
        return ("rd", rd)
    name = obj.get("name")
    if isinstance(name, str) and name:
        return ("name", name)
    return None


key_vlan_group = key_region


def key_vlan(obj: dict[str, Any]) -> Any:
    vid = obj.get("vid")
    if not isinstance(vid, int):
        return None
    group_id = get_related_id(obj.get("group"))
    site_id = get_related_id(obj.get("site"))
    if group_id is not None:
        return ("group", group_id, vid)
    if site_id is not None:
        return ("site", site_id, vid)
    name = obj.get("name")
    if isinstance(name, str) and name:
        return ("global", vid, name)
    return ("global", vid)


def key_aggregate(obj: dict[str, Any]) -> str | None:
    prefix = obj.get("prefix")
    return prefix if isinstance(prefix, str) and prefix else None


def key_asn(obj: dict[str, Any]) -> int | None:
    asn = obj.get("asn")
    return asn if isinstance(asn, int) else None


def key_asn_range(obj: dict[str, Any]) -> Any:
    slug = obj.get("slug")
    if isinstance(slug, str) and slug:
        return ("slug", slug)
    name = obj.get("name")
    if isinstance(name, str) and name:
        return ("name", name)
    start = obj.get("start")
    end = obj.get("end")
    if isinstance(start, int) and isinstance(end, int):
        return ("range", start, end)
    return None


def key_prefix(obj: dict[str, Any]) -> tuple[int | None, str] | None:
    prefix = obj.get("prefix")
    if not isinstance(prefix, str) or not prefix:
        return None
    vrf_id = get_related_id(obj.get("vrf"))
    return (vrf_id, prefix)


def key_ip_range(obj: dict[str, Any]) -> tuple[int | None, str, str] | None:
    start = obj.get("start_address")
    end = obj.get("end_address")
    if not isinstance(start, str) or not start or not isinstance(end, str) or not end:
        return None
    vrf_id = get_related_id(obj.get("vrf"))
    return (vrf_id, start, end)


key_cluster_group = key_region


key_cluster_type = key_region


def key_cluster(obj: dict[str, Any]) -> str | None:
    name = obj.get("name")
    return name if isinstance(name, str) and name else None


def key_virtual_machine(obj: dict[str, Any]) -> Any:
    name = obj.get("name")
    if not isinstance(name, str) or not name:
        return None
    cluster_id = get_related_id(obj.get("cluster"))
    if cluster_id is not None:
        return ("cluster", cluster_id, name)
    return ("name", name)


key_provider = key_region


def key_provider_network(obj: dict[str, Any]) -> Any:
    name = obj.get("name")
    if not isinstance(name, str) or not name:
        return None
    provider_id = get_related_id(obj.get("provider"))
    return (provider_id, name)


key_circuit_type = key_region


def key_circuit(obj: dict[str, Any]) -> str | None:
    cid = obj.get("cid")
    return cid if isinstance(cid, str) and cid else None


def key_circuit_termination(obj: dict[str, Any]) -> Any:
    side = obj.get("term_side")
    circuit_id = get_related_id(obj.get("circuit"))
    if isinstance(side, str) and circuit_id is not None:
        return (circuit_id, side)
    return None


def key_power_panel(obj: dict[str, Any]) -> Any:
    name = obj.get("name")
    if not isinstance(name, str) or not name:
        return None
    site_id = get_related_id(obj.get("site"))
    return (site_id, name)


def key_power_feed(obj: dict[str, Any]) -> Any:
    name = obj.get("name")
    if not isinstance(name, str) or not name:
        return None
    panel_id = get_related_id(obj.get("power_panel"))
    return (panel_id, name)


def key_custom_field_choice_set(obj: dict[str, Any]) -> str | None:
    name = obj.get("name")
    return name if isinstance(name, str) and name else None


def key_custom_field(obj: dict[str, Any]) -> str | None:
    name = obj.get("name")
    return name if isinstance(name, str) and name else None


key_tag = key_region


def key_user(obj: dict[str, Any]) -> str | None:
    username = obj.get("username")
    if isinstance(username, str) and username:
        return username.lower()
    return None


def key_group(obj: dict[str, Any]) -> str | None:
    name = obj.get("name")
    if isinstance(name, str) and name:
        return name
    return None


def key_owner(obj: dict[str, Any]) -> Any:
    group_identity = ref_identity(obj.get("group"))
    name = obj.get("name")
    if group_identity is not None and isinstance(name, str) and name:
        return (group_identity, name)
    return None


def key_permission(obj: dict[str, Any]) -> str | None:
    name = obj.get("name")
    if isinstance(name, str) and name:
        return name
    return None


def key_token(obj: dict[str, Any]) -> Any:
    user_identity = ref_identity(obj.get("user"))
    if user_identity is None:
        return None
    description = obj.get("description") if isinstance(obj.get("description"), str) else ""
    enabled = bool(obj.get("enabled", True))
    write_enabled = bool(obj.get("write_enabled", False))
    expires = obj.get("expires")
    allowed_ips = obj.get("allowed_ips")
    allowed_ips_key = tuple(sorted(allowed_ips)) if isinstance(allowed_ips, list) else None
    return (user_identity, description, enabled, write_enabled, expires, allowed_ips_key)


def ref_identity(ref: Any) -> Any:
    if ref is None:
        return None
    if isinstance(ref, int):
        return ("id", ref)
    if isinstance(ref, str):
        return ("str", ref)
    if isinstance(ref, dict):
        for field in ("slug", "name", "model", "cid", "address", "display", "label"):
            value = ref.get(field)
            if isinstance(value, str) and value:
                return (field, value)
        ref_id = get_related_id(ref)
        if isinstance(ref_id, int):
            return ("id", ref_id)
    return None


def key_contact(obj: dict[str, Any]) -> Any:
    name = obj.get("name")
    email = obj.get("email")
    if isinstance(name, str) and name and isinstance(email, str) and email:
        return ("name_email", name, email.lower())
    if isinstance(name, str) and name:
        return ("name", name)
    if isinstance(email, str) and email:
        return ("email", email.lower())
    return None


def key_contact_assignment(obj: dict[str, Any]) -> Any:
    object_type = obj.get("object_type")
    object_ref = obj.get("object")
    if object_ref is None:
        object_ref = obj.get("object_id")
    object_identity = ref_identity(object_ref)
    contact_identity = ref_identity(obj.get("contact"))
    role_identity = ref_identity(obj.get("role"))
    if (
        isinstance(object_type, str)
        and object_identity is not None
        and contact_identity is not None
        and role_identity is not None
    ):
        return (object_type, object_identity, contact_identity, role_identity)
    return None


def key_rack_type(obj: dict[str, Any]) -> Any:
    slug = obj.get("slug")
    if isinstance(slug, str) and slug:
        return ("slug", slug)
    manufacturer = ref_identity(obj.get("manufacturer"))
    model = obj.get("model")
    if manufacturer is not None and isinstance(model, str) and model:
        return ("manufacturer_model", manufacturer, model)
    return None


def key_rack_reservation(obj: dict[str, Any]) -> Any:
    rack_identity = ref_identity(obj.get("rack"))
    units = obj.get("units")
    units_tuple: tuple[int, ...] = tuple()
    if isinstance(units, list):
        unit_values = [value for value in units if isinstance(value, int)]
        units_tuple = tuple(sorted(unit_values))
    tenant_identity = ref_identity(obj.get("tenant"))
    if rack_identity is None or not units_tuple:
        return None
    # User IDs are instance-local and often differ between source/target.
    # Use rack/units/tenant as the stable identity tuple.
    return (rack_identity, units_tuple, tenant_identity)


def key_module_type_profile(obj: dict[str, Any]) -> str | None:
    name = obj.get("name")
    return name if isinstance(name, str) and name else None


def key_module_type(obj: dict[str, Any]) -> Any:
    manufacturer = ref_identity(obj.get("manufacturer"))
    model = obj.get("model")
    if manufacturer is not None and isinstance(model, str) and model:
        return (manufacturer, model)
    return None


def key_virtual_chassis(obj: dict[str, Any]) -> Any:
    name = obj.get("name")
    domain = obj.get("domain")
    if isinstance(name, str) and name:
        if isinstance(domain, str) and domain:
            return ("name_domain", name, domain)
        return ("name", name)
    return None


def key_virtual_device_context(obj: dict[str, Any]) -> Any:
    device_identity = ref_identity(obj.get("device"))
    identifier = obj.get("identifier")
    name = obj.get("name")
    if device_identity is not None and isinstance(identifier, int):
        return ("device_identifier", device_identity, identifier)
    if device_identity is not None and isinstance(name, str) and name:
        return ("device_name", device_identity, name)
    if isinstance(name, str) and name:
        return ("name", name)
    return None


def key_module_bay(obj: dict[str, Any]) -> Any:
    device_identity = ref_identity(obj.get("device"))
    module_identity = ref_identity(obj.get("module"))
    name = obj.get("name")
    if isinstance(name, str) and name and device_identity is not None:
        return ("device_name", device_identity, module_identity, name)
    return None


def key_module(obj: dict[str, Any]) -> Any:
    module_bay_identity = ref_identity(obj.get("module_bay"))
    if module_bay_identity is not None:
        return ("module_bay", module_bay_identity)
    device_identity = ref_identity(obj.get("device"))
    module_type_identity = ref_identity(obj.get("module_type"))
    serial = obj.get("serial")
    asset_tag = obj.get("asset_tag")
    if device_identity is not None and module_type_identity is not None:
        return ("device_type", device_identity, module_type_identity, serial, asset_tag)
    return None


def key_device_component(obj: dict[str, Any]) -> Any:
    device_identity = ref_identity(obj.get("device"))
    module_identity = ref_identity(obj.get("module"))
    name = obj.get("name")
    if isinstance(name, str) and name and device_identity is not None:
        return (device_identity, module_identity, name)
    return None


def key_device_bay(obj: dict[str, Any]) -> Any:
    return key_device_component(obj)


def key_inventory_item(obj: dict[str, Any]) -> Any:
    device_identity = ref_identity(obj.get("device"))
    parent_identity = ref_identity(obj.get("parent"))
    name = obj.get("name")
    serial = obj.get("serial")
    asset_tag = obj.get("asset_tag")
    if device_identity is None or not isinstance(name, str) or not name:
        return None
    return (device_identity, parent_identity, name, serial, asset_tag)


def key_mac_address(obj: dict[str, Any]) -> Any:
    mac_address = obj.get("mac_address")
    if isinstance(mac_address, str) and mac_address:
        return mac_address.lower()
    return None


def key_fhrp_group(obj: dict[str, Any]) -> Any:
    protocol = get_choice_value(obj.get("protocol")) or obj.get("protocol")
    group_id = obj.get("group_id")
    if isinstance(protocol, str) and isinstance(group_id, int):
        return (protocol, group_id)
    name = obj.get("name")
    if isinstance(name, str) and name:
        return ("name", name)
    return None


def key_fhrp_group_assignment(obj: dict[str, Any]) -> Any:
    group_identity = ref_identity(obj.get("group"))
    interface_type = obj.get("interface_type")
    interface_ref = obj.get("interface")
    if interface_ref is None:
        interface_ref = obj.get("interface_id")
    interface_identity = ref_identity(interface_ref)
    if group_identity is not None and isinstance(interface_type, str) and interface_identity is not None:
        return (group_identity, interface_type, interface_identity)
    return None


def key_ip_address(obj: dict[str, Any]) -> Any:
    address = obj.get("address")
    vrf_identity = ref_identity(obj.get("vrf"))
    if isinstance(address, str) and address:
        return (vrf_identity, address)
    return None


def key_service_template(obj: dict[str, Any]) -> Any:
    name = obj.get("name")
    protocol = get_choice_value(obj.get("protocol")) or obj.get("protocol")
    ports = obj.get("ports")
    ports_text = json.dumps(ports, sort_keys=True) if isinstance(ports, (list, dict)) else str(ports)
    if isinstance(name, str) and name and isinstance(protocol, str):
        return (name, protocol, ports_text)
    if isinstance(name, str) and name:
        return ("name", name)
    return None


def key_service(obj: dict[str, Any]) -> Any:
    parent_type = obj.get("parent_object_type")
    parent_ref = obj.get("parent_object")
    if parent_ref is None:
        parent_ref = obj.get("parent_object_id")
    parent_identity = ref_identity(parent_ref)
    name = obj.get("name")
    protocol = get_choice_value(obj.get("protocol")) or obj.get("protocol")
    ports = obj.get("ports")
    ports_text = json.dumps(ports, sort_keys=True) if isinstance(ports, (list, dict)) else str(ports)
    if (
        isinstance(parent_type, str)
        and parent_identity is not None
        and isinstance(name, str)
        and name
    ):
        return (parent_type, parent_identity, name, protocol, ports_text)
    return None


def key_vlan_translation_policy(obj: dict[str, Any]) -> Any:
    name = obj.get("name")
    return name if isinstance(name, str) and name else None


def key_vlan_translation_rule(obj: dict[str, Any]) -> Any:
    policy_identity = ref_identity(obj.get("policy"))
    local_vid = obj.get("local_vid")
    remote_vid = obj.get("remote_vid")
    if policy_identity is not None and isinstance(local_vid, int) and isinstance(remote_vid, int):
        return (policy_identity, local_vid, remote_vid)
    return None


def key_virtual_disk(obj: dict[str, Any]) -> Any:
    vm_identity = ref_identity(obj.get("virtual_machine"))
    name = obj.get("name")
    if vm_identity is not None and isinstance(name, str) and name:
        return (vm_identity, name)
    return None


def key_virtualization_interface(obj: dict[str, Any]) -> Any:
    vm_identity = ref_identity(obj.get("virtual_machine"))
    name = obj.get("name")
    if vm_identity is not None and isinstance(name, str) and name:
        return (vm_identity, name)
    return None


def key_circuit_group(obj: dict[str, Any]) -> Any:
    return key_name_slug_or_name(obj)


def key_circuit_group_assignment(obj: dict[str, Any]) -> Any:
    group_identity = ref_identity(obj.get("group"))
    member_type = obj.get("member_type")
    member_id = obj.get("member_id")
    if group_identity is not None and isinstance(member_type, str):
        return (group_identity, member_type, member_id)
    return None


def key_provider_account(obj: dict[str, Any]) -> Any:
    provider_identity = ref_identity(obj.get("provider"))
    account = obj.get("account")
    name = obj.get("name")
    if provider_identity is not None and isinstance(account, str) and account:
        return (provider_identity, account)
    if provider_identity is not None and isinstance(name, str) and name:
        return (provider_identity, name)
    return None


def key_virtual_circuit_type(obj: dict[str, Any]) -> Any:
    return key_name_slug_or_name(obj)


def key_virtual_circuit(obj: dict[str, Any]) -> Any:
    cid = obj.get("cid")
    return cid if isinstance(cid, str) and cid else None


def key_virtual_circuit_termination(obj: dict[str, Any]) -> Any:
    virtual_circuit_identity = ref_identity(obj.get("virtual_circuit"))
    role = obj.get("role")
    interface_identity = ref_identity(obj.get("interface"))
    if virtual_circuit_identity is not None:
        return (virtual_circuit_identity, role, interface_identity)
    return None


def key_custom_link(obj: dict[str, Any]) -> Any:
    name = obj.get("name")
    group_name = obj.get("group_name")
    if isinstance(name, str) and name:
        return (name, group_name)
    return None


def key_export_template(obj: dict[str, Any]) -> Any:
    name = obj.get("name")
    if isinstance(name, str) and name:
        return name
    return None


def key_saved_filter(obj: dict[str, Any]) -> Any:
    slug = obj.get("slug")
    if isinstance(slug, str) and slug:
        return ("slug", slug)
    name = obj.get("name")
    user_identity = ref_identity(obj.get("user"))
    if isinstance(name, str) and name:
        return ("name", user_identity, name)
    return None


def key_journal_entry(obj: dict[str, Any]) -> Any:
    assigned_type = obj.get("assigned_object_type")
    assigned_id = obj.get("assigned_object_id")
    kind = get_choice_value(obj.get("kind")) or obj.get("kind")
    comments = obj.get("comments")
    if isinstance(kind, (dict, list)):
        kind = json.dumps(kind, ensure_ascii=True, sort_keys=True)
    if isinstance(comments, (dict, list)):
        comments = json.dumps(comments, ensure_ascii=True, sort_keys=True)
    created_by = ref_identity(obj.get("created_by"))
    if isinstance(assigned_type, str) and isinstance(assigned_id, int):
        return (assigned_type, assigned_id, kind, comments, created_by)
    return None


def key_cable(obj: dict[str, Any]) -> Any:
    label = obj.get("label")
    if isinstance(label, str) and label:
        return ("label", label)

    def extract_pairs(term_list: Any) -> tuple[tuple[str, int], ...]:
        pairs: list[tuple[str, int]] = []
        if not isinstance(term_list, list):
            return tuple()
        for item in term_list:
            if not isinstance(item, dict):
                continue
            obj_type = item.get("object_type")
            obj_id = item.get("object_id")
            if isinstance(obj_type, str) and isinstance(obj_id, int):
                pairs.append((obj_type, obj_id))
        return tuple(sorted(pairs))

    a = extract_pairs(obj.get("a_terminations"))
    b = extract_pairs(obj.get("b_terminations"))
    if a or b:
        # Cables are orientation-agnostic; canonicalize side ordering for key stability.
        side_a, side_b = sorted((a, b))
        return ("term", side_a, side_b, str(obj.get("type") or ""))
    return None


def key_name_slug_or_name(obj: dict[str, Any]) -> Any:
    slug = obj.get("slug")
    if isinstance(slug, str) and slug:
        return ("slug", slug)
    name = obj.get("name")
    if isinstance(name, str) and name:
        return ("name", name)
    cid = obj.get("cid")
    if isinstance(cid, str) and cid:
        return ("cid", cid)
    return None


def build_index(
    objects: list[dict[str, Any]], key_fn: Any
) -> dict[Any, dict[str, Any]]:
    index: dict[Any, dict[str, Any]] = {}
    for obj in objects:
        key = key_fn(obj)
        if key is not None and key not in index:
            index[key] = obj
    return index


def resolve_by_ref(
    ref: Any,
    resource_name: str,
    mapping: dict[int, int],
    index: dict[Any, dict[str, Any]],
    key_fn: Any,
) -> int | None:
    if ref is None:
        return None

    if isinstance(ref, int):
        if ref in mapping:
            return mapping[ref]
        return None

    if isinstance(ref, dict):
        source_id = get_related_id(ref)
        if source_id is not None and source_id in mapping:
            return mapping[source_id]
        key = key_fn(ref)
        if key is not None and key in index:
            value = index[key]
            target_id = value.get("id")
            return target_id if isinstance(target_id, int) else None
    return None


def resolve_rack_by_ref(
    ref: Any,
    site_target_id: int,
    rack_mapping: dict[int, int],
    rack_index: dict[Any, dict[str, Any]],
) -> int | None:
    source_id = get_related_id(ref)
    if source_id is not None and source_id in rack_mapping:
        return rack_mapping[source_id]

    if not isinstance(ref, dict):
        return None
    rack_name = ref.get("name") or ref.get("slug")
    if not isinstance(rack_name, str) or not rack_name:
        return None
    rack_obj = rack_index.get((site_target_id, rack_name)) or rack_index.get((None, rack_name))
    if not rack_obj:
        return None
    target_id = rack_obj.get("id")
    return target_id if isinstance(target_id, int) else None


def resolve_scope_by_ref(
    ref: Any,
    scope_type: str | None,
    ctx: ImportContext,
    site_index: dict[Any, dict[str, Any]],
    location_index: dict[Any, dict[str, Any]],
    region_index: dict[Any, dict[str, Any]],
    site_group_index: dict[Any, dict[str, Any]],
) -> tuple[str, int] | None:
    if ref is None:
        return None

    scope_type_value = scope_type
    if not scope_type_value and isinstance(ref, dict):
        ref_url = ref.get("url")
        if isinstance(ref_url, str):
            if "/api/dcim/sites/" in ref_url:
                scope_type_value = "dcim.site"
            elif "/api/dcim/locations/" in ref_url:
                scope_type_value = "dcim.location"
            elif "/api/dcim/regions/" in ref_url:
                scope_type_value = "dcim.region"
            elif "/api/dcim/site-groups/" in ref_url:
                scope_type_value = "dcim.sitegroup"

    if scope_type_value == "dcim.site":
        resolved = resolve_by_ref(ref, "sites", ctx.maps["sites"], site_index, key_site)
        return ("dcim.site", resolved) if resolved is not None else None
    if scope_type_value == "dcim.location":
        resolved = resolve_by_ref(
            ref, "locations", ctx.maps["locations"], location_index, key_location
        )
        return ("dcim.location", resolved) if resolved is not None else None
    if scope_type_value == "dcim.region":
        resolved = resolve_by_ref(ref, "regions", ctx.maps["regions"], region_index, key_region)
        return ("dcim.region", resolved) if resolved is not None else None
    if scope_type_value == "dcim.sitegroup":
        resolved = resolve_by_ref(
            ref, "site_groups", ctx.maps["site_groups"], site_group_index, key_site_group
        )
        return ("dcim.sitegroup", resolved) if resolved is not None else None
    return None


def resolve_list_of_route_targets(
    refs: Any,
    ctx: ImportContext,
    route_target_index: dict[Any, dict[str, Any]],
) -> list[int]:
    if not isinstance(refs, list):
        return []
    target_ids: list[int] = []
    for item in refs:
        target_id = resolve_by_ref(
            item,
            "route_targets",
            ctx.maps["route_targets"],
            route_target_index,
            key_route_target,
        )
        if target_id is not None:
            target_ids.append(target_id)
    return target_ids


def parse_api_ref_url(url: str) -> tuple[str, str, int] | None:
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return None
    path = parsed.path.strip("/")
    parts = path.split("/")
    # expected: api/<app>/<endpoint>/<id>/
    if len(parts) < 4 or parts[0] != "api":
        return None
    app = parts[1]
    endpoint = parts[2]
    try:
        obj_id = int(parts[3])
    except ValueError:
        return None
    return (app, endpoint, obj_id)


def resolve_resource_map_key_from_endpoint(app: str, endpoint: str, ctx: ImportContext) -> str | None:
    alias_key = APP_ENDPOINT_RESOURCE_MAP.get((app, endpoint))
    if alias_key is None:
        return None
    section_name: str | None = None
    for section, specs in SECTION_ENDPOINTS.items():
        for spec in specs:
            if spec[0] == app and spec[1] == endpoint:
                section_name = sanitize_name(section)
                break
        if section_name is not None:
            break
    if section_name is not None:
        prefixed_key = f"{section_name}_{alias_key}"
        if prefixed_key in ctx.maps:
            return prefixed_key
    return alias_key


def resolve_resource_map_key_from_object_type(object_type: str, ctx: ImportContext) -> str | None:
    lowered = object_type.strip().lower()
    key = OBJECT_TYPE_RESOURCE_KEY_MAP.get(lowered)
    if key and key in ctx.maps:
        return key

    if "." not in lowered:
        return key

    app_label, model_label = lowered.split(".", 1)
    endpoint_guess = model_label.replace("_", "-") + "s"
    alias_key = APP_ENDPOINT_RESOURCE_MAP.get((app_label, endpoint_guess))
    if alias_key is not None:
        section_name: str | None = None
        for section, specs in SECTION_ENDPOINTS.items():
            for spec in specs:
                if spec[0] == app_label and spec[1] == endpoint_guess:
                    section_name = sanitize_name(section)
                    break
            if section_name is not None:
                break
        if section_name is not None:
            prefixed_key = f"{section_name}_{alias_key}"
            if prefixed_key in ctx.maps:
                return prefixed_key
        if alias_key in ctx.maps:
            return alias_key
    return key


def resolve_id_from_ref_or_raw(
    value: Any,
    ctx: ImportContext,
) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, dict):
        if "value" in value and not ("id" in value or "url" in value):
            return None
        src_id = get_related_id(value)
        if src_id is None:
            return None
        ref_url = value.get("url")
        if isinstance(ref_url, str):
            parsed = parse_api_ref_url(ref_url)
            if parsed is not None:
                map_key = resolve_resource_map_key_from_endpoint(parsed[0], parsed[1], ctx)
                if map_key and map_key in ctx.maps:
                    if src_id in ctx.maps[map_key]:
                        return ctx.maps[map_key][src_id]
                    # Fallback to raw ID when this run has no source->target map yet.
                    return src_id
        return src_id
    return None


def sanitize_generic_value(value: Any, ctx: ImportContext) -> Any:
    if isinstance(value, dict):
        if "value" in value and not ("id" in value or "url" in value):
            return value.get("value")

        if "rear_port" in value and isinstance(value.get("rear_port"), int):
            mapped_rear = ctx.maps.get("rear_ports", {}).get(value["rear_port"])
            if mapped_rear is None:
                return None
            cloned = dict(value)
            cloned["rear_port"] = mapped_rear
            return cloned

        if "front_port" in value and isinstance(value.get("front_port"), int):
            mapped_front = ctx.maps.get("front_ports", {}).get(value["front_port"])
            if mapped_front is None:
                return None
            cloned = dict(value)
            cloned["front_port"] = mapped_front
            return cloned

        resolved = resolve_id_from_ref_or_raw(value, ctx)
        if resolved is not None:
            return resolved

        # Treat API object dictionaries as unresolved references and drop them
        # rather than sending nested objects into create/update payloads.
        if isinstance(value.get("id"), int) or isinstance(value.get("url"), str):
            return None

        return value

    if isinstance(value, list):
        output: list[Any] = []
        for item in value:
            if isinstance(item, dict):
                if "object_type" in item and "object_id" in item:
                    obj_type = item.get("object_type")
                    obj_id = item.get("object_id")
                    if isinstance(obj_type, str) and isinstance(obj_id, int):
                        endpoint_key = resolve_resource_map_key_from_object_type(obj_type, ctx)
                        mapped = (
                            ctx.maps.get(endpoint_key, {}).get(obj_id)
                            if endpoint_key is not None
                            else None
                        )
                        if endpoint_key is not None:
                            if mapped is None:
                                continue
                            output.append({"object_type": obj_type, "object_id": mapped})
                            continue
                        output.append({"object_type": obj_type, "object_id": obj_id})
                        continue

                if "rear_port" in item and isinstance(item.get("rear_port"), int):
                    mapped_rear = ctx.maps.get("rear_ports", {}).get(item["rear_port"])
                    if mapped_rear is None:
                        continue
                    cloned = dict(item)
                    cloned["rear_port"] = mapped_rear
                    output.append(cloned)
                    continue

                if "front_port" in item and isinstance(item.get("front_port"), int):
                    mapped_front = ctx.maps.get("front_ports", {}).get(item["front_port"])
                    if mapped_front is None:
                        continue
                    cloned = dict(item)
                    cloned["front_port"] = mapped_front
                    output.append(cloned)
                    continue

                resolved = resolve_id_from_ref_or_raw(item, ctx)
                if resolved is not None:
                    output.append(resolved)
                    continue

                if isinstance(item.get("id"), int) or isinstance(item.get("url"), str):
                    continue

                output.append(item)
            else:
                output.append(item)
        return output
    return value


def remap_typed_object_reference(
    payload: dict[str, Any],
    type_field: str,
    id_field: str,
    ctx: ImportContext,
) -> None:
    obj_type = payload.get(type_field)
    obj_id = payload.get(id_field)
    if not isinstance(obj_type, str) or not isinstance(obj_id, int):
        return
    resource_key = resolve_resource_map_key_from_object_type(obj_type, ctx)
    if resource_key is None:
        return
    mapped = ctx.maps.get(resource_key, {}).get(obj_id)
    if mapped is not None:
        payload[id_field] = mapped
    elif resource_key in ctx.maps:
        payload.pop(id_field, None)


def remap_known_typed_object_fields(payload: dict[str, Any], ctx: ImportContext) -> None:
    remap_typed_object_reference(payload, "object_type", "object_id", ctx)
    remap_typed_object_reference(payload, "assigned_object_type", "assigned_object_id", ctx)
    remap_typed_object_reference(payload, "parent_object_type", "parent_object_id", ctx)
    remap_typed_object_reference(payload, "interface_type", "interface_id", ctx)
    remap_typed_object_reference(payload, "member_type", "member_id", ctx)
    remap_typed_object_reference(payload, "action_object_type", "action_object_id", ctx)


def serialize_rollback_key(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [serialize_rollback_key(item) for item in value]
    if isinstance(value, dict):
        return {str(key): serialize_rollback_key(val) for key, val in value.items()}
    return str(value)


def resolve_rollback_manifest_path(input_root: Path, override_path: str | None) -> Path:
    if override_path:
        return Path(override_path).expanduser().resolve()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return (input_root / f"rollback_manifest_{stamp}.json").resolve()


def write_rollback_manifest(
    output_path: Path,
    ctx: ImportContext,
    url: str,
    input_root: Path,
    mode: str,
    selected_resources: set[str] | None,
) -> None:
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "netbox_url": url,
        "input_dir": str(input_root),
        "mode": mode,
        "update_existing": ctx.update_existing,
        "selected_resources": sorted(selected_resources) if selected_resources is not None else None,
        "created_count": len(ctx.rollback_created),
        "rollback_order": "reverse(created_objects)",
        "created_objects": ctx.rollback_created,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True, sort_keys=True)
        handle.write("\n")


def record_error(ctx: ImportContext, message: str) -> None:
    ctx.errors.append(message)
    print(f"  [error] {message}", file=sys.stderr)
    if ctx.fail_fast:
        raise RuntimeError(message)


def infer_existing_id_from_payload(index: dict[Any, dict[str, Any]], payload: dict[str, Any]) -> int | None:
    name = payload.get("name")
    if not isinstance(name, str) or not name:
        return None

    # Component-like uniqueness (device + name)
    device_id = payload.get("device")
    if isinstance(device_id, int):
        for obj in index.values():
            obj_id = obj.get("id")
            if not isinstance(obj_id, int):
                continue
            if obj.get("name") != name:
                continue
            if get_related_id(obj.get("device")) == device_id:
                return obj_id

    # Virtual interface-like uniqueness (virtual_machine + name)
    vm_id = payload.get("virtual_machine")
    if isinstance(vm_id, int):
        for obj in index.values():
            obj_id = obj.get("id")
            if not isinstance(obj_id, int):
                continue
            if obj.get("name") != name:
                continue
            if get_related_id(obj.get("virtual_machine")) == vm_id:
                return obj_id

    # Rack/power feed-like uniqueness (site/panel + name)
    site_id = payload.get("site")
    if isinstance(site_id, int):
        for obj in index.values():
            obj_id = obj.get("id")
            if not isinstance(obj_id, int):
                continue
            if obj.get("name") != name:
                continue
            if get_related_id(obj.get("site")) == site_id:
                return obj_id

    panel_id = payload.get("power_panel")
    if isinstance(panel_id, int):
        for obj in index.values():
            obj_id = obj.get("id")
            if not isinstance(obj_id, int):
                continue
            if obj.get("name") != name:
                continue
            if get_related_id(obj.get("power_panel")) == panel_id:
                return obj_id

    return None


def upsert(
    ctx: ImportContext,
    resource: str,
    endpoint: str,
    index: dict[Any, dict[str, Any]],
    key: Any,
    payload: dict[str, Any],
) -> tuple[int | None, str]:
    existing = index.get(key)
    if existing:
        target_id = existing.get("id")
        if not isinstance(target_id, int):
            return None, "failed"

        if ctx.update_existing:
            if ctx.dry_run:
                return target_id, "updated"
            try:
                updated = ctx.client.update(endpoint, target_id, payload)
            except NetBoxApiError as exc:
                record_error(
                    ctx,
                    f"{resource}: failed to update id={target_id} key={key}: {exc}",
                )
                return None, "failed"
            index[key] = updated
            return target_id, "updated"
        return target_id, "existing"

    if ctx.dry_run:
        fake_id = ctx.next_fake_id()
        index[key] = {"id": fake_id, **payload}
        return fake_id, "created"

    try:
        created = ctx.client.create(endpoint, payload)
    except NetBoxApiError as exc:
        if resource == "cables" and exc.status == 400:
            match = re.search(r"cable\s+(\d+)", exc.body)
            if match:
                return int(match.group(1)), "existing"
        if exc.status == 400 and "already exists" in exc.body.lower():
            inferred_id = infer_existing_id_from_payload(index, payload)
            if inferred_id is not None:
                return inferred_id, "existing"
        record_error(ctx, f"{resource}: failed to create key={key}: {exc}")
        return None, "failed"

    index[key] = created
    target_id = created.get("id")
    if not isinstance(target_id, int):
        return None, "failed"
    if not ctx.dry_run:
        ctx.rollback_created.append(
            {
                "resource": resource,
                "endpoint": endpoint,
                "id": target_id,
                "key": serialize_rollback_key(key),
            }
        )
    return target_id, "created"


def import_regions(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/regions/"
    pending = list(records)
    while pending:
        progressed = 0
        next_round: list[dict[str, Any]] = []
        for rec in pending:
            source_id = get_related_id(rec)
            key = key_region(rec)
            if key is None:
                ctx.add_stat("regions", "failed")
                record_error(ctx, "regions: missing name/slug")
                continue

            parent_target_id = resolve_by_ref(
                rec.get("parent"),
                "regions",
                ctx.maps["regions"],
                index,
                key_region,
            )
            if rec.get("parent") and parent_target_id is None:
                ctx.add_stat("regions", "deferred")
                next_round.append(rec)
                continue

            payload = {
                "name": rec.get("name"),
                "slug": rec.get("slug") or slugify(str(rec.get("name") or key)),
            }
            copy_fields(rec, payload, ["description", "comments"])
            if parent_target_id is not None:
                payload["parent"] = parent_target_id

            target_id, action = upsert(ctx, "regions", endpoint, index, key, payload)
            ctx.add_stat("regions", action)
            if source_id is not None and target_id is not None:
                ctx.maps["regions"][source_id] = target_id
            if action != "failed":
                progressed += 1

        if not next_round:
            break
        if progressed == 0:
            for rec in next_round:
                ctx.add_stat("regions", "failed")
                record_error(
                    ctx,
                    f"regions: unresolved parent for source id={rec.get('id')}",
                )
            break
        pending = next_round


def import_site_groups(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/site-groups/"
    pending = list(records)
    while pending:
        progressed = 0
        next_round: list[dict[str, Any]] = []
        for rec in pending:
            source_id = get_related_id(rec)
            key = key_site_group(rec)
            if key is None:
                ctx.add_stat("site_groups", "failed")
                record_error(ctx, "site_groups: missing name/slug")
                continue

            parent_target_id = resolve_by_ref(
                rec.get("parent"),
                "site_groups",
                ctx.maps["site_groups"],
                index,
                key_site_group,
            )
            if rec.get("parent") and parent_target_id is None:
                ctx.add_stat("site_groups", "deferred")
                next_round.append(rec)
                continue

            payload = {
                "name": rec.get("name"),
                "slug": rec.get("slug") or slugify(str(rec.get("name") or key)),
            }
            copy_fields(rec, payload, ["description"])
            if parent_target_id is not None:
                payload["parent"] = parent_target_id

            target_id, action = upsert(ctx, "site_groups", endpoint, index, key, payload)
            ctx.add_stat("site_groups", action)
            if source_id is not None and target_id is not None:
                ctx.maps["site_groups"][source_id] = target_id
            if action != "failed":
                progressed += 1

        if not next_round:
            break
        if progressed == 0:
            for rec in next_round:
                ctx.add_stat("site_groups", "failed")
                record_error(
                    ctx,
                    f"site_groups: unresolved parent for source id={rec.get('id')}",
                )
            break
        pending = next_round


def import_sites(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    region_index: dict[Any, dict[str, Any]],
    site_group_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/sites/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_site(rec)
        if key is None:
            ctx.add_stat("sites", "failed")
            record_error(ctx, "sites: missing name/slug")
            continue

        payload: dict[str, Any] = {
            "name": rec.get("name"),
            "slug": rec.get("slug") or slugify(str(rec.get("name") or key)),
        }
        status_value = get_choice_value(rec.get("status"))
        payload["status"] = status_value or "active"
        copy_fields(
            rec,
            payload,
            [
                "facility",
                "time_zone",
                "description",
                "physical_address",
                "shipping_address",
                "latitude",
                "longitude",
                "contact_name",
                "contact_phone",
                "contact_email",
                "comments",
            ],
        )

        region_target_id = resolve_by_ref(
            rec.get("region"),
            "regions",
            ctx.maps["regions"],
            region_index,
            key_region,
        )
        if rec.get("region") and region_target_id is None:
            ctx.add_stat("sites", "failed")
            record_error(ctx, f"sites: unresolved region for source id={rec.get('id')}")
            continue
        if region_target_id is not None:
            payload["region"] = region_target_id

        group_target_id = resolve_by_ref(
            rec.get("group"),
            "site_groups",
            ctx.maps["site_groups"],
            site_group_index,
            key_site_group,
        )
        if rec.get("group") and group_target_id is None:
            ctx.add_stat("sites", "failed")
            record_error(ctx, f"sites: unresolved site group for source id={rec.get('id')}")
            continue
        if group_target_id is not None:
            payload["group"] = group_target_id

        target_id, action = upsert(ctx, "sites", endpoint, index, key, payload)
        ctx.add_stat("sites", action)
        if source_id is not None and target_id is not None:
            ctx.maps["sites"][source_id] = target_id


def import_locations(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    site_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/locations/"
    pending = list(records)
    while pending:
        progressed = 0
        next_round: list[dict[str, Any]] = []
        for rec in pending:
            source_id = get_related_id(rec)
            site_target_id = resolve_by_ref(
                rec.get("site"),
                "sites",
                ctx.maps["sites"],
                site_index,
                key_site,
            )
            if site_target_id is None:
                ctx.add_stat("locations", "failed")
                record_error(ctx, f"locations: unresolved site for source id={rec.get('id')}")
                continue

            key_name = rec.get("slug") or rec.get("name")
            if not isinstance(key_name, str) or not key_name:
                ctx.add_stat("locations", "failed")
                record_error(ctx, "locations: missing name/slug")
                continue
            key = (site_target_id, key_name)

            parent_target_id = resolve_by_ref(
                rec.get("parent"),
                "locations",
                ctx.maps["locations"],
                index,
                key_location,
            )
            if rec.get("parent") and parent_target_id is None:
                ctx.add_stat("locations", "deferred")
                next_round.append(rec)
                continue

            payload: dict[str, Any] = {
                "name": rec.get("name"),
                "slug": rec.get("slug") or slugify(str(rec.get("name") or key_name)),
                "site": site_target_id,
            }
            status_value = get_choice_value(rec.get("status"))
            if status_value:
                payload["status"] = status_value
            copy_fields(rec, payload, ["description", "comments"])
            if parent_target_id is not None:
                payload["parent"] = parent_target_id

            target_id, action = upsert(ctx, "locations", endpoint, index, key, payload)
            ctx.add_stat("locations", action)
            if source_id is not None and target_id is not None:
                ctx.maps["locations"][source_id] = target_id
            if action != "failed":
                progressed += 1

        if not next_round:
            break
        if progressed == 0:
            for rec in next_round:
                ctx.add_stat("locations", "failed")
                record_error(
                    ctx,
                    f"locations: unresolved parent for source id={rec.get('id')}",
                )
            break
        pending = next_round


def import_tenant_groups(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/tenancy/tenant-groups/"
    pending = list(records)
    while pending:
        progressed = 0
        next_round: list[dict[str, Any]] = []
        for rec in pending:
            source_id = get_related_id(rec)
            key = key_tenant_group(rec)
            if key is None:
                ctx.add_stat("tenant_groups", "failed")
                record_error(ctx, "tenant_groups: missing name/slug")
                continue

            parent_target_id = resolve_by_ref(
                rec.get("parent"),
                "tenant_groups",
                ctx.maps["tenant_groups"],
                index,
                key_tenant_group,
            )
            if rec.get("parent") and parent_target_id is None:
                ctx.add_stat("tenant_groups", "deferred")
                next_round.append(rec)
                continue

            payload = {
                "name": rec.get("name"),
                "slug": rec.get("slug") or slugify(str(rec.get("name") or key)),
            }
            copy_fields(rec, payload, ["description"])
            if parent_target_id is not None:
                payload["parent"] = parent_target_id

            target_id, action = upsert(ctx, "tenant_groups", endpoint, index, key, payload)
            ctx.add_stat("tenant_groups", action)
            if source_id is not None and target_id is not None:
                ctx.maps["tenant_groups"][source_id] = target_id
            if action != "failed":
                progressed += 1

        if not next_round:
            break
        if progressed == 0:
            for rec in next_round:
                ctx.add_stat("tenant_groups", "failed")
                record_error(
                    ctx,
                    f"tenant_groups: unresolved parent for source id={rec.get('id')}",
                )
            break
        pending = next_round


def import_tenants(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    tenant_group_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/tenancy/tenants/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_tenant(rec)
        if key is None:
            ctx.add_stat("tenants", "failed")
            record_error(ctx, "tenants: missing name/slug")
            continue

        payload = {
            "name": rec.get("name"),
            "slug": rec.get("slug") or slugify(str(rec.get("name") or key)),
        }
        copy_fields(
            rec,
            payload,
            ["description", "comments"],
        )

        group_target_id = resolve_by_ref(
            rec.get("group"),
            "tenant_groups",
            ctx.maps["tenant_groups"],
            tenant_group_index,
            key_tenant_group,
        )
        if rec.get("group") and group_target_id is None:
            ctx.add_stat("tenants", "failed")
            record_error(ctx, f"tenants: unresolved group for source id={rec.get('id')}")
            continue
        if group_target_id is not None:
            payload["group"] = group_target_id

        target_id, action = upsert(ctx, "tenants", endpoint, index, key, payload)
        ctx.add_stat("tenants", action)
        if source_id is not None and target_id is not None:
            ctx.maps["tenants"][source_id] = target_id


def import_device_roles(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/device-roles/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_device_role(rec)
        if key is None:
            ctx.add_stat("device_roles", "failed")
            record_error(ctx, "device_roles: missing name/slug")
            continue

        payload = {
            "name": rec.get("name"),
            "slug": rec.get("slug") or slugify(str(rec.get("name") or key)),
            "color": rec.get("color") or "9e9e9e",
        }
        if "vm_role" in rec:
            payload["vm_role"] = bool(rec.get("vm_role"))
        copy_fields(rec, payload, ["description"])

        target_id, action = upsert(ctx, "device_roles", endpoint, index, key, payload)
        ctx.add_stat("device_roles", action)
        if source_id is not None and target_id is not None:
            ctx.maps["device_roles"][source_id] = target_id


def import_platforms(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    manufacturer_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/platforms/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_platform(rec)
        if key is None:
            ctx.add_stat("platforms", "failed")
            record_error(ctx, "platforms: missing name/slug")
            continue

        payload = {
            "name": rec.get("name"),
            "slug": rec.get("slug") or slugify(str(rec.get("name") or key)),
        }
        copy_fields(rec, payload, ["description"])

        manufacturer_target_id = resolve_by_ref(
            rec.get("manufacturer"),
            "manufacturers",
            ctx.maps["manufacturers"],
            manufacturer_index,
            key_manufacturer,
        )
        if manufacturer_target_id is not None:
            payload["manufacturer"] = manufacturer_target_id

        target_id, action = upsert(ctx, "platforms", endpoint, index, key, payload)
        ctx.add_stat("platforms", action)
        if source_id is not None and target_id is not None:
            ctx.maps["platforms"][source_id] = target_id


def import_rack_roles(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/rack-roles/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_rack_role(rec)
        if key is None:
            ctx.add_stat("rack_roles", "failed")
            record_error(ctx, "rack_roles: missing name/slug")
            continue

        payload = {
            "name": rec.get("name"),
            "slug": rec.get("slug") or slugify(str(rec.get("name") or key)),
            "color": rec.get("color") or "9e9e9e",
        }
        copy_fields(rec, payload, ["description"])

        target_id, action = upsert(ctx, "rack_roles", endpoint, index, key, payload)
        ctx.add_stat("rack_roles", action)
        if source_id is not None and target_id is not None:
            ctx.maps["rack_roles"][source_id] = target_id


def derive_manufacturers_from_device_types(
    device_types: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    seen: set[int] = set()
    output: list[dict[str, Any]] = []
    for dt in device_types:
        manufacturer = dt.get("manufacturer")
        if not isinstance(manufacturer, dict):
            continue
        source_id = manufacturer.get("id")
        if not isinstance(source_id, int):
            continue
        if source_id in seen:
            continue
        seen.add(source_id)
        name = manufacturer.get("name")
        if not isinstance(name, str) or not name:
            name = f"manufacturer-{source_id}"
        slug = manufacturer.get("slug")
        if not isinstance(slug, str) or not slug:
            slug = slugify(name)
        output.append({"id": source_id, "name": name, "slug": slug})
    return output


def merge_records_by_source_id(
    primary: list[dict[str, Any]],
    secondary: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    combined: list[dict[str, Any]] = []
    seen: set[int] = set()
    for item in primary + secondary:
        source_id = get_related_id(item)
        if source_id is not None:
            if source_id in seen:
                continue
            seen.add(source_id)
        combined.append(item)
    return combined


def import_manufacturers(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/manufacturers/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_manufacturer(rec)
        if key is None:
            ctx.add_stat("manufacturers", "failed")
            record_error(ctx, "manufacturers: missing name/slug")
            continue
        payload = {
            "name": rec.get("name"),
            "slug": rec.get("slug") or slugify(str(rec.get("name") or key)),
        }
        target_id, action = upsert(ctx, "manufacturers", endpoint, index, key, payload)
        ctx.add_stat("manufacturers", action)
        if source_id is not None and target_id is not None:
            ctx.maps["manufacturers"][source_id] = target_id


def import_racks(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    site_index: dict[Any, dict[str, Any]],
    location_index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
    rack_role_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/racks/"
    for rec in records:
        source_id = get_related_id(rec)
        site_target_id = resolve_by_ref(
            rec.get("site"),
            "sites",
            ctx.maps["sites"],
            site_index,
            key_site,
        )
        if site_target_id is None:
            ctx.add_stat("racks", "failed")
            record_error(ctx, f"racks: unresolved site for source id={rec.get('id')}")
            continue

        rack_name = rec.get("name") or rec.get("slug")
        if not isinstance(rack_name, str) or not rack_name:
            ctx.add_stat("racks", "failed")
            record_error(ctx, "racks: missing name/slug")
            continue
        key = (site_target_id, rack_name)

        payload: dict[str, Any] = {
            "site": site_target_id,
            "name": rec.get("name") or rack_name,
            "status": get_choice_value(rec.get("status")) or "active",
        }
        copy_fields(
            rec,
            payload,
            [
                "facility",
                "serial",
                "asset_tag",
                "description",
                "comments",
                "u_height",
                "desc_units",
            ],
        )

        width_value = get_choice_value(rec.get("width"))
        if isinstance(width_value, int):
            payload["width"] = width_value
        elif isinstance(rec.get("width"), int):
            payload["width"] = rec["width"]

        rack_type = get_choice_value(rec.get("type"))
        if rack_type:
            payload["type"] = rack_type

        location_target_id = resolve_by_ref(
            rec.get("location"),
            "locations",
            ctx.maps["locations"],
            location_index,
            key_location,
        )
        if location_target_id is not None:
            payload["location"] = location_target_id

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        role_target_id = resolve_by_ref(
            rec.get("role"),
            "rack_roles",
            ctx.maps["rack_roles"],
            rack_role_index,
            key_rack_role,
        )
        if role_target_id is not None:
            payload["role"] = role_target_id

        target_id, action = upsert(ctx, "racks", endpoint, index, key, payload)
        ctx.add_stat("racks", action)
        if source_id is not None and target_id is not None:
            ctx.maps["racks"][source_id] = target_id


def import_users(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/users/users/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_user(rec)
        if key is None:
            ctx.add_stat("users", "failed")
            record_error(ctx, "users: missing username")
            continue

        payload: dict[str, Any] = {
            "username": rec.get("username"),
            "is_active": rec.get("is_active", True),
        }
        copy_fields(rec, payload, ["first_name", "last_name", "email"])

        if "password" in rec and isinstance(rec.get("password"), str) and rec.get("password"):
            payload["password"] = rec["password"]

        existing = index.get(key)
        if existing is None and "password" not in payload:
            if ctx.default_user_password:
                payload["password"] = ctx.default_user_password
            else:
                ctx.add_stat("users", "deferred")
                print(
                    f"  [deferred] users: username={rec.get('username')} requires password. "
                    "Set --default-user-password to create missing users.",
                    file=sys.stderr,
                )
                continue

        target_id, action = upsert(ctx, "users", endpoint, index, key, payload)
        ctx.add_stat("users", action)
        if source_id is not None and target_id is not None:
            ctx.maps["users"][source_id] = target_id


def import_rack_reservations(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    rack_index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
    user_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/rack-reservations/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_rack_reservation(rec)
        if key is None:
            ctx.add_stat("rack_reservations", "failed")
            record_error(ctx, "rack_reservations: could not derive identity key")
            continue

        rack_ref = rec.get("rack")
        rack_target_id = resolve_by_ref(
            rack_ref,
            "racks",
            ctx.maps["racks"],
            rack_index,
            key_rack,
        )
        if rack_target_id is None and isinstance(rack_ref, dict):
            rack_name = rack_ref.get("name") or rack_ref.get("display")
            if isinstance(rack_name, str) and rack_name:
                matches = [obj for obj in rack_index.values() if obj.get("name") == rack_name]
                if len(matches) == 1 and isinstance(matches[0].get("id"), int):
                    rack_target_id = matches[0]["id"]
        if rack_target_id is None:
            ctx.add_stat("rack_reservations", "failed")
            record_error(ctx, f"rack_reservations: unresolved rack for source id={rec.get('id')}")
            continue

        user_target_id = resolve_by_ref(
            rec.get("user"),
            "users",
            ctx.maps["users"],
            user_index,
            key_user,
        )
        if user_target_id is None:
            ctx.add_stat("rack_reservations", "deferred")
            print(
                f"  [deferred] rack_reservations: unresolved user for source id={rec.get('id')}",
                file=sys.stderr,
            )
            continue

        units = rec.get("units")
        units_payload: list[int] = []
        if isinstance(units, list):
            units_payload = sorted([value for value in units if isinstance(value, int)])
        if not units_payload:
            ctx.add_stat("rack_reservations", "failed")
            record_error(ctx, "rack_reservations: missing units")
            continue

        payload: dict[str, Any] = {
            "rack": rack_target_id,
            "units": units_payload,
            "status": get_choice_value(rec.get("status")) or "active",
            "user": user_target_id,
        }
        copy_fields(rec, payload, ["description", "comments", "tags", "custom_fields"])

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        target_id, action = upsert(ctx, "rack_reservations", endpoint, index, key, payload)
        ctx.add_stat("rack_reservations", action)
        if source_id is not None and target_id is not None:
            ctx.maps["rack_reservations"][source_id] = target_id


def import_device_types(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    manufacturer_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/device-types/"
    for rec in records:
        source_id = get_related_id(rec)
        manufacturer_target_id = resolve_by_ref(
            rec.get("manufacturer"),
            "manufacturers",
            ctx.maps["manufacturers"],
            manufacturer_index,
            key_manufacturer,
        )
        if manufacturer_target_id is None:
            ctx.add_stat("device_types", "failed")
            record_error(
                ctx,
                f"device_types: unresolved manufacturer for source id={rec.get('id')}",
            )
            continue

        slug_or_model = rec.get("slug") or rec.get("model")
        if not isinstance(slug_or_model, str) or not slug_or_model:
            ctx.add_stat("device_types", "failed")
            record_error(ctx, "device_types: missing model/slug")
            continue

        key = (manufacturer_target_id, slug_or_model)
        payload: dict[str, Any] = {
            "manufacturer": manufacturer_target_id,
            "model": rec.get("model"),
            "slug": rec.get("slug") or slugify(str(rec.get("model") or slug_or_model)),
        }
        copy_fields(rec, payload, ["part_number", "u_height", "is_full_depth", "comments"])

        subdevice_role = get_choice_value(rec.get("subdevice_role"))
        if subdevice_role:
            payload["subdevice_role"] = subdevice_role
        airflow = get_choice_value(rec.get("airflow"))
        if airflow:
            payload["airflow"] = airflow

        target_id, action = upsert(ctx, "device_types", endpoint, index, key, payload)
        ctx.add_stat("device_types", action)
        if source_id is not None and target_id is not None:
            ctx.maps["device_types"][source_id] = target_id


def import_devices(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    site_index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
    role_index: dict[Any, dict[str, Any]],
    platform_index: dict[Any, dict[str, Any]],
    device_type_index: dict[Any, dict[str, Any]],
    location_index: dict[Any, dict[str, Any]],
    rack_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/devices/"
    for rec in records:
        source_id = get_related_id(rec)
        site_target_id = resolve_by_ref(
            rec.get("site"),
            "sites",
            ctx.maps["sites"],
            site_index,
            key_site,
        )
        if site_target_id is None:
            ctx.add_stat("devices", "failed")
            record_error(ctx, f"devices: unresolved site for source id={rec.get('id')}")
            continue

        role_target_id = resolve_by_ref(
            rec.get("role"),
            "device_roles",
            ctx.maps["device_roles"],
            role_index,
            key_device_role,
        )
        if role_target_id is None:
            ctx.add_stat("devices", "failed")
            record_error(ctx, f"devices: unresolved role for source id={rec.get('id')}")
            continue

        device_type_target_id = resolve_by_ref(
            rec.get("device_type"),
            "device_types",
            ctx.maps["device_types"],
            device_type_index,
            key_device_type,
        )
        if device_type_target_id is None:
            ctx.add_stat("devices", "failed")
            record_error(ctx, f"devices: unresolved type for source id={rec.get('id')}")
            continue

        payload: dict[str, Any] = {
            "site": site_target_id,
            "role": role_target_id,
            "device_type": device_type_target_id,
            "status": get_choice_value(rec.get("status")) or "active",
        }

        name = rec.get("name")
        if isinstance(name, str) and name:
            payload["name"] = name

        serial = rec.get("serial")
        if isinstance(serial, str) and serial:
            payload["serial"] = serial

        copy_fields(rec, payload, ["asset_tag", "description", "comments"])

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        platform_target_id = resolve_by_ref(
            rec.get("platform"),
            "platforms",
            ctx.maps["platforms"],
            platform_index,
            key_platform,
        )
        if platform_target_id is not None:
            payload["platform"] = platform_target_id

        location_target_id = resolve_by_ref(
            rec.get("location"),
            "locations",
            ctx.maps["locations"],
            location_index,
            key_location,
        )
        if location_target_id is not None:
            payload["location"] = location_target_id

        rack_target_id = resolve_rack_by_ref(
            rec.get("rack"),
            site_target_id,
            ctx.maps["racks"],
            rack_index,
        )
        if rack_target_id is not None:
            payload["rack"] = rack_target_id
            if non_empty(rec.get("position")):
                payload["position"] = rec.get("position")
            face = get_choice_value(rec.get("face"))
            if face:
                payload["face"] = face

        key: Any
        if "name" in payload:
            key = ("name", site_target_id, payload["name"])
        elif "serial" in payload:
            key = ("serial", payload["serial"])
        elif "asset_tag" in payload:
            key = ("asset_tag", payload["asset_tag"])
        elif "rack" in payload and "position" in payload:
            key = (
                "rackpos",
                site_target_id,
                payload["rack"],
                str(payload["position"]),
                str(payload.get("face") or ""),
                device_type_target_id,
            )
        else:
            display = rec.get("display")
            if isinstance(display, str) and display:
                key = ("display", site_target_id, display)
            else:
                ctx.add_stat("devices", "failed")
                record_error(
                    ctx,
                    f"devices: no stable key for source id={rec.get('id')}; "
                    "needs one of name/serial/asset_tag/rack+position",
                )
                continue

        target_id, action = upsert(ctx, "devices", endpoint, index, key, payload)
        ctx.add_stat("devices", action)
        if source_id is not None and target_id is not None:
            ctx.maps["devices"][source_id] = target_id


def import_rirs(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/ipam/rirs/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_rir(rec)
        if key is None:
            ctx.add_stat("rirs", "failed")
            record_error(ctx, "rirs: missing name/slug")
            continue

        payload: dict[str, Any] = {
            "name": rec.get("name"),
            "slug": rec.get("slug") or slugify(str(rec.get("name") or key)),
            "is_private": bool(rec.get("is_private")),
        }
        copy_fields(rec, payload, ["description", "comments", "tags", "custom_fields"])

        target_id, action = upsert(ctx, "rirs", endpoint, index, key, payload)
        ctx.add_stat("rirs", action)
        if source_id is not None and target_id is not None:
            ctx.maps["rirs"][source_id] = target_id


def import_ipam_roles(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/ipam/roles/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_ipam_role(rec)
        if key is None:
            ctx.add_stat("prefix_vlan_roles", "failed")
            record_error(ctx, "prefix_vlan_roles: missing name/slug")
            continue

        payload: dict[str, Any] = {
            "name": rec.get("name"),
            "slug": rec.get("slug") or slugify(str(rec.get("name") or key)),
        }
        copy_fields(
            rec,
            payload,
            ["weight", "description", "comments", "tags", "custom_fields"],
        )

        target_id, action = upsert(ctx, "prefix_vlan_roles", endpoint, index, key, payload)
        ctx.add_stat("prefix_vlan_roles", action)
        if source_id is not None and target_id is not None:
            ctx.maps["ipam_roles"][source_id] = target_id


def import_route_targets(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/ipam/route-targets/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_route_target(rec)
        if key is None:
            ctx.add_stat("route_targets", "failed")
            record_error(ctx, "route_targets: missing name")
            continue

        payload: dict[str, Any] = {"name": rec.get("name")}
        copy_fields(rec, payload, ["description", "comments", "tags", "custom_fields"])

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        target_id, action = upsert(ctx, "route_targets", endpoint, index, key, payload)
        ctx.add_stat("route_targets", action)
        if source_id is not None and target_id is not None:
            ctx.maps["route_targets"][source_id] = target_id


def import_vrfs(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
    route_target_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/ipam/vrfs/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_vrf(rec)
        if key is None:
            ctx.add_stat("vrfs", "failed")
            record_error(ctx, "vrfs: missing name/rd")
            continue

        payload: dict[str, Any] = {"name": rec.get("name")}
        copy_fields(
            rec,
            payload,
            ["rd", "enforce_unique", "description", "comments", "tags", "custom_fields"],
        )

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        import_targets = resolve_list_of_route_targets(
            rec.get("import_targets"),
            ctx,
            route_target_index,
        )
        if import_targets:
            payload["import_targets"] = import_targets

        export_targets = resolve_list_of_route_targets(
            rec.get("export_targets"),
            ctx,
            route_target_index,
        )
        if export_targets:
            payload["export_targets"] = export_targets

        target_id, action = upsert(ctx, "vrfs", endpoint, index, key, payload)
        ctx.add_stat("vrfs", action)
        if source_id is not None and target_id is not None:
            ctx.maps["vrfs"][source_id] = target_id


def import_vlan_groups(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
    site_index: dict[Any, dict[str, Any]],
    location_index: dict[Any, dict[str, Any]],
    region_index: dict[Any, dict[str, Any]],
    site_group_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/ipam/vlan-groups/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_vlan_group(rec)
        if key is None:
            ctx.add_stat("vlan_groups", "failed")
            record_error(ctx, "vlan_groups: missing name/slug")
            continue

        payload: dict[str, Any] = {
            "name": rec.get("name"),
            "slug": rec.get("slug") or slugify(str(rec.get("name") or key)),
        }
        copy_fields(
            rec,
            payload,
            ["vid_ranges", "description", "comments", "tags", "custom_fields"],
        )

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        scope_resolved = resolve_scope_by_ref(
            rec.get("scope"),
            rec.get("scope_type"),
            ctx,
            site_index,
            location_index,
            region_index,
            site_group_index,
        )
        if scope_resolved is not None:
            payload["scope_type"] = scope_resolved[0]
            payload["scope_id"] = scope_resolved[1]

        target_id, action = upsert(ctx, "vlan_groups", endpoint, index, key, payload)
        ctx.add_stat("vlan_groups", action)
        if source_id is not None and target_id is not None:
            ctx.maps["vlan_groups"][source_id] = target_id


def import_vlans(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
    site_index: dict[Any, dict[str, Any]],
    role_index: dict[Any, dict[str, Any]],
    vlan_group_index: dict[Any, dict[str, Any]],
    vlan_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/ipam/vlans/"
    pending = list(records)
    while pending:
        progressed = 0
        next_round: list[dict[str, Any]] = []
        for rec in pending:
            source_id = get_related_id(rec)
            vid = rec.get("vid")
            if not isinstance(vid, int):
                ctx.add_stat("vlans", "failed")
                record_error(ctx, "vlans: missing vid")
                continue

            group_target_id = resolve_by_ref(
                rec.get("group"),
                "vlan_groups",
                ctx.maps["vlan_groups"],
                vlan_group_index,
                key_vlan_group,
            )
            site_target_id = resolve_by_ref(
                rec.get("site"),
                "sites",
                ctx.maps["sites"],
                site_index,
                key_site,
            )

            if rec.get("group") and group_target_id is None:
                ctx.add_stat("vlans", "deferred")
                next_round.append(rec)
                continue
            if rec.get("site") and site_target_id is None:
                ctx.add_stat("vlans", "deferred")
                next_round.append(rec)
                continue

            key = (
                ("group", group_target_id, vid)
                if group_target_id is not None
                else ("site", site_target_id, vid)
                if site_target_id is not None
                else ("global", vid, str(rec.get("name") or ""))
            )

            payload: dict[str, Any] = {
                "vid": vid,
                "name": rec.get("name"),
                "status": get_choice_value(rec.get("status")) or "active",
            }
            copy_fields(
                rec,
                payload,
                ["description", "comments", "tags", "custom_fields"],
            )

            if group_target_id is not None:
                payload["group"] = group_target_id
            if site_target_id is not None:
                payload["site"] = site_target_id

            tenant_target_id = resolve_by_ref(
                rec.get("tenant"),
                "tenants",
                ctx.maps["tenants"],
                tenant_index,
                key_tenant,
            )
            if tenant_target_id is not None:
                payload["tenant"] = tenant_target_id

            role_target_id = resolve_by_ref(
                rec.get("role"),
                "ipam_roles",
                ctx.maps["ipam_roles"],
                role_index,
                key_ipam_role,
            )
            if role_target_id is not None:
                payload["role"] = role_target_id

            qinq_role = get_choice_value(rec.get("qinq_role"))
            if qinq_role:
                payload["qinq_role"] = qinq_role

            qinq_svlan_target_id = resolve_by_ref(
                rec.get("qinq_svlan"),
                "vlans",
                ctx.maps["vlans"],
                vlan_index,
                key_vlan,
            )
            if rec.get("qinq_svlan") and qinq_svlan_target_id is None:
                ctx.add_stat("vlans", "deferred")
                next_round.append(rec)
                continue
            if qinq_svlan_target_id is not None:
                payload["qinq_svlan"] = qinq_svlan_target_id

            target_id, action = upsert(ctx, "vlans", endpoint, index, key, payload)
            ctx.add_stat("vlans", action)
            if source_id is not None and target_id is not None:
                ctx.maps["vlans"][source_id] = target_id
            if action != "failed":
                progressed += 1

        if not next_round:
            break
        if progressed == 0:
            for rec in next_round:
                ctx.add_stat("vlans", "failed")
                record_error(ctx, f"vlans: unresolved dependencies for source id={rec.get('id')}")
            break
        pending = next_round


def import_aggregates(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    rir_index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/ipam/aggregates/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_aggregate(rec)
        if key is None:
            ctx.add_stat("aggregates", "failed")
            record_error(ctx, "aggregates: missing prefix")
            continue

        rir_target_id = resolve_by_ref(
            rec.get("rir"),
            "rirs",
            ctx.maps["rirs"],
            rir_index,
            key_rir,
        )
        if rir_target_id is None:
            ctx.add_stat("aggregates", "failed")
            record_error(ctx, f"aggregates: unresolved rir for source id={rec.get('id')}")
            continue

        payload: dict[str, Any] = {"prefix": rec.get("prefix"), "rir": rir_target_id}
        copy_fields(
            rec,
            payload,
            ["date_added", "description", "comments", "tags", "custom_fields"],
        )

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        target_id, action = upsert(ctx, "aggregates", endpoint, index, key, payload)
        ctx.add_stat("aggregates", action)
        if source_id is not None and target_id is not None:
            ctx.maps["aggregates"][source_id] = target_id


def import_asns(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    rir_index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
    site_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/ipam/asns/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_asn(rec)
        if key is None:
            ctx.add_stat("asns", "failed")
            record_error(ctx, "asns: missing asn")
            continue

        rir_target_id = resolve_by_ref(
            rec.get("rir"),
            "rirs",
            ctx.maps["rirs"],
            rir_index,
            key_rir,
        )
        if rir_target_id is None:
            ctx.add_stat("asns", "failed")
            record_error(ctx, f"asns: unresolved rir for source id={rec.get('id')}")
            continue

        payload: dict[str, Any] = {"asn": rec.get("asn"), "rir": rir_target_id}
        copy_fields(rec, payload, ["description", "comments", "tags", "custom_fields"])

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        source_sites = rec.get("sites")
        if isinstance(source_sites, list) and source_sites:
            site_ids: list[int] = []
            for item in source_sites:
                site_target_id = resolve_by_ref(
                    item, "sites", ctx.maps["sites"], site_index, key_site
                )
                if site_target_id is not None:
                    site_ids.append(site_target_id)
            if site_ids:
                payload["sites"] = site_ids

        target_id, action = upsert(ctx, "asns", endpoint, index, key, payload)
        ctx.add_stat("asns", action)
        if source_id is not None and target_id is not None:
            ctx.maps["asns"][source_id] = target_id


def import_asn_ranges(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    rir_index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/ipam/asn-ranges/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_asn_range(rec)
        if key is None:
            ctx.add_stat("asn_ranges", "failed")
            record_error(ctx, "asn_ranges: missing slug/name/start/end")
            continue

        rir_target_id = resolve_by_ref(
            rec.get("rir"),
            "rirs",
            ctx.maps["rirs"],
            rir_index,
            key_rir,
        )
        if rir_target_id is None:
            ctx.add_stat("asn_ranges", "failed")
            record_error(ctx, f"asn_ranges: unresolved rir for source id={rec.get('id')}")
            continue

        payload: dict[str, Any] = {
            "name": rec.get("name"),
            "slug": rec.get("slug") or slugify(str(rec.get("name") or "asn-range")),
            "rir": rir_target_id,
            "start": rec.get("start"),
            "end": rec.get("end"),
        }
        copy_fields(rec, payload, ["description", "comments", "tags", "custom_fields"])

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        target_id, action = upsert(ctx, "asn_ranges", endpoint, index, key, payload)
        ctx.add_stat("asn_ranges", action)
        if source_id is not None and target_id is not None:
            ctx.maps["asn_ranges"][source_id] = target_id


def import_prefixes(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    vrf_index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
    vlan_index: dict[Any, dict[str, Any]],
    role_index: dict[Any, dict[str, Any]],
    site_index: dict[Any, dict[str, Any]],
    location_index: dict[Any, dict[str, Any]],
    region_index: dict[Any, dict[str, Any]],
    site_group_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/ipam/prefixes/"
    for rec in records:
        source_id = get_related_id(rec)
        prefix = rec.get("prefix")
        if not isinstance(prefix, str) or not prefix:
            ctx.add_stat("prefixes", "failed")
            record_error(ctx, "prefixes: missing prefix")
            continue

        vrf_target_id = resolve_by_ref(
            rec.get("vrf"),
            "vrfs",
            ctx.maps["vrfs"],
            vrf_index,
            key_vrf,
        )
        if rec.get("vrf") and vrf_target_id is None:
            ctx.add_stat("prefixes", "failed")
            record_error(ctx, f"prefixes: unresolved vrf for source id={rec.get('id')}")
            continue

        key = (vrf_target_id, prefix)
        payload: dict[str, Any] = {
            "prefix": prefix,
            "status": get_choice_value(rec.get("status")) or "active",
            "is_pool": bool(rec.get("is_pool")),
            "mark_utilized": bool(rec.get("mark_utilized")),
        }
        if vrf_target_id is not None:
            payload["vrf"] = vrf_target_id

        copy_fields(rec, payload, ["description", "comments", "tags", "custom_fields"])

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        vlan_target_id = resolve_by_ref(
            rec.get("vlan"),
            "vlans",
            ctx.maps["vlans"],
            vlan_index,
            key_vlan,
        )
        if rec.get("vlan") and vlan_target_id is None:
            ctx.add_stat("prefixes", "failed")
            record_error(ctx, f"prefixes: unresolved vlan for source id={rec.get('id')}")
            continue
        if vlan_target_id is not None:
            payload["vlan"] = vlan_target_id

        role_target_id = resolve_by_ref(
            rec.get("role"),
            "ipam_roles",
            ctx.maps["ipam_roles"],
            role_index,
            key_ipam_role,
        )
        if rec.get("role") and role_target_id is None:
            ctx.add_stat("prefixes", "failed")
            record_error(ctx, f"prefixes: unresolved role for source id={rec.get('id')}")
            continue
        if role_target_id is not None:
            payload["role"] = role_target_id

        scope_resolved = resolve_scope_by_ref(
            rec.get("scope"),
            rec.get("scope_type"),
            ctx,
            site_index,
            location_index,
            region_index,
            site_group_index,
        )
        if rec.get("scope") and scope_resolved is None:
            ctx.add_stat("prefixes", "failed")
            record_error(ctx, f"prefixes: unresolved scope for source id={rec.get('id')}")
            continue
        if scope_resolved is not None:
            payload["scope_type"] = scope_resolved[0]
            payload["scope_id"] = scope_resolved[1]

        target_id, action = upsert(ctx, "prefixes", endpoint, index, key, payload)
        ctx.add_stat("prefixes", action)
        if source_id is not None and target_id is not None:
            ctx.maps["prefixes"][source_id] = target_id


def import_ip_ranges(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    vrf_index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
    role_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/ipam/ip-ranges/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_ip_range(rec)
        if key is None:
            ctx.add_stat("ip_ranges", "failed")
            record_error(ctx, "ip_ranges: missing start/end address")
            continue

        vrf_target_id = resolve_by_ref(
            rec.get("vrf"),
            "vrfs",
            ctx.maps["vrfs"],
            vrf_index,
            key_vrf,
        )
        if rec.get("vrf") and vrf_target_id is None:
            ctx.add_stat("ip_ranges", "failed")
            record_error(ctx, f"ip_ranges: unresolved vrf for source id={rec.get('id')}")
            continue

        payload: dict[str, Any] = {
            "start_address": rec.get("start_address"),
            "end_address": rec.get("end_address"),
            "status": get_choice_value(rec.get("status")) or "active",
            "mark_populated": bool(rec.get("mark_populated")),
            "mark_utilized": bool(rec.get("mark_utilized")),
        }
        if vrf_target_id is not None:
            payload["vrf"] = vrf_target_id

        copy_fields(rec, payload, ["description", "comments", "tags", "custom_fields"])

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        role_target_id = resolve_by_ref(
            rec.get("role"),
            "ipam_roles",
            ctx.maps["ipam_roles"],
            role_index,
            key_ipam_role,
        )
        if role_target_id is not None:
            payload["role"] = role_target_id

        target_id, action = upsert(ctx, "ip_ranges", endpoint, index, key, payload)
        ctx.add_stat("ip_ranges", action)
        if source_id is not None and target_id is not None:
            ctx.maps["ip_ranges"][source_id] = target_id


def import_clusters(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    cluster_type_index: dict[Any, dict[str, Any]],
    cluster_group_index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
    site_index: dict[Any, dict[str, Any]],
    location_index: dict[Any, dict[str, Any]],
    region_index: dict[Any, dict[str, Any]],
    site_group_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/virtualization/clusters/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_cluster(rec)
        if key is None:
            ctx.add_stat("clusters", "failed")
            record_error(ctx, "clusters: missing name")
            continue

        payload: dict[str, Any] = {
            "name": rec.get("name"),
            "status": get_choice_value(rec.get("status")) or "active",
        }
        copy_fields(rec, payload, ["description", "comments", "tags", "custom_fields"])

        type_target_id = resolve_by_ref(
            rec.get("type"),
            "cluster_types",
            ctx.maps["cluster_types"],
            cluster_type_index,
            key_cluster_type,
        )
        if rec.get("type") and type_target_id is None:
            ctx.add_stat("clusters", "failed")
            record_error(ctx, f"clusters: unresolved type for source id={rec.get('id')}")
            continue
        if type_target_id is not None:
            payload["type"] = type_target_id

        group_target_id = resolve_by_ref(
            rec.get("group"),
            "cluster_groups",
            ctx.maps["cluster_groups"],
            cluster_group_index,
            key_cluster_group,
        )
        if group_target_id is not None:
            payload["group"] = group_target_id

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        scope_resolved = resolve_scope_by_ref(
            rec.get("scope"),
            rec.get("scope_type"),
            ctx,
            site_index,
            location_index,
            region_index,
            site_group_index,
        )
        if rec.get("scope") and scope_resolved is None:
            ctx.add_stat("clusters", "failed")
            record_error(ctx, f"clusters: unresolved scope for source id={rec.get('id')}")
            continue
        if scope_resolved is not None:
            payload["scope_type"] = scope_resolved[0]
            payload["scope_id"] = scope_resolved[1]

        target_id, action = upsert(ctx, "clusters", endpoint, index, key, payload)
        ctx.add_stat("clusters", action)
        if source_id is not None and target_id is not None:
            ctx.maps["clusters"][source_id] = target_id


def import_virtual_machines(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    cluster_index: dict[Any, dict[str, Any]],
    site_index: dict[Any, dict[str, Any]],
    device_index: dict[Any, dict[str, Any]],
    role_index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
    platform_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/virtualization/virtual-machines/"
    for rec in records:
        source_id = get_related_id(rec)
        name = rec.get("name")
        if not isinstance(name, str) or not name:
            ctx.add_stat("virtual_machines", "failed")
            record_error(ctx, "virtual_machines: missing name")
            continue

        cluster_target_id = resolve_by_ref(
            rec.get("cluster"),
            "clusters",
            ctx.maps["clusters"],
            cluster_index,
            key_cluster,
        )

        key: Any = ("cluster", cluster_target_id, name) if cluster_target_id is not None else ("name", name)
        payload: dict[str, Any] = {
            "name": name,
            "status": get_choice_value(rec.get("status")) or "active",
        }
        copy_fields(
            rec,
            payload,
            [
                "serial",
                "vcpus",
                "memory",
                "disk",
                "description",
                "comments",
                "config_template",
                "local_context_data",
                "tags",
                "custom_fields",
            ],
        )

        start_on_boot = get_choice_value(rec.get("start_on_boot"))
        if start_on_boot is not None and start_on_boot != "":
            payload["start_on_boot"] = start_on_boot

        if cluster_target_id is not None:
            payload["cluster"] = cluster_target_id

        site_target_id = resolve_by_ref(rec.get("site"), "sites", ctx.maps["sites"], site_index, key_site)
        if site_target_id is not None:
            payload["site"] = site_target_id

        device_target_id = resolve_by_ref(
            rec.get("device"),
            "devices",
            ctx.maps["devices"],
            device_index,
            key_device,
        )
        if device_target_id is not None:
            payload["device"] = device_target_id

        role_target_id = resolve_by_ref(
            rec.get("role"),
            "device_roles",
            ctx.maps["device_roles"],
            role_index,
            key_device_role,
        )
        if role_target_id is not None:
            payload["role"] = role_target_id

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        platform_target_id = resolve_by_ref(
            rec.get("platform"),
            "platforms",
            ctx.maps["platforms"],
            platform_index,
            key_platform,
        )
        if platform_target_id is not None:
            payload["platform"] = platform_target_id

        primary_ip4 = resolve_id_from_ref_or_raw(rec.get("primary_ip4"), ctx)
        primary_ip6 = resolve_id_from_ref_or_raw(rec.get("primary_ip6"), ctx)
        if primary_ip4 is not None:
            payload["primary_ip4"] = primary_ip4
        if primary_ip6 is not None:
            payload["primary_ip6"] = primary_ip6

        target_id, action = upsert(ctx, "virtual_machines", endpoint, index, key, payload)
        ctx.add_stat("virtual_machines", action)
        if source_id is not None and target_id is not None:
            ctx.maps["virtual_machines"][source_id] = target_id


def import_providers(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    asn_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/circuits/providers/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_provider(rec)
        if key is None:
            ctx.add_stat("providers", "failed")
            record_error(ctx, "providers: missing name/slug")
            continue

        payload: dict[str, Any] = {
            "name": rec.get("name"),
            "slug": rec.get("slug") or slugify(str(rec.get("name") or key)),
        }
        copy_fields(rec, payload, ["description", "comments", "tags", "custom_fields"])

        asns = rec.get("asns")
        if isinstance(asns, list) and asns:
            target_asns: list[int] = []
            for item in asns:
                mapped = resolve_by_ref(item, "asns", ctx.maps["asns"], asn_index, key_asn)
                if mapped is not None:
                    target_asns.append(mapped)
            if target_asns:
                payload["asns"] = target_asns

        target_id, action = upsert(ctx, "providers", endpoint, index, key, payload)
        ctx.add_stat("providers", action)
        if source_id is not None and target_id is not None:
            ctx.maps["providers"][source_id] = target_id


def import_provider_networks(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    provider_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/circuits/provider-networks/"
    for rec in records:
        source_id = get_related_id(rec)
        provider_target_id = resolve_by_ref(
            rec.get("provider"),
            "providers",
            ctx.maps["providers"],
            provider_index,
            key_provider,
        )
        if provider_target_id is None:
            ctx.add_stat("provider_networks", "failed")
            record_error(ctx, f"provider_networks: unresolved provider for source id={rec.get('id')}")
            continue

        key = (provider_target_id, rec.get("name"))
        payload: dict[str, Any] = {
            "provider": provider_target_id,
            "name": rec.get("name"),
        }
        copy_fields(rec, payload, ["service_id", "description", "comments", "tags", "custom_fields"])

        target_id, action = upsert(ctx, "provider_networks", endpoint, index, key, payload)
        ctx.add_stat("provider_networks", action)
        if source_id is not None and target_id is not None:
            ctx.maps["provider_networks"][source_id] = target_id


def import_circuits(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    provider_index: dict[Any, dict[str, Any]],
    circuit_type_index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/circuits/circuits/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_circuit(rec)
        if key is None:
            ctx.add_stat("circuits", "failed")
            record_error(ctx, "circuits: missing cid")
            continue

        provider_target_id = resolve_by_ref(
            rec.get("provider"),
            "providers",
            ctx.maps["providers"],
            provider_index,
            key_provider,
        )
        type_target_id = resolve_by_ref(
            rec.get("type"),
            "circuit_types",
            ctx.maps["circuit_types"],
            circuit_type_index,
            key_circuit_type,
        )
        if provider_target_id is None or type_target_id is None:
            ctx.add_stat("circuits", "failed")
            record_error(ctx, f"circuits: unresolved provider/type for source id={rec.get('id')}")
            continue

        payload: dict[str, Any] = {
            "cid": rec.get("cid"),
            "provider": provider_target_id,
            "type": type_target_id,
            "status": get_choice_value(rec.get("status")) or "active",
        }
        copy_fields(
            rec,
            payload,
            [
                "install_date",
                "termination_date",
                "commit_rate",
                "description",
                "distance",
                "distance_unit",
                "comments",
                "tags",
                "custom_fields",
            ],
        )

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        provider_account_id = resolve_id_from_ref_or_raw(rec.get("provider_account"), ctx)
        if provider_account_id is not None:
            payload["provider_account"] = provider_account_id

        target_id, action = upsert(ctx, "circuits", endpoint, index, key, payload)
        ctx.add_stat("circuits", action)
        if source_id is not None and target_id is not None:
            ctx.maps["circuits"][source_id] = target_id


def import_circuit_terminations(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    circuit_index: dict[Any, dict[str, Any]],
    provider_network_index: dict[Any, dict[str, Any]],
    site_index: dict[Any, dict[str, Any]],
    location_index: dict[Any, dict[str, Any]],
    region_index: dict[Any, dict[str, Any]],
    site_group_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/circuits/circuit-terminations/"
    for rec in records:
        source_id = get_related_id(rec)
        circuit_target_id = resolve_by_ref(
            rec.get("circuit"),
            "circuits",
            ctx.maps["circuits"],
            circuit_index,
            key_circuit,
        )
        side = rec.get("term_side")
        if circuit_target_id is None or not isinstance(side, str):
            ctx.add_stat("circuit_terminations", "failed")
            record_error(
                ctx, f"circuit_terminations: unresolved circuit/side for source id={rec.get('id')}"
            )
            continue

        key = (circuit_target_id, side)
        payload: dict[str, Any] = {"circuit": circuit_target_id, "term_side": side}
        copy_fields(
            rec,
            payload,
            ["port_speed", "upstream_speed", "xconnect_id", "pp_info", "description", "mark_connected", "tags", "custom_fields"],
        )

        term_type = rec.get("termination_type")
        if isinstance(term_type, str) and term_type:
            payload["termination_type"] = term_type

        termination_id: int | None = None
        if term_type == "circuits.providernetwork":
            termination_id = resolve_by_ref(
                rec.get("termination"),
                "provider_networks",
                ctx.maps["provider_networks"],
                provider_network_index,
                key_provider_network,
            )
        elif term_type in {"dcim.site", "dcim.location", "dcim.region", "dcim.sitegroup"}:
            scope = resolve_scope_by_ref(
                rec.get("termination"),
                term_type,
                ctx,
                site_index,
                location_index,
                region_index,
                site_group_index,
            )
            termination_id = scope[1] if scope is not None else None
        else:
            termination_id = resolve_id_from_ref_or_raw(rec.get("termination"), ctx)
            if termination_id is None and isinstance(rec.get("termination_id"), int):
                termination_id = rec.get("termination_id")

        if term_type and termination_id is not None:
            payload["termination_id"] = termination_id

        target_id, action = upsert(ctx, "circuit_terminations", endpoint, index, key, payload)
        ctx.add_stat("circuit_terminations", action)
        if source_id is not None and target_id is not None:
            ctx.maps["circuit_terminations"][source_id] = target_id


def import_power_panels(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    site_index: dict[Any, dict[str, Any]],
    location_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/power-panels/"
    for rec in records:
        source_id = get_related_id(rec)
        site_target_id = resolve_by_ref(
            rec.get("site"),
            "sites",
            ctx.maps["sites"],
            site_index,
            key_site,
        )
        if site_target_id is None:
            ctx.add_stat("power_panels", "failed")
            record_error(ctx, f"power_panels: unresolved site for source id={rec.get('id')}")
            continue

        name = rec.get("name")
        if not isinstance(name, str) or not name:
            ctx.add_stat("power_panels", "failed")
            record_error(ctx, "power_panels: missing name")
            continue

        key = (site_target_id, name)
        payload: dict[str, Any] = {"site": site_target_id, "name": name}
        copy_fields(rec, payload, ["description", "comments", "tags", "custom_fields"])

        location_target_id = resolve_by_ref(
            rec.get("location"),
            "locations",
            ctx.maps["locations"],
            location_index,
            key_location,
        )
        if location_target_id is not None:
            payload["location"] = location_target_id

        target_id, action = upsert(ctx, "power_panels", endpoint, index, key, payload)
        ctx.add_stat("power_panels", action)
        if source_id is not None and target_id is not None:
            ctx.maps["power_panels"][source_id] = target_id


def import_power_feeds(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    panel_index: dict[Any, dict[str, Any]],
    rack_index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/power-feeds/"
    for rec in records:
        source_id = get_related_id(rec)
        panel_target_id = resolve_by_ref(
            rec.get("power_panel"),
            "power_panels",
            ctx.maps["power_panels"],
            panel_index,
            key_power_panel,
        )
        if panel_target_id is None:
            ctx.add_stat("power_feeds", "failed")
            record_error(ctx, f"power_feeds: unresolved panel for source id={rec.get('id')}")
            continue

        name = rec.get("name")
        if not isinstance(name, str) or not name:
            ctx.add_stat("power_feeds", "failed")
            record_error(ctx, "power_feeds: missing name")
            continue

        key = (panel_target_id, name)
        payload: dict[str, Any] = {
            "power_panel": panel_target_id,
            "name": name,
            "status": get_choice_value(rec.get("status")) or "active",
        }
        copy_fields(
            rec,
            payload,
            [
                "voltage",
                "amperage",
                "max_utilization",
                "mark_connected",
                "description",
                "comments",
                "tags",
                "custom_fields",
            ],
        )

        feed_type = get_choice_value(rec.get("type"))
        if feed_type:
            payload["type"] = feed_type
        supply = get_choice_value(rec.get("supply"))
        if supply:
            payload["supply"] = supply
        phase = get_choice_value(rec.get("phase"))
        if phase:
            payload["phase"] = phase

        rack_target_id = resolve_by_ref(
            rec.get("rack"),
            "racks",
            ctx.maps["racks"],
            rack_index,
            key_rack,
        )
        if rack_target_id is not None:
            payload["rack"] = rack_target_id

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        target_id, action = upsert(ctx, "power_feeds", endpoint, index, key, payload)
        ctx.add_stat("power_feeds", action)
        if source_id is not None and target_id is not None:
            ctx.maps["power_feeds"][source_id] = target_id


def import_custom_fields(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    choice_set_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/extras/custom-fields/"
    for rec in records:
        source_id = get_related_id(rec)
        key = key_custom_field(rec)
        if key is None:
            ctx.add_stat("custom_fields", "failed")
            record_error(ctx, "custom_fields: missing name")
            continue

        payload: dict[str, Any] = {"name": rec.get("name")}
        fields = [
            "object_types",
            "related_object_type",
            "label",
            "group_name",
            "description",
            "required",
            "unique",
            "search_weight",
            "filter_logic",
            "ui_visible",
            "ui_editable",
            "is_cloneable",
            "default",
            "related_object_filter",
            "weight",
            "validation_minimum",
            "validation_maximum",
            "validation_regex",
            "comments",
        ]
        copy_fields(rec, payload, fields)

        for enum_field in ("filter_logic", "ui_visible", "ui_editable"):
            enum_value = get_choice_value(rec.get(enum_field))
            if enum_value is not None and enum_value != "":
                payload[enum_field] = enum_value

        data_type = get_choice_value(rec.get("type"))
        if data_type:
            payload["type"] = data_type

        choice_set_target = resolve_by_ref(
            rec.get("choice_set"),
            "custom_field_choice_sets",
            ctx.maps["custom_field_choice_sets"],
            choice_set_index,
            key_custom_field_choice_set,
        )
        if choice_set_target is not None:
            payload["choice_set"] = choice_set_target

        target_id, action = upsert(ctx, "custom_fields", endpoint, index, key, payload)
        ctx.add_stat("custom_fields", action)
        if source_id is not None and target_id is not None:
            ctx.maps["custom_fields"][source_id] = target_id


def import_cables(
    ctx: ImportContext,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    tenant_index: dict[Any, dict[str, Any]],
) -> None:
    endpoint = "/api/dcim/cables/"
    for rec in records:
        source_id = get_related_id(rec)

        payload: dict[str, Any] = {
            "type": get_choice_value(rec.get("type")) or rec.get("type") or "cat6",
            "status": get_choice_value(rec.get("status")) or "connected",
        }
        copy_fields(
            rec,
            payload,
            [
                "profile",
                "label",
                "color",
                "length",
                "length_unit",
                "description",
                "comments",
                "tags",
                "custom_fields",
            ],
        )

        tenant_target_id = resolve_by_ref(
            rec.get("tenant"),
            "tenants",
            ctx.maps["tenants"],
            tenant_index,
            key_tenant,
        )
        if tenant_target_id is not None:
            payload["tenant"] = tenant_target_id

        a_terms = sanitize_generic_value(rec.get("a_terminations"), ctx)
        b_terms = sanitize_generic_value(rec.get("b_terminations"), ctx)
        if isinstance(a_terms, list) and a_terms:
            payload["a_terminations"] = a_terms
        if isinstance(b_terms, list) and b_terms:
            payload["b_terminations"] = b_terms

        key_payload = {
            "label": payload.get("label"),
            "a_terminations": payload.get("a_terminations"),
            "b_terminations": payload.get("b_terminations"),
            "type": payload.get("type"),
        }
        key = key_cable(key_payload)
        if key is None:
            ctx.add_stat("cables", "failed")
            record_error(ctx, "cables: missing stable key")
            continue

        target_id, action = upsert(ctx, "cables", endpoint, index, key, payload)
        ctx.add_stat("cables", action)
        if source_id is not None and target_id is not None:
            ctx.maps["cables"][source_id] = target_id


def import_generic_resource(
    ctx: ImportContext,
    section: str,
    resource: str,
    records: list[dict[str, Any]],
    index: dict[Any, dict[str, Any]],
    key_fn: Any = key_name_slug_or_name,
    include_fields: list[str] | None = None,
) -> None:
    endpoint = RESOURCE_ENDPOINT_LOOKUP.get((section, resource))
    if not endpoint:
        for _ in records:
            ctx.add_stat(resource, "failed")
        if records:
            record_error(ctx, f"{resource}: missing endpoint mapping for {section}/{resource}")
        return

    for rec in records:
        source_id = get_related_id(rec)
        key = key_fn(rec)
        if key is None:
            ctx.add_stat(resource, "failed")
            record_error(ctx, f"{resource}: could not derive identity key")
            continue

        payload: dict[str, Any] = {}
        fields = include_fields
        if fields is None:
            fields = load_template_fields(section, resource) or list(rec.keys())
        for field in fields:
            if field not in rec:
                continue
            value = rec.get(field)
            if value is None or value == "":
                continue
            sanitized = sanitize_generic_value(value, ctx)
            if sanitized is None or sanitized == "":
                continue
            payload[field] = sanitized

        remap_known_typed_object_fields(payload, ctx)

        target_id, action = upsert(ctx, resource, endpoint, index, key, payload)
        ctx.add_stat(resource, action)
        if source_id is not None and target_id is not None:
            ctx.maps[resource][source_id] = target_id


def print_stats(ctx: ImportContext, stats_order: list[str] | None = None) -> None:
    print("")
    print("Import summary:")
    order = stats_order or IMPORT_STATS_ORDER
    for name in order:
        stat = ctx.stats.get(name) or ResourceStats(name=name)
        print(
            f"- {name}: created={stat.created} updated={stat.updated} "
            f"existing={stat.existing} failed={stat.failed} deferred={stat.deferred}"
        )
    if ctx.errors:
        print("")
        print(f"Errors: {len(ctx.errors)} (showing up to 20)")
        for message in ctx.errors[:20]:
            print(f"- {message}")


