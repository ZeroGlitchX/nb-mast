#!/usr/bin/env python3
"""
CLI orchestration entrypoint for NetBox import.
"""

from __future__ import annotations

from import_core import *
from import_resources import *


def main() -> int:
    args = parse_args()
    if not args.url:
        print("Error: NetBox URL is required via --url or NETBOX_URL", file=sys.stderr)
        return 2
    if not args.token:
        print("Error: API token is required via --token or NETBOX_API_TOKEN", file=sys.stderr)
        return 2
    if args.preflight_strict and not args.preflight:
        print("Error: --preflight-strict requires --preflight", file=sys.stderr)
        return 2

    input_root = Path(args.input_dir).resolve()
    try:
        selected_resources = parse_only_resources(args.only)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    if args.preflight:
        return run_preflight(
            url=args.url,
            token=args.token,
            auth_scheme=args.auth_scheme,
            timeout=args.timeout,
            input_root=input_root,
            include_resources=selected_resources,
            strict_missing_files=args.preflight_strict,
            verify_ssl=not args.no_verify_ssl,
        )

    try:
        created_json_files, converted_records = materialize_missing_json_from_csv(
            input_root, include_resources=selected_resources
        )
        if created_json_files:
            print(
                "Materialized missing JSON from CSV: "
                f"files={created_json_files} records={converted_records}"
            )
        records = load_import_records(input_root, include_resources=selected_resources)
    except (OSError, ValueError, json.JSONDecodeError, csv.Error) as exc:
        print(f"Error loading input files: {exc}", file=sys.stderr)
        return 1

    regions = records["regions"]
    site_groups = records["site_groups"]
    sites = records["sites"]
    locations = records["locations"]
    tenant_groups = records["tenant_groups"]
    tenants = records["tenants"]
    contact_groups = records["contact_groups"]
    contact_roles = records["contact_roles"]
    contacts = records["contacts"]
    contact_assignments = records["contact_assignments"]
    rack_roles = records["rack_roles"]
    rack_types = records["rack_types"]
    racks = records["racks"]
    groups = records["groups"]
    owner_groups = records["owner_groups"]
    owners = records["owners"]
    users = records["users"]
    permissions = records["permissions"]
    tokens = records["tokens"]
    rack_reservations = records["rack_reservations"]
    device_roles = records["device_roles"]
    manufacturers_file = records["manufacturers"]
    module_type_profiles = records["module_type_profiles"]
    module_types = records["module_types"]
    platforms = records["platforms"]
    device_types = records["device_types"]
    devices = records["devices"]
    virtual_chassis = records["virtual_chassis"]
    virtual_device_contexts = records["virtual_device_contexts"]
    module_bays = records["module_bays"]
    modules = records["modules"]
    interfaces = records["interfaces"]
    front_ports = records["front_ports"]
    rear_ports = records["rear_ports"]
    console_ports = records["console_ports"]
    console_server_ports = records["console_server_ports"]
    power_ports = records["power_ports"]
    power_outlets = records["power_outlets"]
    device_bays = records["device_bays"]
    inventory_items = records["inventory_items"]
    mac_addresses = records["mac_addresses"]
    cables = records["cables"]
    wireless_lan_groups = records["wireless_lan_groups"]
    wireless_lans = records["wireless_lans"]
    wireless_links = records["wireless_links"]
    rirs = records["rirs"]
    prefix_vlan_roles = records["prefix_vlan_roles"]
    route_targets = records["route_targets"]
    vrfs = records["vrfs"]
    vlan_groups = records["vlan_groups"]
    vlans = records["vlans"]
    vlan_translation_policies = records["vlan_translation_policies"]
    vlan_translation_rules = records["vlan_translation_rules"]
    aggregates = records["aggregates"]
    asns = records["asns"]
    asn_ranges = records["asn_ranges"]
    prefixes = records["prefixes"]
    ip_ranges = records["ip_ranges"]
    fhrp_groups = records["fhrp_groups"]
    ip_addresses = records["ip_addresses"]
    fhrp_group_assignments = records["fhrp_group_assignments"]
    service_templates = records["service_templates"]
    services = records["services"]
    ike_policies = records["ike_policies"]
    ike_proposals = records["ike_proposals"]
    ipsec_policies = records["ipsec_policies"]
    ipsec_profiles = records["ipsec_profiles"]
    ipsec_proposals = records["ipsec_proposals"]
    l2vpns = records["l2vpns"]
    l2vpn_terminations = records["l2vpn_terminations"]
    tunnel_groups = records["tunnel_groups"]
    tunnels = records["tunnels"]
    tunnel_terminations = records["tunnel_terminations"]
    cluster_groups = records["cluster_groups"]
    cluster_types = records["cluster_types"]
    clusters = records["clusters"]
    virtual_machines = records["virtual_machines"]
    virtual_disks = records["virtual_disks"]
    virtualization_interfaces = records["virtualization_interfaces"]
    providers = records["providers"]
    provider_accounts = records["provider_accounts"]
    provider_networks = records["provider_networks"]
    circuit_groups = records["circuit_groups"]
    circuit_types = records["circuit_types"]
    circuits = records["circuits"]
    circuit_terminations = records["circuit_terminations"]
    circuit_group_assignments = records["circuit_group_assignments"]
    virtual_circuit_types = records["virtual_circuit_types"]
    virtual_circuits = records["virtual_circuits"]
    virtual_circuit_terminations = records["virtual_circuit_terminations"]
    power_panels = records["power_panels"]
    power_feeds = records["power_feeds"]
    config_context_profiles = records["config_context_profiles"]
    config_contexts = records["config_contexts"]
    config_templates = records["config_templates"]
    custom_field_choice_sets = records["custom_field_choice_sets"]
    custom_fields = records["custom_fields"]
    tags = records["tags"]
    custom_links = records["custom_links"]
    export_templates = records["export_templates"]
    saved_filters = records["saved_filters"]
    data_sources = records["data_sources"]
    event_rules = records["event_rules"]
    notification_groups = records["notification_groups"]
    journal_entries = records["journal_entries"]
    webhooks = records["webhooks"]

    manufacturers = merge_records_by_source_id(
        manufacturers_file,
        derive_manufacturers_from_device_types(device_types),
    )

    client = NetBoxClient(
        args.url,
        args.token,
        timeout=args.timeout,
        auth_scheme=args.auth_scheme,
        verify_ssl=not args.no_verify_ssl,
    )
    required_index_resources = build_required_index_resources(selected_resources)
    client.endpoint_allowlist = build_index_endpoint_allowlist(required_index_resources)
    ctx = ImportContext(
        client=client,
        dry_run=args.dry_run,
        update_existing=args.update_existing,
        fail_fast=args.fail_fast,
        default_user_password=args.default_user_password,
    )

    print("Loading existing target indexes...")
    if client.endpoint_allowlist is not None:
        print(f"Index preload scope: {len(client.endpoint_allowlist)} endpoints")
    try:
        region_index = build_index(client.get_paginated("/api/dcim/regions/"), key_region)
        site_group_index = build_index(
            client.get_paginated("/api/dcim/site-groups/"), key_site_group
        )
        site_index = build_index(client.get_paginated("/api/dcim/sites/"), key_site)
        location_index = build_index(
            client.get_paginated("/api/dcim/locations/"), key_location
        )
        tenant_group_index = build_index(
            client.get_paginated("/api/tenancy/tenant-groups/"), key_tenant_group
        )
        tenant_index = build_index(client.get_paginated("/api/tenancy/tenants/"), key_tenant)
        contact_group_index = build_index(
            client.get_paginated("/api/tenancy/contact-groups/"), key_region
        )
        contact_role_index = build_index(
            client.get_paginated("/api/tenancy/contact-roles/"), key_region
        )
        contact_index = build_index(client.get_paginated("/api/tenancy/contacts/"), key_contact)
        contact_assignment_index = build_index(
            client.get_paginated("/api/tenancy/contact-assignments/"), key_contact_assignment
        )
        role_index = build_index(
            client.get_paginated("/api/dcim/device-roles/"), key_device_role
        )
        manufacturer_index = build_index(
            client.get_paginated("/api/dcim/manufacturers/"), key_manufacturer
        )
        platform_index = build_index(client.get_paginated("/api/dcim/platforms/"), key_platform)
        rack_role_index = build_index(client.get_paginated("/api/dcim/rack-roles/"), key_rack_role)
        rack_type_index = build_index(client.get_paginated("/api/dcim/rack-types/"), key_rack_type)
        device_type_index = build_index(
            client.get_paginated("/api/dcim/device-types/"), key_device_type
        )
        rack_index = build_index(client.get_paginated("/api/dcim/racks/"), key_rack)
        group_index = build_index(client.get_paginated("/api/users/groups/"), key_group)
        owner_group_index = build_index(client.get_paginated("/api/users/owner-groups/"), key_group)
        owner_index = build_index(client.get_paginated("/api/users/owners/"), key_owner)
        user_index = build_index(client.get_paginated("/api/users/users/"), key_user)
        permission_index = build_index(client.get_paginated("/api/users/permissions/"), key_permission)
        token_index = build_index(client.get_paginated("/api/users/tokens/"), key_token)
        rack_reservation_index = build_index(
            client.get_paginated("/api/dcim/rack-reservations/"), key_rack_reservation
        )
        device_index = build_index(client.get_paginated("/api/dcim/devices/"), key_device)
        module_type_profile_index = build_index(
            client.get_paginated("/api/dcim/module-type-profiles/"), key_module_type_profile
        )
        module_type_index = build_index(
            client.get_paginated("/api/dcim/module-types/"), key_module_type
        )
        virtual_chassis_index = build_index(
            client.get_paginated("/api/dcim/virtual-chassis/"), key_virtual_chassis
        )
        virtual_device_context_index = build_index(
            client.get_paginated("/api/dcim/virtual-device-contexts/"), key_virtual_device_context
        )
        module_bay_index = build_index(client.get_paginated("/api/dcim/module-bays/"), key_module_bay)
        module_index = build_index(client.get_paginated("/api/dcim/modules/"), key_module)
        interface_index = build_index(client.get_paginated("/api/dcim/interfaces/"), key_device_component)
        front_port_index = build_index(client.get_paginated("/api/dcim/front-ports/"), key_device_component)
        rear_port_index = build_index(client.get_paginated("/api/dcim/rear-ports/"), key_device_component)
        console_port_index = build_index(
            client.get_paginated("/api/dcim/console-ports/"), key_device_component
        )
        console_server_port_index = build_index(
            client.get_paginated("/api/dcim/console-server-ports/"), key_device_component
        )
        power_port_index = build_index(client.get_paginated("/api/dcim/power-ports/"), key_device_component)
        power_outlet_index = build_index(
            client.get_paginated("/api/dcim/power-outlets/"), key_device_component
        )
        device_bay_index = build_index(client.get_paginated("/api/dcim/device-bays/"), key_device_bay)
        inventory_item_index = build_index(
            client.get_paginated("/api/dcim/inventory-items/"), key_inventory_item
        )
        mac_address_index = build_index(client.get_paginated("/api/dcim/mac-addresses/"), key_mac_address)
        rir_index = build_index(client.get_paginated("/api/ipam/rirs/"), key_rir)
        ipam_role_index = build_index(client.get_paginated("/api/ipam/roles/"), key_ipam_role)
        route_target_index = build_index(
            client.get_paginated("/api/ipam/route-targets/"), key_route_target
        )
        vrf_index = build_index(client.get_paginated("/api/ipam/vrfs/"), key_vrf)
        vlan_group_index = build_index(
            client.get_paginated("/api/ipam/vlan-groups/"), key_vlan_group
        )
        vlan_index = build_index(client.get_paginated("/api/ipam/vlans/"), key_vlan)
        vlan_translation_policy_index = build_index(
            client.get_paginated("/api/ipam/vlan-translation-policies/"), key_vlan_translation_policy
        )
        vlan_translation_rule_index = build_index(
            client.get_paginated("/api/ipam/vlan-translation-rules/"), key_vlan_translation_rule
        )
        aggregate_index = build_index(client.get_paginated("/api/ipam/aggregates/"), key_aggregate)
        asn_index = build_index(client.get_paginated("/api/ipam/asns/"), key_asn)
        asn_range_index = build_index(
            client.get_paginated("/api/ipam/asn-ranges/"), key_asn_range
        )
        prefix_index = build_index(client.get_paginated("/api/ipam/prefixes/"), key_prefix)
        ip_range_index = build_index(client.get_paginated("/api/ipam/ip-ranges/"), key_ip_range)
        fhrp_group_index = build_index(client.get_paginated("/api/ipam/fhrp-groups/"), key_fhrp_group)
        ip_address_index = build_index(client.get_paginated("/api/ipam/ip-addresses/"), key_ip_address)
        fhrp_group_assignment_index = build_index(
            client.get_paginated("/api/ipam/fhrp-group-assignments/"), key_fhrp_group_assignment
        )
        service_template_index = build_index(
            client.get_paginated("/api/ipam/service-templates/"), key_service_template
        )
        service_index = build_index(client.get_paginated("/api/ipam/services/"), key_service)
        cluster_group_index = build_index(
            client.get_paginated("/api/virtualization/cluster-groups/"), key_cluster_group
        )
        cluster_type_index = build_index(
            client.get_paginated("/api/virtualization/cluster-types/"), key_cluster_type
        )
        cluster_index = build_index(client.get_paginated("/api/virtualization/clusters/"), key_cluster)
        virtual_machine_index = build_index(
            client.get_paginated("/api/virtualization/virtual-machines/"), key_virtual_machine
        )
        virtual_disk_index = build_index(
            client.get_paginated("/api/virtualization/virtual-disks/"), key_virtual_disk
        )
        virtualization_interface_index = build_index(
            client.get_paginated("/api/virtualization/interfaces/"), key_virtualization_interface
        )
        provider_index = build_index(client.get_paginated("/api/circuits/providers/"), key_provider)
        provider_account_index = build_index(
            client.get_paginated("/api/circuits/provider-accounts/"), key_provider_account
        )
        provider_network_index = build_index(
            client.get_paginated("/api/circuits/provider-networks/"), key_provider_network
        )
        circuit_group_index = build_index(
            client.get_paginated("/api/circuits/circuit-groups/"), key_circuit_group
        )
        circuit_type_index = build_index(
            client.get_paginated("/api/circuits/circuit-types/"), key_circuit_type
        )
        circuit_index = build_index(client.get_paginated("/api/circuits/circuits/"), key_circuit)
        circuit_termination_index = build_index(
            client.get_paginated("/api/circuits/circuit-terminations/"), key_circuit_termination
        )
        circuit_group_assignment_index = build_index(
            client.get_paginated("/api/circuits/circuit-group-assignments/"),
            key_circuit_group_assignment,
        )
        virtual_circuit_type_index = build_index(
            client.get_paginated("/api/circuits/virtual-circuit-types/"), key_virtual_circuit_type
        )
        virtual_circuit_index = build_index(
            client.get_paginated("/api/circuits/virtual-circuits/"), key_virtual_circuit
        )
        virtual_circuit_termination_index = build_index(
            client.get_paginated("/api/circuits/virtual-circuit-terminations/"),
            key_virtual_circuit_termination,
        )
        power_panel_index = build_index(
            client.get_paginated("/api/dcim/power-panels/"), key_power_panel
        )
        power_feed_index = build_index(client.get_paginated("/api/dcim/power-feeds/"), key_power_feed)
        custom_field_choice_set_index = build_index(
            client.get_paginated("/api/extras/custom-field-choice-sets/"),
            key_custom_field_choice_set,
        )
        custom_field_index = build_index(
            client.get_paginated("/api/extras/custom-fields/"), key_custom_field
        )
        tag_index = build_index(client.get_paginated("/api/extras/tags/"), key_tag)
        custom_link_index = build_index(
            client.get_paginated("/api/extras/custom-links/"), key_custom_link
        )
        export_template_index = build_index(
            client.get_paginated("/api/extras/export-templates/"), key_export_template
        )
        saved_filter_index = build_index(
            client.get_paginated("/api/extras/saved-filters/"), key_saved_filter
        )
        config_context_profile_index = build_index(
            client.get_paginated("/api/extras/config-context-profiles/"), key_name_slug_or_name
        )
        config_context_index = build_index(
            client.get_paginated("/api/extras/config-contexts/"), key_name_slug_or_name
        )
        config_template_index = build_index(
            client.get_paginated("/api/extras/config-templates/"), key_name_slug_or_name
        )
        data_source_index = build_index(
            client.get_paginated("/api/core/data-sources/"), key_name_slug_or_name
        )
        event_rule_index = build_index(
            client.get_paginated("/api/extras/event-rules/"), key_name_slug_or_name
        )
        notification_group_index = build_index(
            client.get_paginated("/api/extras/notification-groups/"), key_name_slug_or_name
        )
        journal_entry_index = build_index(
            client.get_paginated("/api/extras/journal-entries/"), key_journal_entry
        )
        webhook_index = build_index(
            client.get_paginated("/api/extras/webhooks/"), key_name_slug_or_name
        )
        wireless_lan_group_index = build_index(
            client.get_paginated("/api/wireless/wireless-lan-groups/"), key_name_slug_or_name
        )
        wireless_lan_index = build_index(
            client.get_paginated("/api/wireless/wireless-lans/"), key_name_slug_or_name
        )
        wireless_link_index = build_index(
            client.get_paginated("/api/wireless/wireless-links/"), key_name_slug_or_name
        )
        ike_policy_index = build_index(
            client.get_paginated("/api/vpn/ike-policies/"), key_name_slug_or_name
        )
        ike_proposal_index = build_index(
            client.get_paginated("/api/vpn/ike-proposals/"), key_name_slug_or_name
        )
        ipsec_policy_index = build_index(
            client.get_paginated("/api/vpn/ipsec-policies/"), key_name_slug_or_name
        )
        ipsec_profile_index = build_index(
            client.get_paginated("/api/vpn/ipsec-profiles/"), key_name_slug_or_name
        )
        ipsec_proposal_index = build_index(
            client.get_paginated("/api/vpn/ipsec-proposals/"), key_name_slug_or_name
        )
        l2vpn_index = build_index(client.get_paginated("/api/vpn/l2vpns/"), key_name_slug_or_name)
        l2vpn_termination_index = build_index(
            client.get_paginated("/api/vpn/l2vpn-terminations/"), key_name_slug_or_name
        )
        tunnel_group_index = build_index(
            client.get_paginated("/api/vpn/tunnel-groups/"), key_name_slug_or_name
        )
        tunnel_index = build_index(client.get_paginated("/api/vpn/tunnels/"), key_name_slug_or_name)
        tunnel_termination_index = build_index(
            client.get_paginated("/api/vpn/tunnel-terminations/"), key_name_slug_or_name
        )
        cable_index = build_index(client.get_paginated("/api/dcim/cables/"), key_cable)
    except (RuntimeError, NetBoxApiError, ValueError) as exc:
        print(f"Error loading existing data from target: {exc}", file=sys.stderr)
        return 1

    mode = "DRY-RUN" if args.dry_run else "APPLY"
    print(f"Starting import mode: {mode}")
    print(f"Input directory: {input_root}")
    if selected_resources is not None:
        selected_sections = sorted({RESOURCE_TO_SECTION[name] for name in selected_resources})
        print(
            "Scope filter (--only): "
            f"{len(selected_resources)} resources across {len(selected_sections)} sections"
        )
        print(f"Filtered sections: {', '.join(selected_sections)}")

    selected_stats_order = [
        name for name in IMPORT_STATS_ORDER if selected_resources is None or name in selected_resources
    ]
    rollback_manifest_path: Path | None = None
    if not args.dry_run:
        rollback_manifest_path = resolve_rollback_manifest_path(input_root, args.rollback_manifest)

    import_exit_code = 0
    try:
        steps: list[tuple[str, Any]] = [
            ("regions", lambda: import_regions(ctx, regions, region_index)),
            ("site_groups", lambda: import_site_groups(ctx, site_groups, site_group_index)),
            ("sites", lambda: import_sites(ctx, sites, site_index, region_index, site_group_index)),
            ("locations", lambda: import_locations(ctx, locations, location_index, site_index)),
            ("tenant_groups", lambda: import_tenant_groups(ctx, tenant_groups, tenant_group_index)),
            ("tenants", lambda: import_tenants(ctx, tenants, tenant_index, tenant_group_index)),
            (
                "contact_groups",
                lambda: import_generic_resource(
                    ctx,
                    "organization",
                    "contact_groups",
                    contact_groups,
                    contact_group_index,
                    key_region,
                ),
            ),
            (
                "contact_roles",
                lambda: import_generic_resource(
                    ctx,
                    "organization",
                    "contact_roles",
                    contact_roles,
                    contact_role_index,
                    key_region,
                ),
            ),
            (
                "contacts",
                lambda: import_generic_resource(
                    ctx,
                    "organization",
                    "contacts",
                    contacts,
                    contact_index,
                    key_contact,
                ),
            ),
            (
                "contact_assignments",
                lambda: import_generic_resource(
                    ctx,
                    "organization",
                    "contact_assignments",
                    contact_assignments,
                    contact_assignment_index,
                    key_contact_assignment,
                ),
            ),
            ("rack_roles", lambda: import_rack_roles(ctx, rack_roles, rack_role_index)),
            (
                "rack_types",
                lambda: import_generic_resource(
                    ctx,
                    "racks",
                    "rack_types",
                    rack_types,
                    rack_type_index,
                    key_rack_type,
                ),
            ),
            (
                "racks",
                lambda: import_racks(
                    ctx,
                    racks,
                    rack_index,
                    site_index,
                    location_index,
                    tenant_index,
                    rack_role_index,
                ),
            ),
            (
                "groups",
                lambda: import_generic_resource(
                    ctx,
                    "admin",
                    "groups",
                    groups,
                    group_index,
                    key_group,
                    ["name", "description"],
                ),
            ),
            (
                "owner_groups",
                lambda: import_generic_resource(
                    ctx,
                    "admin",
                    "owner_groups",
                    owner_groups,
                    owner_group_index,
                    key_group,
                    ["name", "description"],
                ),
            ),
            (
                "owners",
                lambda: import_generic_resource(
                    ctx,
                    "admin",
                    "owners",
                    owners,
                    owner_index,
                    key_owner,
                    ["name", "description", "group"],
                ),
            ),
            (
                "users",
                lambda: import_users(
                    ctx,
                    users,
                    user_index,
                ),
            ),
            (
                "permissions",
                lambda: import_generic_resource(
                    ctx,
                    "admin",
                    "permissions",
                    permissions,
                    permission_index,
                    key_permission,
                    [
                        "name",
                        "description",
                        "enabled",
                        "object_types",
                        "actions",
                        "constraints",
                        "groups",
                        "users",
                    ],
                ),
            ),
            (
                "tokens",
                lambda: import_generic_resource(
                    ctx,
                    "admin",
                    "tokens",
                    tokens,
                    token_index,
                    key_token,
                    ["user", "description", "enabled", "write_enabled", "allowed_ips", "expires"],
                ),
            ),
            (
                "rack_reservations",
                lambda: import_rack_reservations(
                    ctx,
                    rack_reservations,
                    rack_reservation_index,
                    rack_index,
                    tenant_index,
                    user_index,
                ),
            ),
            ("device_roles", lambda: import_device_roles(ctx, device_roles, role_index)),
            ("manufacturers", lambda: import_manufacturers(ctx, manufacturers, manufacturer_index)),
            (
                "module_type_profiles",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "module_type_profiles",
                    module_type_profiles,
                    module_type_profile_index,
                    key_module_type_profile,
                ),
            ),
            (
                "module_types",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "module_types",
                    module_types,
                    module_type_index,
                    key_module_type,
                ),
            ),
            ("platforms", lambda: import_platforms(ctx, platforms, platform_index, manufacturer_index)),
            (
                "device_types",
                lambda: import_device_types(ctx, device_types, device_type_index, manufacturer_index),
            ),
            (
                "devices",
                lambda: import_devices(
                    ctx,
                    devices,
                    device_index,
                    site_index,
                    tenant_index,
                    role_index,
                    platform_index,
                    device_type_index,
                    location_index,
                    rack_index,
                ),
            ),
            (
                "virtual_chassis",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "virtual_chassis",
                    virtual_chassis,
                    virtual_chassis_index,
                    key_virtual_chassis,
                ),
            ),
            (
                "virtual_device_contexts",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "virtual_device_contexts",
                    virtual_device_contexts,
                    virtual_device_context_index,
                    key_virtual_device_context,
                ),
            ),
            (
                "module_bays",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "module_bays",
                    module_bays,
                    module_bay_index,
                    key_module_bay,
                ),
            ),
            (
                "modules",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "modules",
                    modules,
                    module_index,
                    key_module,
                ),
            ),
            (
                "interfaces",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "interfaces",
                    interfaces,
                    interface_index,
                    key_device_component,
                ),
            ),
            (
                "front_ports",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "front_ports",
                    front_ports,
                    front_port_index,
                    key_device_component,
                ),
            ),
            (
                "rear_ports",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "rear_ports",
                    rear_ports,
                    rear_port_index,
                    key_device_component,
                    [
                        "device",
                        "module",
                        "name",
                        "label",
                        "type",
                        "color",
                        "positions",
                        "description",
                        "mark_connected",
                        "tags",
                        "custom_fields",
                    ],
                ),
            ),
            (
                "console_ports",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "console_ports",
                    console_ports,
                    console_port_index,
                    key_device_component,
                ),
            ),
            (
                "console_server_ports",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "console_server_ports",
                    console_server_ports,
                    console_server_port_index,
                    key_device_component,
                ),
            ),
            (
                "power_ports",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "power_ports",
                    power_ports,
                    power_port_index,
                    key_device_component,
                ),
            ),
            (
                "power_outlets",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "power_outlets",
                    power_outlets,
                    power_outlet_index,
                    key_device_component,
                ),
            ),
            (
                "device_bays",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "device_bays",
                    device_bays,
                    device_bay_index,
                    key_device_bay,
                ),
            ),
            (
                "inventory_items",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "inventory_items",
                    inventory_items,
                    inventory_item_index,
                    key_inventory_item,
                ),
            ),
            (
                "mac_addresses",
                lambda: import_generic_resource(
                    ctx,
                    "devices",
                    "mac_addresses",
                    mac_addresses,
                    mac_address_index,
                    key_mac_address,
                ),
            ),
            (
                "wireless_lan_groups",
                lambda: import_generic_resource(
                    ctx,
                    "wireless",
                    "wireless_lan_groups",
                    wireless_lan_groups,
                    wireless_lan_group_index,
                ),
            ),
            (
                "wireless_lans",
                lambda: import_generic_resource(
                    ctx,
                    "wireless",
                    "wireless_lans",
                    wireless_lans,
                    wireless_lan_index,
                ),
            ),
            (
                "wireless_links",
                lambda: import_generic_resource(
                    ctx,
                    "wireless",
                    "wireless_links",
                    wireless_links,
                    wireless_link_index,
                ),
            ),
            ("rirs", lambda: import_rirs(ctx, rirs, rir_index)),
            (
                "prefix_vlan_roles",
                lambda: import_ipam_roles(ctx, prefix_vlan_roles, ipam_role_index),
            ),
            (
                "route_targets",
                lambda: import_route_targets(ctx, route_targets, route_target_index, tenant_index),
            ),
            (
                "vrfs",
                lambda: import_vrfs(
                    ctx,
                    vrfs,
                    vrf_index,
                    tenant_index,
                    route_target_index,
                ),
            ),
            (
                "vlan_groups",
                lambda: import_vlan_groups(
                    ctx,
                    vlan_groups,
                    vlan_group_index,
                    tenant_index,
                    site_index,
                    location_index,
                    region_index,
                    site_group_index,
                ),
            ),
            (
                "vlans",
                lambda: import_vlans(
                    ctx,
                    vlans,
                    vlan_index,
                    tenant_index,
                    site_index,
                    ipam_role_index,
                    vlan_group_index,
                    vlan_index,
                ),
            ),
            (
                "vlan_translation_policies",
                lambda: import_generic_resource(
                    ctx,
                    "ipam",
                    "vlan_translation_policies",
                    vlan_translation_policies,
                    vlan_translation_policy_index,
                    key_vlan_translation_policy,
                ),
            ),
            (
                "vlan_translation_rules",
                lambda: import_generic_resource(
                    ctx,
                    "ipam",
                    "vlan_translation_rules",
                    vlan_translation_rules,
                    vlan_translation_rule_index,
                    key_vlan_translation_rule,
                ),
            ),
            (
                "aggregates",
                lambda: import_aggregates(
                    ctx,
                    aggregates,
                    aggregate_index,
                    rir_index,
                    tenant_index,
                ),
            ),
            (
                "asns",
                lambda: import_asns(
                    ctx,
                    asns,
                    asn_index,
                    rir_index,
                    tenant_index,
                    site_index,
                ),
            ),
            (
                "asn_ranges",
                lambda: import_asn_ranges(
                    ctx,
                    asn_ranges,
                    asn_range_index,
                    rir_index,
                    tenant_index,
                ),
            ),
            (
                "prefixes",
                lambda: import_prefixes(
                    ctx,
                    prefixes,
                    prefix_index,
                    vrf_index,
                    tenant_index,
                    vlan_index,
                    ipam_role_index,
                    site_index,
                    location_index,
                    region_index,
                    site_group_index,
                ),
            ),
            (
                "ip_ranges",
                lambda: import_ip_ranges(
                    ctx,
                    ip_ranges,
                    ip_range_index,
                    vrf_index,
                    tenant_index,
                    ipam_role_index,
                ),
            ),
            (
                "fhrp_groups",
                lambda: import_generic_resource(
                    ctx,
                    "ipam",
                    "fhrp_groups",
                    fhrp_groups,
                    fhrp_group_index,
                    key_fhrp_group,
                ),
            ),
            (
                "ip_addresses",
                lambda: import_generic_resource(
                    ctx,
                    "ipam",
                    "ip_addresses",
                    ip_addresses,
                    ip_address_index,
                    key_ip_address,
                ),
            ),
            (
                "fhrp_group_assignments",
                lambda: import_generic_resource(
                    ctx,
                    "ipam",
                    "fhrp_group_assignments",
                    fhrp_group_assignments,
                    fhrp_group_assignment_index,
                    key_fhrp_group_assignment,
                ),
            ),
            (
                "service_templates",
                lambda: import_generic_resource(
                    ctx,
                    "ipam",
                    "service_templates",
                    service_templates,
                    service_template_index,
                    key_service_template,
                ),
            ),
            (
                "services",
                lambda: import_generic_resource(
                    ctx,
                    "ipam",
                    "services",
                    services,
                    service_index,
                    key_service,
                ),
            ),
            (
                "ike_policies",
                lambda: import_generic_resource(
                    ctx, "vpn", "ike_policies", ike_policies, ike_policy_index
                ),
            ),
            (
                "ike_proposals",
                lambda: import_generic_resource(
                    ctx, "vpn", "ike_proposals", ike_proposals, ike_proposal_index
                ),
            ),
            (
                "ipsec_policies",
                lambda: import_generic_resource(
                    ctx, "vpn", "ipsec_policies", ipsec_policies, ipsec_policy_index
                ),
            ),
            (
                "ipsec_profiles",
                lambda: import_generic_resource(
                    ctx, "vpn", "ipsec_profiles", ipsec_profiles, ipsec_profile_index
                ),
            ),
            (
                "ipsec_proposals",
                lambda: import_generic_resource(
                    ctx, "vpn", "ipsec_proposals", ipsec_proposals, ipsec_proposal_index
                ),
            ),
            (
                "l2vpns",
                lambda: import_generic_resource(ctx, "vpn", "l2vpns", l2vpns, l2vpn_index),
            ),
            (
                "l2vpn_terminations",
                lambda: import_generic_resource(
                    ctx, "vpn", "l2vpn_terminations", l2vpn_terminations, l2vpn_termination_index
                ),
            ),
            (
                "tunnel_groups",
                lambda: import_generic_resource(
                    ctx, "vpn", "tunnel_groups", tunnel_groups, tunnel_group_index
                ),
            ),
            (
                "tunnels",
                lambda: import_generic_resource(ctx, "vpn", "tunnels", tunnels, tunnel_index),
            ),
            (
                "tunnel_terminations",
                lambda: import_generic_resource(
                    ctx, "vpn", "tunnel_terminations", tunnel_terminations, tunnel_termination_index
                ),
            ),
            (
                "cluster_groups",
                lambda: import_generic_resource(
                    ctx,
                    "virtualization",
                    "cluster_groups",
                    cluster_groups,
                    cluster_group_index,
                    key_cluster_group,
                    ["name", "slug", "description", "comments", "tags", "custom_fields"],
                ),
            ),
            (
                "cluster_types",
                lambda: import_generic_resource(
                    ctx,
                    "virtualization",
                    "cluster_types",
                    cluster_types,
                    cluster_type_index,
                    key_cluster_type,
                    ["name", "slug", "description", "comments", "tags", "custom_fields"],
                ),
            ),
            (
                "clusters",
                lambda: import_clusters(
                    ctx,
                    clusters,
                    cluster_index,
                    cluster_type_index,
                    cluster_group_index,
                    tenant_index,
                    site_index,
                    location_index,
                    region_index,
                    site_group_index,
                ),
            ),
            (
                "virtual_machines",
                lambda: import_virtual_machines(
                    ctx,
                    virtual_machines,
                    virtual_machine_index,
                    cluster_index,
                    site_index,
                    device_index,
                    role_index,
                    tenant_index,
                    platform_index,
                ),
            ),
            (
                "virtual_disks",
                lambda: import_generic_resource(
                    ctx,
                    "virtualization",
                    "virtual_disks",
                    virtual_disks,
                    virtual_disk_index,
                    key_virtual_disk,
                ),
            ),
            (
                "virtualization_interfaces",
                lambda: import_generic_resource(
                    ctx,
                    "virtualization",
                    "virtualization_interfaces",
                    virtualization_interfaces,
                    virtualization_interface_index,
                    key_virtualization_interface,
                ),
            ),
            (
                "providers",
                lambda: import_providers(
                    ctx,
                    providers,
                    provider_index,
                    asn_index,
                ),
            ),
            (
                "provider_accounts",
                lambda: import_generic_resource(
                    ctx,
                    "circuits",
                    "provider_accounts",
                    provider_accounts,
                    provider_account_index,
                    key_provider_account,
                ),
            ),
            (
                "provider_networks",
                lambda: import_provider_networks(
                    ctx,
                    provider_networks,
                    provider_network_index,
                    provider_index,
                ),
            ),
            (
                "circuit_groups",
                lambda: import_generic_resource(
                    ctx,
                    "circuits",
                    "circuit_groups",
                    circuit_groups,
                    circuit_group_index,
                    key_circuit_group,
                ),
            ),
            (
                "circuit_types",
                lambda: import_generic_resource(
                    ctx,
                    "circuits",
                    "circuit_types",
                    circuit_types,
                    circuit_type_index,
                    key_circuit_type,
                    ["name", "slug", "color", "description", "comments", "tags", "custom_fields"],
                ),
            ),
            (
                "circuits",
                lambda: import_circuits(
                    ctx,
                    circuits,
                    circuit_index,
                    provider_index,
                    circuit_type_index,
                    tenant_index,
                ),
            ),
            (
                "circuit_terminations",
                lambda: import_circuit_terminations(
                    ctx,
                    circuit_terminations,
                    circuit_termination_index,
                    circuit_index,
                    provider_network_index,
                    site_index,
                    location_index,
                    region_index,
                    site_group_index,
                ),
            ),
            (
                "cables",
                lambda: import_cables(
                    ctx,
                    cables,
                    cable_index,
                    tenant_index,
                ),
            ),
            (
                "circuit_group_assignments",
                lambda: import_generic_resource(
                    ctx,
                    "circuits",
                    "circuit_group_assignments",
                    circuit_group_assignments,
                    circuit_group_assignment_index,
                    key_circuit_group_assignment,
                ),
            ),
            (
                "virtual_circuit_types",
                lambda: import_generic_resource(
                    ctx,
                    "circuits",
                    "virtual_circuit_types",
                    virtual_circuit_types,
                    virtual_circuit_type_index,
                    key_virtual_circuit_type,
                ),
            ),
            (
                "virtual_circuits",
                lambda: import_generic_resource(
                    ctx,
                    "circuits",
                    "virtual_circuits",
                    virtual_circuits,
                    virtual_circuit_index,
                    key_virtual_circuit,
                ),
            ),
            (
                "virtual_circuit_terminations",
                lambda: import_generic_resource(
                    ctx,
                    "circuits",
                    "virtual_circuit_terminations",
                    virtual_circuit_terminations,
                    virtual_circuit_termination_index,
                    key_virtual_circuit_termination,
                ),
            ),
            (
                "power_panels",
                lambda: import_power_panels(
                    ctx,
                    power_panels,
                    power_panel_index,
                    site_index,
                    location_index,
                ),
            ),
            (
                "power_feeds",
                lambda: import_power_feeds(
                    ctx,
                    power_feeds,
                    power_feed_index,
                    power_panel_index,
                    rack_index,
                    tenant_index,
                ),
            ),
            (
                "config_context_profiles",
                lambda: import_generic_resource(
                    ctx,
                    "provisioning",
                    "config_context_profiles",
                    config_context_profiles,
                    config_context_profile_index,
                    key_name_slug_or_name,
                    ["name", "description", "schema", "tags", "data_source", "data_file"],
                ),
            ),
            (
                "config_contexts",
                lambda: import_generic_resource(
                    ctx,
                    "provisioning",
                    "config_contexts",
                    config_contexts,
                    config_context_index,
                    key_name_slug_or_name,
                    [
                        "name",
                        "weight",
                        "profile",
                        "description",
                        "is_active",
                        "regions",
                        "site_groups",
                        "sites",
                        "locations",
                        "device_types",
                        "roles",
                        "platforms",
                        "cluster_types",
                        "cluster_groups",
                        "clusters",
                        "tenant_groups",
                        "tenants",
                        "tags",
                        "data_source",
                        "data_file",
                        "data",
                    ],
                ),
            ),
            (
                "config_templates",
                lambda: import_generic_resource(
                    ctx,
                    "provisioning",
                    "config_templates",
                    config_templates,
                    config_template_index,
                    key_name_slug_or_name,
                    [
                        "name",
                        "description",
                        "environment_params",
                        "template_code",
                        "mime_type",
                        "file_name",
                        "file_extension",
                        "as_attachment",
                        "data_source",
                        "data_file",
                        "auto_sync_enabled",
                        "tags",
                    ],
                ),
            ),
            (
                "custom_field_choice_sets",
                lambda: import_generic_resource(
                    ctx,
                    "customization",
                    "custom_field_choice_sets",
                    custom_field_choice_sets,
                    custom_field_choice_set_index,
                    key_custom_field_choice_set,
                    [
                        "name",
                        "description",
                        "base_choices",
                        "extra_choices",
                        "order_alphabetically",
                    ],
                ),
            ),
            (
                "custom_fields",
                lambda: import_custom_fields(
                    ctx,
                    custom_fields,
                    custom_field_index,
                    custom_field_choice_set_index,
                ),
            ),
            (
                "tags",
                lambda: import_generic_resource(
                    ctx,
                    "customization",
                    "tags",
                    tags,
                    tag_index,
                    key_tag,
                    ["name", "slug", "color", "description", "weight", "object_types"],
                ),
            ),
            (
                "custom_links",
                lambda: import_generic_resource(
                    ctx,
                    "customization",
                    "custom_links",
                    custom_links,
                    custom_link_index,
                    key_custom_link,
                ),
            ),
            (
                "export_templates",
                lambda: import_generic_resource(
                    ctx,
                    "customization",
                    "export_templates",
                    export_templates,
                    export_template_index,
                    key_export_template,
                ),
            ),
            (
                "saved_filters",
                lambda: import_generic_resource(
                    ctx,
                    "customization",
                    "saved_filters",
                    saved_filters,
                    saved_filter_index,
                    key_saved_filter,
                ),
            ),
            (
                "data_sources",
                lambda: import_generic_resource(
                    ctx,
                    "operations",
                    "data_sources",
                    data_sources,
                    data_source_index,
                    key_name_slug_or_name,
                    [
                        "name",
                        "type",
                        "source_url",
                        "enabled",
                        "description",
                        "sync_interval",
                        "parameters",
                        "ignore_rules",
                        "comments",
                        "custom_fields",
                    ],
                ),
            ),
            (
                "event_rules",
                lambda: import_generic_resource(
                    ctx,
                    "operations",
                    "event_rules",
                    event_rules,
                    event_rule_index,
                    key_name_slug_or_name,
                    [
                        "object_types",
                        "name",
                        "enabled",
                        "event_types",
                        "conditions",
                        "action_type",
                        "action_object_type",
                        "action_object_id",
                        "description",
                        "custom_fields",
                        "tags",
                    ],
                ),
            ),
            (
                "notification_groups",
                lambda: import_generic_resource(
                    ctx,
                    "operations",
                    "notification_groups",
                    notification_groups,
                    notification_group_index,
                    key_name_slug_or_name,
                    ["name", "description", "groups", "users"],
                ),
            ),
            (
                "journal_entries",
                lambda: import_generic_resource(
                    ctx,
                    "operations",
                    "journal_entries",
                    journal_entries,
                    journal_entry_index,
                    key_journal_entry,
                ),
            ),
            (
                "webhooks",
                lambda: import_generic_resource(
                    ctx,
                    "operations",
                    "webhooks",
                    webhooks,
                    webhook_index,
                    key_name_slug_or_name,
                    [
                        "name",
                        "description",
                        "payload_url",
                        "http_method",
                        "http_content_type",
                        "additional_headers",
                        "body_template",
                        "secret",
                        "ssl_verification",
                        "ca_file_path",
                        "custom_fields",
                        "tags",
                    ],
                ),
            ),
        ]
        if selected_resources is not None:
            steps = [
                (step_name, step_fn)
                for step_name, step_fn in steps
                if step_name in selected_resources
            ]
            if not steps:
                print("Error: no import steps matched --only filter", file=sys.stderr)
                return 2
        total_steps = len(steps)
        selected_stats_order = [step_name for step_name, _ in steps]
        for idx, (step_name, step_fn) in enumerate(steps, start=1):
            print(f"[{idx}/{total_steps}] {step_name}")
            step_fn()
    except RuntimeError as exc:
        print(f"Import halted: {exc}", file=sys.stderr)
        print_stats(ctx, stats_order=selected_stats_order)
        import_exit_code = 1

    if import_exit_code == 0:
        print_stats(ctx, stats_order=selected_stats_order)
        if ctx.errors:
            import_exit_code = 1

    if rollback_manifest_path is not None:
        try:
            write_rollback_manifest(
                rollback_manifest_path,
                ctx,
                args.url,
                input_root,
                mode,
                selected_resources,
            )
        except OSError as exc:
            print(f"Error writing rollback manifest: {exc}", file=sys.stderr)
            return 1
        print(f"[done] Rollback manifest: {rollback_manifest_path}")

    return import_exit_code


if __name__ == "__main__":
    raise SystemExit(main())