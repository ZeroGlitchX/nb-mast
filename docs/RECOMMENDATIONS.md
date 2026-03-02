## Recommendations

**Structural:**

The import script at 5K lines is manageable today but will get harder to maintain as NetBox adds resource types. The natural split would be: `import_core.py` (client, context, upsert, resolve utilities) + `import_resources.py` (all the `import_*` and `key_*` functions) + `import_cli.py` (main, args, orchestration). The `key_*` functions specifically could use a registry pattern since ~15 of them are identical to `key_region` (slug-or-name lookup).

**Operational gaps:**

There's no selective import. If I only want to sync IPAM resources, I still load and resolve everything. A `--only organization,ipam` flag that filters the steps list would save significant time for targeted operations - especially for your Lifetime workflow where you're iterating on one section at a time.

No rollback capability. If an import creates 200 objects and fails on #201, there's no record of what was created that could be used to undo. The `ctx.maps` hold the created IDs - writing those to a rollback manifest would make cleanup possible.

**For your specific NetBox work:**

The scope resolution (`resolve_scope_by_ref`) handles dcim.site, dcim.location, dcim.region, and dcim.sitegroup - exactly what you needed for the prefix scope work we've been doing. The import script already handles the `scope_type` + `scope_id` pattern correctly through `remap_known_typed_object_fields`. That's a good sign - it means the prefix imports you've been doing by hand with my one-off scripts are already handled natively in this framework.