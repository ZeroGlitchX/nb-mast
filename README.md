# NetBox Export/Import Toolkit

## NetBox Migration and Sync Tool (NB-MAST)

This workspace includes:

- `generate_export_templates.py` to build header-only CSV templates for importable NetBox resources
- `netbox_export.py` to export ordered resources to JSON and CSV
- `netbox_import.py` compatibility wrapper for importer CLI execution
- `import_core.py` shared importer constants/client/preflight/file loaders
- `import_resources.py` importer keying, normalization, and resource handlers
- `import_cli.py` importer CLI orchestration (`main`)

## Canonical directories

- `export_templates/` generated CSV templates
- `exports/` exported JSON/CSV data plus `export_manifest.json`


## Repository policy

- GitHub is used for source code and project docs only.
- Generated runtime artifacts stay local and are intentionally ignored (for example: `exports/`, `exports_flat_*`, `export_templates/`, `export_templates_flat/`, rollback manifests, and `.env`).
- Smoke tests are currently local-only and optional for contributors.


## Environment file (.env)

All three scripts now auto-load `.env` from the current directory (or script directory).

Example `.env` (copy from `.env.example`):

```bash
NETBOX_URL=http://192.168.65.40:8000
NETBOX_AUTH_SCHEME=Bearer
NETBOX_API_TOKEN=<FULL_V2_TOKEN>
NETBOX_AUTH_SCHEME_FALLBACK=Token
NETBOX_API_TOKEN_FALLBACK=<V1_TOKEN>
```

With `.env` in place, `--url` and `--token` are optional.
Use `NETBOX_AUTH_SCHEME_FALLBACK` + `NETBOX_API_TOKEN_FALLBACK` for v1 failback by overriding env vars per command.

V2 key Bearer test:

```bash
set -a; source .env; set +a
curl -X GET \
  -H "Authorization: Bearer ${NETBOX_API_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json; indent=4" \
  "${NETBOX_URL}/api/status/"
```

## Shared resource catalog

`netbox_export.py` and `generate_export_templates.py` both use `SECTION_ENDPOINTS` from `generate_export_templates.py`, so export ordering and template naming stay synchronized.

## Smoke tests (local optional)

Run smoke tests locally when desired (no required GitHub Actions check is configured at this time):

```bash
python3 -m unittest discover -s smoke_tests -p "test_*.py" -v
```

Current local smoke coverage:
- `--only` valid/invalid selector behavior and fail-fast error path
- preflight warning vs blocker behavior (`--preflight` vs strict mode semantics)
- rollback manifest structure and apply/dry-run write behavior


## Generate templates

```bash
python3 generate_export_templates.py \
  --url http://192.168.65.40:8000 \
  --token "<API_TOKEN>" \
  --output-dir export_templates
```

Flat/safe templates (nested/multi-value fields excluded):

```bash
python3 generate_export_templates.py \
  --url http://192.168.65.40:8000 \
  --token "<API_TOKEN>" \
  --output-dir export_templates_flat \
  --csv-flat
```

## Export data

```bash
python3 netbox_export.py \
  --url http://192.168.65.40:8000 \
  --token "<API_TOKEN>" \
  --auth-scheme Token \
  --output-dir exports
```

Or:

```bash
export NETBOX_API_TOKEN="<API_TOKEN>"
python3 netbox_export.py --url http://192.168.65.40:8000 --output-dir exports
```

For instances with self-signed certificates, add `--no-verify-ssl`:

```bash
python3 netbox_export.py \
  --url http://192.168.65.40:8000 \
  --token "<API_TOKEN>" \
  --auth-scheme Token \
  --no-verify-ssl \
  --output-dir exports
```

Flat/safe CSV export (nested/list columns excluded from CSV):

```bash
python3 netbox_export.py \
  --url http://192.168.65.40:8000 \
  --token "<API_TOKEN>" \
  --output-dir exports_flat \
  --csv-flat
```

## Import data

Dry-run:

```bash
python3 netbox_import.py \
  --url http://192.168.65.40:8000 \
  --token "<API_TOKEN>" \
  --input-dir exports \
  --dry-run
```

Apply:

```bash
python3 netbox_import.py \
  --url http://192.168.65.40:8000 \
  --token "<API_TOKEN>" \
  --input-dir exports
```

For instances with self-signed certificates, add `--no-verify-ssl` to any import command:

```bash
python3 netbox_import.py \
  --url http://192.168.65.40:8000 \
  --token "<API_TOKEN>" \
  --input-dir exports \
  --no-verify-ssl
```

Apply and update matched existing objects:

```bash
python3 netbox_import.py \
  --url http://192.168.65.40:8000 \
  --token "<API_TOKEN>" \
  --input-dir exports \
  --update-existing
```

Import only selected sections/resources:

```bash
python3 netbox_import.py \
  --url http://192.168.65.40:8000 \
  --token "<API_TOKEN>" \
  --input-dir exports \
  --dry-run \
  --only ipam,organization/sites
```

`--only` accepts comma-separated section names (for example `ipam`) and/or section/resource selectors (for example `organization/sites`).

For admin user imports, if exported user records do not include passwords, provide a default password:

```bash
python3 netbox_import.py \
  --url http://192.168.65.40:8000 \
  --token "<API_TOKEN>" \
  --input-dir exports \
  --default-user-password "<PASSWORD>"
```


JSON import behavior:

- Import execution is JSON-first and JSON-ingest only.
- If a resource JSON file is missing but the matching CSV exists, the importer materializes `<resource>.json` from CSV first, then imports from JSON.
- If both JSON and CSV exist, JSON is used.

Missing endpoint behavior:

- If an endpoint returns HTTP 404 (endpoint not available in the target NetBox version), the export writes empty JSON/CSV files for that resource and logs a skip message rather than aborting.
- The importer similarly returns an empty result set for any 404 endpoint during index preload, allowing the run to continue cleanly.
- This applies to version-gated resources such as `owner_groups` and `owners` in the `admin` section.

## Preflight checks

Readiness-only validation (no import execution):

```bash
python3 netbox_import.py \
  --url http://192.168.65.40:8000 \
  --token "<API_TOKEN>" \
  --input-dir exports \
  --preflight
```

Strict preflight (missing files are blockers):

```bash
python3 netbox_import.py \
  --url http://192.168.65.40:8000 \
  --token "<API_TOKEN>" \
  --input-dir exports \
  --preflight \
  --preflight-strict
```

Both preflight modes also accept `--no-verify-ssl` for self-signed certificate environments.

## Current importer scope

`netbox_import.py` now imports the full template catalog in dependency order.

- Total resources: `106`
- Sections:
  - `organization` (10)
  - `racks` (4)
  - `devices` (21)
  - `connections` (1)
  - `wireless` (3)
  - `ipam` (18)
  - `vpn` (10)
  - `virtualization` (6)
  - `circuits` (11)
  - `power` (2)
  - `provisioning` (3)
  - `customization` (6)
  - `operations` (5)
  - `admin` (6)

The importer and exporter are both aligned with `SECTION_ENDPOINTS` in `generate_export_templates.py`.

### Admin subsection coverage

The `admin` section currently includes these importable subsections:

- `groups`
- `owner_groups`
- `owners`
- `users`
- `permissions`
- `tokens`