# Recommended V2 Plan

## Goal

Evolve NB-MAST v1 into a faster, safer, and more maintainable v2 while preserving current import/export behavior.

## Current Status Snapshot

- Full template-aligned import scope: 106 resources.
- Step 1 (selective import) implemented via `--only`.
- Follow-up optimization implemented: when `--only` is used, index preloading is now endpoint-scoped instead of always loading all indexes.
- Phase 3 completed: `--preflight` and `--preflight-strict` readiness checks are implemented and validated.
- Phase 5 completed: smoke tests and GitHub Actions CI validation passed (run `22561377015` on March 1, 2026 CST / March 2, 2026 UTC).

## Phased Plan

## Phase 1: Selective Import

Status: `Completed`

Deliverables:
- `--only` supports comma-separated selectors.
- Section selectors (example: `ipam`) and section/resource selectors (example: `organization/sites`) are supported.
- Import steps are filtered to selected resources only.
- Summary output is scoped to selected resources.

Acceptance criteria:
- `--only ipam` executes only IPAM steps.
- Invalid selectors fail fast with clear error text and exit code `2`.

## Phase 1.1: Selective Index Preload Optimization

Status: `Completed`

Deliverables:
- Index preload allowlist derived from selected resources and section-level dependency expansion.
- Non-allowlisted index endpoints return empty index sets without API fetches.
- Runtime prints scoped preload count (example: `Index preload scope: 23 endpoints`).

Acceptance criteria:
- `--only` runs show reduced preload endpoint count.
- Dry-run/apply behavior remains valid for selected scope.

## Phase 2: Rollback Manifest

Status: `Completed`

Deliverables:
- Record created objects during apply in a rollback manifest (resource, endpoint, id, key).
- Write manifest to `exports/rollback_manifest_<timestamp>.json` (or configurable path).
- Include execution metadata (URL, timestamp, mode, filter scope).

Acceptance criteria:
- Failed apply with partial creates produces a complete manifest of created objects.
- Manifest supports deterministic reverse-delete workflow.

Validation:
- Local target tested on March 1, 2026 (`http://192.168.65.40:8000`) using scoped apply (`organization,ipam`).
- Rollback manifest generated successfully with `created_count=505`.

## Phase 3: Preflight Checks

Status: `Completed`

Deliverables:
- `--preflight` mode for readiness checks without import execution.
- Validate URL reachability, token auth/write capability, input structure, and selector validity.
- Optional strict mode for missing expected files/resources.

Acceptance criteria:
- Preflight exits non-zero on blockers with concise actionable diagnostics.
- Preflight output can be used in CI gate.

Validation:
- CLI now exposes `--preflight` and `--preflight-strict`.
- Preflight against `organization,ipam` scope validates input structure and emits blockers when target is unreachable.

## Phase 4: Module Refactor

Status: `Completed`

Deliverables:
- Split `netbox_import.py` into:
  - `import_core.py` (client, context, shared helpers)
  - `import_resources.py` (resource/key handlers)
  - `import_cli.py` (argparse, orchestration)
- Preserved CLI compatibility and behavior via wrapper `netbox_import.py` importing `import_cli.main`.

Acceptance criteria:
- Existing commands run unchanged.
- Dry-run/apply summaries are equivalent on same dataset.

Validation:
- `python3 -m py_compile netbox_import.py import_core.py import_resources.py import_cli.py` passed.
- Wrapper CLI help output matched baseline exactly (pre/post split diff: no changes).
- Scoped preflight parity check passed and matched baseline output (`--only organization --preflight`).
- Full-scope dry-run completed successfully (`106/106` steps, exit code `0`).

## Phase 5: Regression Tests

Status: `Completed`

Deliverables:
- Add smoke tests for:
  - CLI parsing (`--only`, invalid selectors)
  - filtered step execution
  - rollback manifest generation (Phase 2)
  - preflight mode (Phase 3)
- Add CI workflow to run compile checks and smoke tests on push/PR.

Acceptance criteria:
- Tests run non-interactively and pass in local/CI environments.

Validation:
- Added `tests/test_phase5_smoke.py` with 8 smoke tests for `--only` selector parsing/fail-fast, preflight strict vs warn behavior, scoped step filtering, and rollback manifest apply/dry-run semantics.
- Added GitHub Actions workflow `.github/workflows/phase5-smoke-tests.yml` to run py_compile checks + smoke tests on push/PR.
- Local execution baseline: `python3 -m unittest discover -s tests -p 'test_*.py' -v` => 8/8 passing.
- Remote CI baseline: GitHub Actions run `22561377015` (`Phase5 Smoke Tests`) on branch `main` completed with `success` at `2026-03-02T04:26:29Z` (March 1, 2026 22:26:29 CST).
- Run URL: `https://github.com/ZeroGlitchX/nb-mast/actions/runs/22561377015`.

## Phase 5.1: CSV Materialization Regression Addendum

Status: `Completed`

Deliverables:
- Add network-isolated regression tests for CSV-to-JSON materialization path.
- Validate create behavior when JSON is missing (`<resource>.json` generated from `<resource>.csv`).
- Validate skip behavior when JSON already exists.

Acceptance criteria:
- CSV materialization path is validated without live NetBox dependency.
- Full smoke suite remains passing after addendum changes.

Validation:
- Added `TestCsvMaterialization` in `tests/test_phase5_smoke.py` with 2 tests for create/skip behavior.
- Local execution after addendum: `python3 -m unittest discover -s tests -p 'test_*.py' -v` => 10/10 passing.
- Remote CI addendum validation: GitHub Actions run `22561697814` on branch `main` completed with `success` at `2026-03-02T04:40:52Z` (March 1, 2026 22:40:52 CST).
- Run URL: `https://github.com/ZeroGlitchX/nb-mast/actions/runs/22561697814`.

## Suggested Execution Order (Next)

1. Optional: incremental cleanup in `import_resources.py` (registry dedupe for repeated key handlers) now that CI baseline is stable.
2. Optional: add branch-protection requirements in GitHub so Phase5 Smoke Tests must pass before merge.
3. Optional: split smoke suite into targeted files (`test_only_selectors.py`, `test_preflight.py`, `test_rollback.py`, `test_csv_materialization.py`) for maintainability as coverage grows.
