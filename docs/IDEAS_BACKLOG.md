# Ideas Backlog

This document tracks follow-up improvements and experiments for the NetBox restore toolkit.

## How to Use This File

- Add new ideas under `Candidate Ideas`.
- Promote ready items into `Planned`.
- Move completed items into `Done`.
- Keep notes short and actionable.

## Planned

1. One-command workflow wrapper (`make restore` or shell script)
- Goal: Run export/import flows with a single command.
- Timing: After current optimization pass.
- Notes:
  - Include flags for dry-run vs apply.
  - Standardize logs/output path.

2. Import preflight checks
- Goal: Catch common issues before apply (missing token, missing input files, URL reachability, schema mismatches).
- Timing: After current optimization pass.
- Notes:
  - Add a fail-fast precheck mode.
  - Print a concise readiness summary.

## Candidate Ideas

- Add future ideas here.

## Done

- Move completed ideas here with date and short outcome.
