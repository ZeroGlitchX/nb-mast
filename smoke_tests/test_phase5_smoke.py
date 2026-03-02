from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import import_cli
import import_core
import import_resources


class _FakePreflightClient:
    def __init__(self, base_url: str, token: str, timeout: int = 30, auth_scheme: str = "Token") -> None:
        self.base_url = base_url
        self.token = token
        self.timeout = timeout
        self.auth_scheme = auth_scheme

    def request_json(self, method: str, endpoint_or_url: str, data=None, absolute_url: bool = False):
        if method == "GET" and endpoint_or_url == "/api/status/":
            return {"status": "ok"}
        if method == "OPTIONS" and endpoint_or_url == "/api/dcim/regions/":
            return {"actions": {"POST": {"name": {"type": "string"}}}}
        raise AssertionError(f"Unexpected preflight probe: method={method} endpoint={endpoint_or_url}")


class _FakeImportClient:
    def __init__(self, base_url: str, token: str, timeout: int = 30, auth_scheme: str = "Token") -> None:
        self.base_url = base_url
        self.token = token
        self.timeout = timeout
        self.auth_scheme = auth_scheme
        self.endpoint_allowlist = None

    def get_paginated(self, endpoint: str, query=None):
        return []


class TestOnlySelectors(unittest.TestCase):
    def test_parse_only_valid_section_selects_expected_resources(self) -> None:
        selected = import_core.parse_only_resources("organization")
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected, set(import_core.SECTION_RESOURCE_ORDER["organization"]))

    def test_parse_only_invalid_selector_raises_clear_error(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            import_core.parse_only_resources("orgnization")
        message = str(ctx.exception)
        self.assertIn("Unknown --only selectors", message)
        self.assertIn("orgnization", message)

    def test_cli_invalid_only_fails_early_with_exit_2(self) -> None:
        stderr = io.StringIO()
        with patch.object(
            sys,
            "argv",
            ["netbox_import.py", "--url", "http://example.invalid", "--token", "dummy", "--only", "orgnization"],
        ):
            with redirect_stderr(stderr):
                exit_code = import_cli.main()
        self.assertEqual(exit_code, 2)
        self.assertIn("Unknown --only selectors", stderr.getvalue())


class TestPreflightModes(unittest.TestCase):
    def test_preflight_non_strict_warns_and_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_root = Path(tmpdir)
            stdout = io.StringIO()
            with patch.object(import_core, "NetBoxClient", _FakePreflightClient):
                with redirect_stdout(stdout):
                    exit_code = import_core.run_preflight(
                        url="http://example.invalid",
                        token="dummy",
                        auth_scheme="Token",
                        timeout=5,
                        input_root=input_root,
                        include_resources={"regions"},
                        strict_missing_files=False,
                    )

        output = stdout.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("- WARN: Missing section directory: organization/", output)
        self.assertIn("Preflight PASSED.", output)

    def test_preflight_strict_blocks_on_missing_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_root = Path(tmpdir)
            stdout = io.StringIO()
            with patch.object(import_core, "NetBoxClient", _FakePreflightClient):
                with redirect_stdout(stdout):
                    exit_code = import_core.run_preflight(
                        url="http://example.invalid",
                        token="dummy",
                        auth_scheme="Token",
                        timeout=5,
                        input_root=input_root,
                        include_resources={"regions"},
                        strict_missing_files=True,
                    )

        output = stdout.getvalue()
        self.assertEqual(exit_code, 1)
        self.assertIn("- BLOCKER: Missing section directory: organization/", output)
        self.assertIn("Preflight FAILED", output)


class TestRollbackManifest(unittest.TestCase):
    def test_manifest_structure_contains_expected_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "rollback_manifest_test.json"
            ctx = import_core.ImportContext(
                client=_FakeImportClient("http://example.invalid", "dummy"),
                dry_run=False,
                update_existing=True,
                fail_fast=False,
            )
            ctx.rollback_created.append(
                {
                    "resource": "regions",
                    "endpoint": "/api/dcim/regions/",
                    "id": 123,
                    "key": "us-east",
                }
            )

            import_resources.write_rollback_manifest(
                output_path=output_path,
                ctx=ctx,
                url="http://example.invalid",
                input_root=Path(tmpdir),
                mode="APPLY",
                selected_resources={"regions"},
            )

            payload = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["mode"], "APPLY")
        self.assertEqual(payload["created_count"], 1)
        self.assertEqual(payload["selected_resources"], ["regions"])
        self.assertEqual(payload["created_objects"][0]["resource"], "regions")
        self.assertEqual(payload["created_objects"][0]["id"], 123)
        self.assertIsInstance(payload["generated_at_utc"], str)
        datetime.fromisoformat(payload["generated_at_utc"])

    def test_dry_run_does_not_write_rollback_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_root = Path(tmpdir) / "exports"
            input_root.mkdir(parents=True, exist_ok=True)
            rollback_path = Path(tmpdir) / "rollback_from_dry_run.json"

            stdout = io.StringIO()
            stderr = io.StringIO()

            with patch.object(import_cli, "NetBoxClient", _FakeImportClient):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "netbox_import.py",
                        "--url",
                        "http://example.invalid",
                        "--token",
                        "dummy",
                        "--input-dir",
                        str(input_root),
                        "--dry-run",
                        "--only",
                        "organization",
                        "--rollback-manifest",
                        str(rollback_path),
                    ],
                ):
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        exit_code = import_cli.main()

        self.assertEqual(exit_code, 0, msg=stderr.getvalue())
        output = stdout.getvalue()
        self.assertFalse(rollback_path.exists())
        self.assertNotIn("Rollback manifest", output)
        self.assertIn("Scope filter (--only): 10 resources across 1 sections", output)
        self.assertNotIn("- device_roles:", output)


    def test_apply_writes_rollback_manifest_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_root = Path(tmpdir) / "exports"
            input_root.mkdir(parents=True, exist_ok=True)
            rollback_path = Path(tmpdir) / "rollback_from_apply.json"

            stdout = io.StringIO()
            stderr = io.StringIO()

            with patch.object(import_cli, "NetBoxClient", _FakeImportClient):
                with patch.object(
                    sys,
                    "argv",
                    [
                        "netbox_import.py",
                        "--url",
                        "http://example.invalid",
                        "--token",
                        "dummy",
                        "--input-dir",
                        str(input_root),
                        "--only",
                        "organization",
                        "--rollback-manifest",
                        str(rollback_path),
                    ],
                ):
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        exit_code = import_cli.main()

            self.assertEqual(exit_code, 0, msg=stderr.getvalue())
            self.assertTrue(rollback_path.exists())
            payload = json.loads(rollback_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["mode"], "APPLY")
            self.assertEqual(payload["created_count"], 0)
            self.assertIn("[done] Rollback manifest:", stdout.getvalue())


class TestCsvMaterialization(unittest.TestCase):
    def test_materialize_missing_json_from_csv_creates_json_and_parses_types(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_root = Path(tmpdir)
            org_dir = input_root / "organization"
            org_dir.mkdir(parents=True, exist_ok=True)

            csv_path = org_dir / "regions.csv"
            csv_path.write_text(
                "id,name,is_active,weight,meta,tags,empty_value\n"
                '101,Region A,true,7,"{""owner"": ""ops""}","[1, 2]",\n',
                encoding="utf-8",
            )

            created_files, converted_records = import_core.materialize_missing_json_from_csv(
                input_root,
                include_resources={"regions"},
            )

            json_path = org_dir / "regions.json"
            self.assertEqual(created_files, 1)
            self.assertEqual(converted_records, 1)
            self.assertTrue(json_path.exists())

            payload = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertIsInstance(payload, list)
            self.assertEqual(len(payload), 1)
            row = payload[0]
            self.assertEqual(row["id"], 101)
            self.assertEqual(row["name"], "Region A")
            self.assertEqual(row["is_active"], True)
            self.assertEqual(row["weight"], 7)
            self.assertEqual(row["meta"], {"owner": "ops"})
            self.assertEqual(row["tags"], [1, 2])
            self.assertNotIn("empty_value", row)

    def test_materialize_missing_json_from_csv_skips_when_json_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_root = Path(tmpdir)
            org_dir = input_root / "organization"
            org_dir.mkdir(parents=True, exist_ok=True)

            json_path = org_dir / "regions.json"
            json_path.write_text('[{"id": 999, "name": "Existing Region"}]', encoding="utf-8")

            csv_path = org_dir / "regions.csv"
            csv_path.write_text(
                "id,name\n"
                "101,Region A\n",
                encoding="utf-8",
            )

            created_files, converted_records = import_core.materialize_missing_json_from_csv(
                input_root,
                include_resources={"regions"},
            )

            self.assertEqual(created_files, 0)
            self.assertEqual(converted_records, 0)
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(payload, [{"id": 999, "name": "Existing Region"}])


if __name__ == "__main__":
    unittest.main()
