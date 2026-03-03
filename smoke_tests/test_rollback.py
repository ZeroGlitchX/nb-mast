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
from smoke_test_helpers import FakeImportClient


class TestRollbackManifest(unittest.TestCase):
    def test_manifest_structure_contains_expected_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "rollback_manifest_test.json"
            ctx = import_core.ImportContext(
                client=FakeImportClient("http://example.invalid", "dummy"),
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

            with patch.object(import_cli, "NetBoxClient", FakeImportClient):
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

            with patch.object(import_cli, "NetBoxClient", FakeImportClient):
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


if __name__ == "__main__":
    unittest.main()
