from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import import_core
from smoke_test_helpers import FakePreflightClient


class TestPreflightModes(unittest.TestCase):
    def test_preflight_non_strict_warns_and_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_root = Path(tmpdir)
            stdout = io.StringIO()
            with patch.object(import_core, "NetBoxClient", FakePreflightClient):
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
            with patch.object(import_core, "NetBoxClient", FakePreflightClient):
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


if __name__ == "__main__":
    unittest.main()
