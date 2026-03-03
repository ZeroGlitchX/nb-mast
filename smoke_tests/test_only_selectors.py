from __future__ import annotations

import io
import sys
import unittest
from contextlib import redirect_stderr
from unittest.mock import patch

import import_cli
import import_core


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


if __name__ == "__main__":
    unittest.main()
