from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import import_core


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
