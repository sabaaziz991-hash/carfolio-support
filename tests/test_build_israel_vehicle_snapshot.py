import json
import sys
import tempfile
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import build_israel_vehicle_snapshot as snapshot_builder  # noqa: E402
from build_israel_vehicle_snapshot import build  # noqa: E402


class IsraelVehicleSnapshotBuilderTests(unittest.TestCase):
    def test_datastore_ingestion_pages_without_loading_the_registry_at_once(self):
        pages = [
            {
                "total": 3,
                "records": [
                    {
                        "mispar_rechev": 82925003,
                        "shnat_yitzur": 2024,
                        "tozeret_nm": "בי ווי די סין",
                        "degem_nm": "SC2EXQ",
                        "kinuy_mishari": "ATTO 3",
                    },
                    {
                        "mispar_rechev": 82960303,
                        "shnat_yitzur": 2024,
                        "tozeret_nm": "בי ווי די סין",
                        "degem_nm": "SC2EXQ",
                        "kinuy_mishari": "ATTO 3",
                    },
                ],
            },
            {
                "total": 3,
                "records": [
                    {
                        "mispar_rechev": 83859202,
                        "shnat_yitzur": 2022,
                        "tozeret_nm": "אאודי",
                        "degem_nm": "A3",
                        "kinuy_mishari": "A3",
                    }
                ],
            },
        ]
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            with patch.object(
                snapshot_builder,
                "datastore_page",
                side_effect=pages,
            ) as request:
                count = snapshot_builder.ingest_datastore(
                    resource_id=snapshot_builder.PASSENGER_RESOURCE_ID,
                    output_directory=output,
                    required_fields=snapshot_builder.PASSENGER_REQUIRED_FIELDS,
                    compactor=snapshot_builder.compact_passenger_record,
                    page_size=2,
                )

            self.assertEqual(count, 3)
            self.assertEqual(request.call_count, 2)
            self.assertEqual(request.call_args_list[1].kwargs["offset"], 2)
            self.assertTrue((output / "82.ndjson").exists())
            self.assertTrue((output / "83.ndjson").exists())

    def test_builds_plate_prefix_shards_and_manifest(self):
        fixture = ROOT / "tests" / "fixtures" / "israel-vehicles-sample.csv"
        heavy_fixture = ROOT / "tests" / "fixtures" / "israel-heavy-vehicles-sample.csv"
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "data"
            build(
                "https://official.example/vehicles.csv",
                fixture,
                output,
                heavy_source_url="https://official.example/heavy.csv",
                heavy_local_file=heavy_fixture,
            )

            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest["schemaVersion"], 2)
            self.assertEqual(manifest["recordEncoding"], "ordered-json-arrays")
            self.assertEqual(manifest["recordCount"], 5)
            self.assertEqual(manifest["shardCount"], 4)
            self.assertEqual(manifest["platePrefixLength"], 2)
            self.assertEqual(manifest["compression"], "zlib")
            self.assertEqual(manifest["sources"][0]["recordCount"], 3)
            self.assertEqual(manifest["sources"][1]["recordCount"], 2)

            shard = json.loads(
                zlib.decompress(
                    (output / "shards" / "82.json.zlib").read_bytes()
                )
            )
            self.assertEqual(
                [record[0] for record in shard["passengerRecords"]],
                [82925003, 82960303],
            )
            self.assertEqual(
                shard["passengerRecords"][0][9], "ATTO 3"
            )

            heavy_shard = json.loads(
                zlib.decompress(
                    (output / "shards" / "50.json.zlib").read_bytes()
                )
            )
            self.assertEqual(heavy_shard["passengerRecords"], [])
            self.assertEqual(
                heavy_shard["heavyRecords"][0][0], 50294701
            )
            self.assertEqual(
                heavy_shard["heavyRecords"][0][12], "מרצדס בנץ גרמנ"
            )
            self.assertEqual(
                heavy_shard["heavyRecords"][0][6], 5500
            )
            self.assertLess(manifest["outputBytes"], 100_000)

    def test_rejects_truncated_snapshot_without_replacing_previous(self):
        fixture = ROOT / "tests" / "fixtures" / "israel-vehicles-sample.csv"
        heavy_fixture = ROOT / "tests" / "fixtures" / "israel-heavy-vehicles-sample.csv"
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "data"
            output.mkdir()
            marker = output / "previous.txt"
            marker.write_text("keep")

            with self.assertRaises(RuntimeError):
                build(
                    "https://official.example/vehicles.csv",
                    fixture,
                    output,
                    heavy_source_url="https://official.example/heavy.csv",
                    heavy_local_file=heavy_fixture,
                    minimum_passenger_records=4,
                )

            self.assertEqual(marker.read_text(), "keep")

    def test_rejects_oversized_snapshot_without_replacing_previous(self):
        fixture = ROOT / "tests" / "fixtures" / "israel-vehicles-sample.csv"
        heavy_fixture = ROOT / "tests" / "fixtures" / "israel-heavy-vehicles-sample.csv"
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "data"
            output.mkdir()
            marker = output / "previous.txt"
            marker.write_text("keep")

            with self.assertRaises(RuntimeError):
                build(
                    "https://official.example/vehicles.csv",
                    fixture,
                    output,
                    heavy_source_url="https://official.example/heavy.csv",
                    heavy_local_file=heavy_fixture,
                    maximum_output_bytes=1,
                )

            self.assertEqual(marker.read_text(), "keep")

    def test_rejects_schema_drift_without_replacing_previous(self):
        heavy_fixture = ROOT / "tests" / "fixtures" / "israel-heavy-vehicles-sample.csv"
        with tempfile.TemporaryDirectory() as temporary:
            temporary_path = Path(temporary)
            broken_passenger = temporary_path / "broken.csv"
            broken_passenger.write_text("mispar_rechev,shnat_yitzur\n82925003,2024\n")
            output = temporary_path / "data"
            output.mkdir()
            marker = output / "previous.txt"
            marker.write_text("keep")

            with self.assertRaises(RuntimeError):
                build(
                    "https://official.example/vehicles.csv",
                    broken_passenger,
                    output,
                    heavy_source_url="https://official.example/heavy.csv",
                    heavy_local_file=heavy_fixture,
                )

            self.assertEqual(marker.read_text(), "keep")


if __name__ == "__main__":
    unittest.main()
