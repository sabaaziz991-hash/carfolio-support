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


class _JSONResponse:
    """Minimal stand-in for the urlopen context manager."""

    def __init__(self, payload):
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def read(self, *args):
        return self._body


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
            {"records": []},
        ]
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            with patch.object(
                snapshot_builder, "datastore_exact_total", return_value=3
            ), patch.object(
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
            self.assertEqual(request.call_count, 3)
            self.assertEqual(request.call_args_list[1].kwargs["offset"], 2)
            self.assertTrue((output / "82.ndjson").exists())
            self.assertTrue((output / "83.ndjson").exists())

    def _passenger_row(self, plate):
        return {
            "mispar_rechev": plate,
            "shnat_yitzur": 2022,
            "tozeret_nm": "אאודי",
            "degem_nm": "A3",
            "kinuy_mishari": "A3",
        }

    def _ingest(self, pages, total):
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(
                snapshot_builder, "datastore_exact_total", return_value=total
            ), patch.object(snapshot_builder, "datastore_page", side_effect=pages):
                return snapshot_builder.ingest_datastore(
                    resource_id=snapshot_builder.PASSENGER_RESOURCE_ID,
                    output_directory=Path(temporary),
                    required_fields=snapshot_builder.PASSENGER_REQUIRED_FIELDS,
                    compactor=snapshot_builder.compact_passenger_record,
                    page_size=2,
                )

    def test_ingestion_reads_past_an_understated_count(self):
        # A count lower than the real row count must never truncate the tail.
        pages = [
            {"records": [self._passenger_row(82925003), self._passenger_row(82960303)]},
            {"records": [self._passenger_row(83859202)]},
            {"records": []},
        ]
        self.assertEqual(self._ingest(pages, total=2), 3)

    def test_ingestion_rejects_paging_that_stops_before_the_exact_count(self):
        pages = [
            {"records": [self._passenger_row(82925003), self._passenger_row(82960303)]},
            {"records": []},
        ]
        with self.assertRaisesRegex(RuntimeError, "stopped at 2 of 5"):
            self._ingest(pages, total=5)

    def test_exact_total_retries_missing_and_estimated_counts(self):
        envelopes = [
            {"success": True, "result": {"records": []}},
            {"success": True, "result": {"total": 4181719, "total_was_estimated": True}},
            {"success": True, "result": {"total": 4181719, "total_was_estimated": False}},
        ]
        requested_urls = []

        def respond(request, timeout):
            requested_urls.append(request.full_url)
            return _JSONResponse(envelopes[len(requested_urls) - 1])

        with patch.object(snapshot_builder.urllib.request, "urlopen", side_effect=respond), \
                patch.object(snapshot_builder.time, "sleep"):
            total = snapshot_builder.datastore_exact_total(
                resource_id=snapshot_builder.PASSENGER_RESOURCE_ID
            )

        self.assertEqual(total, 4181719)
        self.assertEqual(len(requested_urls), 3)
        self.assertIn("total_estimation_threshold=1000000000", requested_urls[0])

    def test_exact_total_gives_up_after_bounded_retries(self):
        def respond(request, timeout):
            return _JSONResponse({"success": True, "result": {"records": []}})

        with patch.object(snapshot_builder.urllib.request, "urlopen", side_effect=respond) as urlopen, \
                patch.object(snapshot_builder.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "no total count"):
                snapshot_builder.datastore_exact_total(
                    resource_id=snapshot_builder.PASSENGER_RESOURCE_ID
                )
        self.assertEqual(urlopen.call_count, snapshot_builder.DATASTORE_ATTEMPTS)

    def test_pages_skip_the_per_page_count(self):
        requested_urls = []

        def respond(request, timeout):
            requested_urls.append(request.full_url)
            return _JSONResponse({"success": True, "result": {"records": []}})

        with patch.object(snapshot_builder.urllib.request, "urlopen", side_effect=respond):
            snapshot_builder.datastore_page(
                resource_id=snapshot_builder.PASSENGER_RESOURCE_ID, limit=2, offset=4
            )
        self.assertIn("include_total=false", requested_urls[0])
        self.assertIn("offset=4", requested_urls[0])

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
