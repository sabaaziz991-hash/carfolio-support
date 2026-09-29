#!/usr/bin/env python3
"""Build plate-prefix shards from Israel's official vehicle registries.

The Ministry publishes separate daily CSV resources for passenger/commercial
vehicles up to 3,500 kg and for heavy/code-less vehicles. This builder streams
both resources, retains only fields consumed by Carvetted, and produces one
mixed shard per plate prefix. A lookup therefore needs one small static file
and can resolve either registry without guessing the vehicle class first.

Generated data remains official Ministry data. The manifest preserves source,
resource, generation, and license metadata so consumers can attribute it and
identify stale snapshots.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import shutil
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


PASSENGER_RESOURCE_ID = "053cea08-09bc-40ec-8f7a-156f0677aff3"
PASSENGER_PACKAGE_ID = "7a338622-63bb-4cfd-b3f7-0d2e8cf71033"
HEAVY_RESOURCE_ID = "cd3acc5c-03c3-4c89-9c54-d40f93c0d790"
HEAVY_PACKAGE_ID = "df773fd2-e598-4772-b6ec-d0b7e12f8762"

DEFAULT_PASSENGER_SOURCE_URL = (
    "https://e.data.gov.il/dataset/"
    f"{PASSENGER_PACKAGE_ID}/resource/{PASSENGER_RESOURCE_ID}/download/"
    f"{PASSENGER_RESOURCE_ID}.csv"
)
DEFAULT_HEAVY_SOURCE_URL = (
    "https://e.data.gov.il/dataset/"
    f"{HEAVY_PACKAGE_ID}/resource/{HEAVY_RESOURCE_ID}/download/"
    f"{HEAVY_RESOURCE_ID}.csv"
)
LICENSE_URL = "https://data.gov.il/terms-of-use"
ATTRIBUTION = "Israel Ministry of Transport via data.gov.il"
PLATE_PREFIX_LENGTH = 2

PASSENGER_TEXT_FIELDS = (
    "sug_degem",
    "tozeret_nm",
    "degem_nm",
    "kinuy_mishari",
    "ramat_gimur",
    "degem_manoa",
    "baalut",
    "misgeret",
    "tzeva_rechev",
    "sug_delek_nm",
    "moed_aliya_lakvish",
    "mivchan_acharon_dt",
    "tokef_dt",
    "zmig_kidmi",
    "zmig_ahori",
    "horaat_rishum",
)
PASSENGER_INTEGER_FIELDS = (
    "mispar_rechev",
    "tozeret_cd",
    "degem_cd",
    "shnat_yitzur",
    "ramat_eivzur_betihuty",
    "kvutzat_zihum",
)
PASSENGER_RETAINED_FIELDS = (
    "mispar_rechev",
    *PASSENGER_INTEGER_FIELDS[1:],
    *PASSENGER_TEXT_FIELDS,
)
PASSENGER_REQUIRED_FIELDS = (
    "mispar_rechev",
    "shnat_yitzur",
    "tozeret_nm",
    "degem_nm",
    "kinuy_mishari",
)

HEAVY_TEXT_FIELDS = (
    "mispar_shilda",
    "tkina_EU",
    "tozeret_nm",
    "tozeret_eretz_nm",
    "sug_delek_nm",
    "degem_nm",
    "degem_manoa",
    "hanaa_nm",
    "moed_aliya_lakvish",
    "kvutzat_sug_rechev",
    "grira_nm",
    "zmig_kidmi",
    "zmig_ahori",
    "mispar_manoa",
)
HEAVY_INTEGER_FIELDS = (
    "mispar_rechev",
    "shnat_yitzur",
    "tozeret_cd",
    "horaat_rishum",
    "mispar_mekomot_leyd_nahag",
    "mispar_mekomot",
)
HEAVY_NUMBER_FIELDS = (
    "mishkal_kolel",
    "mishkal_azmi",
    "nefach_manoa",
    "mishkal_mitan_harama",
)
HEAVY_RETAINED_FIELDS = (
    "mispar_rechev",
    *HEAVY_INTEGER_FIELDS[1:],
    *HEAVY_NUMBER_FIELDS,
    *HEAVY_TEXT_FIELDS,
)
HEAVY_REQUIRED_FIELDS = (
    "mispar_rechev",
    "shnat_yitzur",
    "tozeret_nm",
    "degem_nm",
    "mishkal_kolel",
    "mispar_shilda",
    "tkina_EU",
)


class ShardWriters:
    def __init__(self, directory: Path, maximum_open: int = 48) -> None:
        self.directory = directory
        self.maximum_open = maximum_open
        self.handles: OrderedDict[str, io.TextIOWrapper] = OrderedDict()

    def write(self, prefix: str, record: list[object | None]) -> None:
        handle = self.handles.pop(prefix, None)
        if handle is None:
            handle = (self.directory / f"{prefix}.ndjson").open(
                "a", encoding="utf-8", newline=""
            )
        self.handles[prefix] = handle
        json.dump(record, handle, ensure_ascii=False, separators=(",", ":"))
        handle.write("\n")
        if len(self.handles) > self.maximum_open:
            _, oldest = self.handles.popitem(last=False)
            oldest.close()

    def close(self) -> None:
        for handle in self.handles.values():
            handle.close()
        self.handles.clear()


def integer(value: object | None) -> int | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def number(value: object | None) -> int | float | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    try:
        parsed = float(cleaned)
    except ValueError:
        return None
    return int(parsed) if parsed.is_integer() else parsed


def valid_plate(value: int | None) -> bool:
    # Modern app input uses 7–8 digits. The heavy registry also contains
    # shorter historic active registrations, so retain every 3–8 digit record
    # rather than silently dropping valid Ministry data during ingestion.
    return value is not None and 3 <= len(str(value)) <= 8


def compact_passenger_record(row: dict[str, object]) -> list[object | None] | None:
    plate = integer(row.get("mispar_rechev"))
    if not valid_plate(plate):
        return None
    result: dict[str, object] = {"mispar_rechev": plate}
    for field in PASSENGER_INTEGER_FIELDS[1:]:
        value = integer(row.get(field))
        if value is not None:
            result[field] = value
    for field in PASSENGER_TEXT_FIELDS:
        value = str(row.get(field) or "").strip()
        if value:
            result[field] = value
    # Ordered arrays avoid repeating long official column names millions of
    # times. The field order is declared once in the manifest and fixed by the
    # v2 iOS decoder.
    return [result.get(field) for field in PASSENGER_RETAINED_FIELDS]


def compact_heavy_record(row: dict[str, object]) -> list[object | None] | None:
    plate = integer(row.get("mispar_rechev"))
    if not valid_plate(plate):
        return None
    result: dict[str, object] = {"mispar_rechev": plate}
    for field in HEAVY_INTEGER_FIELDS[1:]:
        value = integer(row.get(field))
        if value is not None:
            result[field] = value
    for field in HEAVY_NUMBER_FIELDS:
        value = number(row.get(field))
        if value is not None:
            result[field] = value
    for field in HEAVY_TEXT_FIELDS:
        value = str(row.get(field) or "").strip()
        if value:
            result[field] = value
    return [result.get(field) for field in HEAVY_RETAINED_FIELDS]


def open_source(url: str, local_file: Path | None):
    if local_file is not None:
        return local_file.open("rb")
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Carvetted official-data snapshot builder/2.0"},
    )
    return urllib.request.urlopen(request, timeout=180)


# Above any realistic registry size, so CKAN returns an exact count instead of
# its default planner estimate (`total_was_estimated: true`).
EXACT_TOTAL_THRESHOLD = 1_000_000_000
DATASTORE_ATTEMPTS = 6


def datastore_request(
    parameters: dict[str, object],
    *,
    description: str,
    validate: Callable[[dict[str, object]], None] | None = None,
) -> dict[str, object]:
    """Fetch one DataStore result, retrying transport errors and incomplete
    envelopes alike. data.gov.il occasionally answers successfully without the
    fields a caller needs (for example no `total` while a resource reloads), so
    validation runs inside the retry loop rather than failing the whole build."""
    query = urllib.parse.urlencode(parameters)
    url = f"https://data.gov.il/api/3/action/datastore_search?{query}"
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Carvetted official-data snapshot builder/2.0"},
    )
    last_error: Exception | None = None
    for attempt in range(DATASTORE_ATTEMPTS):
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                envelope = json.load(response)
            if not envelope.get("success"):
                raise RuntimeError("Official DataStore returned success=false")
            result = envelope.get("result")
            if not isinstance(result, dict):
                raise RuntimeError("Official DataStore response has no result object")
            if validate is not None:
                validate(result)
            return result
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError,
            RuntimeError,
        ) as error:
            last_error = error
            if isinstance(error, urllib.error.HTTPError) and error.code < 500:
                if error.code != 429:
                    break
            if attempt < DATASTORE_ATTEMPTS - 1:
                time.sleep(min(60, 2 ** (attempt + 1)))
    raise RuntimeError(
        f"Official DataStore {description} failed: {last_error}"
    ) from last_error


def datastore_exact_total(*, resource_id: str) -> int:
    def validate(result: dict[str, object]) -> None:
        total = integer(result.get("total"))
        if total is None or total <= 0:
            raise RuntimeError("Official DataStore response has no total count")
        if result.get("total_was_estimated"):
            raise RuntimeError("Official DataStore returned an estimated total")

    result = datastore_request(
        {
            "resource_id": resource_id,
            "limit": 0,
            "include_total": "true",
            "total_estimation_threshold": EXACT_TOTAL_THRESHOLD,
        },
        description="exact record count",
        validate=validate,
    )
    total = integer(result.get("total"))
    assert total is not None
    return total


def datastore_page(
    *,
    resource_id: str,
    limit: int,
    offset: int,
) -> dict[str, object]:
    def validate(result: dict[str, object]) -> None:
        if not isinstance(result.get("records"), list):
            raise RuntimeError("Official DataStore response has no records array")

    return datastore_request(
        {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
            "sort": "_id asc",
            # The exact count is fetched once up front; counting on every page
            # would only return a planner estimate and cost a scan.
            "include_total": "false",
        },
        description=f"page at offset {offset:,}",
        validate=validate,
    )


def ingest_datastore(
    *,
    resource_id: str,
    output_directory: Path,
    required_fields: tuple[str, ...],
    compactor: Callable[[dict[str, object]], list[object | None] | None],
    page_size: int,
) -> int:
    writers = ShardWriters(output_directory)
    record_count = 0
    offset = 0
    schema_checked = False
    total = datastore_exact_total(resource_id=resource_id)
    try:
        # Page until the DataStore returns an empty page. The count only
        # verifies completeness; it never ends the loop early, so rows added
        # after it was taken are still included.
        while True:
            result = datastore_page(
                resource_id=resource_id,
                limit=page_size,
                offset=offset,
            )
            records = result.get("records")
            if not isinstance(records, list):
                raise RuntimeError("Official DataStore response has no records array")
            if not records:
                if offset < total:
                    raise RuntimeError(
                        f"Official DataStore stopped at {offset:,} of {total:,} records"
                    )
                break

            if not schema_checked:
                missing = set(required_fields) - set(records[0])
                if missing:
                    raise RuntimeError(
                        "Official DataStore is missing required columns: "
                        + ", ".join(sorted(missing))
                    )
                schema_checked = True

            for row in records:
                if not isinstance(row, dict):
                    continue
                record = compactor(row)
                if record is None:
                    continue
                plate = str(record[0])
                writers.write(plate[:PLATE_PREFIX_LENGTH], record)
                record_count += 1

            offset += len(records)
            if offset % 250_000 < len(records):
                print(
                    f"{resource_id}: processed {offset:,} of {total:,}",
                    flush=True,
                )
    finally:
        writers.close()
    print(f"{resource_id}: read {offset:,} rows (exact count {total:,})", flush=True)
    return record_count


def source_encoding(sample: bytes) -> str:
    if sample.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "cp1255"


def ingest(
    *,
    source_url: str,
    local_file: Path | None,
    output_directory: Path,
    required_fields: tuple[str, ...],
    compactor: Callable[[dict[str, object]], list[object | None] | None],
) -> int:
    writers = ShardWriters(output_directory)
    record_count = 0
    try:
        with open_source(source_url, local_file) as binary:
            buffered = io.BufferedReader(binary)
            sample_bytes = buffered.peek(16_384)[:16_384]
            encoding = source_encoding(sample_bytes)
            sample = sample_bytes.decode(encoding, errors="replace")
            delimiter = "|" if sample.count("|") > sample.count(",") else ","
            text = io.TextIOWrapper(
                buffered,
                encoding=encoding,
                errors="replace",
                newline="",
            )
            reader = csv.DictReader(text, delimiter=delimiter)
            missing = set(required_fields) - set(reader.fieldnames or [])
            if missing:
                raise RuntimeError(
                    "Official CSV is missing required columns: "
                    + ", ".join(sorted(missing))
                )

            for row in reader:
                record = compactor(row)
                if record is None:
                    continue
                plate = str(record[0])
                writers.write(plate[:PLATE_PREFIX_LENGTH], record)
                record_count += 1
    finally:
        writers.close()
    return record_count


def read_records(path: Path) -> list[list[object | None]]:
    if not path.exists():
        return []
    records = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    records.sort(key=lambda record: record[0])
    return records


def build(
    passenger_source_url: str,
    passenger_local_file: Path | None,
    output_root: Path,
    *,
    heavy_source_url: str | None = None,
    heavy_local_file: Path | None = None,
    minimum_passenger_records: int = 0,
    minimum_heavy_records: int = 0,
    maximum_output_bytes: int = 0,
    remote_delivery: str = "datastore",
    datastore_page_size: int = 25_000,
) -> None:
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    output_root.parent.mkdir(parents=True, exist_ok=True)
    staging_root = Path(
        tempfile.mkdtemp(prefix=f".{output_root.name}-", dir=output_root.parent)
    )
    try:
        shards_directory = staging_root / "shards"
        shards_directory.mkdir(parents=True)

        with tempfile.TemporaryDirectory(prefix="carfolio-israel-snapshot-") as temporary:
            temporary_path = Path(temporary)
            passenger_directory = temporary_path / "passenger"
            heavy_directory = temporary_path / "heavy"
            passenger_directory.mkdir()
            heavy_directory.mkdir()

            if passenger_local_file is None and remote_delivery == "datastore":
                passenger_count = ingest_datastore(
                    resource_id=PASSENGER_RESOURCE_ID,
                    output_directory=passenger_directory,
                    required_fields=PASSENGER_REQUIRED_FIELDS,
                    compactor=compact_passenger_record,
                    page_size=datastore_page_size,
                )
            else:
                passenger_count = ingest(
                    source_url=passenger_source_url,
                    local_file=passenger_local_file,
                    output_directory=passenger_directory,
                    required_fields=PASSENGER_REQUIRED_FIELDS,
                    compactor=compact_passenger_record,
                )
            if passenger_count < minimum_passenger_records:
                raise RuntimeError(
                    f"Passenger snapshot has {passenger_count:,} records; "
                    f"expected at least {minimum_passenger_records:,}"
                )

            heavy_count = 0
            if heavy_source_url is not None or heavy_local_file is not None:
                if heavy_local_file is None and remote_delivery == "datastore":
                    heavy_count = ingest_datastore(
                        resource_id=HEAVY_RESOURCE_ID,
                        output_directory=heavy_directory,
                        required_fields=HEAVY_REQUIRED_FIELDS,
                        compactor=compact_heavy_record,
                        page_size=datastore_page_size,
                    )
                else:
                    heavy_count = ingest(
                        source_url=heavy_source_url or "",
                        local_file=heavy_local_file,
                        output_directory=heavy_directory,
                        required_fields=HEAVY_REQUIRED_FIELDS,
                        compactor=compact_heavy_record,
                    )
            if heavy_count < minimum_heavy_records:
                raise RuntimeError(
                    f"Heavy snapshot has {heavy_count:,} records; "
                    f"expected at least {minimum_heavy_records:,}"
                )

            prefixes = {
                path.stem
                for directory in (passenger_directory, heavy_directory)
                for path in directory.glob("*.ndjson")
            }
            for prefix in sorted(prefixes):
                payload = {
                    "schemaVersion": 2,
                    "generatedAt": generated_at,
                    "sourceResourceIDs": {
                        "passenger": PASSENGER_RESOURCE_ID,
                        "heavy": HEAVY_RESOURCE_ID,
                    },
                    "passengerRecords": read_records(
                        passenger_directory / f"{prefix}.ndjson"
                    ),
                    "heavyRecords": read_records(heavy_directory / f"{prefix}.ndjson"),
                }
                encoded = json.dumps(
                    payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
                (shards_directory / f"{prefix}.json.zlib").write_bytes(
                    zlib.compress(encoded, level=9)
                )

        manifest = {
            "schemaVersion": 2,
            "recordEncoding": "ordered-json-arrays",
            "compression": "zlib",
            "shardFilePattern": "shards/{plate-prefix}.json.zlib",
            "generatedAt": generated_at,
            "recordCount": passenger_count + heavy_count,
            "shardCount": len(prefixes),
            "platePrefixLength": PLATE_PREFIX_LENGTH,
            "attribution": ATTRIBUTION,
            "licenseURL": LICENSE_URL,
            "sourceDelivery": (
                "data.gov.il CKAN DataStore pagination"
                if remote_delivery == "datastore"
                else "official bulk CSV"
            ),
            "sources": [
                {
                    "registry": "passenger",
                    "sourceURL": passenger_source_url,
                    "sourceResourceID": PASSENGER_RESOURCE_ID,
                    "recordCount": passenger_count,
                    "fields": list(PASSENGER_RETAINED_FIELDS),
                },
                {
                    "registry": "heavy",
                    "sourceURL": heavy_source_url,
                    "sourceResourceID": HEAVY_RESOURCE_ID,
                    "recordCount": heavy_count,
                    "fields": list(HEAVY_RETAINED_FIELDS),
                },
            ],
        }
        (staging_root / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

        output_bytes = sum(
            path.stat().st_size for path in staging_root.rglob("*") if path.is_file()
        )
        manifest["outputBytes"] = output_bytes
        (staging_root / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        # Recalculate after adding the outputBytes field. The tiny difference
        # matters only for accurate operations metadata, not the ceiling.
        manifest["outputBytes"] = sum(
            path.stat().st_size for path in staging_root.rglob("*") if path.is_file()
        )
        (staging_root / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        if maximum_output_bytes and manifest["outputBytes"] > maximum_output_bytes:
            raise RuntimeError(
                f"Snapshot is {manifest['outputBytes']:,} bytes; "
                f"maximum is {maximum_output_bytes:,}"
            )

        previous_root: Path | None = None
        if output_root.exists():
            previous_root = Path(
                tempfile.mkdtemp(
                    prefix=f".{output_root.name}-previous-",
                    dir=output_root.parent,
                )
            )
            previous_root.rmdir()
            output_root.replace(previous_root)
        try:
            staging_root.replace(output_root)
        except Exception:
            if previous_root is not None and not output_root.exists():
                previous_root.replace(output_root)
            raise
        else:
            if previous_root is not None:
                shutil.rmtree(previous_root)
        print(json.dumps(manifest, ensure_ascii=False))
    except Exception:
        shutil.rmtree(staging_root, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--passenger-source-url", default=DEFAULT_PASSENGER_SOURCE_URL
    )
    parser.add_argument("--passenger-source-file", type=Path)
    parser.add_argument("--heavy-source-url", default=DEFAULT_HEAVY_SOURCE_URL)
    parser.add_argument("--heavy-source-file", type=Path)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/israel-vehicles/v2"),
    )
    parser.add_argument("--minimum-passenger-records", type=int, default=0)
    parser.add_argument("--minimum-heavy-records", type=int, default=0)
    parser.add_argument("--maximum-output-bytes", type=int, default=0)
    parser.add_argument(
        "--remote-delivery",
        choices=("datastore", "csv"),
        default="datastore",
    )
    parser.add_argument("--datastore-page-size", type=int, default=25_000)
    args = parser.parse_args()
    build(
        args.passenger_source_url,
        args.passenger_source_file,
        args.output_root,
        heavy_source_url=args.heavy_source_url,
        heavy_local_file=args.heavy_source_file,
        minimum_passenger_records=args.minimum_passenger_records,
        minimum_heavy_records=args.minimum_heavy_records,
        maximum_output_bytes=args.maximum_output_bytes,
        remote_delivery=args.remote_delivery,
        datastore_page_size=args.datastore_page_size,
    )


if __name__ == "__main__":
    main()
