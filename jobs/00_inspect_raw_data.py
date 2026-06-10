"""Inspect raw dataset files before EDA or modeling.

This job inventories files under data/raw, checks whether Spark can read them,
prints basic metadata, and writes artifacts/metadata/raw_data_summary.json.
It does not modify raw data.
"""

from __future__ import annotations

import gzip
import json
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_PATH = PROJECT_ROOT / "artifacts" / "metadata" / "raw_data_summary.json"
SAFE_COUNT_MAX_BYTES = 250 * 1024 * 1024
TAR_MEMBER_LIMIT = 200


def human_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{num_bytes} B"


def infer_format(path: Path) -> str:
    name = path.name.lower()
    suffixes = [suffix.lower() for suffix in path.suffixes]

    if name.endswith(".tar.gz") or name.endswith(".tgz"):
        return "tar.gz archive"
    if name.endswith(".csv.gz"):
        return "csv.gz"
    if name.endswith(".json.gz") or name.endswith(".jsonl.gz") or name.endswith(".ndjson.gz"):
        return "json.gz"
    if name.endswith(".parquet.gz"):
        return "parquet.gz"
    if ".parquet" in suffixes:
        return "parquet"
    if ".csv" in suffixes:
        return "csv"
    if ".json" in suffixes or ".jsonl" in suffixes or ".ndjson" in suffixes:
        return "json"
    if ".gz" in suffixes:
        return "gzip compressed file"
    return path.suffix.lower().lstrip(".") or "unknown"


def spark_reader_for(inferred_format: str) -> str | None:
    if inferred_format in {"csv", "csv.gz"}:
        return "csv"
    if inferred_format in {"json", "json.gz"}:
        return "json"
    if inferred_format == "parquet":
        return "parquet"
    return None


def is_spark_readable_candidate(inferred_format: str) -> bool:
    return spark_reader_for(inferred_format) is not None


def schema_to_json(df: Any) -> list[dict[str, Any]]:
    return json.loads(df.schema.json())


def short_error_summary(context: str, exc: Exception | None = None) -> str:
    if exc is None:
        return context
    return f"{context} ({exc.__class__.__name__})"


def try_tar_members(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "archive_readable": False,
        "archive_member_count": None,
        "archive_members_sample": [],
        "error_summary": None,
    }
    try:
        with tarfile.open(path, mode="r:gz") as archive:
            members = archive.getmembers()
            result["archive_readable"] = True
            result["archive_member_count"] = len(members)
            result["archive_members_sample"] = [
                {
                    "name": member.name,
                    "size_bytes": member.size,
                    "size_human": human_size(member.size),
                    "inferred_format": infer_format(Path(member.name)),
                    "spark_readable_candidate": is_spark_readable_candidate(
                        infer_format(Path(member.name))
                    ),
                    "is_file": member.isfile(),
                }
                for member in members[:TAR_MEMBER_LIMIT]
            ]
            if len(members) > TAR_MEMBER_LIMIT:
                result["archive_members_truncated"] = True
                result["archive_member_sample_limit"] = TAR_MEMBER_LIMIT
            else:
                result["archive_members_truncated"] = False
    except Exception as exc:  # noqa: BLE001 - keep per-file inspection resilient.
        result["error_summary"] = short_error_summary("Archive member inspection failed.", exc)
    return result


def check_gzip_readable(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"gzip_readable": False, "error_summary": None}
    try:
        with gzip.open(path, mode="rb") as gz_file:
            gz_file.read(1)
            result["gzip_readable"] = True
    except Exception as exc:  # noqa: BLE001
        result["error_summary"] = short_error_summary("Gzip readability check failed.", exc)
    return result


def start_spark() -> tuple[Any | None, str | None]:
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:  # noqa: BLE001
        return None, short_error_summary("PySpark is not available.", exc)

    try:
        spark = (
            SparkSession.builder.appName("raw-data-inspection")
            .master("local[*]")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark, None
    except Exception as exc:  # noqa: BLE001
        return None, short_error_summary("Could not start SparkSession.", exc)


def inspect_with_spark(spark: Any, path: Path, inferred_format: str, size_bytes: int) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "spark_read_attempted": False,
        "readable_by_spark": False,
        "schema": None,
        "row_count": None,
        "row_count_status": None,
        "row_count_skipped_reason": None,
        "error_summary": None,
    }

    reader_format = spark_reader_for(inferred_format)
    if reader_format is None:
        metadata["error_summary"] = (
            "No direct Spark reader selected for this format. "
            "Archive or format-specific handling may be required."
        )
        return metadata

    metadata["spark_read_attempted"] = True
    try:
        if reader_format == "csv":
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(path))
        elif reader_format == "json":
            df = spark.read.option("inferSchema", "true").json(str(path))
        elif reader_format == "parquet":
            df = spark.read.parquet(str(path))
        else:
            raise ValueError(f"Unsupported reader format: {reader_format}")

        metadata["readable_by_spark"] = True
        metadata["schema"] = schema_to_json(df)

        if size_bytes <= SAFE_COUNT_MAX_BYTES:
            metadata["row_count"] = df.count()
            metadata["row_count_status"] = "counted"
        else:
            metadata["row_count_status"] = "skipped"
            metadata["row_count_skipped_reason"] = (
                f"Skipped because file size {human_size(size_bytes)} is above "
                f"safe count threshold {human_size(SAFE_COUNT_MAX_BYTES)}."
            )
    except Exception as exc:  # noqa: BLE001
        metadata["row_count_status"] = "not_available"
        metadata["error_summary"] = short_error_summary(
            "Spark could not read this file with the selected reader.",
            exc,
        )

    return metadata


def inspect_file(path: Path, spark: Any | None, spark_startup_error: str | None) -> dict[str, Any]:
    size_bytes = path.stat().st_size
    inferred_format = infer_format(path)
    item: dict[str, Any] = {
        "file_name": path.name,
        "relative_path": path.relative_to(PROJECT_ROOT).as_posix(),
        "size_bytes": size_bytes,
        "size_human": human_size(size_bytes),
        "inferred_format": inferred_format,
        "spark_readable_candidate": is_spark_readable_candidate(inferred_format),
        "readable_by_spark": False,
        "schema": None,
        "row_count": None,
        "row_count_status": None,
        "row_count_skipped_reason": None,
        "error_summary": None,
        "notes": [],
    }

    if inferred_format == "tar.gz archive":
        item.update(try_tar_members(path))
        item["error_summary"] = (
            "Spark does not directly read .tar.gz archives as tables. "
            "Archive-specific handling is required before Spark table ingestion."
        )
        item["notes"].append("Raw archive was inspected without extracting or modifying data.")
        return item

    if inferred_format == "gzip compressed file":
        item.update(check_gzip_readable(path))
        item["error_summary"] = item.get("error_summary") or (
            "Generic .gz file detected. Spark needs to know the underlying format "
            "such as CSV, JSON, or text before reading it as a table."
        )
        return item

    if spark is None:
        item["error_summary"] = spark_startup_error or "SparkSession is not available."
        return item

    spark_metadata = inspect_with_spark(spark, path, inferred_format, size_bytes)
    item.update(spark_metadata)
    item["readable_by_spark"] = bool(spark_metadata["readable_by_spark"])
    item["schema"] = spark_metadata["schema"]
    item["row_count"] = spark_metadata["row_count"]
    item["row_count_status"] = spark_metadata["row_count_status"]
    item["row_count_skipped_reason"] = spark_metadata["row_count_skipped_reason"]
    item["error_summary"] = spark_metadata["error_summary"]
    return item


def print_file_summary(item: dict[str, Any]) -> None:
    print(f"\nFile: {item['file_name']}")
    print(f"  Path: {item['relative_path']}")
    print(f"  Size: {item['size_human']}")
    print(f"  Inferred format: {item['inferred_format']}")
    print(f"  Readable by Spark: {item['readable_by_spark']}")

    if item.get("archive_readable"):
        print(f"  Archive members: {item.get('archive_member_count')}")
        sample = item.get("archive_members_sample", [])
        for member in sample[:20]:
            print(
                "    - "
                f"{member['name']} ({member['size_human']}, "
                f"{member['inferred_format']}, "
                f"Spark candidate: {member['spark_readable_candidate']})"
            )
        if item.get("archive_members_truncated"):
            print(f"    ... sample truncated at {item.get('archive_member_sample_limit')} members")

    if item.get("schema"):
        print("  Schema:")
        for field in item["schema"].get("fields", []):
            print(f"    - {field.get('name')}: {field.get('type')}")

    if item.get("row_count") is not None:
        print(f"  Row count: {item['row_count']}")
    if item.get("row_count_skipped_reason"):
        print(f"  Row count skipped: {item['row_count_skipped_reason']}")
    if item.get("error_summary"):
        print(f"  Note/Error: {item['error_summary']}")


def main() -> int:
    print("Raw data inspection job")
    print("Raw data directory: data/raw")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if not RAW_DATA_DIR.exists():
        summary = {
            "generated_at_date": datetime.now(timezone.utc).date().isoformat(),
            "safe_count_max_bytes": SAFE_COUNT_MAX_BYTES,
            "file_count": 0,
            "error_summary": "data/raw directory does not exist.",
            "files": [],
        }
        OUTPUT_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print("data/raw directory does not exist.")
        print("Wrote summary: artifacts/metadata/raw_data_summary.json")
        return 1

    files = sorted([path for path in RAW_DATA_DIR.rglob("*") if path.is_file()])
    print(f"Discovered {len(files)} file(s).")

    spark, spark_startup_error = start_spark()
    if spark_startup_error:
        print(f"Spark startup note: {spark_startup_error}")

    inspected_files = []
    try:
        for path in files:
            item = inspect_file(path, spark, spark_startup_error)
            inspected_files.append(item)
            print_file_summary(item)
    finally:
        if spark is not None:
            spark.stop()

    summary = {
        "generated_at_date": datetime.now(timezone.utc).date().isoformat(),
        "safe_count_max_bytes": SAFE_COUNT_MAX_BYTES,
        "file_count": len(inspected_files),
        "notes": [
            "Metadata is sanitized and contains table-level information only.",
            "Absolute local paths, row samples, and environment details are not persisted.",
        ],
        "files": inspected_files,
    }
    OUTPUT_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print("\nWrote summary: artifacts/metadata/raw_data_summary.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
