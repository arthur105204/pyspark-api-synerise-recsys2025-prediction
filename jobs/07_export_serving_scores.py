"""Export batch scores to a local SQLite lookup store.

This Phase 7 job reads the Phase 6 Spark Parquet score output and exports only
the serving columns needed by the API lookup layer. The generated SQLite
database is local serving data and must not be committed.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "pipeline_config.yaml"
DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "scoring" / "purchase_propensity_scores"
DEFAULT_OUTPUT_DB = PROJECT_ROOT / "data" / "serving" / "purchase_propensity_scores.sqlite"
REQUIRED_COLUMNS = ["client_id", "prediction_score", "prediction_label", "model_version", "scored_at"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export purchase propensity scores to SQLite.")
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative pipeline config path. Reserved for consistency.",
    )
    parser.add_argument(
        "--input-path",
        default=DEFAULT_INPUT_PATH.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative Phase 6 score table path.",
    )
    parser.add_argument(
        "--output-db",
        default=DEFAULT_OUTPUT_DB.relative_to(PROJECT_ROOT).as_posix(),
        help="Repo-relative SQLite output path.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit for local demo exports. Omit for full export.",
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=None,
        help="Optional sample fraction for testing only. Omit for full export.",
    )
    parser.add_argument("--batch-size", type=int, default=10000, help="SQLite insert batch size.")
    return parser.parse_args()


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        raise ValueError("path arguments must be repo-relative")
    return PROJECT_ROOT / path


def relative_path(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


def start_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("export-purchase-propensity-serving-scores")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_columns(df: DataFrame) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Score table is missing required columns: {', '.join(missing)}")


def prepare_output_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE scores (
            client_id TEXT PRIMARY KEY,
            prediction_score REAL NOT NULL,
            prediction_label INTEGER NOT NULL,
            model_version TEXT NOT NULL,
            scored_at TEXT NOT NULL
        )
        """
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_scores_client_id ON scores(client_id)")
    return connection


def row_batches(rows: Iterable[object], batch_size: int) -> Iterable[list[tuple[str, float, int, str, str]]]:
    batch: list[tuple[str, float, int, str, str]] = []
    for row in rows:
        batch.append(
            (
                str(row["client_id"]),
                float(row["prediction_score"]),
                int(row["prediction_label"]),
                str(row["model_version"]),
                str(row["scored_at"]),
            )
        )
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def export_to_sqlite(df: DataFrame, output_db: Path, batch_size: int) -> int:
    connection = prepare_output_db(output_db)
    total = 0
    try:
        insert_sql = """
            INSERT OR REPLACE INTO scores
            (client_id, prediction_score, prediction_label, model_version, scored_at)
            VALUES (?, ?, ?, ?, ?)
        """
        for batch in row_batches(df.toLocalIterator(), batch_size):
            connection.executemany(insert_sql, batch)
            connection.commit()
            total += len(batch)
    finally:
        connection.close()
    return total


def main() -> int:
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        raise ValueError("--limit must be positive when provided")
    if args.sample_fraction is not None and not 0 < args.sample_fraction <= 1:
        raise ValueError("--sample-fraction must be greater than 0 and at most 1.0")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")

    input_path = resolve_repo_path(args.input_path)
    output_db = resolve_repo_path(args.output_db)
    spark = start_spark()
    try:
        scores = spark.read.parquet(str(input_path))
        validate_columns(scores)
        export_df = scores.select(*REQUIRED_COLUMNS)
        if args.sample_fraction is not None:
            export_df = export_df.sample(withReplacement=False, fraction=args.sample_fraction, seed=42)
        if args.limit is not None:
            export_df = export_df.limit(args.limit)

        null_count = export_df.where(
            F.col("client_id").isNull()
            | F.col("prediction_score").isNull()
            | F.col("prediction_label").isNull()
            | F.col("model_version").isNull()
            | F.col("scored_at").isNull()
        ).count()
        if null_count:
            raise ValueError("Export data contains null values in required serving columns")

        duplicate_count = (
            export_df.groupBy("client_id")
            .count()
            .where(F.col("count") > 1)
            .agg(F.count(F.lit(1)).alias("duplicate_client_id_count"))
            .collect()[0]["duplicate_client_id_count"]
            or 0
        )
        if int(duplicate_count):
            raise ValueError("Export data contains duplicate client_id values")

        exported_count = export_to_sqlite(export_df, output_db, args.batch_size)
        print("Serving score export completed.")
        print(f"Exported rows: {exported_count}")
        print(f"SQLite output: {relative_path(output_db)}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
