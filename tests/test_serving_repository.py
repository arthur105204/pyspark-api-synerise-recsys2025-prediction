from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from api.repository import ScoreDatabaseNotFoundError, ScoreRepository


def create_test_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
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
        connection.executemany(
            """
            INSERT INTO scores
            (client_id, prediction_score, prediction_label, model_version, scored_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("client_test_001", 0.8234, 1, "baseline_lr_v1", "2026-06-12T00:00:00+00:00"),
                ("client_test_002", 0.1234, 0, "baseline_lr_v1", "2026-06-12T00:00:00+00:00"),
            ],
        )
        connection.commit()
    finally:
        connection.close()


def test_repository_returns_score(tmp_path: Path) -> None:
    db_path = tmp_path / "scores.sqlite"
    create_test_db(db_path)
    record = ScoreRepository(db_path).get_score("client_test_001")
    assert record is not None
    assert record.client_id == "client_test_001"
    assert 0 <= record.prediction_score <= 1
    assert record.prediction_label in {0, 1}


def test_repository_returns_none_for_missing_client(tmp_path: Path) -> None:
    db_path = tmp_path / "scores.sqlite"
    create_test_db(db_path)
    assert ScoreRepository(db_path).get_score("client_missing") is None


def test_repository_missing_database_raises_clear_error(tmp_path: Path) -> None:
    with pytest.raises(ScoreDatabaseNotFoundError):
        ScoreRepository(tmp_path / "missing.sqlite").get_score("client_test_001")
