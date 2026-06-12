"""SQLite repository for purchase propensity score lookup."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


class ScoreDatabaseNotFoundError(RuntimeError):
    """Raised when the configured score database does not exist."""


@dataclass(frozen=True)
class ScoreRecord:
    client_id: str
    prediction_score: float
    prediction_label: int
    model_version: str
    scored_at: str


class ScoreRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        if not self.db_path.exists():
            raise ScoreDatabaseNotFoundError("score database is not available")
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def get_score(self, client_id: str) -> ScoreRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT client_id, prediction_score, prediction_label, model_version, scored_at
                FROM scores
                WHERE client_id = ?
                """,
                (str(client_id),),
            ).fetchone()
        if row is None:
            return None
        return ScoreRecord(
            client_id=str(row["client_id"]),
            prediction_score=float(row["prediction_score"]),
            prediction_label=int(row["prediction_label"]),
            model_version=str(row["model_version"]),
            scored_at=str(row["scored_at"]),
        )
