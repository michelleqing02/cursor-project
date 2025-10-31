"""DuckDB/Parquet-backed storage and retrieval helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd
from loguru import logger

from ..config import get_settings
from ..models import PlayerStatRecord, PropRecord


class DataStore:
    """Central access point for data persistence."""

    def __init__(self, base_dir: Path | None = None) -> None:
        settings = get_settings()
        self.base_dir = base_dir or settings.data_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self.base_dir / "props.db"))

    # ------------------------------------------------------------------
    # Prop storage
    # ------------------------------------------------------------------
    def props_path(self) -> Path:
        return self.base_dir / "props.parquet"

    def write_props(self, records: Iterable[PropRecord]) -> int:
        data = [record.model_dump() for record in records]
        if not data:
            logger.warning("No prop data supplied; skipping write")
            return 0

        df = pd.DataFrame(data)
        df.to_parquet(self.props_path(), index=False)
        logger.info("Wrote %d prop records to %s", len(df), self.props_path())
        return len(df)

    def load_props(self) -> pd.DataFrame:
        path = self.props_path()
        if not path.exists():
            logger.warning("Prop data file missing at %s", path)
            return pd.DataFrame()
        return pd.read_parquet(path)

    # ------------------------------------------------------------------
    # Player stats storage
    # ------------------------------------------------------------------
    def stats_path(self) -> Path:
        return self.base_dir / "player_stats.parquet"

    def write_stats(self, records: pd.DataFrame | Iterable[PlayerStatRecord]) -> int:
        if isinstance(records, pd.DataFrame):
            df = records
        else:
            df = pd.DataFrame(record.model_dump() for record in records)

        if df.empty:
            logger.warning("No stat data supplied; skipping write")
            return 0

        df.to_parquet(self.stats_path(), index=False)
        logger.info("Wrote %d player stat rows to %s", len(df), self.stats_path())
        return len(df)

    def load_stats(self) -> pd.DataFrame:
        path = self.stats_path()
        if not path.exists():
            logger.warning("Stats data file missing at %s", path)
            return pd.DataFrame()
        return pd.read_parquet(path)

    # ------------------------------------------------------------------
    # Team stats storage
    # ------------------------------------------------------------------
    def team_stats_path(self) -> Path:
        return self.base_dir / "team_stats.parquet"

    def write_team_stats(self, records: pd.DataFrame) -> int:
        if records.empty:
            logger.warning("No team stat data supplied; skipping write")
            return 0

        records.to_parquet(self.team_stats_path(), index=False)
        logger.info("Wrote %d team stat rows to %s", len(records), self.team_stats_path())
        return len(records)

    def load_team_stats(self) -> pd.DataFrame:
        path = self.team_stats_path()
        if not path.exists():
            logger.warning("Team stats data file missing at %s", path)
            return pd.DataFrame()
        return pd.read_parquet(path)

    # ------------------------------------------------------------------
    # Advanced receiving / Next Gen stats storage
    # ------------------------------------------------------------------
    def ngs_receiving_path(self) -> Path:
        return self.base_dir / "ngs_receiving.parquet"

    def write_ngs_receiving(self, records: pd.DataFrame) -> int:
        if records.empty:
            logger.warning("No Next Gen receiving data supplied; skipping write")
            return 0

        records.to_parquet(self.ngs_receiving_path(), index=False)
        logger.info(
            "Wrote %d Next Gen receiving rows to %s",
            len(records),
            self.ngs_receiving_path(),
        )
        return len(records)

    def load_ngs_receiving(self) -> pd.DataFrame:
        path = self.ngs_receiving_path()
        if not path.exists():
            logger.warning("Next Gen receiving data file missing at %s", path)
            return pd.DataFrame()
        return pd.read_parquet(path)

    # ------------------------------------------------------------------
    # Pro Football Reference advanced receiving storage
    # ------------------------------------------------------------------
    def pfr_receiving_path(self) -> Path:
        return self.base_dir / "pfr_advanced_receiving.parquet"

    def write_pfr_receiving(self, records: pd.DataFrame) -> int:
        if records.empty:
            logger.warning("No PFR receiving data supplied; skipping write")
            return 0

        records.to_parquet(self.pfr_receiving_path(), index=False)
        logger.info(
            "Wrote %d PFR advanced receiving rows to %s",
            len(records),
            self.pfr_receiving_path(),
        )
        return len(records)

    def load_pfr_receiving(self) -> pd.DataFrame:
        path = self.pfr_receiving_path()
        if not path.exists():
            logger.warning("PFR advanced receiving data file missing at %s", path)
            return pd.DataFrame()
        return pd.read_parquet(path)

    # ------------------------------------------------------------------
    # ESPN QBR storage
    # ------------------------------------------------------------------
    def espn_qbr_path(self) -> Path:
        return self.base_dir / "espn_qbr.parquet"

    def write_espn_qbr(self, records: pd.DataFrame) -> int:
        if records.empty:
            logger.warning("No ESPN QBR data supplied; skipping write")
            return 0

        records.to_parquet(self.espn_qbr_path(), index=False)
        logger.info("Wrote %d ESPN QBR rows to %s", len(records), self.espn_qbr_path())
        return len(records)

    def load_espn_qbr(self) -> pd.DataFrame:
        path = self.espn_qbr_path()
        if not path.exists():
            logger.warning("ESPN QBR data file missing at %s", path)
            return pd.DataFrame()
        return pd.read_parquet(path)

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def query_stats_by_player(self, player_name: str) -> pd.DataFrame:
        stats = self.load_stats()
        if stats.empty:
            return stats
        return stats[stats["player_name"].str.lower() == player_name.lower()]

    def query_props_by_player(self, player_name: str) -> pd.DataFrame:
        props = self.load_props()
        if props.empty:
            return props
        return props[props["player_name"].str.lower() == player_name.lower()]


def get_datastore() -> DataStore:
    """Return a shared datastore instance."""

    return DataStore()
