"""Historical stats ingestion and query utilities."""

from __future__ import annotations

from functools import lru_cache
from typing import Iterable, Sequence

import pandas as pd
from loguru import logger

from ..config import get_settings
from ..models import PlayerStatRecord


try:  # Optional import: nfl_data_py pulls large deps
    import nfl_data_py as nfl
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    nfl = None  # type: ignore


def load_player_weekly_stats(seasons: Sequence[int] | None = None) -> pd.DataFrame:
    """Fetch weekly player stats for the requested seasons."""

    seasons = seasons or get_settings().seasons
    if nfl is None:
        msg = "nfl_data_py not installed. Please install dependencies via requirements.txt."
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info("Importing weekly data for seasons=%s", seasons)
    df = nfl.import_player_weekly_data(list(seasons))
    logger.info("Loaded %d weekly rows", len(df))
    return df


def to_stat_records(df: pd.DataFrame) -> Iterable[PlayerStatRecord]:
    """Convert a dataframe into PlayerStatRecord objects."""

    if df.empty:
        return []

    records: list[PlayerStatRecord] = []
    stat_columns = [
        col
        for col in df.columns
        if pd.api.types.is_numeric_dtype(df[col]) and col not in {"season", "week"}
    ]

    for _, row in df.iterrows():
        player_name = row.get("player_display_name") or row.get("player_name")
        if not player_name:
            continue
        season = int(row.get("season")) if row.get("season") else None
        week = int(row.get("week")) if row.get("week") else None
        team = row.get("recent_team")
        opponent = row.get("opponent")

        for column in stat_columns:
            value = row[column]
            if pd.isna(value):
                continue
            records.append(
                PlayerStatRecord(
                    player_name=player_name,
                    season=season or 0,
                    week=week,
                    team=team,
                    opponent=opponent,
                    stat_category=column,
                    stat_value=float(value),
                )
            )
    return records


@lru_cache(maxsize=1)
def get_player_name_index() -> set[str]:
    """Return a cached set of known player names from the stats dataset."""

    from ..data_access.datastore import get_datastore

    df = get_datastore().load_stats()
    if df.empty:
        logger.warning("Stats dataset empty when building player index")
        return set()
    return set(df["player_name"].str.lower().unique())
