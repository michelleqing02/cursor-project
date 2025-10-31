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


def _call_import(function_names: Sequence[str], *args, **kwargs) -> pd.DataFrame:
    """Call the first available nfl_data_py import function from the list."""

    if nfl is None:
        msg = "nfl_data_py not installed. Please install dependencies via requirements.txt."
        logger.error(msg)
        raise RuntimeError(msg)

    for name in function_names:
        func = getattr(nfl, name, None)
        if func is None:
            continue
        logger.debug("Using nfl_data_py.%s for dataset import", name)
        return func(*args, **kwargs)

    msg = f"None of the importers {function_names} exist in nfl_data_py=={getattr(nfl, '__version__', 'unknown')}"
    logger.error(msg)
    raise RuntimeError(msg)


def load_player_weekly_stats(seasons: Sequence[int] | None = None) -> pd.DataFrame:
    """Fetch weekly player stats for the requested seasons."""

    seasons = seasons or get_settings().seasons
    logger.info("Importing weekly player data for seasons=%s", seasons)
    df = _call_import(["import_player_weekly_data"], list(seasons))
    logger.info("Loaded %d weekly rows", len(df))
    return df


def load_team_weekly_stats(seasons: Sequence[int] | None = None) -> pd.DataFrame:
    """Fetch weekly team-level stats."""

    seasons = seasons or get_settings().seasons
    logger.info("Importing weekly team data for seasons=%s", seasons)
    df = _call_import(["import_team_weekly_data"], list(seasons))
    logger.info("Loaded %d team weekly rows", len(df))
    return df


def load_ngs_receiving_stats(seasons: Sequence[int] | None = None) -> pd.DataFrame:
    """Fetch Next Gen Stats receiving data filtered to the requested seasons."""

    seasons = seasons or get_settings().seasons
    logger.info("Importing Next Gen receiving data for seasons=%s", seasons)
    df = _call_import(["import_ngs_receiving_data", "import_ngs_receiving"])
    if "season" in df.columns:
        df = df[df["season"].isin(seasons)]
        logger.debug("Filtered NGS receiving data down to %d rows", len(df))
    else:
        logger.warning("NGS receiving dataset missing 'season' column; returning full dataset")
    return df.reset_index(drop=True)


def load_pfr_advanced_receiving_stats(seasons: Sequence[int] | None = None) -> pd.DataFrame:
    """Fetch Pro Football Reference advanced receiving data."""

    seasons = seasons or get_settings().seasons
    logger.info("Importing PFR advanced receiving data for seasons=%s", seasons)
    df = _call_import([
        "import_pfr_advanced_receiving_stats",
        "import_pfr_advanced_receiving",
    ])
    if "season" in df.columns:
        df = df[df["season"].isin(seasons)]
        logger.debug("Filtered PFR receiving data down to %d rows", len(df))
    else:
        logger.warning("PFR advanced receiving dataset missing 'season' column; returning full dataset")
    return df.reset_index(drop=True)


def load_espn_qbr(seasons: Sequence[int] | None = None) -> pd.DataFrame:
    """Fetch ESPN QBR data for quarterback efficiency."""

    seasons = seasons or get_settings().seasons
    logger.info("Importing ESPN QBR data for seasons=%s", seasons)
    df = _call_import(["import_espn_qbr"])
    if "season" in df.columns:
        df = df[df["season"].isin(seasons)]
        logger.debug("Filtered ESPN QBR data down to %d rows", len(df))
    return df.reset_index(drop=True)


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
