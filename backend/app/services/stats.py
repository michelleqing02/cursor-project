"""Historical stats ingestion and query utilities."""

from __future__ import annotations

from functools import lru_cache
from typing import Iterable, Sequence

import pandas as pd
from loguru import logger

from ..config import get_settings
from ..models import PlayerStatRecord
from .nfl_scraper import scrape_player_stats, scrape_team_stats


def load_player_weekly_stats(seasons: Sequence[int] | None = None) -> pd.DataFrame:
    """Fetch weekly player stats for the requested seasons."""

    settings = get_settings()
    seasons = tuple(seasons or settings.seasons)
    logger.info("Scraping player stats for seasons=%s", seasons)
    df = scrape_player_stats(seasons, timeout=settings.http_timeout)
    logger.info("Scraped player stats rows=%d", len(df))
    return df


def load_team_weekly_stats(seasons: Sequence[int] | None = None) -> pd.DataFrame:
    """Fetch weekly team-level stats."""

    settings = get_settings()
    seasons = tuple(seasons or settings.seasons)
    logger.info("Scraping team stats for seasons=%s", seasons)
    df = scrape_team_stats(seasons, timeout=settings.http_timeout)
    logger.info("Scraped team stats rows=%d", len(df))
    return df


def load_ngs_receiving_stats(seasons: Sequence[int] | None = None) -> pd.DataFrame:
    """Fetch Next Gen Stats receiving data filtered to the requested seasons."""

    logger.info("Next Gen Stats receiving scraping not implemented; returning empty DataFrame")
    return pd.DataFrame()


def load_pfr_advanced_receiving_stats(seasons: Sequence[int] | None = None) -> pd.DataFrame:
    """Fetch Pro Football Reference advanced receiving data."""

    logger.info("PFR advanced receiving scraping not implemented; returning empty DataFrame")
    return pd.DataFrame()


def load_espn_qbr(seasons: Sequence[int] | None = None) -> pd.DataFrame:
    """Fetch ESPN QBR data for quarterback efficiency."""

    logger.info("ESPN QBR scraping not implemented; returning empty DataFrame")
    return pd.DataFrame()


def to_stat_records(df: pd.DataFrame) -> Iterable[PlayerStatRecord]:
    """Convert a dataframe into PlayerStatRecord objects."""

    if df.empty:
        return []

    records: list[PlayerStatRecord] = []
    stat_columns = [
        col
        for col in df.columns
        if pd.api.types.is_numeric_dtype(df[col])
        and col not in {"season", "week", "team", "team_abbr", "opponent"}
    ]

    for _, row in df.iterrows():
        player_name = row.get("player_display_name") or row.get("player_name")
        if not player_name:
            continue
        season = int(row.get("season")) if row.get("season") else None
        week = int(row.get("week")) if row.get("week") else None
        team = row.get("recent_team") or row.get("team")
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
