"""CLI for refreshing prop and stat datasets."""

from __future__ import annotations

import argparse
import asyncio
from typing import Sequence

import pandas as pd
from loguru import logger

from ..app.config import get_settings
from ..app.data_access.datastore import DataStore
from ..app.services.props import collect_props
from ..app.services.stats import load_player_weekly_stats, to_stat_records


async def refresh_data(
    datastore: DataStore,
    seasons: Sequence[int],
    include_props: bool = True,
    include_stats: bool = True,
) -> None:
    if include_props:
        logger.info("Collecting prop data?")
        prop_records = await collect_props()
        datastore.write_props(prop_records)

    if include_stats:
        logger.info("Collecting player stats for seasons=%s", seasons)
        stats_df = load_player_weekly_stats(seasons)
        stat_records = list(to_stat_records(stats_df))
        stats_records_df = pd.DataFrame(record.model_dump() for record in stat_records)
        datastore.write_stats(stats_records_df)

    logger.success("Data refresh completed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh NFL prop and player stats datasets")
    parser.add_argument("--seasons", nargs="*", type=int, help="NFL seasons to fetch (e.g. 2023 2024)")
    parser.add_argument("--no-props", action="store_true", help="Skip prop scraping")
    parser.add_argument("--no-stats", action="store_true", help="Skip stats ingestion")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    seasons = args.seasons or settings.seasons
    datastore = DataStore()

    asyncio.run(
        refresh_data(
            datastore=datastore,
            seasons=seasons,
            include_props=not args.no_props,
            include_stats=not args.no_stats,
        )
    )


if __name__ == "__main__":
    main()
