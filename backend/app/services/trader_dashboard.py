"""Filtering and aggregation logic for the trader-facing stats endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd
from loguru import logger

from ..data_access.datastore import DataStore
from ..models import StatsDataset, StatsResponse


DEFAULT_LIMIT = 100


def _normalize_key(series: pd.Series) -> pd.Series:
    """Normalize player names for joins."""

    return series.fillna("").str.replace(r"[^a-zA-Z\s]", "", regex=True).str.lower().str.strip()


@dataclass(slots=True)
class StatsFilterService:
    datastore: DataStore

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def fetch(
        self,
        dataset: StatsDataset,
        *,
        season: int | None = None,
        week: int | None = None,
        team: str | None = None,
        player: str | None = None,
        limit: int | None = None,
        sort: str | None = None,
        descending: bool = True,
    ) -> StatsResponse:
        """Return a StatsResponse for the requested dataset."""

        limit = limit or DEFAULT_LIMIT
        limit = max(1, min(limit, 500))

        loaders = {
            StatsDataset.PLAYER: self._player_metrics,
            StatsDataset.TEAM: self._team_metrics,
            StatsDataset.RECEIVING_EFFICIENCY: self._receiving_efficiency_metrics,
            StatsDataset.QUARTERBACK_EFFICIENCY: self._quarterback_metrics,
        }

        loader = loaders.get(dataset)
        if loader is None:  # pragma: no cover - guard
            raise ValueError(f"Unsupported dataset: {dataset}")

        frame = loader(season=season, week=week, team=team, player=player)

        if frame.empty:
            filters = _compact_filters(season=season, week=week, team=team, player=player, sort=sort)
            return StatsResponse(dataset=dataset, columns=[], rows=[], filters=filters, metadata=self.get_metadata(dataset))

        if sort and sort in frame.columns:
            frame = frame.sort_values(by=sort, ascending=not descending)
        else:
            frame = self._apply_default_sort(dataset, frame)

        trimmed = frame.head(limit)

        filters = _compact_filters(
            season=season,
            week=week,
            team=team,
            player=player,
            sort=sort or self._default_sort_key(dataset, frame),
            limit=limit,
        )

        response = StatsResponse(
            dataset=dataset,
            columns=list(trimmed.columns),
            rows=trimmed.to_dict(orient="records"),
            filters=filters,
            metadata=self.get_metadata(dataset),
        )

        logger.debug("Prepared stats response for dataset=%s with %d rows", dataset, len(response.rows))
        return response

    def get_metadata(self, dataset: StatsDataset | None = None) -> dict[str, Any]:
        """Return available season/week/team values for one or all datasets."""

        datasets: Iterable[StatsDataset]
        if dataset is None:
            datasets = StatsDataset
        else:
            datasets = (dataset,)

        metadata: dict[str, Any] = {}
        for ds in datasets:
            frame = self._load_dataset(ds)
            if frame.empty:
                metadata[ds.value] = {
                    "seasons": [],
                    "weeks": [],
                    "teams": [],
                }
                continue

            seasons = sorted(frame["season"].dropna().unique().tolist()) if "season" in frame.columns else []
            weeks = sorted(frame["week"].dropna().unique().tolist()) if "week" in frame.columns else []
            if "team" in frame.columns:
                team_col = "team"
            elif "posteam" in frame.columns:
                team_col = "posteam"
            elif "team_abbr" in frame.columns:
                team_col = "team_abbr"
            else:
                team_col = None
            teams = (
                sorted(frame[team_col].dropna().unique().tolist())
                if team_col and team_col in frame.columns
                else []
            )

            metadata[ds.value] = {
                "seasons": seasons,
                "weeks": weeks,
                "teams": teams,
            }
        return metadata

    # ------------------------------------------------------------------
    # Dataset loaders
    # ------------------------------------------------------------------
    def _load_dataset(self, dataset: StatsDataset) -> pd.DataFrame:
        if dataset == StatsDataset.PLAYER:
            return self.datastore.load_stats()
        if dataset == StatsDataset.TEAM:
            return self.datastore.load_team_stats()
        if dataset == StatsDataset.RECEIVING_EFFICIENCY:
            return self.datastore.load_ngs_receiving()
        if dataset == StatsDataset.QUARTERBACK_EFFICIENCY:
            return self.datastore.load_espn_qbr()
        raise ValueError(f"Unsupported dataset: {dataset}")

    def _player_metrics(
        self,
        *,
        season: int | None,
        week: int | None,
        team: str | None,
        player: str | None,
    ) -> pd.DataFrame:
        stats_df = self.datastore.load_stats()
        if stats_df.empty:
            return pd.DataFrame()

        frame = stats_df.copy()
        if season is not None and "season" in frame.columns:
            frame = frame[frame["season"] == season]
        if week is not None and "week" in frame.columns:
            frame = frame[frame["week"] == week]
        if team:
            team_lower = team.lower()
            if "team" in frame.columns:
                frame = frame[frame["team"].str.lower() == team_lower]
            elif "recent_team" in frame.columns:
                frame = frame[frame["recent_team"].str.lower() == team_lower]
        if player:
            frame = frame[frame["player_name"].str.contains(player, case=False, na=False)]

        if frame.empty:
            return pd.DataFrame()

        index_cols = [col for col in ["player_name", "season", "week", "team", "opponent"] if col in frame.columns]

        metric_frame = (
            frame.pivot_table(
                index=index_cols,
                columns="stat_category",
                values="stat_value",
                aggfunc="sum",
                fill_value=0.0,
            )
            .reset_index()
        )

        metric_frame = self._enrich_receiving_metrics(metric_frame)
        metric_frame = self._enrich_rushing_metrics(metric_frame)

        return metric_frame

    def _team_metrics(
        self,
        *,
        season: int | None,
        week: int | None,
        team: str | None,
        player: str | None,
    ) -> pd.DataFrame:
        del player  # teams dataset ignores player filters
        team_df = self.datastore.load_team_stats()
        if team_df.empty:
            return pd.DataFrame()

        frame = team_df.copy()
        if season is not None and "season" in frame.columns:
            frame = frame[frame["season"] == season]
        if week is not None and "week" in frame.columns:
            frame = frame[frame["week"] == week]
        if team:
            team_lower = team.lower()
            team_col = "team" if "team" in frame.columns else "posteam" if "posteam" in frame.columns else None
            if team_col:
                frame = frame[frame[team_col].str.lower() == team_lower]

        if frame.empty:
            return pd.DataFrame()

        # Select a core subset of columns to keep the response focused
        preferred_cols = [
            "team",
            "posteam",
            "season",
            "week",
            "opponent",
            "total_yards",
            "passing_yards",
            "rushing_yards",
            "plays_offense",
            "points",
        ]
        existing = [col for col in preferred_cols if col in frame.columns]
        numeric_cols = [col for col in frame.columns if col not in existing and pd.api.types.is_numeric_dtype(frame[col])]

        columns = existing + numeric_cols[:20]  # cap extra columns to avoid huge payloads
        subset = frame[columns].copy()

        if "total_yards" in subset.columns and "plays_offense" in subset.columns:
            plays = subset["plays_offense"].replace({0: pd.NA})
            subset["yards_per_play"] = (subset["total_yards"] / plays).round(3)
        subset = subset.rename(columns={"posteam": "team", "points": "points_for"})
        subset = subset.loc[:, ~subset.columns.duplicated()]

        return subset

    def _receiving_efficiency_metrics(
        self,
        *,
        season: int | None,
        week: int | None,
        team: str | None,
        player: str | None,
    ) -> pd.DataFrame:
        player_metrics = self._player_metrics(season=season, week=week, team=team, player=player)
        if player_metrics.empty:
            return pd.DataFrame()

        columns_to_keep = [
            col
            for col in [
                "player_name",
                "team",
                "season",
                "week",
                "receptions",
                "targets",
                "receiving_yards",
                "receiving_tds",
                "yards_per_catch",
                "yards_per_target",
                "catch_rate",
                "yards_after_catch",
            ]
            if col in player_metrics.columns
        ]
        efficiency = player_metrics[columns_to_keep].copy()

        # Merge Next Gen Stats metrics
        ngs_df = self.datastore.load_ngs_receiving()
        if not ngs_df.empty:
            ngs = ngs_df.copy()
            rename_map = {
                "player_display_name": "player_name",
                "avg_cushion": "ngs_avg_cushion",
                "avg_separation": "ngs_avg_separation",
                "avg_yac": "ngs_avg_yac",
                "avg_intended_air_yards": "ngs_avg_air_yards",
            }
            ngs = ngs.rename(columns={k: v for k, v in rename_map.items() if k in ngs.columns})

            join_cols = [col for col in ["season", "week"] if col in ngs.columns and col in efficiency.columns]
            ngs["__key"] = _normalize_key(ngs.get("player_name", ngs.get("player_display_name", pd.Series(dtype=str))))
            efficiency["__key"] = _normalize_key(efficiency["player_name"])

            keep_cols = ["__key"] + join_cols + [col for col in rename_map.values() if col in ngs.columns]
            keep_cols = [col for col in keep_cols if col in ngs.columns]
            ngs = ngs[keep_cols].drop_duplicates()

            if join_cols:
                efficiency = efficiency.merge(ngs, how="left", on=["__key"] + join_cols, suffixes=("", "_ngs"))
            else:
                efficiency = efficiency.merge(ngs, how="left", on="__key", suffixes=("", "_ngs"))

        # Merge PFR advanced receiving metrics
        pfr_df = self.datastore.load_pfr_receiving()
        if not pfr_df.empty:
            pfr = pfr_df.copy()
            rename_map = {
                "player": "player_name",
                "tm": "team",
                "trg": "pfr_targets",
                "rec": "pfr_receptions",
                "rec_perc": "pfr_catch_rate",
                "yds": "pfr_yards",
                "yds_per_rec": "pfr_yards_per_catch",
                "yac": "pfr_yards_after_catch",
            }
            pfr = pfr.rename(columns={k: v for k, v in rename_map.items() if k in pfr.columns})

            if "season" in pfr.columns and season is not None:
                pfr = pfr[pfr["season"] == season]

            pfr["__key"] = _normalize_key(pfr.get("player_name", pfr.get("player", pd.Series(dtype=str))))

            join_cols = [col for col in ["season"] if col in pfr.columns and col in efficiency.columns]
            keep_cols = ["__key"] + join_cols + [col for col in rename_map.values() if col in pfr.columns]
            keep_cols = [col for col in keep_cols if col in pfr.columns]
            pfr = pfr[keep_cols].drop_duplicates()
            if join_cols:
                efficiency = efficiency.merge(pfr, how="left", on=["__key"] + join_cols, suffixes=("", "_pfr"))
            else:
                efficiency = efficiency.merge(pfr, how="left", on="__key", suffixes=("", "_pfr"))

        efficiency = efficiency.loc[:, ~efficiency.columns.duplicated()].copy()
        if "__key" in efficiency.columns:
            efficiency = efficiency.drop(columns="__key")

        return efficiency

    def _quarterback_metrics(
        self,
        *,
        season: int | None,
        week: int | None,
        team: str | None,
        player: str | None,
    ) -> pd.DataFrame:
        del week  # QBR dataset is seasonal
        qbr_df = self.datastore.load_espn_qbr()
        if qbr_df.empty:
            return pd.DataFrame()

        frame = qbr_df.copy()

        if season is not None and "season" in frame.columns:
            frame = frame[frame["season"] == season]
        if team:
            team_lower = team.lower()
            team_col = next((col for col in ["team_abbr", "team", "abbr"] if col in frame.columns), None)
            if team_col:
                frame = frame[frame[team_col].str.lower() == team_lower]
        if player:
            name_col = next((col for col in ["player_name", "player", "player_display_name"] if col in frame.columns), None)
            if name_col:
                frame = frame[frame[name_col].str.contains(player, case=False, na=False)]

        if frame.empty:
            return pd.DataFrame()

        name_col = next((col for col in ["player_name", "player", "player_display_name"] if col in frame.columns), None)
        team_col = next((col for col in ["team_abbr", "team", "abbr"] if col in frame.columns), None)
        rename_map = {}
        if name_col and name_col != "player_name":
            rename_map[name_col] = "player_name"
        if team_col and team_col != "team":
            rename_map[team_col] = "team"
        if "qbr_total" in frame.columns:
            rename_map["qbr_total"] = "total_qbr"
        if "raw_qbr" in frame.columns:
            rename_map["raw_qbr"] = "raw_qbr"
        if "rank" in frame.columns and "qbr_rank" not in frame.columns:
            rename_map["rank"] = "qbr_rank"
        if "qbr_rank" in frame.columns:
            rename_map["qbr_rank"] = "qbr_rank"
        if "games_played" in frame.columns:
            rename_map["games_played"] = "games"
        if rename_map:
            frame = frame.rename(columns=rename_map)

        preferred_cols = [
            "player_name",
            "team",
            "season",
            "games",
            "total_qbr",
            "raw_qbr",
            "qbr_rank",
            "qb_points_added",
            "qb_total_hits",
            "qb_sacked_yards_lost",
            "qb_plays",
        ]
        existing = [col for col in preferred_cols if col in frame.columns]

        subset = frame[existing].copy() if existing else frame.copy()
        if "total_qbr" in subset.columns:
            subset["total_qbr"] = subset["total_qbr"].round(2)
        if "raw_qbr" in subset.columns:
            subset["raw_qbr"] = subset["raw_qbr"].round(2)
        if "games" in subset.columns and "total_qbr" in subset.columns:
            games = subset["games"].replace({0: pd.NA})
            subset["qbr_per_game"] = (subset["total_qbr"] / games).round(3)

        return subset.loc[:, ~subset.columns.duplicated()]

    # ------------------------------------------------------------------
    # Metric enrichment helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _enrich_receiving_metrics(frame: pd.DataFrame) -> pd.DataFrame:
        df = frame.copy()
        if "receiving_yards" in df.columns and "receptions" in df.columns:
            receptions = df["receptions"].replace({0: pd.NA})
            df["yards_per_catch"] = (df["receiving_yards"] / receptions).round(3)
        if "receiving_yards" in df.columns and "targets" in df.columns:
            targets = df["targets"].replace({0: pd.NA})
            df["yards_per_target"] = (df["receiving_yards"] / targets).round(3)
        if "receptions" in df.columns and "targets" in df.columns:
            targets = df["targets"].replace({0: pd.NA})
            df["catch_rate"] = (df["receptions"] / targets).round(3)
        if "receiving_yards" in df.columns and "yards_after_catch" not in df.columns:
            yak_candidates = [col for col in df.columns if "yac" in col.lower()]
            if yak_candidates:
                df["yards_after_catch"] = df[yak_candidates[0]]
        return df

    @staticmethod
    def _enrich_rushing_metrics(frame: pd.DataFrame) -> pd.DataFrame:
        df = frame.copy()
        if "rushing_yards" in df.columns and "rushing_attempts" in df.columns:
            attempts = df["rushing_attempts"].replace({0: pd.NA})
            df["yards_per_carry"] = (df["rushing_yards"] / attempts).round(3)
        return df

    # ------------------------------------------------------------------
    # Sorting helpers
    # ------------------------------------------------------------------
    def _apply_default_sort(self, dataset: StatsDataset, frame: pd.DataFrame) -> pd.DataFrame:
        key = self._default_sort_key(dataset, frame)
        if key and key in frame.columns:
            return frame.sort_values(by=key, ascending=False)
        return frame

    @staticmethod
    def _default_sort_key(dataset: StatsDataset, frame: pd.DataFrame) -> str | None:
        priorities = {
            StatsDataset.PLAYER: ["receiving_yards", "rushing_yards", "passing_yards"],
            StatsDataset.TEAM: ["total_yards", "points_for"],
            StatsDataset.RECEIVING_EFFICIENCY: ["yards_per_catch", "receiving_yards"],
            StatsDataset.QUARTERBACK_EFFICIENCY: ["total_qbr", "raw_qbr"],
        }
        for candidate in priorities.get(dataset, []):
            if candidate in frame.columns:
                return candidate
        return None


def _compact_filters(**filters: Any) -> dict[str, Any]:
    return {key: value for key, value in filters.items() if value is not None}

