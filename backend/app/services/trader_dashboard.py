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
            StatsDataset.SNAP_COUNTS: self._snap_count_metrics,
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
            if dataset == StatsDataset.SNAP_COUNTS and weeks:
                weeks = [week for week in weeks if isinstance(week, (int, float)) and week > 0]
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
        if dataset == StatsDataset.SNAP_COUNTS:
            return self.datastore.load_snap_counts()
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

    def _snap_count_metrics(
        self,
        *,
        season: int | None,
        week: int | None,
        team: str | None,
        player: str | None,
    ) -> pd.DataFrame:
        snap_df = self.datastore.load_snap_counts()
        if snap_df.empty:
            return pd.DataFrame()

        frame = snap_df.copy()
        if "player_name" not in frame.columns and "player" in frame.columns:
            frame = frame.rename(columns={"player": "player_name"})
        if "team" not in frame.columns and "recent_team" in frame.columns:
            frame = frame.rename(columns={"recent_team": "team"})

        for column in ("season", "week"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")

        if season is not None and "season" in frame.columns:
            frame = frame[frame["season"] == season]
        if week is not None and "week" in frame.columns:
            frame = frame[frame["week"] == week]
        if team:
            team_col = next((col for col in ("team", "recent_team") if col in frame.columns), None)
            if team_col:
                frame = frame[frame[team_col].astype(str).str.lower() == team.lower()]
        if player:
            name_col = next((col for col in ("player_name", "player") if col in frame.columns), None)
            if name_col:
                frame = frame[frame[name_col].str.contains(player, case=False, na=False)]

        if frame.empty:
            return pd.DataFrame()

        if "week" in frame.columns:
            frame = frame[frame["week"].notna()]
            frame["week"] = frame["week"].astype(int)
            frame = frame[frame["week"] >= 1]

        if frame.empty:
            return pd.DataFrame()

        percent_cols = [col for col in frame.columns if col.endswith("_pct")]
        for column in percent_cols:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

        numeric_cols = [col for col in frame.columns if col.endswith("_snaps")]
        for column in numeric_cols:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

        if week is not None:
            return self._format_weekly_snap_counts(frame)
        return self._format_multiweek_snap_counts(frame)

    def _format_weekly_snap_counts(self, frame: pd.DataFrame) -> pd.DataFrame:
        columns = [
            "season",
            "week",
            "team",
            "player_name",
            "position",
            "offense_snaps",
            "offense_pct",
            "defense_snaps",
            "defense_pct",
            "special_teams_snaps",
            "special_teams_pct",
        ]
        existing = [col for col in columns if col in frame.columns]
        if not existing:
            return pd.DataFrame()

        weekly = frame[existing].copy()
        percent_cols = [col for col in weekly.columns if col.endswith("_pct")]
        for column in percent_cols:
            weekly[column] = weekly[column].apply(self._normalize_pct)

        sort_key = "offense_pct" if "offense_pct" in weekly.columns else None
        if sort_key:
            weekly = weekly.sort_values(by=sort_key, ascending=False, na_position="last")

        weekly = weekly.reset_index(drop=True)

        for column in percent_cols:
            weekly[column] = weekly[column].apply(self._format_pct_label)

        return weekly

    def _format_multiweek_snap_counts(self, frame: pd.DataFrame) -> pd.DataFrame:
        if "offense_snaps" not in frame.columns or "offense_pct" not in frame.columns:
            return frame

        weeks = sorted({int(week) for week in frame["week"].dropna().unique().tolist()})
        index_cols: list[str] = []
        if "position" in frame.columns:
            index_cols.append("position")
        index_cols.append("player_name")

        grouped = (
            frame.groupby(index_cols, dropna=False)
            .agg(
                team=("team", self._first_non_null),
                total_offense_snaps=("offense_snaps", "sum"),
                avg_offense_pct=("offense_pct", "mean"),
                games_played=("week", lambda s: int(s.dropna().nunique())),
            )
        )

        counts_pivot = frame.pivot_table(
            index=index_cols,
            columns="week",
            values="offense_snaps",
            aggfunc="sum",
            fill_value=0,
        )
        pct_pivot = frame.pivot_table(
            index=index_cols,
            columns="week",
            values="offense_pct",
            aggfunc="mean",
        )

        combined_rows: list[dict[str, object]] = []
        for idx, data in grouped.iterrows():
            if isinstance(idx, tuple):
                if len(index_cols) == 2:
                    position, player_name = idx
                else:
                    position = None
                    player_name = idx[0]
            else:
                position = None
                player_name = idx

            row: dict[str, object] = {}
            if "position" in index_cols:
                row["position"] = position or ""
            row["player_name"] = player_name
            row["team"] = data.get("team") or ""
            row["games_played"] = int(data.get("games_played") or 0)

            total_snaps = data.get("total_offense_snaps")
            row["total_offense_snaps"] = int(total_snaps) if pd.notna(total_snaps) else 0

            avg_pct = self._normalize_pct(data.get("avg_offense_pct"))
            row["avg_offense_pct"] = round(avg_pct, 1) if avg_pct is not None else None

            try:
                counts_row = counts_pivot.loc[idx]
            except KeyError:
                counts_row = pd.Series(dtype=float)
            if not isinstance(counts_row, pd.Series):
                counts_row = pd.Series({weeks[0]: counts_row}) if weeks else pd.Series(dtype=float)

            try:
                pct_row = pct_pivot.loc[idx]
            except KeyError:
                pct_row = pd.Series(dtype=float)
            if not isinstance(pct_row, pd.Series):
                pct_row = pd.Series({weeks[0]: pct_row}) if weeks else pd.Series(dtype=float)

            for week in weeks:
                column_name = f"wk_{week}"
                snaps_value = counts_row.get(week, pd.NA)
                pct_value = pct_row.get(week, pd.NA)
                row[column_name] = self._format_snap_cell(snaps_value, pct_value)

            combined_rows.append(row)

        result = pd.DataFrame(combined_rows)
        if result.empty:
            return result

        base_columns: list[str] = []
        if "position" in result.columns:
            base_columns.append("position")
        base_columns.extend([
            "player_name",
            "team",
            "games_played",
            "total_offense_snaps",
            "avg_offense_pct",
        ])
        week_columns = [f"wk_{week}" for week in weeks]
        ordered = [col for col in base_columns + week_columns if col in result.columns]
        result = result[ordered]

        if "avg_offense_pct" in result.columns:
            result = result.sort_values(by="avg_offense_pct", ascending=False, na_position="last")
        elif "total_offense_snaps" in result.columns:
            result = result.sort_values(by="total_offense_snaps", ascending=False, na_position="last")

        result = result.reset_index(drop=True)

        if "avg_offense_pct" in result.columns:
            result["avg_offense_pct"] = result["avg_offense_pct"].apply(
                lambda value: self._format_pct_label(value) if value is not None else None
            )

        return result

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
    @staticmethod
    def _normalize_pct(value: float | int | None) -> float | None:
        if value is None or pd.isna(value):
            return None
        pct = float(value)
        if pct <= 1:
            pct *= 100
        return pct

    @staticmethod
    def _format_pct_label(value: float | None) -> str | None:
        if value is None or pd.isna(value):
            return None
        return f"{float(value):.1f}%"

    @staticmethod
    def _format_snap_cell(snaps: float | int | None, pct_value: float | int | None) -> str:
        parts: list[str] = []
        if snaps is not None and not pd.isna(snaps):
            snaps_int = int(round(float(snaps)))
            parts.append(str(snaps_int))
        pct_normalized = StatsFilterService._normalize_pct(pct_value)
        if pct_normalized is not None:
            parts.append(f"{pct_normalized:.0f}%")
        return "\n".join(parts) if parts else ""

    @staticmethod
    def _first_non_null(series: pd.Series) -> str | None:
        for value in series:
            if pd.notna(value):
                return value
        return None

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
            StatsDataset.SNAP_COUNTS: ["avg_offense_pct", "total_offense_snaps"],
        }
        for candidate in priorities.get(dataset, []):
            if candidate in frame.columns:
                return candidate
        return None


def _compact_filters(**filters: Any) -> dict[str, Any]:
    return {key: value for key, value in filters.items() if value is not None}

