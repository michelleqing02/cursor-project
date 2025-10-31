"""Natural language question parsing and response generation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

from rapidfuzz import fuzz, process

from ..data_access.datastore import DataStore
from ..models import QuestionAnswer, QuestionIntent


PLAYER_NAME_PATTERN = re.compile(r"[a-zA-Z]+(?:\s+[a-zA-Z\-']+)+")
SEASON_PATTERN = re.compile(r"(20\d{2})")
WEEK_PATTERN = re.compile(r"week\s+(\d{1,2})", re.IGNORECASE)

PROP_KEYWORDS = {
    "prop",
    "line",
    "projection",
    "odds",
    "over",
    "under",
}

STAT_KEYWORDS = {
    "stat",
    "stats",
    "yards",
    "touchdowns",
    "completions",
    "attempts",
    "targets",
    "receptions",
}


@dataclass(slots=True)
class QuestionAnsweringService:
    datastore: DataStore
    player_names: Iterable[str] | None = None

    def __post_init__(self) -> None:
        if self.player_names is None:
            stats = self.datastore.load_stats()
            props = self.datastore.load_props()
            names = set()
            if not stats.empty:
                names |= set(stats["player_name"].dropna().str.lower().unique())
            if not props.empty:
                names |= set(props["player_name"].dropna().str.lower().unique())
            self.player_names = sorted(names)
        else:
            self.player_names = sorted({name.lower() for name in self.player_names})

    def answer(self, question: str) -> QuestionAnswer:
        normalized = question.strip().lower()
        intent = self._infer_intent(normalized)
        player_name = self._extract_player(question)
        season = self._extract_season(normalized)
        week = self._extract_week(normalized)
        intent.player_name = player_name
        intent.season = season
        intent.week = week

        if intent.intent == "props_lookup" and player_name:
            return self._answer_props(intent)
        if intent.intent == "player_stats" and player_name:
            return self._answer_stats(intent)

        return QuestionAnswer(intent=intent, answer="I could not determine an answer from the available data.")

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------
    def _infer_intent(self, normalized_question: str) -> QuestionIntent:
        tokens = set(normalized_question.split())
        if tokens & PROP_KEYWORDS:
            return QuestionIntent(intent="props_lookup")
        if tokens & STAT_KEYWORDS:
            return QuestionIntent(intent="player_stats")
        # fallback: if contains "prop" substring
        if "prop" in normalized_question:
            return QuestionIntent(intent="props_lookup")
        if "stat" in normalized_question or "stats" in normalized_question:
            return QuestionIntent(intent="player_stats")
        return QuestionIntent(intent="unknown")

    def _extract_player(self, question: str) -> str | None:
        if not self.player_names:
            return None

        candidate = None
        # Use regex to pick proper-noun sequences
        matches = PLAYER_NAME_PATTERN.findall(question)
        for match in matches:
            match_lower = match.lower()
            result = process.extractOne(match_lower, self.player_names, scorer=fuzz.token_sort_ratio)
            if result is None:
                continue
            player, score, _ = result
            if score > 80:
                candidate = player
                break

        if candidate is None:
            # fallback: fuzzy entire question
            result = process.extractOne(question.lower(), self.player_names, scorer=fuzz.partial_ratio)
            if result is not None:
                player, score, _ = result
                if score > 75:
                    candidate = player

        if candidate:
            return candidate.title()
        return None

    def _extract_season(self, normalized_question: str) -> int | None:
        if match := SEASON_PATTERN.search(normalized_question):
            return int(match.group(1))
        if "last season" in normalized_question:
            stats = self.datastore.load_stats()
            if stats.empty:
                return None
            return int(stats["season"].max())
        return None

    def _extract_week(self, normalized_question: str) -> int | None:
        if match := WEEK_PATTERN.search(normalized_question):
            return int(match.group(1))
        return None

    # ------------------------------------------------------------------
    # Intent handlers
    # ------------------------------------------------------------------
    def _answer_props(self, intent: QuestionIntent) -> QuestionAnswer:
        props_df = self.datastore.load_props()
        if props_df.empty:
            return QuestionAnswer(intent=intent, answer="No prop data available. Please refresh the dataset.")

        mask = props_df["player_name"].str.lower() == (intent.player_name or "").lower()
        player_props = props_df.loc[mask]
        if player_props.empty:
            return QuestionAnswer(intent=intent, answer=f"No props found for {intent.player_name}.")

        player_props = player_props.sort_values(by="retrieved_at", ascending=False)
        latest = player_props.groupby(["market", "bookmaker"], as_index=False).first()

        table = latest[["market", "line", "over_odds", "under_odds", "bookmaker"]]
        answer = f"Found {len(table)} prop lines for {intent.player_name}."
        return QuestionAnswer(intent=intent, answer=answer, data=table.to_dict(orient="records"))

    def _answer_stats(self, intent: QuestionIntent) -> QuestionAnswer:
        stats_df = self.datastore.load_stats()
        if stats_df.empty:
            return QuestionAnswer(intent=intent, answer="No player stats available. Please refresh the dataset.")

        mask = stats_df["player_name"].str.lower() == (intent.player_name or "").lower()
        filtered = stats_df.loc[mask]
        if intent.season:
            filtered = filtered[filtered["season"] == intent.season]
        if intent.week:
            filtered = filtered[filtered["week"] == intent.week]

        if filtered.empty:
            qualifier = f" in {intent.season}" if intent.season else ""
            return QuestionAnswer(intent=intent, answer=f"No stats found for {intent.player_name}{qualifier}.")

        summary = (
            filtered.groupby("stat_category")["stat_value"].sum().sort_values(ascending=False).head(10).reset_index()
        )
        answer = f"Found {len(summary)} stat categories for {intent.player_name}."
        return QuestionAnswer(intent=intent, answer=answer, data=summary.to_dict(orient="records"))
