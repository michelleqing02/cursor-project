"""Prop scraping utilities and provider implementations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Protocol

import httpx
from loguru import logger

from ..config import get_settings
from ..models import PropRecord


class PropProvider(Protocol):
    """Interface for sportsbook prop data providers."""

    provider: str

    async def fetch_props(self) -> Iterable[PropRecord]:
        """Return an iterable of prop records."""


@dataclass(slots=True)
class PrizePicksProvider:
    """Fetch projections from PrizePicks public API."""

    base_url: str = "https://api.prizepicks.com/projections"
    league_id: int = 7  # NFL

    provider: str = "prizepicks"

    async def fetch_props(self) -> Iterable[PropRecord]:
        logger.info("Fetching props from PrizePicks")
        params = {"league_id": self.league_id}
        timeout = get_settings().http_timeout
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(self.base_url, params=params)
            response.raise_for_status()
            payload = response.json()

        included = {item["id"]: item for item in payload.get("included", [])}
        retrieved_at = datetime.now(timezone.utc)
        for item in payload.get("data", []):
            attributes = item.get("attributes", {})
            player_id = attributes.get("new_player_id")
            player = included.get(player_id, {})
            player_name = player.get("attributes", {}).get("name")
            team = attributes.get("team_name")
            market_name = (attributes.get("stat_type_display_name") or attributes.get("stat_type"))
            line = attributes.get("line_score")
            over_odds = attributes.get("over_under", {}).get("over") if attributes.get("over_under") else None
            under_odds = attributes.get("over_under", {}).get("under") if attributes.get("over_under") else None

            if not player_name:
                continue

            yield PropRecord(
                player_name=player_name,
                team=team,
                market=market_name,
                line=float(line) if line is not None else None,
                projection=float(attributes.get("projection_value")) if attributes.get("projection_value") else None,
                over_odds=float(over_odds) if over_odds else None,
                under_odds=float(under_odds) if under_odds else None,
                bookmaker=self.provider,
                retrieved_at=retrieved_at,
            )


@dataclass(slots=True)
class UnderdogProvider:
    """Fetch projections from Underdog Fantasy public API."""

    base_url: str = "https://api.underdogfantasy.com/beta/projections"
    provider: str = "underdog"

    async def fetch_props(self) -> Iterable[PropRecord]:
        logger.info("Fetching props from Underdog")
        timeout = get_settings().http_timeout
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(self.base_url)
            response.raise_for_status()
            payload = response.json()

        retrieved_at = datetime.now(timezone.utc)
        players = {player["id"]: player for player in payload.get("players", [])}
        teams = {team["id"]: team for team in payload.get("teams", [])}

        for projection in payload.get("projections", []):
            player = players.get(projection.get("player_id"), {})
            team = teams.get(projection.get("team_id"), {})
            player_name = player.get("name")
            market = projection.get("over_under")
            statistic_type = projection.get("stat_type")
            line = projection.get("line")
            if not player_name:
                continue

            yield PropRecord(
                player_name=player_name,
                team=team.get("abbr"),
                market=statistic_type or market or "Unknown",
                line=float(line) if line is not None else None,
                projection=None,
                over_odds=None,
                under_odds=None,
                bookmaker=self.provider,
                retrieved_at=retrieved_at,
            )


async def collect_props() -> list[PropRecord]:
    """Fetch props from all enabled providers."""

    settings = get_settings()
    providers: list[PropProvider] = []
    if settings.enable_prizepicks:
        providers.append(PrizePicksProvider())
    if settings.enable_underdog:
        providers.append(UnderdogProvider())

    collected: list[PropRecord] = []
    for provider in providers:
        try:
            async for item in _iterate_async(provider.fetch_props()):
                collected.append(item)
        except httpx.HTTPError as exc:
            logger.warning("Provider {provider} failed: {error}", provider=provider.provider, error=str(exc))
    return collected


async def _iterate_async(iterable: Iterable[PropRecord]):
    """Helper to iterate over async generator or iterable uniformly."""

    if hasattr(iterable, "__aiter__"):
        async for item in iterable:  # type: ignore[attr-defined]
            yield item
    else:
        for item in iterable:
            yield item
