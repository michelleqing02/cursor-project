"""Utilities to scrape public NFL stats tables into structured data."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Mapping, MutableMapping, Sequence

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger


USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"


PLAYER_CATEGORY_CONFIG: Mapping[str, dict[str, Mapping[str, str]]] = {
    "passing": {
        "sort": "passingyards",
        "columns": {
            "Pass Yds": "passing_yards",
            "Yds/Att": "yards_per_attempt",
            "Att": "passing_attempts",
            "Cmp": "completions",
            "Cmp %": "completion_pct",
            "TD": "passing_tds",
            "INT": "interceptions",
            "Rate": "passer_rating",
            "1st": "passing_first_downs",
            "1st%": "passing_first_down_pct",
            "20+": "passing_20_plus",
            "40+": "passing_40_plus",
            "Lng": "longest_pass",
            "Sck": "times_sacked",
            "SckY": "sack_yards",
        },
    },
    "rushing": {
        "sort": "rushingyards",
        "columns": {
            "Rush Yds": "rushing_yards",
            "Att": "rushing_attempts",
            "TD": "rushing_tds",
            "20+": "rushing_20_plus",
            "40+": "rushing_40_plus",
            "Lng": "rushing_long",
            "Rush 1st": "rushing_first_downs",
            "Rush 1st%": "rushing_first_down_pct",
            "Rush FUM": "rushing_fumbles",
        },
    },
    "receiving": {
        "sort": "receivingreceptions",
        "columns": {
            "Rec": "receptions",
            "Yds": "receiving_yards",
            "TD": "receiving_tds",
            "20+": "receiving_20_plus",
            "40+": "receiving_40_plus",
            "LNG": "receiving_long",
            "Rec 1st": "receiving_first_downs",
            "1st%": "receiving_first_down_pct",
            "Rec FUM": "receiving_fumbles",
            "Rec YAC/R": "yards_after_catch_per_reception",
            "Tgts": "targets",
        },
    },
}


TEAM_STATS_COLUMNS: Mapping[str, str] = {
    "Att": "passing_attempts",
    "Cmp": "completions",
    "Cmp %": "completion_pct",
    "Yds/Att": "yards_per_attempt",
    "Pass Yds": "passing_yards",
    "TD": "passing_tds",
    "INT": "interceptions",
    "Rate": "passer_rating",
    "1st": "first_downs",
    "1st%": "first_down_pct",
    "20+": "plays_20_plus",
    "40+": "plays_40_plus",
    "Lng": "longest_play",
    "Sck": "times_sacked",
    "SckY": "sack_yards",
}


@dataclass(slots=True)
class ScrapedTable:
    """Simple container for parsed HTML tables."""

    rows: list[dict[str, object]]

    def to_frame(self) -> pd.DataFrame:
        if not self.rows:
            return pd.DataFrame()
        return pd.DataFrame(self.rows)


class NflStatsScraper:
    """Scrape public-facing NFL stats tables exposed on nfl.com."""

    def __init__(self, timeout: float = 15.0) -> None:
        self._client = httpx.Client(timeout=timeout, headers={"User-Agent": USER_AGENT})

    def close(self) -> None:
        self._client.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape_player_stats(self, seasons: Sequence[int]) -> pd.DataFrame:
        """Return combined player stats for the supplied seasons."""

        aggregated: MutableMapping[tuple[int, str], dict[str, object]] = {}

        for season in seasons:
            for category, config in PLAYER_CATEGORY_CONFIG.items():
                url = self._player_category_url(season, category, config["sort"])
                try:
                    html = self._fetch(url)
                except httpx.HTTPError:
                    logger.exception("Failed to scrape player %s stats for season=%s", category, season)
                    continue

                table = self._parse_player_table(html, config["columns"])
                for row in table.rows:
                    name = row.pop("player_name", None)
                    if not name:
                        continue
                    key = (season, str(name))
                    entry = aggregated.setdefault(key, {
                        "player_name": name,
                        "season": season,
                    })
                    entry.update(row)

        if not aggregated:
            return pd.DataFrame()

        frame = pd.DataFrame(aggregated.values())
        numeric_cols = [col for col in frame.columns if col not in {"player_name", "season", "week", "team", "opponent"}]
        frame[numeric_cols] = frame[numeric_cols].apply(pd.to_numeric, errors="coerce")
        frame["week"] = pd.NA  # scraped data is season-level
        frame["team"] = frame.get("team") if "team" in frame.columns else pd.NA
        frame["opponent"] = pd.NA
        return frame

    def scrape_team_stats(self, seasons: Sequence[int]) -> pd.DataFrame:
        """Return aggregated team stats for the supplied seasons."""

        rows: list[dict[str, object]] = []

        for season in seasons:
            url = f"https://www.nfl.com/stats/team-stats/?season={season}&view=summary"
            try:
                html = self._fetch(url)
            except httpx.HTTPError:
                logger.exception("Failed to scrape team summary stats for season=%s", season)
                continue

            table = self._parse_team_table(html)
            for row in table.rows:
                row["season"] = season
                row["week"] = pd.NA
                rows.append(row)

        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame

        numeric_cols = [col for col in frame.columns if col not in {"team", "team_abbr", "season", "week"}]
        frame[numeric_cols] = frame[numeric_cols].apply(pd.to_numeric, errors="coerce")
        return frame

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch(self, url: str) -> str:
        logger.debug("Fetching %s", url)
        response = self._client.get(url)
        response.raise_for_status()
        return response.text

    @staticmethod
    def _player_category_url(season: int, category: str, sort: str) -> str:
        return (
            "https://www.nfl.com/stats/player-stats/category/"
            f"{category}/{season}/reg/all/{sort}/desc"
        )

    @staticmethod
    def _parse_player_table(html: str, mapping: Mapping[str, str]) -> ScrapedTable:
        soup = BeautifulSoup(html, "html.parser")
        header_cells = [th.get_text(strip=True) for th in soup.select("table thead th")]
        if not header_cells:
            logger.warning("Player stats table missing header" )
            return ScrapedTable(rows=[])

        rows: list[dict[str, object]] = []
        for tr in soup.select("table tbody tr"):
            name_tag = tr.select_one("a.d3-o-player-fullname")
            if name_tag is None:
                continue

            record: dict[str, object] = {"player_name": name_tag.get_text(strip=True)}
            value_cells = tr.select("td")
            if len(value_cells) != len(header_cells):
                continue

            for header, cell in zip(header_cells[1:], value_cells[1:]):
                key = mapping.get(header)
                if not key:
                    continue
                value = _parse_numeric(cell.get_text(strip=True))
                record[key] = value

            rows.append(record)

        return ScrapedTable(rows=rows)

    @staticmethod
    def _parse_team_table(html: str) -> ScrapedTable:
        soup = BeautifulSoup(html, "html.parser")
        header_cells = [th.get_text(strip=True) for th in soup.select("table thead th")]
        if not header_cells:
            logger.warning("Team stats table missing header")
            return ScrapedTable(rows=[])

        rows: list[dict[str, object]] = []
        for tr in soup.select("table tbody tr"):
            club = tr.select_one("div.d3-o-club-info")
            if club is None:
                continue

            team_name = club.select_one("div.d3-o-club-fullname")
            team = team_name.get_text(strip=True) if team_name else None
            logo = club.select_one("div.d3-o-club-logo img")
            team_abbr = _extract_team_abbr(logo["src"]) if logo and logo.has_attr("src") else None

            value_cells = tr.select("td")
            if len(value_cells) != len(header_cells):
                continue

            record: dict[str, object] = {
                "team": team,
                "team_abbr": team_abbr,
            }

            for header, cell in zip(header_cells[1:], value_cells[1:]):
                key = TEAM_STATS_COLUMNS.get(header)
                if not key:
                    continue
                record[key] = _parse_numeric(cell.get_text(strip=True))

            rows.append(record)

        return ScrapedTable(rows=rows)


def _parse_numeric(raw: str) -> float | int | None:
    cleaned = raw.strip().replace(",", "")
    if not cleaned or cleaned in {"--", "-"}:
        return None

    cleaned = cleaned.replace("%", "")
    cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
    if cleaned in {"", "."}:
        return None

    try:
        value = float(cleaned)
    except ValueError:
        return None

    if value.is_integer():
        return int(value)
    return value


def _extract_team_abbr(src: str) -> str | None:
    match = re.search(r"/([A-Z]{2,3})(?:\.|$)", src)
    if match:
        return match.group(1)
    return None


def scrape_player_stats(seasons: Sequence[int], timeout: float = 15.0) -> pd.DataFrame:
    """Convenience wrapper to fetch player stats without managing scraper lifecycle."""

    scraper = NflStatsScraper(timeout=timeout)
    try:
        return scraper.scrape_player_stats(seasons)
    finally:
        scraper.close()


def scrape_team_stats(seasons: Sequence[int], timeout: float = 15.0) -> pd.DataFrame:
    scraper = NflStatsScraper(timeout=timeout)
    try:
        return scraper.scrape_team_stats(seasons)
    finally:
        scraper.close()

