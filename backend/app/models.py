"""Pydantic models shared across API endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


class PropRecord(BaseModel):
    player_name: str = Field(..., example="Travis Kelce")
    team: str | None = Field(None, example="KC")
    market: str = Field(..., example="Receiving Yards")
    line: float | None = Field(None, example=72.5)
    projection: float | None = Field(None, example=74.2)
    over_odds: float | None = Field(None, example=-110)
    under_odds: float | None = Field(None, example=-110)
    bookmaker: str = Field(..., example="prizepicks")
    retrieved_at: datetime = Field(..., example="2024-12-01T12:30:00Z")


class PlayerStatRecord(BaseModel):
    player_name: str
    season: int
    week: int | None = None
    stat_category: str
    stat_value: float
    team: str | None = None
    opponent: str | None = None


class QuestionRequest(BaseModel):
    question: str


class QuestionIntent(BaseModel):
    intent: Literal["props_lookup", "player_stats", "unknown"]
    player_name: str | None = None
    season: Optional[int] = None
    week: Optional[int] = None
    prop_market: Optional[str] = None


class QuestionAnswer(BaseModel):
    intent: QuestionIntent
    answer: str
    data: list[dict] | None = None


class DataRefreshRequest(BaseModel):
    seasons: list[int] | None = Field(None, description="Seasons to refresh")
    include_props: bool = Field(True)
    include_stats: bool = Field(True)


class DatasetStatus(BaseModel):
    dataset: str
    records: int
    message: str | None = None


class DataRefreshResponse(BaseModel):
    statuses: list[DatasetStatus]
