"""Application configuration management."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Sequence

from pydantic import BaseSettings, Field, validator


class Settings(BaseSettings):
    """Runtime configuration sourced from environment variables or .env files."""

    environment: str = Field("local", description="Runtime environment identifier")
    data_dir: Path = Field(Path(__file__).resolve().parents[2] / "data", description="Base directory for datasets")
    seasons: Sequence[int] = Field((2023, 2024), description="NFL seasons to ingest by default")
    http_timeout: float = Field(10.0, description="Default timeout for outbound HTTP requests")
    enable_prizepicks: bool = Field(True, description="Toggle PrizePicks prop provider")
    enable_underdog: bool = Field(True, description="Toggle Underdog prop provider")

    class Config:
        env_file = ".env"
        env_prefix = "NFL_PROPS_"

    @validator("data_dir", pre=True)
    def _expand_data_dir(cls, value: Path | str) -> Path:
        return Path(value).expanduser().resolve()


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""

    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    return settings
