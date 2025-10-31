"""FastAPI entrypoint for the NFL prop data trader app."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi import Depends, FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from .config import get_settings
from .data_access.datastore import DataStore, get_datastore
from .models import (
    DataRefreshRequest,
    DataRefreshResponse,
    DatasetStatus,
    QuestionAnswer,
    QuestionRequest,
    StatsDataset,
    StatsResponse,
)
from .services.props import collect_props
from .services.question_answering import QuestionAnsweringService
from .services.stats import load_player_weekly_stats, to_stat_records
from .services.trader_dashboard import StatsFilterService


settings = get_settings()
app = FastAPI(title="NFL Prop Data Trader", version="0.1.0")

# CORS defaults (open for local experimentation)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static + templates configuration
base_path = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(base_path / "web" / "templates"))
static_dir = base_path / "web" / "static"
static_dir.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


QA_SERVICE: QuestionAnsweringService | None = None
STATS_SERVICE: StatsFilterService | None = None


def get_qa_service(datastore: DataStore = Depends(get_datastore)) -> QuestionAnsweringService:
    global QA_SERVICE
    if QA_SERVICE is None:
        QA_SERVICE = QuestionAnsweringService(datastore=datastore)
    return QA_SERVICE


def get_stats_service(datastore: DataStore = Depends(get_datastore)) -> StatsFilterService:
    global STATS_SERVICE
    if STATS_SERVICE is None:
        STATS_SERVICE = StatsFilterService(datastore=datastore)
    return STATS_SERVICE


@app.on_event("startup")
async def on_startup() -> None:
    logger.info("Starting NFL Prop Data Trader app in %s environment", settings.environment)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/ask", response_model=QuestionAnswer)
async def ask_question(payload: QuestionRequest, qa: QuestionAnsweringService = Depends(get_qa_service)) -> QuestionAnswer:
    logger.info("Question received: %s", payload.question)
    return qa.answer(payload.question)


@app.post("/api/update-data", response_model=DataRefreshResponse)
async def update_data(
    payload: DataRefreshRequest,
    datastore: DataStore = Depends(get_datastore),
) -> DataRefreshResponse:
    statuses: list[DatasetStatus] = []

    if payload.include_props:
        try:
            prop_records = await collect_props()
            count = datastore.write_props(prop_records)
            statuses.append(DatasetStatus(dataset="props", records=count))
        except Exception as exc:  # pragma: no cover - runtime only
            logger.exception("Prop refresh failed")
            statuses.append(DatasetStatus(dataset="props", records=0, message=str(exc)))

    if payload.include_stats:
        seasons = payload.seasons or list(settings.seasons)
        try:
            stats_df = load_player_weekly_stats(seasons)
            records = to_stat_records(stats_df)
            stats_records_df = pd.DataFrame(record.model_dump() for record in records)
            count = datastore.write_stats(stats_records_df)
            statuses.append(DatasetStatus(dataset="player_stats", records=count))
        except Exception as exc:  # pragma: no cover - runtime only
            logger.exception("Stats refresh failed")
            statuses.append(DatasetStatus(dataset="player_stats", records=0, message=str(exc)))

    # Reset QA service with fresh indices
    global QA_SERVICE
    QA_SERVICE = QuestionAnsweringService(datastore=datastore)

    global STATS_SERVICE
    STATS_SERVICE = StatsFilterService(datastore=datastore)

    return DataRefreshResponse(statuses=statuses)


@app.get("/api/stats", response_model=StatsResponse)
async def get_stats(
    dataset: StatsDataset = Query(..., description="Dataset to query"),
    season: int | None = Query(None, description="Season to filter by"),
    week: int | None = Query(None, description="Week to filter by"),
    team: str | None = Query(None, description="Team abbreviation"),
    player: str | None = Query(None, description="Player name to match"),
    limit: int | None = Query(100, ge=1, le=500, description="Maximum rows to return"),
    sort: str | None = Query(None, description="Column to sort by"),
    descending: bool = Query(True, description="Sort descending order"),
    service: StatsFilterService = Depends(get_stats_service),
) -> StatsResponse:
    """Return filtered stats tailored for sports trading workflows."""

    return service.fetch(
        dataset,
        season=season,
        week=week,
        team=team,
        player=player,
        limit=limit,
        sort=sort,
        descending=descending,
    )


@app.get("/api/stats/metadata")
async def stats_metadata(
    dataset: StatsDataset | None = Query(None, description="Dataset to describe"),
    service: StatsFilterService = Depends(get_stats_service),
) -> dict[str, dict[str, list]]:
    """Expose available filters (seasons/weeks/teams) for dataset selectors."""

    return service.get_metadata(dataset)
