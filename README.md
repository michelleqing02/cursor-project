## NFL Prop Data Trader App

This project provides a full-stack application tailored for sports traders who need quick access to NFL prop markets and historical performance data. It bundles four core capabilities:

- Automated ingestion of live NFL prop offerings from publicly available sportsbooks (e.g. PrizePicks, Underdog).
- Historical player statistics sourcing (via the nflverse data pipeline) to contextualise props with real performance data.
- An interactive trader dashboard that filters official NFL, Next Gen Stats, Pro Football Reference, and ESPN data by season, week, team, and player.
- A natural-language Q&A layer so you can ask questions like "What were Travis Kelce's stats last season?" and receive a structured answer immediately in the UI.

### Project Layout

```
backend/
  app/
    main.py             # FastAPI entrypoint and routes
    config.py           # Runtime configuration & environment settings
    models.py           # Pydantic schemas for props, stats, and responses
    services/
      __init__.py
      props.py          # Scrapers / collectors for live prop lines
      stats.py          # Historical stats ingestion utilities
      question_answering.py  # Lightweight NL ? data intent parsing
    data_access/
      __init__.py
      datastore.py      # DuckDB/Parquet-backed storage utilities
    web/
      templates/
        index.html      # Minimal search interface
      static/
        app.js          # Frontend logic for search + display
        styles.css
  scripts/
    update_data.py      # CLI to refresh prop + stats datasets
  requirements.txt

data/
  README.md             # Storage guidance (contains generated Parquet files)

Makefile                # Helpful shortcuts (run server, refresh data, lint)
```

### Getting Started

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt

# Fetch fresh data (props + stats for the last two NFL seasons by default)
python backend/scripts/update_data.py --seasons 2023 2024

# Run the API + web UI
uvicorn backend.app.main:app --reload
```

Once the server is running, open http://127.0.0.1:8000/ to access the search interface.

### Trader Dashboard

The new dashboard card on the home page surfaces a filter-driven workflow:

- Choose a dataset: `Player Weekly`, `Team Efficiency`, `Receiving Efficiency (NGS + PFR)`, or `Quarterback Efficiency (ESPN QBR)`.
- Filter by season, week (where supported), team, player, and limit the number of rows returned.
- Sort by any column exposed in the response (yards per catch, total QBR, total yards, etc.).
- Download-ready tables update instantly as you change filters.

The front-end calls the same API surface exposed below, so you can embed the data into scripts or spreadsheets with identical semantics.

### API Endpoints

- `GET /api/stats` - Filtered stats for trader workflows. Query parameters:
  - `dataset` (required): `player`, `team`, `receiving_efficiency`, `quarterback_efficiency`.
  - Optional filters: `season`, `week`, `team`, `player`, `limit` (1-500, default 100), `sort`, and `descending` (boolean).
  - Returns `StatsResponse` containing ordered `columns`, `rows`, applied `filters`, and dataset `metadata`.
- `GET /api/stats/metadata` - Returns available seasons/weeks/teams for each dataset (or one dataset when `?dataset=` is supplied). Useful for populating dropdowns.
- `POST /api/update-data` - Refresh props and all stat datasets (NFL player + team, Next Gen receiving, PFR advanced receiving, ESPN QBR).

Example:

```bash
curl "http://127.0.0.1:8000/api/stats?dataset=receiving_efficiency&season=2024&team=KC&limit=25"
```

### Data Sources Covered

All backend ingestion uses `nfl_data_py`, which aggregates and normalises data from:

- Official NFL GSIS feeds (`import_player_weekly_data`, `import_team_weekly_data`).
- Next Gen Stats receiving leaderboards (`import_ngs_receiving_data`).
- Pro Football Reference advanced receiving (`import_pfr_advanced_receiving_stats`).
- ESPN Total QBR (`import_espn_qbr`).

Each dataset is materialised into parquet files under `data/` and served through the unified `/api/stats` endpoint.

### Question Answering

The Q&A pipeline currently supports:

- Player stat lookups: "Show Justin Jefferson receiving stats in 2023 playoffs".
- Prop line lookups: "What's Travis Kelce's current receiving yards prop?"
- Book comparisons: "Compare Josh Allen rushing props across books".

It relies on fuzzy name matching (`rapidfuzz`) and intent classification driven by keyword rules. Intent and extraction logic are centralized in `backend/app/services/question_answering.py`.

### Extending the Scrapers

Prop collectors are implemented as provider classes under `backend/app/services/props.py`. They share a common interface so you can plug in new sources quickly. Each provider should:

1. Advertise a `provider` string identifier.
2. Implement `fetch_props()` returning a `list[PropRecord]`.
3. Handle provider-specific normalization (market names, team codes, etc.).

### Security Notes

- The project honours Cursor's Security-First Development Guidelines (no credentials checked in, safe network usage, graceful failure when endpoints are unreachable).
- All external HTTP calls have conservative timeouts and surface actionable errors back to the caller.

### Roadmap Ideas

- Add Redis caching for high-frequency prop refreshes.
- Integrate LLM-backed natural language interpretation for broader query support.
- Implement alerting when props move beyond a configured threshold.
- Build a richer React dashboard with sortable tables and sparkline charts.

