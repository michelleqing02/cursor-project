Generated datasets live in this directory:

- `props.parquet` ? Latest snapshot of prop lines scraped from external providers.
- `player_stats.parquet` ? Aggregated player statistics derived from nfl_data_py weekly data.

These files are produced by `python backend/scripts/update_data.py`. They are ignored by git by default.
