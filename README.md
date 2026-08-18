# Job Listing Ingestion Demo

Pulls live job listings from a public, scraping-friendly source (We Work
Remotely's RSS feed), with automatic fallback to RemoteOK's public JSON API
if the primary source is unreachable, rate-limited, or changes shape.

Built for the Acdyon frontend challenge, Part 1. See `DECISIONS.md` for the
design writeup (detection surface, ingestion strategy, resilience, ToS line).

## Run locally

```bash
pip install -r requirements.txt
cd app
python main.py
# -> http://localhost:5000/jobs
```

Or run the scraper directly without the API wrapper:

```bash
cd app
python scraper.py
```

## Run tests

```bash
pip install pytest
pytest tests/ -v
```

Tests run offline against saved fixtures in `fixtures/` (sample WWR RSS and
RemoteOK JSON responses), so they don't depend on live sources being
reachable or unthrottled — useful for CI, and for verifying parsing/failover
logic in isolation from network flakiness.

## Deploy (Render)

1. Push this repo to GitHub.
2. On Render: New → Web Service → connect the repo. It will pick up
   `render.yaml` automatically (build: `pip install -r requirements.txt`,
   start: `gunicorn main:app`).
3. Once live, hit `https://<your-app>.onrender.com/jobs`.

Railway works the same way using the included `Procfile`.

## Endpoints

- `GET /` — health check
- `GET /jobs` — returns listings as JSON. Cached in-memory for 60s so page
  reloads don't re-trigger a live scrape on every request.
- `GET /jobs?refresh=1` — force a fresh pull, bypassing the in-memory cache.

## Project layout

```
app/
  scraper.py   — ingestion engine: sources, rate limiter, retry/fallback, cache
  main.py      — Flask API wrapper
fixtures/      — sample responses for offline testing
tests/         — pytest suite against fixtures
DECISIONS.md   — design writeup
```
