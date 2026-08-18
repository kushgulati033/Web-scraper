"""
Minimal Flask API around the scraper.

Endpoints:
  GET /              -> health check
  GET /jobs           -> run the ingestion pipeline, return JSON
  GET /jobs?refresh=1  -> bypass any request-level throttling below and force a fresh run

Note: this process-level throttle (separate from the per-domain RateLimiter
in scraper.py) exists so that hammering *this* API doesn't turn into
hammering the upstream source on every page load.
"""

import time

from flask import Flask, jsonify, request

from scraper import build_default_orchestrator

app = Flask(__name__)

_orchestrator = build_default_orchestrator()
_last_run_at = 0.0
_last_result = None
MIN_SECONDS_BETWEEN_RUNS = 60  # don't let public traffic re-trigger scraping every request


@app.get("/")
def health():
    return jsonify({"status": "ok", "service": "job-listing-ingestion-demo"})


@app.get("/jobs")
def jobs():
    global _last_run_at, _last_result

    force = request.args.get("refresh") == "1"
    now = time.time()

    if not force and _last_result is not None and (now - _last_run_at) < MIN_SECONDS_BETWEEN_RUNS:
        resp = dict(_last_result)
        resp["served_from"] = "in-memory (throttled)"
        resp["age_seconds"] = round(now - _last_run_at)
        return jsonify(resp)

    result = _orchestrator.run()
    _last_result = result
    _last_run_at = now
    result["served_from"] = "live"
    return jsonify(result)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
