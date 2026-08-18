"""
Job listing ingestion engine.

Design goals (see DECISIONS.md for the "why"):
  1. Never hammer a source: pacing + jitter on every request, per-domain.
  2. Never look like the same client twice: header/UA rotation.
  3. Never die on a bad response: retry with backoff, then fall back to a
     secondary source, then fall back to cache, then fail loudly (not silently).
  4. Stay inside ToS: only sources that publish an intentionally public feed
     (RSS / documented JSON API). No login walls, no headless browser evasion,
     no CAPTCHA solving. That's the personal/technical line for this build.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("scraper")

CACHE_PATH = Path(__file__).parent / "cache.json"
CACHE_TTL_SECONDS = 60 * 30  # serve stale-but-recent data if every source fails

# Rotate through a small pool of realistic desktop UAs. This is NOT trying to
# defeat fingerprinting (see DECISIONS.md on why that's out of scope for a
# public-RSS source) -- it just avoids the single-static-UA tell, which is
# good hygiene even against benign rate limiters.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]


def _headers() -> dict:
    """Build a header set that looks like an ordinary browser request,
    with a rotated UA and Accept-Language jitter. Not spoofing identity --
    just not shouting 'I am a script' with a bare requests/2.x UA string."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/rss+xml, application/xml, application/json, text/html, */*",
        "Accept-Language": random.choice(["en-US,en;q=0.9", "en-GB,en;q=0.8,en-US;q=0.6"]),
        "Connection": "keep-alive",
    }


@dataclass
class JobListing:
    source: str
    title: str
    company: str
    location: str
    url: str
    posted: Optional[str] = None
    tags: list = field(default_factory=list)
    fetched_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def fingerprint(self) -> str:
        """Stable id so re-runs can dedupe instead of re-emitting duplicates."""
        raw = f"{self.source}|{self.title}|{self.company}|{self.url}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


class RateLimiter:
    """Per-domain pacing. Guarantees a minimum gap between requests to the
    same host, plus randomized jitter so requests don't fall on a predictable
    cadence (a clock-regular polling interval is itself a bot signal)."""

    def __init__(self, min_interval: float = 2.5, jitter: float = 1.5):
        self.min_interval = min_interval
        self.jitter = jitter
        self._last_hit: dict[str, float] = {}

    def wait(self, domain: str):
        now = time.monotonic()
        last = self._last_hit.get(domain)
        if last is not None:
            elapsed = now - last
            required = self.min_interval + random.uniform(0, self.jitter)
            if elapsed < required:
                sleep_for = required - elapsed
                log.info("Pacing: sleeping %.2fs before hitting %s", sleep_for, domain)
                time.sleep(sleep_for)
        self._last_hit[domain] = time.monotonic()


class SourceBlocked(Exception):
    """Raised when a source signals it's not going to cooperate right now
    (429, 403, CAPTCHA redirect, empty body where content was expected)."""


class Source:
    """Base class for a single ingestion source. Each source knows how to
    fetch + parse itself, and reports failure via SourceBlocked so the
    orchestrator can decide whether to retry, back off, or fail over."""

    name = "base"
    domain = "example.com"

    def __init__(self, limiter: RateLimiter, session: requests.Session):
        self.limiter = limiter
        self.session = session

    def fetch_raw(self, url: str, timeout: float = 10.0) -> requests.Response:
        self.limiter.wait(self.domain)
        resp = self.session.get(url, headers=_headers(), timeout=timeout)
        if resp.status_code == 429:
            raise SourceBlocked(f"{self.name}: rate limited (429)")
        if resp.status_code in (403, 401):
            raise SourceBlocked(f"{self.name}: blocked/forbidden ({resp.status_code})")
        if resp.status_code >= 500:
            raise SourceBlocked(f"{self.name}: upstream error ({resp.status_code})")
        resp.raise_for_status()
        if not resp.content or len(resp.content) < 20:
            raise SourceBlocked(f"{self.name}: suspiciously empty response")
        return resp

    def fetch(self) -> list[JobListing]:
        raise NotImplementedError


class WeWorkRemotelySource(Source):
    """Primary source: public RSS feed, explicitly published for consumption.
    No auth, no ToS conflict -- this is the 'low-risk source' the brief asks for."""

    name = "weworkremotely"
    domain = "weworkremotely.com"
    FEED_URL = "https://weworkremotely.com/remote-jobs.rss"

    def fetch(self) -> list[JobListing]:
        resp = self.fetch_raw(self.FEED_URL)
        return self.parse(resp.content)

    def parse(self, raw_xml: bytes) -> list[JobListing]:
        try:
            root = ET.fromstring(raw_xml)
        except ET.ParseError as e:
            # Markup changed shape overnight -> don't crash the pipeline,
            # surface it as a blocked/degraded source instead.
            raise SourceBlocked(f"{self.name}: unparseable feed ({e})")

        items = root.findall(".//item")
        if not items:
            raise SourceBlocked(f"{self.name}: feed parsed but had zero items")

        listings = []
        for item in items:
            title_raw = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            # WWR titles are conventionally "Company: Job Title"
            if ":" in title_raw:
                company, _, title = title_raw.partition(":")
            else:
                company, title = "Unknown", title_raw
            listings.append(
                JobListing(
                    source=self.name,
                    title=title.strip() or "Untitled",
                    company=company.strip(),
                    location="Remote",
                    url=link,
                    posted=pub_date or None,
                )
            )
        return listings


class RemoteOKSource(Source):
    """Fallback source: RemoteOK's documented public JSON API. Kicks in if
    WWR is unreachable, rate-limited, or its markup breaks parsing."""

    name = "remoteok"
    domain = "remoteok.com"
    API_URL = "https://remoteok.com/api"

    def fetch(self) -> list[JobListing]:
        resp = self.fetch_raw(self.API_URL)
        return self.parse(resp.json())

    def parse(self, data: list) -> list[JobListing]:
        if not isinstance(data, list) or len(data) < 2:
            raise SourceBlocked(f"{self.name}: unexpected payload shape")
        # First element is a legend/metadata row on this API, not a job.
        rows = data[1:]
        listings = []
        for row in rows:
            if not isinstance(row, dict) or "position" not in row:
                continue
            listings.append(
                JobListing(
                    source=self.name,
                    title=row.get("position", "Untitled"),
                    company=row.get("company", "Unknown"),
                    location=row.get("location") or "Remote",
                    url=row.get("url", ""),
                    posted=row.get("date"),
                    tags=row.get("tags", []) or [],
                )
            )
        if not listings:
            raise SourceBlocked(f"{self.name}: parsed payload but found no jobs")
        return listings


class Orchestrator:
    """Runs sources in priority order with retry/backoff per source, falls
    back to the next source on failure, and falls back to cache if every
    live source fails. This is the 'what keeps the pipeline running' piece."""

    def __init__(self, sources: list[Source], max_retries: int = 3):
        self.sources = sources
        self.max_retries = max_retries

    def _fetch_with_retry(self, source: Source) -> list[JobListing]:
        last_err = None
        for attempt in range(1, self.max_retries + 1):
            try:
                log.info("Attempt %d/%d: fetching from %s", attempt, self.max_retries, source.name)
                return source.fetch()
            except SourceBlocked as e:
                last_err = e
                backoff = (2 ** attempt) + random.uniform(0, 1)
                log.warning("%s failed (attempt %d): %s -- backing off %.1fs", source.name, attempt, e, backoff)
                time.sleep(backoff)
            except requests.RequestException as e:
                last_err = e
                backoff = (2 ** attempt) + random.uniform(0, 1)
                log.warning("%s network error (attempt %d): %s -- backing off %.1fs", source.name, attempt, e, backoff)
                time.sleep(backoff)
        raise SourceBlocked(f"{source.name}: exhausted {self.max_retries} retries ({last_err})")

    def run(self) -> dict:
        errors = []
        for source in self.sources:
            try:
                listings = self._fetch_with_retry(source)
                deduped = self._dedupe(listings)
                self._write_cache(source.name, deduped)
                return {
                    "status": "ok",
                    "source_used": source.name,
                    "count": len(deduped),
                    "listings": [asdict(j) for j in deduped],
                    "errors": errors,
                }
            except SourceBlocked as e:
                log.error("Source exhausted, failing over: %s", e)
                errors.append(str(e))
                continue

        # Every live source failed -- degrade to cache instead of returning
        # nothing. This is the "don't silently fail" requirement: we still
        # tell the caller it's stale data and how old it is.
        cached = self._read_cache()
        if cached:
            age = time.time() - cached["cached_at"]
            log.warning("All sources failed. Serving cache (%.0fs old).", age)
            return {
                "status": "degraded",
                "source_used": f"cache:{cached['source']}",
                "cache_age_seconds": round(age),
                "count": len(cached["listings"]),
                "listings": cached["listings"],
                "errors": errors,
            }

        log.error("All sources failed and no cache available.")
        return {"status": "failed", "count": 0, "listings": [], "errors": errors}

    @staticmethod
    def _dedupe(listings: list[JobListing]) -> list[JobListing]:
        seen = set()
        out = []
        for j in listings:
            fp = j.fingerprint()
            if fp not in seen:
                seen.add(fp)
                out.append(j)
        return out

    @staticmethod
    def _write_cache(source_name: str, listings: list[JobListing]):
        payload = {
            "source": source_name,
            "cached_at": time.time(),
            "listings": [asdict(j) for j in listings],
        }
        CACHE_PATH.write_text(json.dumps(payload))

    @staticmethod
    def _read_cache() -> Optional[dict]:
        if not CACHE_PATH.exists():
            return None
        try:
            data = json.loads(CACHE_PATH.read_text())
        except (json.JSONDecodeError, OSError):
            return None
        if time.time() - data["cached_at"] > CACHE_TTL_SECONDS * 4:
            # Even the fallback cache is too stale to be useful.
            return None
        return data


def build_default_orchestrator() -> Orchestrator:
    limiter = RateLimiter(min_interval=2.5, jitter=1.5)
    session = requests.Session()
    sources = [
        WeWorkRemotelySource(limiter, session),
        RemoteOKSource(limiter, session),
    ]
    return Orchestrator(sources)


if __name__ == "__main__":
    result = build_default_orchestrator().run()
    print(json.dumps(result, indent=2)[:2000])
