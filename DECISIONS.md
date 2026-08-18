# DECISIONS.md

## Detection surface — what I'm accounting for, and what I'm deliberately not

For a real target like LinkedIn/Indeed, the detection surface includes headless
browser fingerprints (missing `navigator.webdriver` spoofing, canvas/WebGL
fingerprints, TLS/JA3 fingerprints), request timing (clockwork polling
intervals), missing/inconsistent headers, and behavioral signals (no mouse
movement, instant form fills).

This build **doesn't fight any of that**, on purpose: it targets We Work
Remotely's RSS feed and RemoteOK's public JSON API — both intentionally
published for machine consumption, no login wall, no anti-bot layer to
defeat. What I *do* account for, because it's good hygiene even against a
benign source: per-domain pacing with jitter (no fixed-interval polling
signature), UA/header rotation (no static `python-requests/2.x` UA), and
treating 429/403/5xx/empty-body as "back off," not "retry immediately."

## Ingestion strategy — rotation, pacing, fallback

`RateLimiter` enforces a minimum gap + random jitter per domain, so requests
to the same source never land on a predictable cadence. `_headers()` rotates
UA and Accept-Language per request. The `Orchestrator` tries sources in
priority order (WWR → RemoteOK), retrying each with exponential backoff
before failing over to the next. If every live source fails, it serves the
last successful pull from an on-disk cache rather than returning nothing —
degraded but honest (`status: "degraded"` in the response, with cache age).

**Plan B if WWR gets shut down in a week:** RemoteOK is already wired in as
the live fallback. A third tier (a job-board aggregator API, or a second
RSS feed) is a one-class addition — `Source` is the extension point.

## Resilience — what keeps it running

- Malformed/changed markup → `ET.ParseError` or zero-items is caught and
  raised as `SourceBlocked`, not an uncaught crash.
- Empty response body → explicitly checked before parsing, treated as a
  block signal (some anti-bot layers return 200 + empty body).
- Rate limiting → 429/403/5xx trigger retry-with-backoff, then failover.
- Total failure → serve cache with an explicit "degraded" status rather than
  silently returning stale data as if it were fresh, or crashing the endpoint.

## Where I'd stop

Personal/technical line: public RSS feeds and documented public APIs, yes.
Anything requiring login, CAPTCHA-solving, IP rotation to evade a ban, or
headless-browser fingerprint spoofing — no, regardless of technical
feasibility, because that's the point where "getting data out" becomes
"circumventing access controls," which most of these platforms' ToS
explicitly prohibit and which I'm not comfortable building even as a demo.
The design doc addresses the *harder* problem (LinkedIn-style targets)
conceptually, but the shipped code only touches consenting sources.

## Trade-off made under time limit

I didn't build a persistent job queue / scheduler (e.g. cron-triggered
periodic scrapes with a real database) — the API scrapes on-demand with a
60s in-memory cache instead. With a real week: move to a scheduled worker
(APScheduler or a cron job) writing to Postgres, add per-source circuit
breakers (stop hitting a source entirely for N minutes after repeated
failures, not just per-request backoff), and add structured metrics
(success rate per source, latency) instead of just logs.

## Why this ingestion strategy over the obvious alternative

The obvious alternative is a headless-browser scraper (Playwright/Puppeteer)
pointed at LinkedIn/Indeed directly, since that's what the brief's framing
suggests. I rejected it for this deliverable because the brief's own scope
guardrail says not to run it against a real account, and because a headless
scraper aimed at a source that's actively hostile to bots would spend most
of its complexity budget on fingerprint evasion (undetectable browser
patches, proxy rotation, CAPTCHA handling) rather than on the resilience/
fallback architecture the brief actually grades. RSS/public-API ingestion
lets the demo be honest, fully runnable, and still exercise the same
failover/backoff/resilience patterns that would carry over to a harder
source.

## AI tool usage

Used Claude to scaffold the source/orchestrator class structure and the
pytest fixtures. I changed: the cache degrade-path (initial version
returned nothing on total failure — I added the stale-cache fallback with
explicit `status: "degraded"` labeling), the per-domain rate limiter (initial
version was global, not per-domain, which would have throttled unrelated
sources against each other), and tightened the WWR title-parsing regex
logic to handle listings with no colon in the title without crashing. Can
walk through every function in the follow-up call.
