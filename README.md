# Stremio Link Ranker (FastAPI)

Async Stremio **stream addon** that proxies an upstream addon (e.g. AIOStreams) and reorders streams using:
1) fast runtime health + Range probes, then
2) lightweight MKV (EBML) metadata parsing on the top candidates.

## Run in 30 seconds

```bash
docker compose up --build
```

Open:
- `http://localhost:8000/manifest.json`
- `http://localhost:8000/healthz`

## Configure

Set `UPSTREAM_BASE_URL` to your upstream addon base (the service appends `/stream/{type}/{id}.json`):

```bash
export UPSTREAM_BASE_URL="http://localhost:7000"
docker compose up --build
```

Common knobs:
- `TOP_K_PHASE1` (default `10`), `TOP_M_PHASE2` (default `5`), `TOP_P_WEIRD` (default `3`)
- `T_PROBE_TOTAL_MS` (default `1800`), `T_TTFB_MS` (default `250`)
- `UPSTREAM_TIMEOUT_MS` (default `10000`)
- `MAX_CONCURRENCY` (default `12`)
- `PREFERRED_AUDIO_LANGS` (default `nl,en`)
- `REQUIRE_SEEKABLE` (default `true`)
- `MKV_META_MAX_BYTES` (default `2097152`), `MKV_META_CHUNK_BYTES` (default `262144`)
- `STREAM_CACHE_TTL_S`, `PROBE_CACHE_TTL_S`, `META_CACHE_TTL_S` for short-lived in-memory caching

## Install in Stremio

Add this addon URL in Stremio:
- `http://localhost:8000/manifest.json`

## How ranking works

- Phase 0: keep upstream order as tie-breaker.
- Pre-filter: parse stream `title` / `name` / `description` text for resolution, codec, HDR/DV, source, and size hints so obvious junk or giant outliers do not get priority.
- Phase 1: parallel `Range: bytes=0-1024` probe on Top-K prefiltered HTTP(S) streams (timeouts + seekability + magic-bytes).
- Phase 2: incrementally fetch MKV (EBML) prefix on Top-M and extract tracks plus duration when available.
- Phase 3: optional extra Range probes on Top-P using known file size to reduce “stalls later” risk.
- Final scoring prefers candidates close to the median size or bitrate for their quality bucket rather than blindly preferring the largest file.

Stream titles include `P2✓/P2×/P2-` and `P3✓/P3×/P3-` so you can see what ran.

## Local dev

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
make test
uvicorn app.main:create_app --factory --reload
```

## Debugging

- Run `uvicorn app.main:create_app --factory --reload --log-level debug` to see Phase timings and MKV SeekHead/Tracks parsing logs.
