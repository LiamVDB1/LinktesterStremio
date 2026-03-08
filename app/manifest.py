from __future__ import annotations

from app.config import Settings


def build_manifest(settings: Settings) -> dict:
    return {
        "id": "stremio-link-ranker",
        "version": "1.0.0",
        "name": "Link Ranker (Async)",
        "description": "Proxies an upstream stream addon and reorders links via runtime probes.",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt", "kitsu", "anilist", "tmdb"],
        "catalogs": [],
        "behaviorHints": {"configurable": False},
        "logo": "https://raw.githubusercontent.com/Stremio/stremio-addon-sdk/master/docs/logo.png",
        "background": "https://raw.githubusercontent.com/Stremio/stremio-addon-sdk/master/docs/background.jpg",
        "contactEmail": "noreply@example.invalid",
    }
