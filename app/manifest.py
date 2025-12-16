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
        "behaviorHints": {"configurable": True, "configurationRequired": True},
        "logo": "https://raw.githubusercontent.com/Stremio/stremio-addon-sdk/master/docs/logo.png",
        "background": "https://raw.githubusercontent.com/Stremio/stremio-addon-sdk/master/docs/background.jpg",
        "contactEmail": "noreply@example.invalid",
        "config": [
            {
                "key": "UPSTREAM_BASE_URL",
                "title": "Upstream base URL",
                "type": "text",
                "required": True,
                "default": str(settings.upstream_base_url),
            }
        ],
    }
