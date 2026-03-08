from app.config import Settings
from app.manifest import build_manifest


def test_manifest_is_installable_without_configuration() -> None:
    manifest = build_manifest(Settings(UPSTREAM_BASE_URL="https://example.com/stremio"))

    assert manifest["behaviorHints"] == {"configurable": False}
    assert "config" not in manifest
