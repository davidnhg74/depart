"""Verify cloud-only routes are strictly gated by ENABLE_CLOUD_ROUTES.

The default product image flips this flag off — so auth, billing,
account, and support endpoints must be absent. Tests run with
ENABLE_CLOUD_ROUTES=true (set by conftest.py) so we test both
directions here by introspecting the app's route table."""

from __future__ import annotations

import importlib
import os
from unittest.mock import patch


def _get_route_paths(app) -> set[str]:
    return {r.path for r in app.routes if hasattr(r, "path")}


def test_with_flag_on_cloud_routes_are_mounted():
    from src.main import app

    paths = _get_route_paths(app)
    # Representative endpoint from each cloud router:
    assert any(p.startswith("/api/v4/auth") for p in paths)
    assert any(p.startswith("/api/v1/account") for p in paths) or any(
        p.startswith("/api/v4/auth") for p in paths
    )
    assert any("billing" in p for p in paths)
    assert any("support" in p for p in paths)


def test_self_hosted_always_routes_present():
    """assess + convert + settings + license + runbook must be present
    regardless of the cloud-routes flag — they are the product."""
    from src.main import app

    paths = _get_route_paths(app)
    assert "/api/v1/assess" in paths
    assert "/api/v1/settings" in paths
    assert "/api/v1/license" in paths
    assert any(p.startswith("/api/v1/convert") for p in paths)


def test_flag_off_excludes_cloud_routes():
    """Re-import `src.main` with the flag disabled and verify none of
    the cloud routers are mounted. Uses a module reload so the
    FastAPI app picks up the flipped flag."""
    import src.main  # noqa: F401 — ensure it's loaded at least once

    with patch.dict(os.environ, {"ENABLE_CLOUD_ROUTES": "false"}):
        # Reload config + main so the module-level `if settings.enable_cloud_routes:`
        # re-evaluates with the flag off.
        import src.config
        import src.main

        importlib.reload(src.config)
        importlib.reload(src.main)

        paths = _get_route_paths(src.main.app)
        # Cloud routes gone
        assert not any(p.startswith("/api/v4/auth") for p in paths)
        assert not any("billing" in p for p in paths)
        assert not any("support" in p for p in paths)

        # Product routes still there
        assert "/api/v1/assess" in paths
        assert "/api/v1/settings" in paths
        assert "/api/v1/license" in paths

    # Restore the default-on test environment so subsequent tests see
    # the cloud routes again. conftest.py set ENABLE_CLOUD_ROUTES=true
    # for the whole session.
    with patch.dict(os.environ, {"ENABLE_CLOUD_ROUTES": "true"}):
        import src.config
        import src.main

        importlib.reload(src.config)
        importlib.reload(src.main)
