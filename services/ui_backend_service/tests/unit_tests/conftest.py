"""
conftest.py — pre-load cache/store/refinery modules into sys.modules before
pytest collection using the cumulative partial-import trick:

  Attempt N fails partway through, leaving already-loaded submodules cached.
  Attempt N+1 mocks the failing module and finds the earlier submodules in
  sys.modules, bypassing deeper (unavailable) transitive dependencies.

This lets unit tests import the real implementations of cache_async_client,
store, and refinery without needing Docker-level deps (aiopg, pygit2, etc.).

WARNING: _bootstrap() injects MagicMock stubs into sys.modules for missing
dependencies (aiopg, pygit2, etc.). These stubs are tracked in
_MOCKED_MODULES and removed by the session-scoped fixture below so they do
not pollute other test files in the same pytest session (DEF-A05-D3 fix).
"""
import sys
import pytest
from unittest.mock import MagicMock

# Track every module name stubbed by _bootstrap so we can clean up later.
_MOCKED_MODULES = []


def _bootstrap(import_fn, max_attempts=15):
    """Run import_fn repeatedly, mocking each ModuleNotFoundError until it succeeds.

    Returns True on success. Raises RuntimeError on failure so the problem is
    immediately visible rather than silently causing vacuous test passes
    (DEF-B05-D2 fix).
    """
    needed = []
    for _ in range(max_attempts):
        for m in needed:
            if m not in sys.modules:
                sys.modules[m] = MagicMock()
                _MOCKED_MODULES.append(m)
        try:
            import_fn()
            return True
        except ModuleNotFoundError as e:
            mod = str(e).split("'")[1]
            if mod not in needed:
                needed.append(mod)
            else:
                raise RuntimeError(
                    "Bootstrap stuck: '{}' keeps failing after being mocked. "
                    "Check import chain.".format(mod)
                )
        except Exception:
            return False
    raise RuntimeError(
        "Bootstrap exceeded {} attempts for {}. "
        "A new transitive dependency may have been added.".format(max_attempts, import_fn.__name__)
    )


def _load_cache_async_client():
    from services.ui_backend_service.data.cache.client.cache_async_client import CacheAsyncClient  # noqa


def _load_store():
    from services.ui_backend_service.data.cache.store import (  # noqa
        CacheStore, ArtifactCacheStore, DAGCacheStore, LogCacheStore, CardCacheStore,
    )


def _load_refinery():
    from services.ui_backend_service.data.refiner.refinery import Refinery  # noqa


_bootstrap(_load_cache_async_client)
_bootstrap(_load_store)
_bootstrap(_load_refinery)


@pytest.fixture(scope="session", autouse=True)
def _cleanup_bootstrap_mocks():
    """Remove MagicMock stubs injected by _bootstrap after the session ends.

    This prevents sys.modules pollution from leaking into other test files
    that may expect real imports or ImportError (DEF-A05-D3 fix).
    """
    yield
    for mod in _MOCKED_MODULES:
        sys.modules.pop(mod, None)
