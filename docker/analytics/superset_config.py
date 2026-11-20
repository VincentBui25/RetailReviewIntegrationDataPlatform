"""Single-process local BI; separate metadata DB and read-only serving connection."""

from pathlib import Path
from sqlalchemy.engine import URL


def secret(name):
    return (Path("/run/secrets") / name).read_text().strip()


SECRET_KEY = secret("superset_secret_key")
SQLALCHEMY_DATABASE_URI = URL.create(
    "postgresql+psycopg2",
    username="fnb_superset_metadata",
    password=secret("superset_metadata_password"),
    host="postgres",
    database="fnb_superset",
).render_as_string(hide_password=False)
SQLALCHEMY_ECHO = False
WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False  # Localhost-only HTTP service, not a public deployment.
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ENABLE_JAVASCRIPT_CONTROLS": True,
    "GLOBAL_ASYNC_QUERIES": False,
}
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 60}
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "NullCache"
}  # Never mix cached KPI values across atomic release switches.
TALISMAN_CONFIG = {
    "force_https": False,
    # Talisman otherwise overrides Flask's SESSION_COOKIE_SECURE=False.
    # Docker's internal HTTP hostname needs the same local-session behavior.
    "session_cookie_secure": False,
    "content_security_policy": {
        "default-src": ["'self'"],
        "script-src": ["'self'", "'unsafe-eval'"],
        "style-src": ["'self'", "'unsafe-inline'"],
        "img-src": [
            "'self'",
            "data:",
            "blob:",
            "https://tile.openstreetmap.org",
            "https://tiles.stadiamaps.com",
        ],
        "connect-src": [
            "'self'",
            "https://tile.openstreetmap.org",
            "https://tiles.stadiamaps.com",
        ],
        "worker-src": ["'self'", "blob:"],
        "font-src": ["'self'", "data:"],
        "object-src": ["'none'"],
    },
    "content_security_policy_nonce_in": ["script-src"],
}
MAPBOX_API_KEY = ""  # OSM raster tiles; no token embedded or required.
SQL_MAX_ROW = 10000
ROW_LIMIT = 10000
ENABLE_PROXY_FIX = False
