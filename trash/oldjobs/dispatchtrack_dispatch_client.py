# jobs/dispatchtrack_dispatch_client.py

import os
import requests
from pathlib import Path

from dotenv import load_dotenv

# -------------------------------------------------
# Load .env that is ONE LEVEL ABOVE the project dir
# -------------------------------------------------
# This file is in: PROYECTO_REPORTES/orchestrator/jobs/...
# Project root:    PROYECTO_REPORTES
# .env location:   PROYECTOS_TI/.env  (one level above project root)

CURRENT_FILE = Path(__file__).resolve()
PROJECT_DIR = CURRENT_FILE.parents[2]       # .../PROYECTO_REPORTES/orchestrator/jobs -> parents[2] = PROYECTO_REPORTES
ENV_PATH = PROJECT_DIR.parent / ".env"      # one level up -> .../PROYECTOS_TI/.env

load_dotenv(ENV_PATH)

# -------------------------------------------------
# API config
# -------------------------------------------------
DISPATCHTRACK_API_BASE_URL = os.getenv(
    "DISPATCHTRACK_API_BASE_URL",
    "https://paris.dispatchtrack.com/api/external/v1",
)

# In your .env you should have:
# DISPATCHTRACK_TOKEN=your-token-here
DISPATCHTRACK_TOKEN = os.getenv("DISPATCHTRACK_TOKEN")


class DispatchTrackAPIError(Exception):
    """Custom exception for DispatchTrack API errors."""
    pass


def fetch_dispatch_details(identifier: str) -> dict:
    """
    Call DispatchTrack external API to fetch a single dispatch by identifier.

    Endpoint:
      GET /dispatches/:identifier

    Returns:
      A dict with the dispatch payload (unwrapped), or raises DispatchTrackAPIError
      on any non-2xx response or unexpected format.
    """
    if not DISPATCHTRACK_TOKEN:
        raise DispatchTrackAPIError(
            "DISPATCHTRACK_TOKEN is not set in the environment (check your .env)."
        )

    url = f"{DISPATCHTRACK_API_BASE_URL}/dispatches/{identifier}"

    headers = {
        # Adjust if your API uses a different scheme
        "X-AUTH-TOKEN": f"{DISPATCHTRACK_TOKEN}",
        "Accept": "application/json",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as e:
        raise DispatchTrackAPIError(
            f"Network error while calling DispatchTrack for dispatch {identifier}: {e}"
        ) from e

    if not resp.ok:
        raise DispatchTrackAPIError(
            f"DispatchTrack returned {resp.status_code} for dispatch {identifier}: {resp.text}"
        )

    try:
        data = resp.json()
    except ValueError as e:
        raise DispatchTrackAPIError(
            f"Invalid JSON response for dispatch {identifier}: {e}"
        ) from e

    # Defensive unwrapping
    if isinstance(data, dict):
        if "response" in data and isinstance(data["response"], dict):
            data = data["response"]

        if "dispatch" in data and isinstance(data["dispatch"], dict):
            return data["dispatch"]

        if "identifier" in data:
            return data

    raise DispatchTrackAPIError(
        f"Unexpected response format for dispatch {identifier}: "
        f"keys={list(data.keys()) if isinstance(data, dict) else type(data)}"
    )
