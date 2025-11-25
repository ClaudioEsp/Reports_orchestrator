# jobs/get_dispatches_details.py

import os
import time
import datetime as dt
from pymongo import MongoClient

from jobs.dispatchtrack_dispatch_client import (
    fetch_dispatch_details,
    DispatchTrackAPIError,
)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "dispatchtrack")
MONGO_DISPATCHES_COLLECTION_NAME = os.getenv(
    "MONGO_DISPATCHES_COLLECTION_NAME",
    "dispatches",
)


def _get_dispatches_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    return db[MONGO_DISPATCHES_COLLECTION_NAME]


def run(limit: int = 100, throttle_seconds: float = 0.2):
    """
    Job 3: fetch full details for dispatches whose details_status != 'ok'.

    - Reads from the `dispatches` collection.
    - For each pending/stale/error dispatch:
        * Calls DispatchTrack: GET /dispatches/:identifier
        * Stores full payload and key fields (CT, dispatch_date, tags)
        * Updates details_status to 'ok' or 'error'
    """
    coll = _get_dispatches_collection()

    pending = list(
        coll.find(
            {"details_status": {"$ne": "ok"}},
            {"_id": 1, "identifier": 1},
        ).limit(limit)
    )

    if not pending:
        print("[get_dispatches_details] No dispatches needing details.")
        return

    print(
        f"[get_dispatches_details] Found {len(pending)} dispatches needing details."
    )

    for doc in pending:
        _id = doc["_id"]
        identifier = doc.get("identifier")
        if not identifier:
            continue

        now = dt.datetime.utcnow()

        try:
            dispatch_payload = fetch_dispatch_details(identifier)

            # TODO: adjust these keys to the real payload structure
            ct_value = dispatch_payload.get("CT")  # or "ct_corresponde" / etc.
            dispatch_date = dispatch_payload.get("dispatch_date")
            tags = dispatch_payload.get("tags", [])

            coll.update_one(
                {"_id": _id},
                {
                    "$set": {
                        "details_status": "ok",
                        "details_last_fetched_at": now,
                        "details_error": None,
                        "full_payload": dispatch_payload,
                        "CT": ct_value,
                        "dispatch_date": dispatch_date,
                        "tags": tags,
                    }
                },
            )
            print(f"[get_dispatches_details] OK dispatch={identifier}")

        except DispatchTrackAPIError as e:
            print(f"[get_dispatches_details] ERROR dispatch={identifier}: {e}")
            coll.update_one(
                {"_id": _id},
                {
                    "$set": {
                        "details_status": "error",
                        "details_last_fetched_at": now,
                        "details_error": str(e),
                    }
                },
            )

        time.sleep(throttle_seconds)
