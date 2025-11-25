# jobs/backfill_substatus_statuses_for_dispatches.py

import os
import logging
import math
from pymongo import MongoClient

logger = logging.getLogger("job.backfill_substatus_statuses_for_dispatches")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

# DB with dispatch documents
DISPATCHTRACK_DB = os.getenv("DISPATCHTRACK_DB", "dispatchtrack")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "dispatches")

# Substatus / mapping DB
SUB_STATUS_DATABASE = os.getenv("SUB_STATUS_DATABASE", "SUB_STATUS_DATABASE")
SUB_STATUS_COLLECTION = os.getenv("SUB_STATUS_COLLECTION", "SUB_STATUS_COLLECTION")

# Orchestrator metadata (optional but handy)
JOB_NAME = "backfill_substatus_statuses_for_dispatches"


def _is_bad_number(value) -> bool:
    """
    Returns True if value is NaN or infinite (float('nan'), inf, -inf).
    Mainly defensive; your mapping fields are usually strings.
    """
    if isinstance(value, float):
        return math.isnan(value) or math.isinf(value)
    return False


def run():
    """
    Core logic of the job.
    Can be called directly, or via run_job() from the orchestrator.
    """
    logger.info("[SUBSTATUS_BACKFILL] Starting substatus->status backfill job on 'dispatches'...")

    client = MongoClient(MONGO_URI)

    dispatches_col = client[DISPATCHTRACK_DB][DISPATCHES_COLLECTION]
    substatus_col = client[SUB_STATUS_DATABASE][SUB_STATUS_COLLECTION]

    # 1) Build lookup map:
    #    { "Código sub" (str) -> { betrack_status, guide_status, closing_status } }
    logger.info("[SUBSTATUS_BACKFILL] Loading substatus status map from collection...")

    substatus_map: dict[str, dict] = {}

    mapping_cursor = substatus_col.find(
        {},
        {
            "Código Sub": 1,          # 👈 exact field name from your DB
            "Estado Beetrack": 1,
            "Estado Guía": 1,
            "Cierre": 1,
            "_id": 0,
        },
    )

    for doc in mapping_cursor:
        code = doc.get("Código Sub")
        if code is None:
            continue

        key = str(code).strip()

        betrack_status = doc.get("Estado Beetrack")
        guide_status = doc.get("Estado Guía")
        closing_status = doc.get("Cierre")

        # Defensive: ignore weird NaN/inf values
        if _is_bad_number(betrack_status):
            betrack_status = None
        if _is_bad_number(guide_status):
            guide_status = None
        if _is_bad_number(closing_status):
            closing_status = None

        substatus_map[key] = {
            "betrack_status": betrack_status,
            "guide_status": guide_status,
            "closing_status": closing_status,
        }

    logger.info(
        f"[SUBSTATUS_BACKFILL] Loaded {len(substatus_map)} substatus entries into memory."
    )

    updated_dispatches = 0
    scanned_dispatches = 0

    # 2) Scan dispatches (only need substatus + current mapped fields)
    dispatch_cursor = dispatches_col.find(
        {"details_status": "ok"},   # 👈 only where we trust the details
        {
            "_id": 1,
            "substatus_code": 1,
            "full_payload.substatus_code": 1,
            "betrack_status": 1,
            "guide_status": 1,
            "closing_status": 1,
        },
    )

    for doc in dispatch_cursor:
        scanned_dispatches += 1

        # Top-level substatus_code first
        sub_code = doc.get("substatus_code")

        # Optional fallback to full_payload if top-level is missing
        if sub_code is None:
            full_payload = doc.get("full_payload") or {}
            sub_code = full_payload.get("substatus_code")

        # If dispatch has no substatus_code at all, we can't map anything
        if sub_code is None:
            continue

        key = str(sub_code).strip()
        mapped = substatus_map.get(key)

        if not mapped:
            # No mapping for this substatus_code
            continue

        # Current values in dispatch
        current_betrack = doc.get("betrack_status")
        current_guide = doc.get("guide_status")
        current_closing = doc.get("closing_status")

        new_betrack = mapped.get("betrack_status")
        new_guide = mapped.get("guide_status")
        new_closing = mapped.get("closing_status")

        update_fields = {}

        # Update betrack_status if different / missing
        if current_betrack != new_betrack:
            update_fields["betrack_status"] = new_betrack

        # Update guide_status if different / missing
        if current_guide != new_guide:
            update_fields["guide_status"] = new_guide

        # Update closing_status if different / missing
        if current_closing != new_closing:
            update_fields["closing_status"] = new_closing

        if update_fields:
            dispatches_col.update_one(
                {"_id": doc["_id"]},
                {"$set": update_fields},
            )
            updated_dispatches += 1

    logger.info(
        f"[SUBSTATUS_BACKFILL] Finished. Scanned {scanned_dispatches} ok-dispatches, "
        f"updated {updated_dispatches} of them."
    )


def run_job():
    """
    Entry point for the orchestrator.
    Keeps logging config local so importing the module doesn't change global logging.
    """
    logging.basicConfig(level=logging.INFO)
    run()


if __name__ == "__main__":
    # Manual execution: `python jobs/backfill_substatus_statuses_for_dispatches.py`
    run_job()
