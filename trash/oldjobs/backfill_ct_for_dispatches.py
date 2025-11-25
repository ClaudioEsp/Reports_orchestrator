# jobs/backfill_ct_for_dispatches.py

import os
import logging
import math
from pymongo import MongoClient

logger = logging.getLogger("job.backfill_ct_for_dispatches")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

# DB with dispatch documents
DISPATCHTRACK_DB = os.getenv("DISPATCHTRACK_DB", "dispatchtrack")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "dispatches")

# CT DB
CT_DATABASE = os.getenv("CT_DATABASE", "CT_DATABASE")
CT_COLLECTION = os.getenv("CT_COLLECTION", "CT_COLLECTION")

# Default CT value when no CT CORRESPONDE can be found
DEFAULT_CT_VALUE = os.getenv("CT_DEFAULT_VALUE", "UNKNOWN")


def _get_tag_value(tags, name):
    """Return value of tag with given name, or None."""
    if not tags:
        return None
    for t in tags:
        if t.get("name") == name:
            return t.get("value")
    return None


def _is_bad_number(value) -> bool:
    """
    Returns True if value is NaN or infinite (float('nan'), inf, -inf).
    Only applies to floats; ints / strings are considered OK.
    """
    if isinstance(value, float):
        return math.isnan(value) or math.isinf(value)
    return False


def run():
    """
    For each dispatch in DISPATCHTRACK_DB.DISPATCHES_COLLECTION
    with details_status == "ok":

      - If document has no 'CT' field OR CT == 'UNKNOWN':
          - Find CODCOMU in document['tags'] (or, if needed, in full_payload['tags'])
          - Look up CT in CT_DATABASE.CT_COLLECTION where 'Id Externo' == CODCOMU
          - If 'CT CORRESPONDE' is present and valid -> use it
          - Otherwise -> use DEFAULT_CT_VALUE (by default 'UNKNOWN')

      - Save updated CT back on that dispatch document.

    Idempotent: if CT changes in the CT collection, running again upgrades UNKNOWN entries.
    """
    logger.info("[CT_BACKFILL] Starting CT backfill job on 'dispatches' collection...")
    logger.info(f"[CT_BACKFILL] Using DEFAULT_CT_VALUE={DEFAULT_CT_VALUE!r}")

    client = MongoClient(MONGO_URI)

    dispatches_col = client[DISPATCHTRACK_DB][DISPATCHES_COLLECTION]
    ct_col = client[CT_DATABASE][CT_COLLECTION]

    # 1) Build lookup map: { "Id Externo" (str) -> "CT CORRESPONDE or default" }
    logger.info("[CT_BACKFILL] Loading CT map from CT collection...")
    ct_map: dict[str, str] = {}

    for doc in ct_col.find({}, {"Id Externo": 1, "CT CORRESPONDE": 1, "_id": 0}):
        id_externo = doc.get("Id Externo")
        ct_value = doc.get("CT CORRESPONDE")

        if id_externo is None:
            continue

        # If CT is missing or NaN/inf, fall back to default
        if ct_value is None or _is_bad_number(ct_value):
            effective_ct = DEFAULT_CT_VALUE
        else:
            effective_ct = ct_value

        key = str(id_externo).strip()
        ct_map[key] = effective_ct

    logger.info(f"[CT_BACKFILL] Loaded {len(ct_map)} CT entries into memory.")

    updated_dispatches = 0
    scanned_dispatches = 0

    # 2) Scan only dispatches with details_status == "ok"
    cursor = dispatches_col.find(
      {"details_status": "ok"},   # 👈 IMPORTANT FILTER
      {
          "_id": 1,
          "CT": 1,
          "tags": 1,
          # optional, in case you ever want fallback:
          "full_payload.tags": 1,
      },
    )

    for doc in cursor:
        scanned_dispatches += 1
        current_ct = doc.get("CT")

        # Only update when CT is missing or marked as UNKNOWN
        if current_ct not in (None, "UNKNOWN"):
            continue

        tags = doc.get("tags") or []

        # Optional fallback: if top-level tags is empty, try full_payload.tags
        if not tags:
            full_payload = doc.get("full_payload") or {}
            tags = full_payload.get("tags") or []

        codcomu = _get_tag_value(tags, "CODCOMU")

        if codcomu is None:
            # No CODCOMU -> can't map, but we still want DEFAULT if CT is None
            if current_ct is None:
                dispatches_col.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"CT": DEFAULT_CT_VALUE}},
                )
                updated_dispatches += 1
            continue

        key = str(codcomu).strip()

        # Get CT from map, or default if not present (-> 'UNKNOWN')
        ct_value = ct_map.get(key, DEFAULT_CT_VALUE)

        # Extra safety: if ct_value is still a bad number, force default
        if _is_bad_number(ct_value) or ct_value is None:
            ct_value = DEFAULT_CT_VALUE

        dispatches_col.update_one(
            {"_id": doc["_id"]},
            {"$set": {"CT": ct_value}},
        )
        updated_dispatches += 1

    logger.info(
        f"[CT_BACKFILL] Finished. Scanned {scanned_dispatches} ok-dispatches, "
        f"updated CT in {updated_dispatches} of them."
    )
