import os
import logging
from typing import List, Optional

from pymongo import MongoClient

logger = logging.getLogger("job.backfill_compromise_date_from_tags")
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

# Mongo env
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "dispatchtrack")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "dispatches")


def extract_fecsoldes(tags: List[dict]) -> Optional[str]:
    """
    Given the tags array, return FECSOLDES value (YYYYMMDD)
    or None if not found.
    """
    if not isinstance(tags, list):
        return None

    for tag in tags:
        if isinstance(tag, dict) and tag.get("name") == "FECSOLDES":
            return tag.get("value")

    return None


def normalize_compromise_date(raw: str) -> Optional[str]:
    """
    Convert YYYYMMDD → YYYY-MM-DD.
    Return None if format invalid.
    """
    if not raw:
        return None

    s = str(raw).strip()
    if len(s) != 8 or not s.isdigit():
        return None

    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"


def run() -> int:
    """
    Backfill compromise_date for all dispatches:

      - Reads tag 'FECSOLDES' from tags
      - If valid (YYYYMMDD), sets:
          compromise_date_raw = FECSOLDES
          compromise_date     = YYYY-MM-DD
      - Only touches docs where compromise_date does NOT exist.
    """
    logger.info(
        "Starting job: backfill_compromise_date_from_tags "
        "on %s.%s",
        MONGO_DB_NAME,
        MONGO_COLLECTION,
    )

    mongo = MongoClient(MONGO_URI)
    col = mongo[MONGO_DB_NAME][MONGO_COLLECTION]

    # Only docs lacking compromise_date
    docs = col.find(
        {
            "compromise_date": {"$exists": False},
        },
        {
            "_id": 1,
            "tags": 1,
        },
    )

    count = 0
    updated = 0

    for doc in docs:
        count += 1

        _id = doc.get("_id")
        tags = doc.get("tags", [])

        raw_fecsoldes = extract_fecsoldes(tags)
        compromise_date = normalize_compromise_date(raw_fecsoldes)

        if raw_fecsoldes is None:
            logger.info("%s: FECSOLDES not found → skipped", _id)
            continue

        if compromise_date is None:
            logger.info(
                "%s: FECSOLDES invalid format '%s' → skipped",
                _id,
                raw_fecsoldes,
            )
            continue

        col.update_one(
            {"_id": _id},
            {
                "$set": {
                    "compromise_date_raw": raw_fecsoldes,
                    "compromise_date": compromise_date,
                }
            },
        )

        logger.info(
            "Updated %s: FECSOLDES=%s → %s",
            _id,
            raw_fecsoldes,
            compromise_date,
        )
        updated += 1

    mongo.close()

    logger.info(
        "Finished backfill_compromise_date_from_tags. "
        "Scanned: %d docs — Updated: %d docs.",
        count,
        updated,
    )
    return updated


if __name__ == "__main__":
    run()
