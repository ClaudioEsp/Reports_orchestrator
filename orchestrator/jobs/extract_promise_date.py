import os
import logging
from pymongo import MongoClient

logger = logging.getLogger("job.backfill_promise_date")
logger.setLevel(logging.INFO)

# Mongo env
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "dispatchtrack")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "dispatches")


def extract_fecsoldes(tags: list):
    """
    Given the tags array inside dispatch_raw, return FECSOLDES value (YYYYMMDD)
    or None if not found.
    """
    if not isinstance(tags, list):
        return None

    for tag in tags:
        if isinstance(tag, dict) and tag.get("name") == "FECSOLDES":
            return tag.get("value")

    return None


def normalize_promise_date(raw: str):
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


def run():
    logger.info("Starting job: backfill_promise_date")

    mongo = MongoClient(MONGO_URI)
    col = mongo[MONGO_DB_NAME][MONGO_COLLECTION]

    # load all docs (we update only those lacking promise_date)
    docs = col.find(
        {
            "promise_date": {"$exists": False}
        },
        {
            "_id": 1,
            "dispatch_raw.tags": 1
        }
    )

    count = 0
    updated = 0

    for doc in docs:
        count += 1

        _id = doc.get("_id")
        tags = (
            doc.get("dispatch_raw", {})
            .get("tags", [])
        )

        raw_fecsoldes = extract_fecsoldes(tags)
        promise_date = normalize_promise_date(raw_fecsoldes)

        if raw_fecsoldes is None:
            logger.info(f"{_id}: FECSOLDES not found → skipped")
            continue

        if promise_date is None:
            logger.info(f"{_id}: FECSOLDES invalid format '{raw_fecsoldes}' → skipped")
            continue

        col.update_one(
            {"_id": _id},
            {
                "$set": {
                    "promise_date_raw": raw_fecsoldes,
                    "promise_date": promise_date
                }
            }
        )

        logger.info(
            f"Updated {_id}: FECSOLDES={raw_fecsoldes} → {promise_date}"
        )
        updated += 1

    logger.info(f"Finished. Scanned: {count} docs — Updated: {updated} docs.")
    return updated
