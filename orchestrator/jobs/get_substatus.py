# orchestrator/jobs/get_substatus.py

import os
import logging
from typing import Any, Dict, Optional, Set

from pymongo import MongoClient
from dotenv import load_dotenv

# Load env (.env at project root)
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job.get_substatus")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

DISPATCHTRACK_DB = os.getenv("DISPATCHTRACK_DB", "dispatchtrack")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "dispatches")

SUB_STATUS_DATABASE = os.getenv("SUB_STATUS_DATABASE", "substatus_db")
SUB_STATUS_COLLECTION = os.getenv("SUB_STATUS_COLLECTION", "substatus_collection")


def _code_variants(code: Any) -> Optional[Set[Any]]:
    """
    Given a code that might be int or string ("1", 1, " 01 "),
    return a set of possible representations to try in the query.

    Example:
      "1"  -> {"1", 1}
      1    -> {"1", 1}
      "abc"-> {"abc"}
    """
    if code is None:
        return None

    variants: Set[Any] = set()

    # raw
    variants.add(code)

    # string version
    s = str(code).strip()
    variants.add(s)

    # numeric version (if all digits)
    if s.isdigit():
        variants.add(int(s))

    return variants


def _lookup_substatus(sub_col, code: Any) -> Optional[Dict[str, Any]]:
    """
    Look up mapping where "Código Sub" equals dispatch.substatus_code,
    being tolerant about int vs string.
    """
    variants = _code_variants(code)
    if not variants:
        return None

    # Match any of the variants
    mapping = sub_col.find_one({"Código Sub": {"$in": list(variants)}})
    return mapping


def run() -> None:
    """
    For each dispatch that has substatus_code and is missing at least one of:
      - estado_beetrack
      - estado_guia
      - cierre

    Match dispatch.substatus_code with SUB_STATUS_COLLECTION["Código Sub"],
    then set:
      - estado_beetrack = "Estado Beetrack"
      - estado_guia     = "Estado Guía"
      - cierre          = "Cierre"
    """

    logger.info(
        "Starting get_substatus: dispatch_db=%s, substatus_db=%s.%s",
        DISPATCHTRACK_DB,
        SUB_STATUS_DATABASE,
        SUB_STATUS_COLLECTION,
    )

    client = MongoClient(MONGO_URI)

    disp_col = client[DISPATCHTRACK_DB][DISPATCHES_COLLECTION]
    sub_col = client[SUB_STATUS_DATABASE][SUB_STATUS_COLLECTION]

    # Only dispatches that HAVE a substatus_code (not null)
    # and are missing at least one of the target fields
    cursor = disp_col.find(
        {
            "substatus_code": {"$ne": None},
            "$or": [
                {"estado_beetrack": {"$exists": False}},
                {"estado_guia": {"$exists": False}},
                {"cierre": {"$exists": False}},
            ],
        }
    )

    total = 0
    updated = 0
    unmatched = 0

    for disp in cursor:
        total += 1

        code = disp.get("substatus_code")
        mapping = _lookup_substatus(sub_col, code)

        if not mapping:
            unmatched += 1
            # Optional debug for the first few unmatched
            if unmatched <= 5:
                logger.warning(
                    "No substatus mapping for substatus_code=%r (dispatch_id=%s)",
                    code,
                    disp.get("_id"),
                )
            continue

        update_fields = {
            "estado_beetrack": mapping.get("Estado Beetrack"),
            "estado_guia": mapping.get("Estado Guía"),
            "cierre": mapping.get("Cierre"),
        }

        disp_col.update_one({"_id": disp["_id"]}, {"$set": update_fields})
        updated += 1

    client.close()

    logger.info(
        "get_substatus finished. Processed=%d  Updated=%d  Unmatched=%d",
        total,
        updated,
        unmatched,
    )


if __name__ == "__main__":
    run()
