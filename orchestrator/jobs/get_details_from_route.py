# jobs/get_details_from_route.py

import os
import logging
from datetime import datetime

from pymongo import MongoClient

from .dispatchtrack_client import fetch_route_details, DispatchTrackAPIError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job.get_details_from_route")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "dispatchtrack"
ROUTES_COLLECTION = "routes"


def run(date_str: str) -> None:
    """
    For all routes of a given date that don't yet have full details,
    call /routes/{route_number} and store the full payload.

    It sets:
      - full_raw: full route payload from API
      - has_full_details: True
      - last_refreshed_at: now
    """
    logger.info("Starting get_details_from_route for date=%s", date_str)

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    routes_col = db[ROUTES_COLLECTION]

    query = {
        "date": date_str,
        "has_full_details": False,
    }

    cursor = routes_col.find(query)
    total = 0

    for route_doc in cursor:
        route_key = route_doc.get("route_key")
        if not route_key:
            logger.warning("Route doc without route_key: %s", route_doc.get("_id"))
            continue

        logger.info("Fetching details for route_key=%s", route_key)

        try:
            raw = fetch_route_details(route_key)
            # First unwrap "response" if present
            if isinstance(raw, dict) and "response" in raw:
                raw = raw["response"]

            # Then unwrap "route" if present
            if isinstance(raw, dict) and "route" in raw:
                full_payload = raw["route"]
            else:
                full_payload = raw

        except DispatchTrackAPIError as e:
            logger.error("Failed to fetch route %s details: %s", route_key, e)
            continue

        routes_col.update_one(
            {"_id": route_doc["_id"]},
            {
                "$set": {
                    "full_raw": full_payload,
                    "has_full_details": True,
                    "last_refreshed_at": datetime.utcnow(),
                }
            },
        )
        total += 1

    client.close()
    logger.info("Finished get_details_from_route for %s, updated %d routes.", date_str, total)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch full route details for a day.")
    parser.add_argument("--date", required=True, help="Date YYYY-MM-DD")
    args = parser.parse_args()

    run(args.date)
