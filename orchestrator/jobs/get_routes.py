# jobs/get_routes.py

import os
import logging
from datetime import datetime
from typing import Optional

from pymongo import MongoClient, UpdateOne

from .dispatchtrack_client import fetch_routes_page, DispatchTrackAPIError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job.get_routes")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "dispatchtrack"
ROUTES_COLLECTION = "routes"


from typing import Optional

def _extract_route_key(route: dict) -> Optional[str]:
    """
    Try to extract a unique identifier for the route.

    We only accept real values, not None. Prefer number/route_number,
    then fall back to id.
    """
    for field in ("number", "route_number", "id"):
        value = route.get(field)
        if value is not None:
            return str(value)

    return None


def run(date_str: str) -> None:
    """
    Fetch *all* routes for the given date from DispatchTrack
    and upsert into Mongo dispatchtrack.routes.

    - Uses pagination (page=1..N) until API returns no routes.
    - Adds metadata fields: date, ingested_at, last_refreshed_at.
    """
    logger.info("Starting get_routes for date=%s", date_str)

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    routes_col = db[ROUTES_COLLECTION]

    page = 1
    total_routes = 0
    bulk_ops = []

    while True:
        try:
            routes_page = fetch_routes_page(date_str, page)
        except DispatchTrackAPIError:
            # Bubble up after closing Mongo
            client.close()
            raise

        if not routes_page:
            logger.info("No more routes at page=%s", page)
            break

        logger.info("Fetched %d routes for page=%s", len(routes_page), page)

        for route in routes_page:
            key = _extract_route_key(route)
            if not key:
                logger.warning("Skipping route without key: %s", route)
                continue

            meta = {
                "date": date_str,
                "minified_raw": route,
                "last_refreshed_at": datetime.utcnow(),
            }

            # Only set created_at on first insert
            update = {
                "$set": meta,
                "$setOnInsert": {
                    "created_at": datetime.utcnow(),
                    "has_full_details": False,
                },
            }

            bulk_ops.append(
                UpdateOne(
                    {"route_key": key},
                    update,
                    upsert=True,
                )
            )
            total_routes += 1

        if bulk_ops:
            routes_col.bulk_write(bulk_ops, ordered=False)
            bulk_ops = []

        page += 1

    client.close()
    logger.info("Finished get_routes for %s. Upserted/updated ~%d routes.", date_str, total_routes)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch routes for a day.")
    parser.add_argument("--date", required=True, help="Date YYYY-MM-DD")
    args = parser.parse_args()

    run(args.date)
