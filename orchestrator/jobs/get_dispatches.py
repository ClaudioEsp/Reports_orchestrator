# orchestrator/jobs/get_dispatches.py

import os
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from pymongo import MongoClient, UpdateOne

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job.get_dispatches")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

DISPATCHTRACK_DB = os.getenv("DISPATCHTRACK_DB", "dispatchtrack")
ROUTES_COLLECTION = os.getenv("ROUTES_COLLECTION", "routes")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "dispatches")


def _get_route_payload(route_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prefer full_raw (from /routes/:id) and fall back to minified_raw
    if for some reason full_raw is not present.
    """
    return route_doc.get("full_raw") or route_doc.get("minified_raw") or {}


def _extract_dispatch_list(route_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    For your API, routes have a top-level 'dispatches' list.
    """
    dispatches = route_payload.get("dispatches")
    if isinstance(dispatches, list):
        return dispatches

    logger.warning(
        "Route payload does not contain 'dispatches' as a list. Keys=%s",
        list(route_payload.keys()),
    )
    return []


def _extract_route_meta(route_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Metadata propagated to each dispatch:

      - dispatch_date      -> route_payload['dispatch_date']
      - truck_identifier   -> route_payload['truck']['identifier']
    """
    dispatch_date = route_payload.get("dispatch_date")

    truck = route_payload.get("truck") or {}
    truck_identifier = truck.get("identifier")

    return {
        "dispatch_date": dispatch_date,
        "truck_identifier": truck_identifier,
    }


def _extract_dispatch_key(dispatch: Dict[str, Any]) -> Optional[str]:
    """
    Unique key for each dispatch.

    In your example, dispatch['identifier'] is the natural ID:
      "identifier": "5244179189078778"

    We use that as dispatch_key.
    """
    ident = dispatch.get("identifier")
    if ident is None:
        return None
    return str(ident)


def run(date_str: str) -> None:
    """
    From routes with (preferably) full details for a given date, extract
    all dispatches and upsert them into dispatchtrack.dispatches.

    Each dispatch doc will contain:

      - dispatch_key           (identifier from API)
      - route_id               (Mongo _id of the route)
      - route_key              (route identifier, e.g. 44800796)
      - route_dispatch_date    (route's dispatch_date)
      - truck_identifier       (route.truck.identifier)
      - dispatch_raw           (full dispatch payload from the route)
      - flattened fields: status, substatus, substatus_code, etc.
    """
    logger.info("Starting get_dispatches for date=%s", date_str)

    client = MongoClient(MONGO_URI)
    db = client[DISPATCHTRACK_DB]
    routes_col = db[ROUTES_COLLECTION]
    disp_col = db[DISPATCHES_COLLECTION]

    # We target routes for this date that already have details.
    query = {
        "date": date_str,
        "has_full_details": True,
    }

    cursor = routes_col.find(query)
    bulk_ops: List[UpdateOne] = []
    total_dispatches = 0

    for route_doc in cursor:
        route_id = route_doc["_id"]
        route_key = route_doc.get("route_key")

        route_payload = _get_route_payload(route_doc)
        route_meta = _extract_route_meta(route_payload)
        dispatch_list = _extract_dispatch_list(route_payload)

        logger.info(
            "Route %s (id=%s) has %d dispatches",
            route_key,
            route_id,
            len(dispatch_list),
        )

        for disp in dispatch_list:
            disp_key = _extract_dispatch_key(disp)
            if not disp_key:
                logger.warning(
                    "Skipping dispatch without identifier (route_key=%s): %s",
                    route_key,
                    disp,
                )
                continue

            # Some useful flattened fields from the dispatch
            flattened = {
                "status": disp.get("status"),
                "status_id": disp.get("status_id"),
                "substatus": disp.get("substatus"),
                "substatus_code": disp.get("substatus_code"),
                "is_trunk": disp.get("is_trunk"),
                "is_pickup": disp.get("is_pickup"),
                "estimated_at": disp.get("estimated_at"),
                "min_delivery_time": disp.get("min_delivery_time"),
                "max_delivery_time": disp.get("max_delivery_time"),
                "delivery_time": disp.get("delivery_time"),
                "beecode": disp.get("beecode"),
            }

            doc_meta = {
                "route_id": route_id,
                "route_key": route_key,
                "route_dispatch_date": route_meta.get("dispatch_date"),
                "truck_identifier": route_meta.get("truck_identifier"),
                "dispatch_raw": disp,
                "last_refreshed_at": datetime.utcnow(),
                **flattened,
            }

            bulk_ops.append(
                UpdateOne(
                    {"dispatch_key": disp_key},
                    {
                        "$set": doc_meta,
                        "$setOnInsert": {
                            "created_at": datetime.utcnow(),
                            # Will be filled later by get_ct / get_substatus
                            "ct": None,
                        },
                    },
                    upsert=True,
                )
            )
            total_dispatches += 1

            # Flush in batches to avoid huge bulk
            if len(bulk_ops) >= 500:
                disp_col.bulk_write(bulk_ops, ordered=False)
                bulk_ops = []

    # Final flush
    if bulk_ops:
        disp_col.bulk_write(bulk_ops, ordered=False)

    client.close()
    logger.info(
        "Finished get_dispatches for %s. Upserted/updated ~%d dispatches.",
        date_str,
        total_dispatches,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract dispatches from routes.")
    parser.add_argument("--date", required=True, help="Date YYYY-MM-DD")
    args = parser.parse_args()

    run(args.date)
