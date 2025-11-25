# jobs/get_dispatches.py

import os
import datetime as dt
from pymongo import MongoClient

# ---------- Mongo configuration ----------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "dispatchtrack")

MONGO_ROUTES_COLLECTION_NAME = os.getenv("MONGO_ROUTES_COLLECTION_NAME", "routes")
MONGO_DISPATCHES_COLLECTION_NAME = os.getenv("MONGO_DISPATCHES_COLLECTION_NAME", "dispatches")


def _get_routes_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    return db[MONGO_ROUTES_COLLECTION_NAME]


def _get_dispatches_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    coll = db[MONGO_DISPATCHES_COLLECTION_NAME]

    coll.create_index("identifier", unique=True)
    coll.create_index("details_status")
    coll.create_index("dispatch_date_param")  # temporary "day" until we get real dispatch_date
    coll.create_index("route_id")
    return coll


# --------------------------------------------------------------------
# JOB 2: Expand routes into minimal dispatch documents
# --------------------------------------------------------------------
def run(days_back: int = 0):
    """
    For a given route day (today - days_back), read routes from the `routes`
    collection and upsert minimal dispatch documents into `dispatches`.

    NOTE: dispatch_date_param here is the *route day*.
    The real per-dispatch date will be filled later by get_dispatches_details.

    - One document per dispatch (`identifier`).
    - Each new dispatch starts with details_status='pending' until Job 3
      (get_dispatches_details) fetches full details.
    """

    # route day based on days_back
    target_date = dt.date.today() - dt.timedelta(days=days_back)
    date_str = target_date.strftime("%Y-%m-%d")

    routes_coll = _get_routes_collection()
    dispatches_coll = _get_dispatches_collection()

    print(f"[get_dispatches] Loading routes for route day {date_str} from Mongo...")

    routes_cursor = routes_coll.find(
        {"dispatch_date_param": date_str},
        {
            "_id": 0,
            "id": 1,
            "dispatch_date_param": 1,
            "dispatch_identifiers": 1,
            # optional if you stored it in get_routes
            "truck_identifier": 1,
            "truck": 1,
        },
    )

    routes = list(routes_cursor)
    if not routes:
        print(f"[get_dispatches] No routes found in Mongo for route day {date_str}. Nothing to do.")
        return

    print(f"[get_dispatches] Found {len(routes)} routes for route day {date_str}.")

    new_count = 0
    updated_count = 0
    total_dispatches_seen = 0
    now = dt.datetime.utcnow()

    for route in routes:
        route_id = route.get("id")
        if not route_id:
            continue

        route_date_param = route.get("dispatch_date_param", date_str)
        dispatch_ids = route.get("dispatch_identifiers") or []

        truck_identifier = (
            route.get("truck_identifier")
            or (route.get("truck") or {}).get("identifier")
        )

        if not dispatch_ids:
            # Route exists but has no dispatch identifiers → just skip, no error
            continue

        for dispatch_id in dispatch_ids:
            if not dispatch_id:
                continue

            total_dispatches_seen += 1

            filter_doc = {"identifier": str(dispatch_id)}

            update_doc = {
                "$set": {
                    "identifier": str(dispatch_id),
                    "route_id": route_id,
                    # For now this is the route day; later we'll add real dispatch_date
                    "dispatch_date_param": route_date_param,
                    "truck_identifier": truck_identifier,
                    "last_seen_at": now,
                },
                "$setOnInsert": {
                    "details_status": "pending",
                    "details_last_fetched_at": None,
                    "details_error": None,
                },
            }

            result = dispatches_coll.update_one(
                filter_doc,
                update_doc,
                upsert=True,
            )

            if result.upserted_id is not None:
                new_count += 1
            elif result.modified_count > 0:
                updated_count += 1

    if total_dispatches_seen == 0:
        print(
            f"[get_dispatches] Routes found for route day {date_str}, "
            "but none had dispatch identifiers. Nothing to upsert."
        )
    else:
        print(
            f"[get_dispatches] Done for route day {date_str}. "
            f"Dispatches seen: {total_dispatches_seen}, "
            f"New: {new_count}, Updated: {updated_count}"
        )
