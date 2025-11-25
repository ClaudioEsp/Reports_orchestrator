import os
import datetime as dt
from pathlib import Path

import pandas as pd
from pymongo import MongoClient

# --- Mongo config ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "dispatchtrack")
MONGO_ROUTES_COLLECTION_NAME = os.getenv("MONGO_ROUTES_COLLECTION_NAME", "routes")

# --- Output folder (PROYECTO_REPORTES/data/reports) ---
PROJECT_DIR = Path(__file__).resolve().parents[2]   # PROYECTO_REPORTES
DATA_ROOT = PROJECT_DIR.parent / "data"             # <parent_of_project>/data
REPORTS_DIR = DATA_ROOT / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def get_routes_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    return db[MONGO_ROUTES_COLLECTION_NAME]


def _normalize_date(d):
    """
    Accepts:
      - datetime.date -> 'YYYY-MM-DD'
      - 'YYYY-MM-DD' string -> returned as string
      - None -> today's date as string
    """
    if d is None:
        return dt.date.today().strftime("%Y-%m-%d")
    if isinstance(d, dt.date):
        return d.strftime("%Y-%m-%d")
    return str(d)


def generate_daily_completeness_report(start_date=None, end_date=None, save_csv=False):
    """
    Build a daily report of delivery completeness using **max_delivery_time date**
    as the reference day (target day for delivery).

    Also adds, per day:
      - open_routes, closed_routes
      - dispatches_with_substatus_code
      - dispatches_without_substatus_code
    """
    today_str = dt.date.today().strftime("%Y-%m-%d")

    # Decide the date filter used in the Mongo match
    if start_date is None and end_date is None:
        # Case 3: from today onwards (open-ended future)
        start_str = today_str
        end_str = None
        date_filter = {"$gte": start_str}
        range_label = f"from_{start_str}_onwards"
    else:
        # Case 1 or 2: single date or explicit range
        start_str = _normalize_date(start_date)
        if end_date is None:
            end_str = start_str   # single-day report
        else:
            end_str = _normalize_date(end_date)
        date_filter = {"$gte": start_str, "$lte": end_str}
        range_label = f"{start_str}_to_{end_str}"

    coll = get_routes_collection()

    # ---------- 1) DISPATCH-LEVEL STATS ----------
    dispatch_pipeline = [
        {"$unwind": "$dispatches"},
        {
            # "2025-11-19 21:00:00-0300" -> "2025-11-19"
            "$addFields": {
                "target_date": {
                    "$substr": ["$dispatches.max_delivery_time", 0, 10]
                }
            }
        },
        {
            "$match": {
                "target_date": date_filter
            }
        },
        {
            "$group": {
                "_id": "$target_date",
                "total_dispatches": {"$sum": 1},
                # status_id mapping:
                # 1 = on route, 2 = delivered, 3 = not delivered, 4 = partial delivered
                "delivered": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$dispatches.status_id", 2]},
                            1,
                            0,
                        ]
                    }
                },
                "on_route": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$dispatches.status_id", 1]},
                            1,
                            0,
                        ]
                    }
                },
                "not_delivered": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$dispatches.status_id", 3]},
                            1,
                            0,
                        ]
                    }
                },
                "partial_delivered": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$dispatches.status_id", 4]},
                            1,
                            0,
                        ]
                    }
                },
                # dispatches with substatus_code not null/empty
                "dispatches_with_substatus_code": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$dispatches.substatus_code", None]},
                                    {"$ne": ["$dispatches.substatus_code", ""]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
                # NEW: dispatches WITHOUT substatus_code (null or empty string)
                "dispatches_without_substatus_code": {
                    "$sum": {
                        "$cond": [
                            {
                                "$or": [
                                    {"$eq": ["$dispatches.substatus_code", None]},
                                    {"$eq": ["$dispatches.substatus_code", ""]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    dispatch_rows = list(coll.aggregate(dispatch_pipeline))

    if not dispatch_rows:
        if end_str is None:
            print(f"No dispatches found with max_delivery_time on or after {start_str}")
        else:
            print(f"No dispatches found with max_delivery_time between {start_str} and {end_str}")
        return pd.DataFrame()

    df = pd.DataFrame(dispatch_rows)
    df = df.rename(columns={"_id": "target_date"})  # date from max_delivery_time

    # Completion = delivered / total
    df["completion_rate"] = df["delivered"] / df["total_dispatches"]

    # ---------- 2) ROUTE-LEVEL OPEN/CLOSED STATS ----------
    route_pipeline = [
        {"$unwind": "$dispatches"},
        {
            "$addFields": {
                "target_date": {
                    "$substr": ["$dispatches.max_delivery_time", 0, 10]
                }
            }
        },
        {"$match": {"target_date": date_filter}},
        {
            # First group: per route + day
            "$group": {
                "_id": {
                    "route_id": "$id",
                    "target_date": "$target_date",
                },
                "any_on_route": {
                    "$max": {
                        "$cond": [
                            {"$eq": ["$dispatches.status_id", 1]},
                            1,
                            0,
                        ]
                    }
                },
            }
        },
        {
            # Second group: per day, count open/closed routes
            "$group": {
                "_id": "$_id.target_date",
                "open_routes": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$any_on_route", 1]},
                            1,
                            0,
                        ]
                    }
                },
                "closed_routes": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$any_on_route", 0]},
                            1,
                            0,
                        ]
                    }
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    route_rows = list(coll.aggregate(route_pipeline))

    if route_rows:
        df_routes = pd.DataFrame(route_rows).rename(columns={"_id": "target_date"})
        # Merge route-level info into dispatch-level df
        df = df.merge(df_routes, on="target_date", how="left")
    else:
        # No route rows for same filter -> set open/closed to 0
        df["open_routes"] = 0
        df["closed_routes"] = 0

    # Replace NaN open/closed with 0 just in case
    df[["open_routes", "closed_routes"]] = df[["open_routes", "closed_routes"]].fillna(0).astype(int)

    if save_csv:
        # timezone-aware UTC timestamp
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
        fname = f"route_completeness_by_due_date_{range_label}_{ts}.csv"
        out_path = REPORTS_DIR / fname
        df.to_csv(out_path, index=False)
        print(f"Saved route completeness (by max_delivery_time date) to {out_path}")

    return df


if __name__ == "__main__":
    # Example 1: no args -> from today onwards
    df = generate_daily_completeness_report("2025-11-01", "2025-11-28", save_csv=True)
    print(df)
