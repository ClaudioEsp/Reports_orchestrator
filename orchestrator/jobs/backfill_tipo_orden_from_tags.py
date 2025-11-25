# orchestrator/jobs/backfill_tipo_orden_from_tags.py

import os
import logging
from typing import Any, Dict, List, Optional

from pymongo import MongoClient

logger = logging.getLogger("job.backfill_tipo_orden_from_tags")
logging.basicConfig(level=logging.INFO)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DISPATCHTRACK_DB = os.getenv("DISPATCHTRACK_DB", "dispatchtrack")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "dispatches")


def _get_tag_value_from_dispatch(doc: Dict[str, Any], tag_name: str) -> Optional[str]:
    """
    Busca un tag por nombre dentro de dispatch_raw.tags
    y devuelve su 'value' (o None si no existe).
    """
    dispatch_raw = doc.get("dispatch_raw", {})
    tags: List[Dict[str, Any]] = dispatch_raw.get("tags", [])
    if not isinstance(tags, list):
        return None

    for t in tags:
        if isinstance(t, dict) and t.get("name") == tag_name:
            return t.get("value")
    return None


def run():
    """
    Rellena el campo plano 'tipo_orden' en la colección 'dispatches'
    leyendo el tag 'TIPO_ORDEN' desde dispatch_raw.tags.

    Solo toca documentos donde:
      - 'tipo_orden' no existe, es null o cadena vacía.
    """
    client = MongoClient(MONGO_URI)
    col = client[DISPATCHTRACK_DB][DISPATCHES_COLLECTION]

    logger.info(
        "Starting backfill_tipo_orden_from_tags on %s.%s",
        DISPATCHTRACK_DB,
        DISPATCHES_COLLECTION,
    )

    query = {
        "$or": [
            {"tipo_orden": {"$exists": False}},
            {"tipo_orden": None},
            {"tipo_orden": ""},
        ]
    }

    cursor = col.find(
        query,
        projection={"dispatch_raw.tags": 1},  # solo necesitamos los tags
    )

    total = 0
    updated = 0
    missing = 0

    for doc in cursor:
        total += 1
        tipo_orden_value = _get_tag_value_from_dispatch(doc, "TIPO_ORDEN")

        if not tipo_orden_value:
            missing += 1
            continue

        col.update_one(
            {"_id": doc["_id"]},
            {"$set": {"tipo_orden": tipo_orden_value}},
        )
        updated += 1

    logger.info(
        "Finished backfill_tipo_orden_from_tags. scanned=%d updated=%d missing_tag=%d",
        total,
        updated,
        missing,
    )


if __name__ == "__main__":
    run()
