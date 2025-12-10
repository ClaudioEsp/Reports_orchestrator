import os
import logging
from typing import Any, Dict, Optional
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv

# Load environment variables from one directory above the current directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# Logger configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job.normalize_substatus_code_to_int")

MONGO_URI = os.getenv("MONGO_URI")
DATABASE = os.getenv("DATABASE", "FRONTERA")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "DISPATCHES")


def _normalize_string_code(value: Any) -> Optional[int]:
  """
  If value is a string representing an integer (e.g. "1", "001"),
  return its int() equivalent. Otherwise return None.
  """
  if not isinstance(value, str):
      return None

  s = value.strip()
  if s == "":
      return None

  # Only accept pure digits (no decimal points, no signs).
  if not s.isdigit():
      return None

  try:
      return int(s)
  except ValueError:
      return None


def run() -> None:
  """
  Walk all documents in DISPATCHES and convert substatus_code
  from string -> int where possible.
  """
  logger.info(
      "Starting normalize_substatus_code_to_int on %s.%s",
      DATABASE,
      DISPATCHES_COLLECTION,
  )

  client = MongoClient(MONGO_URI)
  disp_col = client[DATABASE][DISPATCHES_COLLECTION]

  BATCH_SIZE = 1000
  last_id: Optional[ObjectId] = None

  processed = 0
  converted = 0
  skipped_non_numeric = 0

  try:
      while True:
          # Only documents where substatus_code is a string
          query: Dict[str, Any] = {"substatus_code": {"$type": "string"}}
          if last_id is not None:
              query["_id"] = {"$gt": last_id}

          cursor = (
              disp_col.find(query, projection={"_id": True, "substatus_code": True})
              .sort("_id", 1)
              .limit(BATCH_SIZE)
          )

          docs = list(cursor)
          if not docs:
              break

          for doc in docs:
              processed += 1
              last_id = doc["_id"]
              raw = doc.get("substatus_code")

              new_val = _normalize_string_code(raw)

              if new_val is None:
                  # string but not numeric -> leave as-is, log a few
                  skipped_non_numeric += 1
                  if skipped_non_numeric <= 10:
                      logger.warning(
                          "Skipping non-numeric substatus_code=%r (doc _id=%s)",
                          raw,
                          doc["_id"],
                      )
                  continue

              # Update document with integer version
              disp_col.update_one(
                  {"_id": doc["_id"]},
                  {"$set": {"substatus_code": new_val}},
              )
              converted += 1

          logger.info(
              "Processed so far: %d docs with string substatus_code, converted=%d, skipped_non_numeric=%d (last_id=%s)",
              processed,
              converted,
              skipped_non_numeric,
              last_id,
          )

  finally:
      client.close()

  logger.info(
      "normalize_substatus_code_to_int finished. "
      "Processed=%d, Converted_to_int=%d, Skipped_non_numeric_strings=%d",
      processed,
      converted,
      skipped_non_numeric,
  )


if __name__ == "__main__":
  run()
