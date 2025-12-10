import os
import logging
from typing import Any, Dict, List
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from one directory above the current directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# Logger configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job.find_duplicated_dispatches")

MONGO_URI = os.getenv("MONGO_URI")
DATABASE = os.getenv("DATABASE", "FRONTERA")
DISPATCHES_COLLECTION = os.getenv("DISPATCHES_COLLECTION", "DISPATCHES")


def run() -> None:
    """
    Find all identifiers that appear in 2 or more dispatch documents.
    A "duplicate" is defined as: two or more docs whose `identifier`
    (cast to string) is the same.
    """
    logger.info(
        "Starting find_duplicated_dispatches on %s.%s",
        DATABASE,
        DISPATCHES_COLLECTION,
    )

    client = MongoClient(MONGO_URI)
    disp_col = client[DATABASE][DISPATCHES_COLLECTION]

    try:
        # Aggregation:
        # 1) Consider only docs that have identifier
        # 2) Group by identifier as STRING (so 123 and "123" collapse)
        # 3) Keep count and list of _id
        # 4) Keep only those with count > 1
        pipeline: List[Dict[str, Any]] = [
            {
                "$match": {
                    "identifier": {"$exists": True, "$ne": None}
                }
            },
            {
                "$group": {
                    "_id": {"$toString": "$identifier"},
                    "count": {"$sum": 1},
                    "doc_ids": {"$push": "$_id"},
                }
            },
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            },
            {
                "$sort": {
                    "count": -1
                }
            },
        ]

        dup_groups = list(disp_col.aggregate(pipeline, allowDiskUse=True))

        if not dup_groups:
            logger.info("No duplicated identifiers found.")
            return

        total_dup_identifiers = len(dup_groups)
        total_dup_docs = sum(g["count"] for g in dup_groups)

        logger.info(
            "Found %d identifiers with duplicates, involving %d documents in total.",
            total_dup_identifiers,
            total_dup_docs,
        )

        # Print a sample of the duplicates to stdout
        MAX_PRINT = 50  # adjust if you want more/less detail
        logger.info(
            "Printing up to %d duplicated identifiers with their document IDs:",
            MAX_PRINT,
        )

        for idx, group in enumerate(dup_groups):
            if idx >= MAX_PRINT:
                logger.info("... (%d more duplicated identifiers omitted)", total_dup_identifiers - MAX_PRINT)
                break

            identifier_str = group["_id"]
            count = group["count"]
            doc_ids = group["doc_ids"]

            print("------------------------------------------------------------")
            print(f"Identifier: {identifier_str!r}  |  Duplicates: {count}")
            print("Document _ids:")
            for _id in doc_ids:
                print(f"  - {str(_id)}")

    finally:
        client.close()
        logger.info("find_duplicated_dispatches finished.")


if __name__ == "__main__":
    run()
