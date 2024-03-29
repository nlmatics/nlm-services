import logging
import os

from server.storage.local.mongo_db import MongoDB

# import server.config as cfg

platform = os.getenv("PLATFORM", "cloud")
logging.info(f"Configured PLATFORM: {platform}")

nosql_db = MongoDB()
