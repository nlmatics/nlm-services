import logging
import os
import pickle
import tempfile
import traceback

from nlm_utils.cache import Cache

from .services import CacheAccess

# import server.config as cfg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


cache = Cache(
    "RedisAgent",
    ttl=60 * 60 * 24 * 7,  # cache file for 1 week
    host=os.getenv("REDIS_HOST", "localhost"),
    port=os.getenv("REDIS_PORT", "6379"),
    prefix="pickle_cache_access_cacher",
)


class PickleCacheAccess(CacheAccess):
    def __init__(self, index_storage):
        self._index_storage = index_storage

    def _make_result_identifier(self, field_bundle_id, doc_id):
        return doc_id + "_" + field_bundle_id

    def load_result(self, field_bundle_id, doc_id):
        result_identifier = self._make_result_identifier(field_bundle_id, doc_id)

        def load_from_storage(result_identifier):
            logger.info(f"Loading result for identifier {result_identifier}")
            data = None
            index_path = None
            try:
                index_path = self._index_storage.load_file(result_identifier)
                if index_path and os.path.exists(index_path):
                    with open(index_path, "rb") as f:
                        data = pickle.load(f)
            except Exception:
                logger.error(f"error loading result {traceback.format_exc()}")
            finally:
                if index_path and os.path.exists(index_path):
                    os.unlink(index_path)
            return data

        return load_from_storage(result_identifier)

    def store_result(self, field_bundle_id, doc_id, result):
        result_identifier = self._make_result_identifier(field_bundle_id, doc_id)
        tmp_index_path = tempfile.mkstemp(result_identifier + ".pickle")[1]
        logger.info(f"Saving result of {result_identifier} to file {tmp_index_path}")
        try:
            with open(tmp_index_path, "wb") as fh:
                pickle.dump(result, fh)
            # upload cache to storage
            self._index_storage.save_file(result_identifier, tmp_index_path)
            # update redis cache
            cache.write_cache(result, result_identifier)
        finally:
            if tmp_index_path and os.path.exists(tmp_index_path):
                os.unlink(tmp_index_path)
