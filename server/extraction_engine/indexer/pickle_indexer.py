import logging
import os

from nlm_utils.cache import Cache

from ..services import Indexer

# import server.config as cfg


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


try:
    from nlm_ingestor.ingestor_utils import encoder_utils as eu
except ImportError:
    logger.error(
        "nlm-services-v2 is running without ingestor. Functions related to extraction are limited.",
        exc_info=True,
    )


pre_processed_cacher = Cache(
    "RedisAgent",
    ttl=60 * 60 * 24 * 7,  # cache file for 1 week
    host=os.getenv("REDIS_HOST", "localhost"),
    port=os.getenv("REDIS_PORT", "6379"),
    prefix="pre_processed_cacher",
)


encoder_cacher = Cache(
    "RedisAgent",
    ttl=60 * 60 * 24 * 7,  # cache file for 1 week
    host=os.getenv("REDIS_HOST", "localhost"),
    port=os.getenv("REDIS_PORT", "6379"),
    prefix="encoder_cacher",
)


class PickleIndexer(Indexer):
    def __init__(self, index_storage):
        self._index_storage = index_storage

    @encoder_cacher
    def load_enc_from_cache(self, doc_id, level):
        return eu.load_enc_from_store(doc_id, level)

    def encode_and_cache(self, doc_id, level, block_texts, rewrite_cache=False):
        return eu.encode_and_cache(doc_id, level, block_texts, rewrite_cache)

    def encode(self, fn_sents, use_cache=False):
        return eu.encode(fn_sents, use_cache)

    @pre_processed_cacher
    def load_pre_processed_document(self, doc_id, level):
        return eu.load_pre_processed_document(doc_id, level)
