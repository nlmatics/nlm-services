import os

from nlm_utils.storage import file_storage

from server.extraction_engine.indexer.indexer_factory import IndexerFactory

# import server.config as cfg

# from server.extraction_engine.indexer.index_storage_factory import IndexStorageFactory

index_storage = file_storage
indexer = IndexerFactory.instance(os.getenv("INDEXER_IMPL", "pickle"), file_storage)
