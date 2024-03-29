# # import server.config as cfg
# class IndexStorageFactory:
#     __instances = dict()
#     supported_implementations = ['cloud', 'local']
#     @classmethod
#     def instance(cls, impl):
#         if impl not in IndexStorageFactory.supported_implementations:
#             raise ValueError(
#                 f'unknown index storage implementation {impl}, supported values {IndexStorageFactory.supported_implementations}')
#         if impl not in IndexStorageFactory.__instances:
#             if impl == 'cloud':
#                 from nlm_utils.storage import
#                 from server.extraction_engine.indexer.gcp.gcp_index_storage import GCPIndexStorage
#                 index_bucket_name = cfg.get_config('INDEX_BUCKET_NAME', 'doc-store-dev')
#                 IndexStorageFactory.__instances[impl] = GCPIndexStorage(index_bucket_name)
#             elif impl == 'local':
#                 # needs to be implemented
#                 from server.extraction_engine.indexer.local.local_index_storage import LocalIndexStorage
#                 index_location = cfg.get_config('INDEX_BUCKET_NAME', 'doc-store-dev')
#                 IndexStorageFactory.__instances[impl] = LocalIndexStorage(index_location)
#         return IndexStorageFactory.__instances[impl]
