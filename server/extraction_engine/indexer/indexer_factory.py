class IndexerFactory:
    __instances = dict()

    supported_implementations = ['pickle']

    @classmethod
    def instance(cls, impl, index_storage):
        if impl not in IndexerFactory.supported_implementations:
            raise NotImplementedError(
                f'unknown impl {impl}, supported values: {IndexerFactory.supported_implementations}')
        if impl.lower() not in IndexerFactory.__instances:
            if impl.lower() == 'pickle':
                from server.extraction_engine.indexer.pickle_indexer import PickleIndexer
                IndexerFactory.__instances[impl.lower()] = PickleIndexer(index_storage)
            else:
                raise ValueError(
                    f'invalid indexer implementation {impl}, supported values: {IndexerFactory.supported_implementations}')
        return IndexerFactory.__instances[impl.lower()]
