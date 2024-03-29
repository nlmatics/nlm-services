import abc


class Indexer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def load_enc_from_cache(self, doc_id, level):
        raise NotImplementedError

    @abc.abstractmethod
    def encode(self, fn_sents):
        raise NotImplementedError

    @abc.abstractmethod
    def encode_and_cache(self, doc_id, level, block_texts, rewrite_cache=False):
        raise NotImplementedError

    @abc.abstractmethod
    def load_pre_processed_document(self, doc_id, level="sents"):
        raise NotImplementedError


class CacheAccess(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def load_result(self, template_filename, source_filename):
        raise NotImplementedError

    @abc.abstractmethod
    def store_result(self, template_filename, source_filename, dfr):
        raise NotImplementedError


class Loader(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def read_fieldbundle(self, bundle_location: str):
        raise NotImplementedError

    @abc.abstractmethod
    def get_blocks(self, doc_id: str):
        raise NotImplementedError

    @abc.abstractmethod
    def is_folder(self, path: str):
        raise NotImplementedError

    @abc.abstractmethod
    def get_contents_by_path(self, path: str):
        raise NotImplementedError

    @abc.abstractmethod
    def get_field_overrides(self, field_id: str):
        raise NotImplementedError


class ServiceFacade:
    def __init__(self, indexer: Indexer, loader: Loader, cache: CacheAccess):
        self.indexer = indexer
        self.loader = loader
        self.cache = cache
