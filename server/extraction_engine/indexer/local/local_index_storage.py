import logging
import os
import shutil
import tempfile

from server.extraction_engine.indexer.index_storage import IndexStorage

# import server.config as cfg


class LocalIndexStorage(IndexStorage):
    def __init__(self, index_location):
        self._index_location = index_location
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        pass

    def load_file(self, identifier):
        blob = self._resolve_blob(identifier)
        if blob:
            return blob
        else:
            return None

    def save_file(self, identifier, file_to_save):
        file_loc = os.path.join(self._index_location, "indexes", identifier)
        if not os.path.exists(os.path.dirname(file_loc)):
            os.makedirs(os.path.dirname(file_loc))
        shutil.copy(file_to_save, file_loc)

    def _resolve_blob(self, identifier):
        blob = os.path.join(self._index_location, "indexes", identifier)
        if os.path.exists(blob):
            tempfile_handler, tmp_file = tempfile.mkstemp()
            os.close(tempfile_handler)
            shutil.copy(blob, tmp_file)
            return tmp_file
        else:
            self.logger.error(f"{blob} does not exist")
            return None
