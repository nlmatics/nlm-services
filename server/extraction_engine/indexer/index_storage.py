class IndexStorage:
    def _not_impl(self):
        raise NotImplementedError("subclasses must implement this method")

    def load_file(self, identifier):
        self._not_impl()

    def save_file(self, identifier, file_to_save):
        self._not_impl()
