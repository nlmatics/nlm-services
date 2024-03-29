class DocumentInfo:
    def __init__(
        self,
        id: str = None,
        name: str = None,
        doc_location: str = None,
        update: bool = False,
        file_size: int = None,
        checksum: str = None,
        mime_type: str = None,
        **kwargs,
    ):
        self._id = id
        self._name = name
        self._doc_location = doc_location
        self._file_size = file_size
        self._checksum = checksum
        self._mime_type = mime_type
        self._update = update

    @property
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def doc_location(self) -> str:
        return self._doc_location

    @doc_location.setter
    def doc_location(self, loc):
        self._doc_location = loc

    @property
    def file_size(self) -> str:
        return self._file_size

    @file_size.setter
    def file_size(self, fsize):
        self._file_size = fsize

    @property
    def checksum(self) -> str:
        return self._checksum

    @checksum.setter
    def checksum(self, cksum):
        self._checksum = cksum

    @property
    def mime_type(self) -> str:
        return self._mime_type

    @mime_type.setter
    def mime_type(self, mtype):
        self._mime_type = mtype

    @property
    def update(self) -> bool:
        return self._update

    @update.setter
    def update(self, update):
        self.update = update


class Message:
    def __init__(self, data: str = None):
        self.data = data.encode("utf-8")

    def __str__(self):
        return str(self.data)

    def ack(self):
        pass

    def nack(self):
        pass
