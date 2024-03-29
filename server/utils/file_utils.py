import hashlib
import json
import logging

import dicttoxml

dicttoxml.LOG.setLevel(logging.ERROR)


def get_file_sha256(filepath):
    """Calculates the SHA256 hash of the file contents
    :param filepath:
    :return:
    """
    sha2 = hashlib.sha256()
    with open(filepath, "rb") as fh:
        buf_size = 131072  # 128kb
        while True:
            data = fh.read(buf_size)
            if not data:
                break
            sha2.update(data)
    return sha2.hexdigest()


def convert_json_to_xml(file_name):
    """
    Convert the JSON to XML and rewrite the data to XML in the same file
    :param file_name:
    :return: Void
    """
    with open(file_name, "r+") as f:
        data = f.read()
        xml = dicttoxml.dicttoxml(json.loads(data))
        xml_decode = xml.decode()
        f.seek(0)
        f.write(xml_decode)
        f.truncate()
