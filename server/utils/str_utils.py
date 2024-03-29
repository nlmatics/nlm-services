import secrets
import uuid
import zlib
from datetime import datetime
from datetime import timedelta

from pytz import timezone

t_zone = timezone("UTC")


def get_unique_string(prefix=None):
    """Generates a unique string that can be used as identifier
    :param prefix: string to use as prefix
    :return: unique string
    """
    # generate random number from 100 to 999 inclusive securely
    r_int = secrets.randbelow(900) + 100
    t_stamp = datetime.now().strftime("%Y%m%d.%H%M%S")
    uid = uuid.uuid1().__str__().split("-")[3]
    if prefix:
        return "%s-%s-%d-%s" % (prefix, t_stamp, r_int, uid)
    else:
        return "%s-%d-%s" % (t_stamp, r_int, uid)


def generate_user_id(email):
    """Generates a unique id for a user based on the provided email.
    For now, this is simply a crc32 (in hex format without the prefix '0x') of the email id.
    :param email:
    :return:
    """
    if email is None:
        raise Exception("email cannot be null")
    return _checksum_helper(email, str(secrets.randbelow(900) + 100))


def generate_workspace_id(user_id, name):
    return _checksum_helper(user_id, name, str(secrets.randbelow(900) + 100))


def generate_field_id(name):
    return _checksum_helper(name, str(secrets.randbelow(900) + 100))


def generate_filter_id(name):
    return _checksum_helper(name, str(secrets.randbelow(900) + 100))


def generate_search_history_id(name):
    return _checksum_helper(name, str(secrets.randbelow(900) + 100))


def generate_unique_document_id(filename, checksum, filesize):
    return _checksum_helper(
        get_unique_string(filename),
        checksum,
        str(filesize),
        str(secrets.randbelow(900) + 100),
    )


def generate_folder_id(folder_name, workspace_id, parent_folder):
    return _checksum_helper(
        workspace_id,
        parent_folder,
        folder_name,
        str(secrets.randbelow(900) + 100),
    )


def _checksum_helper(*args):
    interim = None
    for b in args:
        if b is not None:
            interim = (
                zlib.crc32(bytes(b, "utf-8"), interim)
                if interim is not None
                else zlib.crc32(bytes(b, "utf-8"))
            ) & 0xFFFFFFFF
    return hex(interim)[2:]


def generate_unique_fieldbundle_id(user_id, workspace_id, bundle_name):
    return _checksum_helper(
        user_id,
        workspace_id,
        bundle_name,
        str(secrets.randbelow(900) + 100),
    )


def generate_template_id(field_id, text):
    return _checksum_helper(
        field_id,
        text,
        str(uuid.uuid1()).split("-")[3],
    )  # add randomness as template text may be same


def generate_doc_id(filename):
    return get_unique_string(filename)


def generate_training_sample_id(doc_id, workspace_id):
    return (
        workspace_id
        + "_"
        + doc_id
        + "_"
        + _checksum_helper(doc_id, workspace_id, str(uuid.uuid1()).split("-")[3])
    )


def timestamp_as_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def timestamp_as_utc_str(num_days=0):
    return (datetime.now(t_zone) + timedelta(num_days)).strftime("%Y-%m-%d")
