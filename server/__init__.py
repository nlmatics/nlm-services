import os

from flask import jsonify
from flask import make_response

# # import server.config as cfg
# from server.storage.nosqldb_factory import NoSqlDbFactory
# from server.storage.objectstore_factory import ObjectStoreFactory

required_env_vars = {
    "local": [],
    "cloud": [],
}


def err_response(msg, rc=500, **kwargs):
    resp_dict = {
        "status": "fail",
        "reason": msg,
    }
    if kwargs:
        resp_dict.update(kwargs)
    return make_response(jsonify(resp_dict), rc)


def unauthorized_response(msg="unauthorized"):
    return err_response(msg, 401)
