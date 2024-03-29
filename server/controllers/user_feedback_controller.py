import logging

import connexion
from flask import jsonify
from flask import make_response

from server import unauthorized_response
from server.models.id_with_message import IdWithMessage
from server.models.user_feedback import UserFeedback  # noqa: E501
from server.storage import nosql_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def add_user_feedback(
    user,
    token_info,
    body=None,
):  # noqa: E501
    """Creates / Updates a user feedback.

     # noqa: E501

    :param user:
    :param token_info:
    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        user_feedback = UserFeedback.from_dict(
            connexion.request.get_json(),
        )  # noqa: E501
        user_obj = token_info.get("user_obj", None)
        if not user_obj:
            return unauthorized_response()
        user_feedback.user_id = user_obj["id"]
        ret_id = nosql_db.create_user_feedback(user_feedback)
        return make_response(
            jsonify(IdWithMessage(ret_id, "Feedback registered successfully")),
            200,
        )
    else:
        status, rc, msg = "fail", 422, "invalid json"
    return make_response(jsonify({"status": status, "reason": msg}), rc)
