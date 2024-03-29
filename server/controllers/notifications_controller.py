import logging

import connexion
from flask import jsonify
from flask import make_response

from server import unauthorized_response
from server.storage import nosql_db
from server.utils.notification_utils import retrieve_notifications

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_notifications(
    user,
    token_info,
    include_read=False,
):  # noqa: E501
    """Returns unread notifications for the user

     # noqa: E501


    :rtype: List[Notifications]
    """
    try:
        user_obj = token_info.get("user_obj", None) if token_info else None
        if not user_obj:
            return unauthorized_response()
        notifications = retrieve_notifications(user_obj, include_read)
        return make_response(
            jsonify(notifications),
            200,
        )
    except Exception as e:
        logger.error(
            f"Error retrieving notifications for  {user}, err: {str(e)}",
            exc_info=True,
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def update_is_read(
    user,
    token_info,
    body=None,
):  # noqa: E501
    """Update is_read parameter of the list of notification ids

     # noqa: E501

    :param user:
    :param token_info:
    :param body:
    :type body: List[]

    :rtype: str
    """
    try:
        nosql_db.update_unread_notifications(connexion.request.get_json())
        # After updating, we need to set the flag to false.
        nosql_db.update_user(
            {"has_notifications": False},
            user_id=token_info["user_obj"]["id"],
        )
        return make_response(
            jsonify(
                {
                    "status": "success",
                    "message": "Successfully updated",
                },
            ),
            200,
        )
    except Exception as e:
        logger.error(
            f"Error retrieving notifications for  {user}, err: {str(e)}",
            exc_info=True,
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)
