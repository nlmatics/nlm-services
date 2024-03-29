import logging
import traceback

import connexion
from flask import jsonify
from flask import make_response

from server import err_response
from server import unauthorized_response
from server.models.id_with_message import IdWithMessage
from server.models.user_access_control import UserAccessControl  # noqa: E501
from server.storage import nosql_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def update_user_acl(
    user,
    token_info,
    body=None,
):  # noqa: E501
    """Creates / Updates a user access control list.

     # noqa: E501

    :param user:
    :param token_info:
    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    try:
        if connexion.request.is_json:
            user_acl = UserAccessControl.from_dict(
                connexion.request.get_json(),
            )  # noqa: E501
            user_obj = token_info.get("user_obj", None)
            if not user_obj:
                return unauthorized_response()

            if not user_obj.get("is_admin", False):
                return make_response(
                    jsonify(
                        {
                            "status": "fail",
                            "reason": "Only Admins can create users",
                        },
                    ),
                    403,
                )

            if not user_acl.user_id and not user_acl.email_id:
                err_str = "Either user_id or email_id must be provided to update User ACL information."
                logger.info(err_str)
                return err_response(err_str, 422)

            # If email_id is not provided, add it the acl object.
            if not (user_acl.user_id and user_acl.email_id):
                acl_user_obj = None
                try:
                    acl_user_obj = nosql_db.get_user_obj(
                        user_acl.user_id,
                        user_acl.email_id,
                    )
                except Exception as e:
                    logger.error(
                        f"user not found {str(e)}, err: {traceback.format_exc()}",
                    )

                if acl_user_obj is None and user_acl.user_id:
                    return make_response(
                        jsonify({"status": "fail", "reason": "user not found"}),
                        404,
                    )
                if acl_user_obj:
                    user_acl.user_id = acl_user_obj.id
                    user_acl.email_id = acl_user_obj.email_id

            # Add email id to the access control, if
            # (1) not already added, and (2) if there is an access control list
            if (
                user_acl.access_control_list
                and user_acl.email_id not in user_acl.access_control_list
            ):
                user_acl.access_control_list.append(user_acl.email_id)

            ret_id = nosql_db.update_user_acl(user_acl)
            return make_response(
                jsonify(IdWithMessage(ret_id, "Access Control updated successfully")),
                200,
            )
        else:
            status, rc, msg = "fail", 422, "invalid json"
    except Exception as e:
        logger.error(f"error while updating ACL, err: {traceback.format_exc()}")
        status, rc, msg = "fail", 500, str(e)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_user_acl(
    user,
    token_info,
    user_id=None,
    email_id=None,
):
    """
    Returns the Access Control List for the user..
    """
    user_obj = token_info.get("user_obj", None)
    if not user_obj:
        return unauthorized_response()

    if not user_id and not email_id:
        err_str = (
            "Either user_id or email_id must be provided to retrieve ACL information."
        )
        logger.info(err_str)
        return err_response(err_str, 422)

    user_acl = nosql_db.get_user_acl(
        user_id=user_id,
        email_id=email_id,
    )

    try:
        return make_response(jsonify(user_acl), 200)
    except Exception as e:
        logger.error(
            f"error retrieving access control list, err: {traceback.format_exc()}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)
