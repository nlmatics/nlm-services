import logging

import connexion
import jose
from flask import jsonify
from flask import make_response

from server import err_response
from server.auth import auth_provider
from server.models.developer_api_key import DeveloperApiKey  # noqa: E501
from server.storage import nosql_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_dev_api_key(user, token_info):  # noqa: E501
    """create_dev_api_key
    Creates / re-generates developer API Key for the account.
    :rtype: str
    """
    try:
        if token_info["user_obj"].get("access_type", "").lower() == "viewer":
            # User with viewer access_type cannot create a developer account.
            err_str = f"{user} not authorized to create developer account."
            logger.error(err_str)
            return err_response(err_str, 403)
        app_id, developer_key = auth_provider.generate_developer_key(user)
        updated_user_profile = token_info["user_obj"]
        if (
            not token_info["user_obj"].get(
                "has_developer_account",
                False,
            )
            or not token_info["user_obj"].get("developer_app_id", None)
        ):
            updated_user_profile = nosql_db.update_user(
                {
                    "has_developer_account": True,
                    "developer_app_id": app_id,
                },
                user_id=token_info["user_obj"]["id"],
            )
        return make_response(
            jsonify(
                {
                    "app_id": app_id,
                    "developer_key": developer_key,
                    "user_profile": updated_user_profile,
                },
            ),
            200,
        )
    except jose.JOSEError as e:  # the catch-all of Jose
        logger.error(f"Error during generating developer key (JWT): {e}")
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 403)
    except Exception as e:
        logger.error(f"Error generating developer key: {e}")
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 403)


def generate_developer_access_token():  # noqa: E501
    """generate_developer_access_token

     Generates Access token using the developer api key provided.

    :rtype: AccessToken
    """
    if connexion.request.is_json:
        dev_key = DeveloperApiKey.from_dict(connexion.request.get_json())  # noqa: E501
        try:
            access_token = auth_provider.generate_developer_access_token(
                dev_key.app_id,
                dev_key.api_key,
            )
            return make_response(
                access_token,
                200,
            )
        except jose.JOSEError as e:  # the catch-all of Jose
            logger.error(f"Error during generating developer key (JWT): {e}")
            status, rc, msg = "fail", 403, str(e)
        except Exception as e:
            logger.error(f"Error generating developer key: {e}")
            status, rc, msg = "fail", 403, str(e)
    else:
        status, rc, msg = "fail", 422, "invalid json"
    return make_response(jsonify({"status": status, "reason": msg}), rc)
