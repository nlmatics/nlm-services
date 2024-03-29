import datetime
import logging
import os
import traceback

import connexion
from flask import jsonify
from flask import make_response
from pytz import timezone

from server.auth import auth_provider
from server.models import id_with_message
from server.models import Workspace
from server.models.field_bundle import FieldBundle
from server.models.prefered_workspace import PreferedWorkspace  # noqa: E501
from server.models.user import User  # noqa: E501
from server.storage import nosql_db
from server.utils import metric_utils
from server.utils import str_utils

# import server.config as cfg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
DEFAULT_SUBSCRIPTION_PLAN = os.getenv("DEFAULT_SUBSCRIPTION_PLAN", "BASIC")
# ADMIN > EDITOR > VIEWER
DEFAULT_USER_ACCESS_TYPE = os.getenv("DEFAULT_USER_ACCESS_TYPE", "VIEWER")
DEFAULT_WORKSPACE_DOMAIN = os.getenv("DEFAULT_WORKSPACE_DOMAIN", "general")


def create_user(
    token_info,
    body=None,
):  # noqa: E501
    """Creates a new user

     # noqa: E501

    :param token_info:
    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    user_json = token_info["user_obj"]
    if not user_json.get("is_admin", False):
        return make_response(
            jsonify(
                {
                    "status": "fail",
                    "reason": "Only Admins can create users",
                },
            ),
            403,
        )
    if connexion.request.is_json:
        try:
            user = User.from_dict(connexion.request.get_json())  # noqa: E501
            if user.id:
                logger.info(
                    "ignoring provided user id for new user creation, auto-generating a new one",
                )
            try:
                user_exists = (
                    get_user_by_email(token_info, user.email_id).status_code == 200
                )
            except Exception:
                user_exists = None

            if user_exists:
                return make_response(
                    jsonify(
                        id_with_message.IdWithMessage(
                            id="123",
                            message=f"user with {user.email_id} already exists",
                        ),
                    ),
                    500,
                )
            # create user
            user.id = str_utils.generate_user_id(user.email_id)
            if user.created_on is None:
                user.created_on = str_utils.timestamp_as_str()
            user.active = True
            # Add default 30 days expiry_time
            tz = timezone("UTC")
            user.expiry_time = (
                datetime.datetime.now(tz) + datetime.timedelta(days=30)
            ).strftime("%Y-%m-%d %H:%M:%S %Z%z")
            # Add DEFAULT_SUBSCRIPTION_PLAN if none provided.
            if not user.subscription_plan:
                user.subscription_plan = DEFAULT_SUBSCRIPTION_PLAN
            if not user.access_type:
                user.access_type = DEFAULT_USER_ACCESS_TYPE
            user_id = nosql_db.create_user(user)
            # Update user ACL if present with the email id.
            nosql_db.update_user_id_for_acl(user.id, user.email_id)

            # Create default usage metrics for the user.
            metric_utils.add_default_usage_metrics(user.id)

            # create default workspace for user
            default_workspace = Workspace(
                id=str_utils.generate_workspace_id(user.id, "default"),
                name="default",
                user_id=user.id,
                active=True,
                created_on=str_utils.timestamp_as_str(),
            )
            default_workspace.settings["domain"] = DEFAULT_WORKSPACE_DOMAIN
            default_workspace.settings["search_settings"] = {
                "table_search": False,
            }
            default_workspace.statistics = {
                "document": {
                    "total": 0,
                },
                "field_bundle": {
                    "total": 0,
                },
                "fields": {
                    "total": 0,
                },
            }
            nosql_db.create_workspace(default_workspace)
            default_ws_id = default_workspace.id

            # create preferred workspace
            prefered_workspace = PreferedWorkspace(
                default_workspace_id=default_ws_id,
                user_id=user_id,
            )
            nosql_db.create_prefered_workspace(prefered_workspace)

            # create default field bundle for default workspace
            bundle = {
                "id": str_utils.generate_unique_fieldbundle_id(
                    default_workspace.user_id,
                    default_workspace.id,
                    "default",
                ),
                "cachedFile": None,
                "userId": default_workspace.user_id,
                "workspaceId": default_workspace.id,
                "createdOn": str_utils.timestamp_as_str(),
                "active": True,
                "fieldIds": [],
                "tags": None,
                "bundleName": "Default Field Set",
                "parentBundleId": None,
                "bundleType": "DEFAULT",
            }
            nosql_db.create_field_bundle(FieldBundle.from_dict(bundle))
            logger.info(
                f"default workspace and prefered workspace created for user {user.email_id}",
            )
            return make_response(
                jsonify(
                    id_with_message.IdWithMessage(id=user_id, message="user created"),
                ),
                200,
            )
        except Exception as e:
            logger.error(f"unable to create a new user, err: {traceback.format_exc()}")
            status, rc, err_msg = "fail", 500, str(e)
    else:
        status, rc, err_msg = "fail", 422, "invalid json"
    return make_response(jsonify({"status": status, "reason": err_msg}), rc)


def delete_user_by_id(
    token_info,
    user_id,
):  # noqa: E501
    """Delete user with given id

     # noqa: E501

    :param token_info:
    :param user_id:
    :type user_id: str

    :rtype: IdWithMessage
    """
    user_json = token_info["user_obj"]
    if not (user_json.get("is_admin", False) or user_json["id"] == user_id):
        return make_response(
            jsonify(
                {
                    "status": "fail",
                    "reason": "Only Admins or the user himself can delete this account.",
                },
            ),
            403,
        )
    try:
        deleted_user_id = nosql_db.delete_user(user_id)
        if deleted_user_id is None:
            status, msg, rc = "ok", "user not found", 404
            return make_response(jsonify({"status": status, "reason": msg}), rc)
        else:
            return make_response(
                jsonify(id_with_message.IdWithMessage(deleted_user_id, "user deleted")),
            )
    except Exception as e:
        logger.error(
            f"error deleting user with userid: {user_id}, err: {traceback.format_exc()}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def get_all_users(user):  # noqa: E501
    """Returns list of all users

     # noqa: E501


    :rtype: List[User]
    """
    raise NotImplementedError()
    # try:
    #     logger.info(f"Authenticated user: {user}")
    #     return make_response(jsonify(nosql_db.get_users()), 200)
    # except Exception as e:
    #     logger.error(f"error retrieving users, err: {traceback.format_exc()}")
    #     return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def get_user_by_email(
    token_info,
    email_id,
    user=None,
):
    """Returns the information about the user with emailid"""
    try:
        user_json = token_info["user_obj"]
        if not (user_json.get("is_admin", False) or user_json["email_id"] == email_id):
            return make_response(
                jsonify(
                    {
                        "status": "fail",
                        "reason": "Only Admins or the user himself can access this account.",
                    },
                ),
                403,
            )
        usr = nosql_db.get_user_by_email(email_id, expand=True)
        if not usr:
            logger.error(f"user with email id {email_id} not found")
            status, rc, msg = "fail", 404, "invalid user"
        else:
            if usr.email_id != user:
                status, rc, msg = "fail", 404, "unauthorized"
                logger.error(f"user {user} not authorized to request information")
            else:
                return make_response(jsonify(usr), 200)
    except Exception as e:
        logger.error(
            f"error retrieving user info for user {user}, err: {traceback.format_exc()}",
        )
        status, rc, msg = "fail", 500, str(e)
    finally:
        pass
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_user_emails_by_domain(
    token_info,
):
    """Returns the user emails with the same domain as the requested user."""
    try:
        user_json = token_info["user_obj"]
        domain = user_json["email_id"].split("@")[1]
        return make_response(
            jsonify(
                nosql_db.get_distinct_active_user_profiles(
                    domain_name=domain,
                    include_only_emails=True,
                ),
            ),
            200,
        )
    except Exception as e:
        logger.error(
            f"error retrieving user emails within the same domain, err: {traceback.format_exc()}",
        )
        status, rc, msg = "fail", 500, str(e)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_user_info_by_id(
    user,
    token_info,
    user_id,
):  # noqa: E501
    """Returns information about the user with id

     # noqa: E501

    :param user:
    :param token_info:
    :param user_id: user id
    :type user_id: str

    :rtype: User
    """
    try:
        defaultUser = os.getenv("DEFAULT_USER", "default@nlmatics.com")
        if (
            # user_id == "defaultUser"
            defaultUser
            and os.getenv("AUTH_PROVIDER", None) == "fakeauth"
        ):
            db_user = nosql_db.get_user_by_email(defaultUser)
            return make_response(jsonify(db_user), 200)
        user_json = token_info["user_obj"]
        if not (user_json.get("is_admin", False) or user_json["id"] == user_id):
            return make_response(
                jsonify(
                    {
                        "status": "fail",
                        "reason": "Only Admins or the user himself can access this account.",
                    },
                ),
                403,
            )
        # Hide the fields that are not necessary / sensitive
        projection = {
            "_id": 0,
            "created_on": 0,
            "expiry_time": 0,
            "last_login": 0,
            "is_logged_in": 0,
        }
        user_obj = nosql_db.get_user(user_id, expand=True, projection=projection)
        if user_obj and user_obj.email_id != user:
            logger.error(f"{user} not authorized to query user with id {user_id}")
            return make_response(
                jsonify({"status": "fail", "reason": "unauthorized"}),
                401,
            )
        if user_obj is None:
            return make_response(
                jsonify({"status": "fail", "reason": "user not found"}),
                404,
            )
        else:
            user_dict = user_obj.to_dict()
            subs_name = user_dict.get("subscription_plan", DEFAULT_SUBSCRIPTION_PLAN)
            plan = nosql_db.retrieve_subscription_plans(subs_name)
            if plan:
                user_dict["included_features"] = plan[subs_name].get(
                    "included_features",
                    [],
                )
            user_dict["can_change_subscription"] = (
                nosql_db.db["nlm_subscriptions"].count(
                    {"subs_type": "nlmatics_paid_plan"},
                )
                > 0
            )

            return make_response(jsonify(User(**user_dict)), 200)
    except Exception as e:
        logger.error(
            f"error retrieving user info id: {user_id}, err: {traceback.format_exc()}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def update_user_by_id(
    user,
    token_info,
    user_id,
    body=None,
):  # noqa: E501
    """Update an existing user with id

     # noqa: E501

    :param token_info:
    :param user:
    :param user_id:
    :type user_id: str
    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        user_json = token_info["user_obj"]
        if not (user_json.get("is_admin", False) or user_json["id"] == user_id):
            return make_response(
                jsonify(
                    {
                        "status": "fail",
                        "reason": "Only Admins or the user himself can access this account.",
                    },
                ),
                403,
            )
        try:
            user_obj = User.from_dict(connexion.request.get_json())  # noqa: E50
            # Update the user details in Auth0
            if user_obj.first_name or user_obj.last_name or user_obj.subscription_plan:
                auth_provider.update_user_details_and_plan(
                    user,
                    user_obj.first_name,
                    user_obj.last_name,
                    user_obj.subscription_plan,
                )
            updated_user_profile = nosql_db.update_user(
                user_obj.to_dict(),
                user_id=user,
            )
            if updated_user_profile is None:
                return make_response(
                    jsonify({"status": "fail", "reason": "user not found"}),
                    404,
                )
            else:
                return make_response(
                    jsonify(id_with_message.IdWithMessage(user, "user updated")),
                )
        except Exception as e:
            logger.error(f"error updating user, err: {traceback.format_exc()}")
            status, rc, msg = "fail", 500, str(e)
    else:
        status, rc, msg = "fail", 422, "invalid json"
    return make_response(jsonify({"status": status, "reason": msg}), rc)
