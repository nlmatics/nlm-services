import logging

import connexion
from flask import jsonify
from flask import make_response

from server import err_response
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.models.workspace_filter import WorkspaceFilter  # noqa: E501
from server.storage import nosql_db
from server.utils import str_utils

# import server.config as cfg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def add_workspace_filter(user, token_info, body=None):  # noqa: E501
    """Add New User defined Field (no bundle)

     # noqa: E501

    :param user:
    :param token_info:
    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        workspace_filter = WorkspaceFilter.from_dict(
            connexion.request.get_json(),
        )  # noqa: E501
        user_json = token_info.get("user_obj", None) if token_info else None
        user_permission, _ws = nosql_db.get_user_permission(
            workspace_filter.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to add workspace filter"
            log_str = f"user {user} not authorized to add workspace filter {workspace_filter.workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        workspace_filter.id = str_utils.generate_filter_id(workspace_filter.name)

        workspace_filter.user_id = user_json["id"]
        newly_created_filter_id = nosql_db.add_workspace_filter(workspace_filter)

        # Retrieve the field bundle
        projection = {
            "_id": 0,
            "id": 1,
            "workspace_filter_ids": 1,
        }
        default_field_bundle = nosql_db.get_default_field_bundle_info(
            _ws.id,
            projection=projection,
            return_dict=True,
        )
        if default_field_bundle:
            workspace_filter_ids = default_field_bundle.get("workspace_filter_ids", [])
            if newly_created_filter_id not in workspace_filter_ids:
                workspace_filter_ids.append(newly_created_filter_id)
            nosql_db.update_workspace_filters_in_bundle(
                workspace_filter_ids,
                default_field_bundle["id"],
            )

        return make_response(
            jsonify(
                IdWithMessage(
                    newly_created_filter_id,
                    "workspace filter created",
                ),
            ),
        )
    else:
        _, rc, msg = "fail", 422, "invalid json"
        return err_response(msg, rc)


def get_workspace_filter(user, token_info, user_id="", workspace_id=""):
    if workspace_id:
        user_json = token_info.get("user_obj", None) if token_info else None
        user_permission, _ws = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to retrieve workspace filter"
            log_str = f"user {user} not authorized to retrieve workspace filter {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)
    return nosql_db.get_workspace_filter(user_id, workspace_id)


def modify_workspace_filter(user, token_info, workspace_filter_id):

    if connexion.request.is_json:
        workspace_filter = WorkspaceFilter.from_dict(
            connexion.request.get_json(),
        )  # noqa: E501
        user_json = token_info.get("user_obj", None) if token_info else None
        user_permission, _ws = nosql_db.get_user_permission(
            workspace_filter.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to modify workspace filter"
            log_str = f"user {user} not authorized to modify workspace filter {workspace_filter.workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)
        workspace_filter.id = workspace_filter_id
        newly_created_filter_id = nosql_db.update_filter_by_id(
            workspace_filter_id,
            workspace_filter,
        )
        return make_response(
            jsonify(
                IdWithMessage(
                    newly_created_filter_id,
                    "workspace filter updated",
                ),
            ),
        )


def delete_workspace_filter(user, token_info, workspace_filter_id):
    workspace_filter = nosql_db.get_workspace_filter_by_id(workspace_filter_id)
    user_json = token_info.get("user_obj", None) if token_info else None
    user_permission, _ws = nosql_db.get_user_permission(
        workspace_filter["workspace_id"],
        email=user,
        user_json=user_json,
    )
    if user_permission not in ["admin", "owner", "editor"]:
        err_str = "Not authorized to delete workspace filter"
        log_str = f"user {user} not authorized to delete workspace filter {workspace_filter.workspace_id}"
        logger.info(log_str)
        return err_response(err_str, 403)

    removed_id = nosql_db.delete_workspace_filter(workspace_filter_id)
    # Retrieve the field bundle
    projection = {
        "_id": 0,
        "id": 1,
        "workspace_filter_ids": 1,
    }
    default_field_bundle = nosql_db.get_default_field_bundle_info(
        _ws.id,
        projection=projection,
        return_dict=True,
    )
    if default_field_bundle:
        workspace_filter_ids = default_field_bundle.get("workspace_filter_ids", [])
        if workspace_filter_ids and workspace_filter_id in workspace_filter_ids:
            workspace_filter_ids.remove(workspace_filter_id)
        nosql_db.update_workspace_filters_in_bundle(
            workspace_filter_ids,
            default_field_bundle["id"],
        )

    return make_response(
        jsonify(
            IdWithMessage(
                removed_id,
                "workspace filter created",
            ),
        ),
    )
