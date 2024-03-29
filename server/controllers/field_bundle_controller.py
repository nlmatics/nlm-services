import json
import logging
import os
import traceback
import uuid
from datetime import datetime

import connexion
from flask import jsonify
from flask import make_response
from nlm_utils.utils import ensure_bool

from server import err_response
from server import unauthorized_response
from server.controllers.extraction_controller import perform_extraction_on_fields
from server.models import SearchCriteria
from server.models.field import Field  # noqa: E501
from server.models.field_bundle import FieldBundle  # noqa: E501
from server.models.field_bundle_content import FieldBundleContent
from server.models.field_content import FieldContent
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.storage import nosql_db as nosqldb
from server.utils import str_utils
from server.utils.metric_utils import update_metric_data

# import server.config as cfg

MODIFIABLE_BUNDLE_ATTRIBUTES = {
    "bundleName": "bundle_name",
    "active": "active",
    "cachedFile": "cached_file",
    "bundleType": "bundle_type",
    "tags": "tags",
    "fieldIds": "field_ids",
    "workspaceFilterIds": "workspace_filter_ids",
}

# configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
UPDATE_USAGE_METRICS = ensure_bool(os.getenv("UPDATE_USAGE_METRICS", False))


def add_field_to_field_bundle(
    user,
    token_info,
    field_bundle_id,
    field_id,
    nosql_db=nosqldb,
):  # noqa: E501
    """Adds an existing Field to a FieldBundle (does not create a new field)

     # noqa: E501

    :param token_info:
    :param user:
    :param field_id:
    :param field_bundle_id:
    :type field_bundle_id: str
    :param nosql_db:

    :rtype: IdWithMessage
    """
    try:
        # check if the field exists
        field_bundle = nosql_db.get_field_bundle_info(field_bundle_id)
        user_json = token_info.get("user_obj", None) if token_info else None
        if (not user_json) or (not field_bundle):
            return unauthorized_response()
        user_permission, _ws = nosql_db.get_user_permission(
            field_bundle.workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None),
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"] or (
            user_permission == "viewer" and field_bundle.user_id != user_json["id"]
        ):
            # Viewer can add a field only to his private field bundle.
            err_str = "Not authorized to add field"
            log_str = f"{user} not authorized to add field for user with id: {field_bundle.user_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        if not nosql_db.field_exists(field_bundle.workspace_id, field_id):
            status, rc, msg = "fail", 404, "field does not exists"
        else:
            nosql_db.add_field_to_bundle(field_id, field_bundle_id)
            # Update workspace statistics
            statistics = _ws.statistics or {}
            if not statistics.get("fields", {}) or not statistics["fields"].get(
                "total",
                0,
            ):
                statistics["fields"] = {
                    "total": 1,
                }
            else:
                statistics["fields"]["total"] += 1

            set_data = {
                "statistics": statistics,
            }
            nosql_db.update_workspace_data(_ws.id, set_data)
            return make_response(
                jsonify(IdWithMessage(field_id, "field added to bundle")),
            )

    except Exception as e:
        logger.error(
            f"error adding field to the bundle, err: {traceback.format_exc()}",
        )
        status, rc, msg = "fail", 500, str(e)
    finally:
        pass
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def create_new_field_bundle(
    user,
    token_info,
    field_bundle=None,
    nosql_db=nosqldb,
):  # noqa: E501
    """Create a new field bundle

     # noqa: E501

    :param token_info:
    :param user:
    :param field_bundle:
    :param nosql_db

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        bundle = FieldBundle.from_dict(connexion.request.get_json())  # noqa: E501
        user_json = token_info.get("user_obj", None) if token_info else None
        if not user_json:
            return unauthorized_response()

        bundle.user_id = user_json["id"]
        user_permission, _ws = nosql_db.get_user_permission(
            bundle.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to create bundle"
            log_str = f"{user} not authorized to create bundle for user with id: {bundle.user_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        try:
            bundle.id = bundle.id or str_utils.generate_unique_fieldbundle_id(
                bundle.user_id,
                bundle.workspace_id,
                bundle.bundle_name,
            )
            bundle.created_on = bundle.created_on or datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S",
            )
            bundle.active = True
            bundle.field_ids = []
            bundle_id = nosql_db.create_field_bundle(bundle)
            logger.info(f"bundle id returned is {bundle_id}")
            if bundle_id is None:
                status, msg, rc = (
                    "fail",
                    "field bundle with name " + bundle.bundle_name + " already exists",
                    409,
                )
                return make_response(jsonify({"status": status, "reason": msg}), rc)
            else:
                logger.info(
                    f"field bundle created with id {bundle_id} in workspace "
                    f"{bundle.workspace_id} for user {bundle.user_id}",
                )
                # Update workspace statistics
                statistics = _ws.statistics or {}
                if not statistics.get("field_bundle", {}) or not statistics[
                    "field_bundle"
                ].get("total", 0):
                    statistics["field_bundle"] = {
                        "total": 1,
                    }
                else:
                    statistics["field_bundle"]["total"] += 1

                set_data = {
                    "statistics": statistics,
                }
                nosql_db.update_workspace_data(_ws.id, set_data)
                return make_response(
                    jsonify(
                        IdWithMessage(bundle_id, "field bundle created successfully"),
                    ),
                )
        except Exception as e:
            logger.error(f"error creating field bundle, err: {traceback.format_exc()}")
            status, rc, msg = "fail", 500, str(e)
    else:
        status, rc, msg = "fail", 422, "invalid json"
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def update_field_bundle_by_id(
    user,
    token_info,
    field_bundle_id,
    nosql_db=nosqldb,
):  # noqa: E501
    """Updates the workspace for given id

     # noqa: E501


    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        try:
            orig_field_bundle = nosql_db.get_field_bundle_info(field_bundle_id)
            user_json = token_info.get("user_obj", None) if token_info else None
            if not user_json or not orig_field_bundle:
                return unauthorized_response()

            user_permission, _ws = nosql_db.get_user_permission(
                orig_field_bundle.workspace_id,
                email=user,
                user_json=user_json,
            )
            if user_permission not in ["admin", "owner", "editor", "viewer"] or (
                user_permission == "viewer"
                and orig_field_bundle.user_id != user_json["id"]
            ):
                err_str = "Not authorized to update bundle"
                log_str = f"{user} not authorized to update bundle for user with id: {orig_field_bundle.user_id}"
                logger.info(log_str)
                return err_response(err_str, 403)

            json_req_body = connexion.request.get_json()
            set_data = {}
            is_inactive = False
            for k in json_req_body.keys():
                if k not in MODIFIABLE_BUNDLE_ATTRIBUTES:
                    msg = f"Cannot modify {k} for Field Set"
                    logger.error(msg)
                    return make_response(
                        jsonify({"status": "fail", "reason": msg}),
                        422,
                    )
                else:
                    set_data[MODIFIABLE_BUNDLE_ATTRIBUTES[k]] = json_req_body[k]
                    if k == "active" and not json_req_body[k]:
                        is_inactive = True

            if set_data:
                nosql_db.update_field_bundle_attr(set_data, field_bundle_id)

            if is_inactive:
                # Update Workspace statistics
                statistics = _ws.statistics or {}
                if not statistics.get("field_bundle", {}) or not statistics[
                    "field_bundle"
                ].get("total", 0):
                    statistics["field_bundle"] = {
                        "total": 0,
                    }
                else:
                    statistics["field_bundle"]["total"] = (
                        statistics["field_bundle"]["total"] - 1
                        if statistics["field_bundle"]["total"] > 1
                        else 0
                    )

                set_data = {
                    "statistics": statistics,
                }
                nosql_db.update_workspace_data(_ws.id, set_data)

            logger.info(
                f"field bundle with id {field_bundle_id} updated with set_data {set_data}",
            )
            return make_response(
                jsonify(IdWithMessage(field_bundle_id, "field bundle updated")),
            )

        except Exception as e:
            logger.error(
                f"error updating field bundle with id {field_bundle_id}, err: {traceback.format_exc()}",
            )
            status, rc, msg = "fail", 500, str(e)
    else:
        logger.error("invalid json in request")
        status, rc, msg = "fail", 422, "invalid json in request"
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def delete_field_bundle(
    user,
    token_info,
    workspace_id,
    field_bundle_id,
    nosql_db=nosqldb,
):  # noqa: E501
    """Deletes a field bundle

     # noqa: E501

    :param token_info:
    :param user:
    :param workspace_id:

    :param field_bundle_id:
    :type field_bundle_id: str

    :param nosql_db:

    :rtype: IdWithMessage
    """
    try:
        # check if the field bundle exists and is empty
        field_bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
        user_json = token_info.get("user_obj", None) if token_info else None
        if not user_json or not field_bundle_info:
            return unauthorized_response()

        user_permission, _ws = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"] or (
            user_permission == "viewer" and field_bundle_info.user_id != user_json["id"]
        ):
            err_str = "Not authorized to query workspace"
            log_str = f"{user} not authorized to query workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        field_ids = field_bundle_info.field_ids
        for field_id in field_ids:
            nosql_db.delete_field_by_field_id(field_id)
        # check if the field bundle exists and is empty
        logger.info(
            f"No of fields :{len(nosql_db.get_field_bundle_info(field_bundle_id).field_ids)}",
        )
        if len(nosql_db.get_field_bundle_info(field_bundle_id).field_ids) == 0:
            if UPDATE_USAGE_METRICS:
                update_metric_data(
                    token_info.get("user_obj", None),
                    [("num_fields", -len(field_ids))],
                )
            nosql_db.delete_field_bundle(field_bundle_id)
            # Update Workspace statistics
            statistics = _ws.statistics or {}
            if not statistics.get("field_bundle", {}) or not statistics[
                "field_bundle"
            ].get("total", 0):
                statistics["field_bundle"] = {
                    "total": 0,
                }
            else:
                statistics["field_bundle"]["total"] = (
                    statistics["field_bundle"]["total"] - 1
                    if statistics["field_bundle"]["total"] > 1
                    else 0
                )

            if not statistics.get("fields", {}) or not statistics["fields"].get(
                "total",
                0,
            ):
                statistics["fields"] = {
                    "total": 0,
                }
            else:
                statistics["fields"]["total"] = (
                    statistics["fields"]["total"] - len(field_ids)
                    if statistics["fields"]["total"] > len(field_ids)
                    else 0
                )

            set_data = {
                "statistics": statistics,
            }
            nosql_db.update_workspace_data(_ws.id, set_data)

            status, rc, msg = "success", 200, "field bundle deleted"
        else:
            logger.error("Field bundle is not Empty")
            status, rc, msg = "fail", 422, "field bundle is not empty"

    except Exception as e:
        logger.error(
            f"error deleting field bundle with id {field_bundle_id}, error: {traceback.format_exc()}",
        )
        status, rc, msg = "fail", 500, str(e)
    finally:
        pass
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_field_bundle_info(
    user,
    token_info,
    workspace_id,
    field_bundle_id,
    nosql_db=nosqldb,
):  # noqa: E501
    """Returns fieldBundle information

     # noqa: E501

    :param user:
    :param token_info:
    :param workspace_id:

    :param field_bundle_id:
    :type field_bundle_id: str

    :param nosql_db:

    :rtype: FieldBundle
    """
    try:
        ws = nosql_db.get_workspace_by_id(workspace_id)
        user_json = token_info.get("user_obj", None) if token_info else None
        # user_domain = user.split("@")[1]
        user_permission, _ws = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to retrieve field bundle information"
            log_str = f"{user} not authorized to retrieve field bundle information for {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        if not ws:
            logger.error(f"workspace with id {workspace_id} not found")
            return err_response("invalid workspace")

        bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
        if bundle_info:
            # PRIVATE bundles can be accessed by the owner only.
            if (
                bundle_info.bundle_type == "PRIVATE"
                and bundle_info.user_id != user_json["id"]
            ):
                err_str = "Not authorized to get bundle information"
                log_str = f"{user} not authorized to get bundle information for {field_bundle_id}"
                logger.info(log_str)
                return err_response(err_str, 403)

            if (
                bundle_info.workspace_id is not None
                and bundle_info.workspace_id != workspace_id
            ):
                logger.error(
                    f"bundle workspace {bundle_info.workspace_id} does not match requested workspace id {workspace_id}",
                )
                return err_response("bundle not in workspace", 422)
            return make_response(jsonify(bundle_info), 200)
        else:
            logger.error(
                f"field bundle with id {field_bundle_id} not found in workspace {workspace_id}",
            )
            status, rc, msg = "fail", 404, "bundle not found"
    except Exception as e:
        logger.error(
            f"error retrieving field bundle with id {field_bundle_id} in workspace {workspace_id}, "
            f"err: {traceback.format_exc()}",
        )
        status, rc, msg = "fail", 500, str(e)
    finally:
        pass
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_field_info_from_field_bundle_id(
    user,
    token_info,
    field_bundle_id,
    nosql_db=nosqldb,
):  # noqa: E501
    """Returns fieldBundle information

     # noqa: E501

    :param token_info:

    :param field_bundle_id:
    :type field_bundle_id: str

    :param nosql_db:

    :rtype: FieldBundle
    """
    try:
        user_json = token_info.get("user_obj", None) if token_info else None
        bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
        if not user_json:
            return unauthorized_response()
        if bundle_info:
            user_permission, _ws = nosql_db.get_user_permission(
                bundle_info.workspace_id,
                user_json=user_json,
            )
            if user_permission not in ["admin", "owner", "editor", "viewer"]:
                err_str = "Not authorized to retrieve fields from bundle"
                log_str = f"{user} not authorized to retrieve fields from bundle id: {field_bundle_id}"
                logger.info(log_str)
                return err_response(err_str, 403)

            # PRIVATE bundles can be accessed by the owner only.
            if (
                bundle_info.bundle_type == "PRIVATE"
                and bundle_info.user_id != user_json["id"]
            ):
                err_str = "Not authorized to retrieve bundle information"
                log_str = f"{user} not authorized to retrieve bundle information for {field_bundle_id}"
                logger.info(log_str)
                return err_response(err_str, 403)

            field_ids = list(set(bundle_info.field_ids))
            field_info = nosql_db.get_field_by_ids(
                field_ids,
                {
                    "_id": 0,
                    "id": 1,
                    "name": 1,
                    "status": 1,
                },
            )
            f_info_list = []
            for field in field_info:
                f_info_list.append(
                    {"field_id": field.id, "name": field.name, "status": field.status},
                )
            return make_response(jsonify(f_info_list), 200)
        else:
            logger.error(f"field bundle with id {field_bundle_id} not found")
            status, rc, msg = "fail", 404, "bundle not found"
    except Exception as e:
        logger.error(
            f"error retrieving field bundle with id {field_bundle_id} , err: {traceback.format_exc()}",
        )
        status, rc, msg = "fail", 500, str(e)
    finally:
        pass
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def remove_field_from_field_bundle(
    user,
    token_info,
    workspace_id,
    field_bundle_id,
    field_id,
    nosql_db=nosqldb,
):  # noqa: E501
    """Remove Field from a FieldBundle (does not delete the field itself)

     # noqa: E501
    :param token_info:
    :param user:
    :param workspace_id

    :param field_bundle_id:
    :type field_bundle_id: str
    :param field_id:
    :type field_id: str

    :param nosql_db:

    :rtype: IdWithMessage
    """

    try:
        user_json = token_info.get("user_obj", None) if token_info else None
        user_permission, _ws = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=user_json,
        )
        bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
        if user_permission not in ["admin", "owner", "editor", "viewer"] or (
            user_permission == "viewer" and bundle_info.user_id != user_json["id"]
        ):
            err_str = "Not authorized to remove field bundle"
            log_str = f"{user} not authorized to remove field bundle {field_bundle_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        if field_id in bundle_info.field_ids:
            rc = nosql_db.remove_field_from_field_bundle(
                workspace_id,
                field_bundle_id,
                field_id,
            )
            if rc:
                logger.info(
                    f"field with id {field_id} removed from bundle with id {field_bundle_id}",
                )
                return make_response(
                    jsonify(IdWithMessage(rc, "field removed from bundle")),
                )
        else:
            logger.error(
                f"field with id {field_id} not part of bundle {field_bundle_id}",
            )
            status, rc, msg = "fail", 400, "field not part of bundle"
    except Exception as e:
        logger.error(
            f"error removing field with id {field_id} from bundle with id {field_bundle_id} "
            f"in workspace {workspace_id}, err: {traceback.format_exc()}",
        )
        status, rc, msg = "fail", 500, str(e)
    finally:
        pass
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def update_field_bundle_info(
    user,
    token_info,
    workspace_id,
    field_bundle_id,
    body=None,
    nosql_db=nosqldb,
):  # noqa: E501
    """Updates a field bundle

     # noqa: E501
    :param user:
    :param token_info:
    :param workspace_id:

    :param field_bundle_id:
    :type field_bundle_id: str
    :param body:
    :type body: dict | bytes

    :param nosql_db:
    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        bundle = FieldBundle.from_dict(connexion.request.get_json())  # noqa: E501
        try:
            user_permission, _ws = nosql_db.get_user_permission(
                workspace_id,
                email=user,
                user_json=token_info.get("user_obj", None),
            )
            if user_permission not in ["admin", "owner", "editor"]:
                err_str = "Not authorized to update field bundle"
                log_str = (
                    f"{user} not authorized to update field bundle {field_bundle_id}"
                )
                logger.info(log_str)
                return err_response(err_str, 403)

            if nosql_db.field_bundle_exists(workspace_id, field_bundle_id):
                bundle_id = nosql_db.update_field_bundle(bundle)
                logger.info(f"field bundle with id {field_bundle_id} updated")
                return make_response(
                    jsonify(IdWithMessage(bundle_id, "field bundle updated.")),
                )
            else:
                logger.error(
                    f"field bundle with id {field_bundle_id} does not exists in workspace id {workspace_id}",
                )
                status, rc, msg = "fail", 400, "bundle does not exists"
        except Exception as e:
            logger.error(f"error updating field bundle, err: {traceback.format_exc()}")
            status, rc, msg = "fail", 500, str(e)
    else:
        status, rc, msg = "fail", 422, "invalid json"
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_field_bundles_in_workspace(
    user,
    token_info,
    workspace_id,
    nosql_db=nosqldb,
):
    """Retrieves the list of field bundles available in a workspace
    :param user: Authenticated user
    :param workspace_id: workspace id
    :param token_info: token_info
    :param nosql_db:
    :return: list of field bundles
    """

    try:
        user_json = token_info.get("user_obj", None)
        user_permission, _ws = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to view field bundle"
            log_str = f"{user} not authorized to view field bundle in workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        bundles = nosql_db.get_field_bundles_in_workspace(
            workspace_id,
            user_id=user_json["id"],
        )
        return make_response(jsonify(bundles), 200)
    except Exception as e:
        logger.error(f"error retrieving field bundles, err: {traceback.format_exc()}")
        return err_response(str(e))


def get_field_bundle_contents(
    user,
    token_info,
    workspace_id,
    bundle_id,
    db=nosqldb,
):
    """Retrieves the full contents of the field bundle (including fields, templates)
    :param token_info:
    :param db:
    :param user: authenticated user
    :param workspace_id:
    :param bundle_id:
    :return:
    """

    try:
        user_json = token_info.get("user_obj", None) if token_info else None
        bundle_info = db.get_field_bundle_info(bundle_id)
        user_permission, _ws = db.get_user_permission(
            workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"] or (
            user_permission == "viewer" and bundle_info.user_id != user_json["id"]
        ):
            err_str = "Not authorized to view field bundle"
            log_str = f"{user} not authorized to view field bundle {bundle_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        if not bundle_info:
            logger.error(f"bundle with id {bundle_id} not found")
            return err_response("invalid bundle id")

        if bundle_info.workspace_id and bundle_info.workspace_id != workspace_id:
            err_str = "Not authorized to view field bundle"
            log_str = (
                f"bundle workspace id {bundle_info.workspace_id} does not match "
                f"requested workspace id {workspace_id}"
            )
            logger.info(log_str)
            return err_response(err_str, 403)

        field_contents = []
        fields = db.get_fields_in_bundle(bundle_id)
        # Reset the order
        field_idx2field = {}
        for field in fields:
            field_idx2field[field.id] = field
        fields = [field_idx2field[i] for i in bundle_info.field_ids]
        for field in fields:
            tmpls = db.get_templates_for_field(field.id)
            field_contents.append(FieldContent(**field.to_dict(), templates=tmpls))
        bundle_content = FieldBundleContent(
            **bundle_info.to_dict(),
            fields=field_contents,
        )
        return make_response(jsonify(bundle_content), 200)

    except Exception as e:
        logger.error(
            f"error retrieving field bundle contents, err: {traceback.format_exc()}",
        )
        return err_response(e.__str__())


def retrieve_fields_for_bundle(
    bundle_info,
    return_dict=False,
    nosql_db=nosqldb,
):
    """
    Retrieve fields for bundle object as passed
    :param bundle_info: Bundle information
    :param return_dict: Return a list of dict or object
    :param nosql_db:
    :return: List of fields in the same order as in the field_ids of bundle_info
    """
    fields = nosql_db.get_fields_in_bundle(bundle_info.id, return_dict=return_dict)
    # Reset the order
    field_idx2field = {}
    for field in fields:
        idx = field["id"] if return_dict else field.id
        field_idx2field[idx] = field
    fields = [field_idx2field[i] for i in bundle_info.field_ids]
    return fields


def download_field_definitions(
    user,
    token_info,
    field_bundle_id,
    nosql_db=nosqldb,
):
    try:
        user_json = token_info.get("user_obj", None) if token_info else None
        if not user_json:
            return unauthorized_response()
        bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
        user_permission, _ws = nosql_db.get_user_permission(
            bundle_info.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            log_str = "Not authorized to download field definition"
            logger.info(log_str)
            return err_response(log_str, 403)

        logger.info(bundle_info)
        fields = retrieve_fields_for_bundle(bundle_info)
        return make_response(jsonify(fields), 200)
    except Exception as e:
        logger.error(
            f"error uploading file, stacktrace: {traceback.format_exc()}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, str(e)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def create_new_fields_from_existing_fields(
    fields,
    bundle_info,
    user_json,
    nosql_db=nosqldb,
):
    """
    Creates new fields from existing field information.
    :param fields: List of existing fields as dict
    :param bundle_info: New Field bundle information
    :param user_json: User profile
    :param nosql_db:
    :return: Dict of field id --> field
    """
    num_created_fields = 0
    new_fields = {}
    for field in fields:
        field.update(
            {
                "id": str(uuid.uuid4()),
                "parent_bundle_id": bundle_info.id,
                "workspace_id": bundle_info.workspace_id,
                "user_id": user_json["id"],
                # Reset variables for newly created fields.
                "status": {},
                "distinct_values": [],
            },
        )
        db_field = Field(**field)
        if not db_field.search_criteria:
            print("Recreating search criteria here")
            db_field.search_criteria = SearchCriteria.from_dict(
                field["search_criteria"],
            )
        existing_field = nosql_db.get_field_by_name(
            bundle_info.id,
            field["name"],
        )
        # field exist, override
        if existing_field:
            logger.info(
                f"Existing field {existing_field['id']} with the same name found {existing_field['name']}",
            )
            db_field.name = "Clone of " + existing_field["name"]

        # create new field
        logger.info(f"creating field: {db_field}")
        newly_created_field_id = nosql_db.create_field(db_field)
        if newly_created_field_id:
            logger.info(f"new field with id {db_field.id} / {db_field.name} created")
        new_fields[newly_created_field_id] = db_field
        nosql_db.add_fields_to_bundle(
            newly_created_field_id,
            bundle_info.id,
        )
        num_created_fields += 1

    if num_created_fields and UPDATE_USAGE_METRICS:
        update_metric_data(
            user_json,
            [("num_fields", num_created_fields)],
        )

    # Update Workspace statistics
    ws = nosql_db.get_workspace_by_id(bundle_info.workspace_id)
    statistics = ws.statistics or {}
    if not statistics.get("fields", {}) or not statistics["fields"].get("total", 0):
        statistics["fields"] = {
            "total": num_created_fields,
        }
    else:
        statistics["fields"]["total"] += num_created_fields

    set_data = {
        "statistics": statistics,
    }
    nosql_db.update_workspace_data(ws.id, set_data)

    return new_fields


def upload_fields_from_file(
    user,
    token_info,
    field_bundle_id,
    file=None,
    nosql_db=nosqldb,
):
    """Uploads a new document to a workspace

     # noqa: E501


    :param user:
    :param token_info:
    :param field_bundle_id:
    :param file:
    :param nosql_db:

    :rtype: IdWithMessage
    """
    # check if the workspace exists
    tmp_file = None
    num_created_fields = 0
    try:
        user_json = token_info.get("user_obj", None) if token_info else None
        if not user_json:
            return unauthorized_response()
        bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
        logger.info(bundle_info)
        ws = nosql_db.get_workspace_by_id(bundle_info.workspace_id)
        user_permission, _ws = nosql_db.get_user_permission(
            bundle_info.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"] or (
            user_permission == "viewer" and bundle_info.user_id != user_json["id"]
        ):
            err_str = "Not authorized to upload fields from file"
            log_str = f"{user} not authorized to upload fields from file for {bundle_info.workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        if not ws:
            logger.error(
                f"workspace with id {bundle_info.workspace_id} does not exists",
            )
            status, rc, msg = "fail", 422, "workspace does not exists"
        else:
            fields = json.loads(file.read())
            # Convert to normalized pattern for "create_new_fields_from_existing_fields" to work on
            # Converts Swagger type notation to Field object style notation
            #   e.g. isEnteredField  ==> is_entered_field
            fields = [(Field.from_dict(f)).to_dict() for f in fields]
            create_new_fields_from_existing_fields(
                fields,
                bundle_info,
                user_json,
            )

            return make_response(
                jsonify(IdWithMessage(field_bundle_id, "upload successful")),
            )

    except Exception as e:
        logger.error(
            f"error uploading file, stacktrace: {traceback.format_exc()}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, str(e)

    finally:
        if tmp_file and os.path.exists(tmp_file):
            os.unlink(tmp_file)
    if num_created_fields and UPDATE_USAGE_METRICS:
        update_metric_data(
            token_info.get("user_obj", None),
            [("num_fields", num_created_fields)],
        )
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def replicate_using_field_bundle_id(
    user,
    token_info,
    workspace_id,
    field_bundle_id,
    target_field_bundle_name: str = None,
    create_field_bundle: bool = False,
    nosql_db=nosqldb,
):
    user_json = token_info.get("user_obj", None) if token_info else None
    if not user_json:
        return unauthorized_response()

    bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
    user_permission, _ws = nosql_db.get_user_permission(
        workspace_id,
        email=user,
        user_json=user_json,
    )
    if user_permission not in ["admin", "owner", "editor", "viewer"] or (
        user_permission == "viewer" and bundle_info.user_id != user_json["id"]
    ):
        err_str = "Not authorized to replicate fields"
        log_str = (
            f"{user} not authorized to replicate fields for {bundle_info.workspace_id}"
        )
        logger.info(log_str)
        return err_response(err_str, 403)

    bundle_info.bundle_type = "PRIVATE"  # Update to private all the field bundle types
    bundle_info.bundle_name = (
        target_field_bundle_name
        if target_field_bundle_name and len(target_field_bundle_name)
        else "Clone of " + bundle_info.bundle_name
    )
    field_ids_newly_created, new_bundle_info = replicate_using_field_bundle(
        bundle_info,
        user_json,
        workspace_id,
        do_extract=True,  # Enabling extraction by default.
        create_field_bundle=create_field_bundle,
    )
    return make_response(
        jsonify(
            {
                "bundle_id": new_bundle_info.id,
                "field_ids": field_ids_newly_created,
                "status": "Field Bundle replicated successfully.",
            },
        ),
        200,
    )


def replicate_using_field_bundle(
    bundle_info,
    user_json,
    workspace_id,
    nosql_db=nosqldb,
    do_extract=False,
    create_field_bundle=True,
):
    # Creating new bundle data
    bundle = bundle_info
    user_id = user_json["id"]
    logger.info(f"file bundle contents are {bundle_info}")
    orig_fields = retrieve_fields_for_bundle(bundle_info, return_dict=True)
    # Delete the Original fields from the bundle
    bundle.field_ids = []
    if create_field_bundle:
        # Create a new field bundle
        bundle.user_id = user_id
        bundle.workspace_id = workspace_id
        bundle.id = str_utils.generate_unique_fieldbundle_id(
            bundle.user_id,
            bundle.workspace_id,
            bundle.bundle_name,
        )
        bundle.created_on = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        bundle.active = True
        # create field bundle
        nosql_db.create_field_bundle(bundle)
    else:
        bundle = nosql_db.get_default_field_bundle_info(workspace_id)

    # Update Workspace statistics
    ws = nosql_db.get_workspace_by_id(workspace_id)
    statistics = ws.statistics or {}
    if not statistics.get("field_bundle", {}) or not statistics["field_bundle"].get(
        "total",
        0,
    ):
        statistics["field_bundle"] = {
            "total": 1,
        }
    else:
        statistics["field_bundle"]["total"] += 1

    set_data = {
        "statistics": statistics,
    }
    nosql_db.update_workspace_data(ws.id, set_data)
    # Create fields
    new_fields = create_new_fields_from_existing_fields(
        orig_fields,
        bundle,
        user_json,
    )

    if do_extract:
        logger.info(
            f"Performing extraction on the new created field_bundle {bundle.id}",
        )
        doc_projection = {
            "_id": 0,
            "id": 1,
        }
        docs_in_ws = nosql_db.get_folder_contents(
            workspace_id,
            projection=doc_projection,
            do_sort=False,
        )["documents"]

        perform_extraction_on_fields(
            user_json,
            workspace_id,
            len(docs_in_ws),
            new_fields,
        )

    return list(new_fields.keys()), bundle


def replicate_field_bundles_between_workspaces(
    source_workspace_id,
    target_workspace_id,
    user_profile,
    nosql_db=nosqldb,
):
    """
    Retrieve all the bundles from source workspace and copy them over to target workspace.
    :param source_workspace_id: Source Workspace ID
    :param target_workspace_id: Target Workspace ID
    :param user_profile: User Profile JSON
    :param nosql_db: NOSQL Connection
    :return:
    """
    # Retrieve all field bundles for the workspace
    src_bundles = nosql_db.get_field_bundles_in_workspace(
        source_workspace_id,
        user_id=user_profile["id"],
    )
    # Replicate them
    for bundle in src_bundles:
        # Update the bundle type only for non-default ones.
        if bundle.bundle_type != "DEFAULT":
            bundle.bundle_type = "PRIVATE"
        replicate_using_field_bundle(
            bundle,
            user_profile,
            target_workspace_id,
        )
