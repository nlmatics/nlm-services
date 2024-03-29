import logging
import os
import traceback

import connexion
from flask import jsonify
from flask import make_response
from nlm_utils.utils import ensure_bool
from nlm_utils.utils.answer_type import answer_type_options

from server import err_response
from server import unauthorized_response
from server.controllers.extraction_controller import extract_field_bundle_from_workspace
from server.controllers.extraction_controller import (
    extract_fields_dependent_workflow_fields,
)
from server.controllers.extraction_controller import (
    extract_file_meta_dependent_workflow_fields,
)
from server.controllers.extraction_controller import extract_relation_field
from server.controllers.field_bundle_controller import (
    create_new_fields_from_existing_fields,
)
from server.models.field import Field  # noqa: E501
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.storage import nosql_db
from server.utils import str_utils
from server.utils.dependent_fields_utils import BOOLEAN_MULTI_CAST_FIELD_TYPE
from server.utils.dependent_fields_utils import CAST_FIELD_TYPE
from server.utils.dependent_fields_utils import DEPENDENT_FIELD_ALLOWED_TYPES
from server.utils.dependent_fields_utils import FORMULA_FIELD_TYPE
from server.utils.formula import validate_formula
from server.utils.metric_utils import update_metric_data

# import server.config as cfg
MODIFIABLE_FIELD_ATTRIBUTES = {
    "name": "name",
    "active": "active",
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
UPDATE_USAGE_METRICS = ensure_bool(os.getenv("UPDATE_USAGE_METRICS", False))


def create_field(user, token_info, body=None):  # noqa: E501
    """Add New User defined Field (no bundle)

     # noqa: E501

    :param user:
    :param token_info:
    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        field = Field.from_dict(connexion.request.get_json())  # noqa: E501
        logger.info(f"creating field.....{field}")
        user_json = token_info.get("user_obj", None) if token_info else None
        bundle_info = nosql_db.get_field_bundle_info(field.parent_bundle_id)
        user_permission, _ws = nosql_db.get_user_permission(
            field.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"] or (
            user_permission == "viewer" and bundle_info.user_id != user_json["id"]
        ):
            err_str = "Not authorized to create fields"
            log_str = f"user {user} not authorized to create field in workspace {field.workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)
        # Validate a dependent field
        if field.is_entered_field and field.is_dependent_field and field.options:
            ret_val, err_str = validate_dependent_field(field)
            if not ret_val:
                logger.info(err_str)
                return err_response(err_str, 422)

        field.id = str_utils.generate_field_id(field.name)
        field.active = True
        field.user_id = user_json["id"]
        if not field.is_entered_field or (
            field.is_entered_field and field.is_dependent_field
        ):
            field.status = {"progress": "queued"}
            if field.search_criteria and len(field.search_criteria.criterias) > 0:
                answer_type = field.search_criteria.criterias[0].expected_answer_type
                if answer_type in answer_type_options:
                    field.options = answer_type_options[answer_type]
                else:
                    field.options = {}
        newly_created_field_id = nosql_db.create_field(field)
        if newly_created_field_id is None:
            status, msg, rc = (
                "fail",
                "field with name " + field.name + " already exists",
                409,
            )
            return make_response(jsonify({"status": status, "reason": msg}), rc)
        else:
            logger.info(
                f"new field with id {newly_created_field_id} created {field}",
            )
            if (
                field.is_dependent_field
                and field.options
                and field.options.get("parent_fields", [])
            ):
                parent_fields = field.options.get("parent_fields", [])
                for parent_field_id in parent_fields:
                    # Update each parent field's child_fields parameter.
                    parent_field = nosql_db.get_field_by_id(parent_field_id)
                    if parent_field:
                        if parent_field.options:
                            child_fields = parent_field.options.get("child_fields", [])
                        else:
                            child_fields = []
                        if newly_created_field_id not in child_fields:
                            child_fields.append(newly_created_field_id)
                            set_data = {
                                "options.child_fields": child_fields,
                            }
                            nosql_db.update_field_attr(set_data, parent_field_id)

            if UPDATE_USAGE_METRICS:
                update_metric_data(
                    user_json,
                    [("num_fields", 1)],
                )
            if field.parent_bundle_id:
                nosql_db.add_field_to_bundle(
                    newly_created_field_id,
                    field.parent_bundle_id,
                )

            if newly_created_field_id:  # TODO: test case needed
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

                if not field.is_entered_field or (
                    field.is_entered_field and field.is_dependent_field
                ):
                    logger.info(
                        f"queuing extracting data for field {newly_created_field_id}..",
                    )
                    if field.data_type in ["relation-node", "relation-triple"]:
                        extract_relation_field(
                            field=field,
                            user=user,
                            token_info=token_info,
                        )
                    elif (
                        field.is_entered_field
                        and field.is_dependent_field
                        and field.options
                    ):
                        # We need field.options for dependent fields
                        dependent_field_setup(
                            user,
                            token_info,
                            field,
                            newly_created_field_id,
                        )
                    else:
                        extract_field_bundle_from_workspace(
                            workspace_id=field.workspace_id,
                            field_bundle_id=field.parent_bundle_id,
                            overwrite_cache=newly_created_field_id,
                            user=user,
                            token_info=token_info,
                            return_output=False,
                        )
                    return make_response(
                        jsonify(
                            IdWithMessage(
                                newly_created_field_id,
                                "search field creation queued",
                            ),
                        ),
                    )
                else:
                    return make_response(
                        jsonify(
                            IdWithMessage(
                                newly_created_field_id,
                                "workflow field created",
                            ),
                        ),
                    )
            else:
                logger.error(
                    f"unknown error creating field, got 'null' as id for {newly_created_field_id}",
                )
                _, rc, msg = "fail", 500, "unknown status creating field"
    else:
        _, rc, msg = "fail", 422, "invalid json"
    return err_response(msg, rc)


def get_field_by_field_id(user, token_info, field_id):  # noqa: E501
    """Returns the field by given id

     # noqa: E501

    :param user:
    :param token_info:
    :param field_id:
    :type field_id: str

    :rtype: Field
    """
    try:
        field = nosql_db.get_field_by_id(field_id)
        user_permission, _ws = nosql_db.get_user_permission(
            field.workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None),
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to retrieve fields"
            log_str = f"user {user} not authorized to retrieve fields in workspace {field.workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        if field:
            return make_response(jsonify(field), 200)
        else:
            logger.error(f"field with id {field_id} not found")
            _, rc, msg = "fail", 404, "field not found"
    except Exception as e:
        logger.error(
            f"error retrieving field with id {field_id}, err: {traceback.format_exc()}",
        )
        _, rc, msg = "fail", 500, str(e)
    finally:
        pass
    return err_response(msg, rc)


def get_relation_fields_in_workspace(user, token_info, workspace_id):  # noqa: E501
    """Get all the relation fields from the workspace

     # noqa: E501

    :param user:
    :param token_info:
    :param workspace_id:
    :type workspace_id: str

    :rtype: Field
    """
    try:
        user_permission, workspace = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None) if token_info else None,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to retrieve workspace"
            log_str = f"user {user} not authorized to retrieve workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        fields = nosql_db.get_relation_fields_in_workspace(workspace_id)
        if fields:
            return make_response(jsonify(fields), 200)
        else:
            logger.error(
                f"unable to find relation fields in workspace {workspace_id} not found",
            )
            _, rc, msg = "fail", 404, "field not found"
    except Exception as e:
        logger.error(
            f"error retrieving field from workspace {workspace_id}, err: {traceback.format_exc()}",
        )
        _, rc, msg = "fail", 500, str(e)
    finally:
        pass
    return err_response(msg, rc)


def update_field_by_field_id(user, token_info, field_id, action):  # noqa: E501
    """Updates the field for given id

     # noqa: E501

    :param token_info:
    :param user:
    :param action:
    :param field_id:
    :type field_id: str

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        try:
            field = nosql_db.get_field_by_id(field_id)
            bundle_info = nosql_db.get_field_bundle_info(field.parent_bundle_id)
            user_json = token_info.get("user_obj", None) if token_info else None
            user_permission, _ws = nosql_db.get_user_permission(
                field.workspace_id,
                email=user,
                user_json=token_info.get("user_obj", None),
            )
            if user_permission not in ["admin", "owner", "editor", "viewer"] or (
                user_permission == "viewer"
                and bundle_info
                and bundle_info.user_id != user_json["id"]
            ):
                err_str = "Not authorized to update fields"
                log_str = f"user {user} not authorized to update field in workspace {field.workspace_id}"
                logger.info(log_str)
                return err_response(err_str, 403)

            is_derived_field = (
                field.is_entered_field and field.is_dependent_field and field.options
            )
            new_field = Field.from_dict(connexion.request.get_json())
            if not new_field.is_entered_field and action != "modify":
                new_field.status = {"progress": "queued"}
                if (
                    new_field.search_criteria
                    and len(field.search_criteria.criterias) > 0
                ):
                    answer_type = new_field.search_criteria.criterias[
                        0
                    ].expected_answer_type
                    if answer_type in answer_type_options:
                        new_field.options = answer_type_options[answer_type]
                    else:
                        new_field.options = {}
            if action == "replace":
                # use old field relations
                new_field.id = field_id
                new_field.workspace_id = field.workspace_id
                new_field.user_id = field.user_id
                new_field.parent_bundle_id = field.parent_bundle_id
                is_dependent_field = False
                if field.options:
                    if field.options.get("child_fields", []):
                        new_field.options["child_fields"] = field.options.get(
                            "child_fields",
                            [],
                        )
                    elif field.is_entered_field and field.is_dependent_field:
                        is_dependent_field = True
                        field_ids_to_remove = [
                            f
                            for f in field.options.get("parent_fields", [])
                            if f not in new_field.options.get("parent_fields", [])
                        ]
                        for parent_field_id in field_ids_to_remove:
                            # Update each parent field's child_fields parameter.
                            parent_field = nosql_db.get_field_by_id(parent_field_id)
                            if parent_field:
                                if parent_field.options:
                                    child_fields = parent_field.options.get(
                                        "child_fields",
                                        [],
                                    )
                                else:
                                    child_fields = []
                                if field_id in child_fields:
                                    child_fields.remove(field_id)
                                    set_data = {
                                        "options.child_fields": child_fields,
                                    }
                                    nosql_db.update_field_attr(
                                        set_data,
                                        parent_field_id,
                                    )

                        field_ids_to_add = [
                            f
                            for f in new_field.options.get("parent_fields", [])
                            if f not in field.options.get("parent_fields", [])
                        ]
                        for parent_field_id in field_ids_to_add:
                            # Update each parent field's child_fields parameter.
                            parent_field = nosql_db.get_field_by_id(parent_field_id)
                            if parent_field:
                                if parent_field.options:
                                    child_fields = parent_field.options.get(
                                        "child_fields",
                                        [],
                                    )
                                else:
                                    child_fields = []
                                if field_id not in child_fields:
                                    child_fields.append(field_id)
                                    set_data = {
                                        "options.child_fields": child_fields,
                                    }
                                    nosql_db.update_field_attr(
                                        set_data,
                                        parent_field_id,
                                    )

                nosql_db.update_field_by_id(field_id, new_field)
                if not is_dependent_field:
                    extract_field_bundle_from_workspace(
                        workspace_id=field.workspace_id,
                        field_bundle_id=field.parent_bundle_id,
                        overwrite_cache=field_id,
                        user=user,
                        token_info=token_info,
                        return_output=False,
                    )
                else:
                    dependent_field_setup(user, token_info, new_field, field_id)

                logger.info(f"field with id {field_id} been replaced.")

                return make_response(jsonify(IdWithMessage(field_id, "field updated")))
            elif action == "append":
                for criteria in new_field.search_criteria.criterias:
                    field.search_criteria["criterias"].append(criteria.to_dict())

                nosql_db.update_field_by_id(field_id, field)

                if not is_derived_field:
                    extract_field_bundle_from_workspace(
                        workspace_id=field.workspace_id,
                        field_bundle_id=field.parent_bundle_id,
                        overwrite_cache=field_id,
                        user=user,
                        token_info=token_info,
                        return_output=False,
                    )
                else:
                    dependent_field_setup(user, token_info, field, field_id)

                logger.info(f"field with id {field_id} appended with new criteria")

                return make_response(jsonify(IdWithMessage(field_id, "field updated")))
            elif action == "modify":
                json_req_body = connexion.request.get_json()
                set_data = {}
                is_inactive = False
                for k in json_req_body.keys():
                    if k not in MODIFIABLE_FIELD_ATTRIBUTES:
                        msg = f"Cannot modify {k} for Field"
                        logger.error(msg)
                        return make_response(
                            jsonify({"status": "fail", "reason": msg}),
                            422,
                        )
                    else:
                        set_data[MODIFIABLE_FIELD_ATTRIBUTES[k]] = json_req_body[k]
                        if k == "active" and not json_req_body[k]:
                            is_inactive = True

                if set_data:
                    nosql_db.update_field_attr(set_data, field_id)

                if is_inactive:
                    # Update workspace statistics
                    statistics = _ws.statistics or {}
                    if not statistics.get("fields", {}) or not statistics["fields"].get(
                        "total",
                        0,
                    ):
                        statistics["fields"] = {
                            "total": 0,
                        }
                    else:
                        statistics["fields"]["total"] = (
                            statistics["fields"]["total"] - 1
                            if statistics["fields"]["total"] > 1
                            else 0
                        )

                    set_data = {
                        "statistics": statistics,
                    }
                    nosql_db.update_workspace_data(_ws.id, set_data)

                logger.info(f"field id {field_id} modified with new attributes")
                return make_response(jsonify(IdWithMessage(field_id, "field modified")))
            else:
                logger.error(
                    f"unknown action {action} while updating field id {field_id}",
                )
                status, rc, msg = "fail", 500, "unknown action"

        except Exception as e:
            logger.error(
                f"error updating field with id {field_id}, err: {str(e)}",
                exc_info=True,
            )
            status, rc, msg = "fail", 500, str(e)
    else:
        logger.error(f"invalid json in request for user {user} and field {field_id}")
        status, rc, msg = "fail", 422, "invalid json in request"
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def delete_field_by_field_id(user, token_info, field_id):
    try:
        field = nosql_db.get_field_by_id(field_id)
        bundle_info = nosql_db.get_field_bundle_info(field.parent_bundle_id)
        user_json = token_info.get("user_obj", None) if token_info else None
        user_permission, _ws = nosql_db.get_user_permission(
            field.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"] or (
            user_permission == "viewer" and bundle_info.user_id != user_json["id"]
        ):
            err_str = "Not authorized to delete fields"
            log_str = f"user {user} not authorized to delete field in workspace {field.workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        if field:
            # Cannot delete a field which has fields dependent on it (child fields)
            child_fields = field.options.get("child_fields", [])
            if child_fields:
                err_str = f"Cannot delete field {field.name} when there are child fields {child_fields}"
                log_str = f"User {user} cannot delete field {field.name} when there are child fields {child_fields}"
                logger.info(log_str)
                return err_response(err_str, 424)

            # If there are parent fields, delete self from the parent field information
            parent_fields = field.options.get("parent_fields", [])
            for parent_field_id in parent_fields:
                # Update each parent field's child_fields parameter.
                parent_field = nosql_db.get_field_by_id(parent_field_id)
                if parent_field:
                    if parent_field.options:
                        child_fields = parent_field.options.get("child_fields", [])
                    else:
                        child_fields = []
                    if field_id in child_fields:
                        child_fields.remove(field_id)
                        set_data = {
                            "options.child_fields": child_fields,
                        }
                        nosql_db.update_field_attr(set_data, parent_field_id)

            field_id = nosql_db.delete_field_by_field_id(
                field_id,
                field_details=field,
            )
            if UPDATE_USAGE_METRICS:
                update_metric_data(
                    token_info.get("user_obj", None),
                    [("num_fields", -1)],
                )
            # Update workspace statistics
            statistics = _ws.statistics or {}
            if not statistics.get("fields", {}) or not statistics["fields"].get(
                "total",
                0,
            ):
                statistics["fields"] = {
                    "total": 0,
                }
            else:
                statistics["fields"]["total"] = (
                    statistics["fields"]["total"] - 1
                    if statistics["fields"]["total"] > 1
                    else 0
                )

            set_data = {
                "statistics": statistics,
            }
            nosql_db.update_workspace_data(_ws.id, set_data)

            return make_response(
                jsonify({"field_id": field_id, "status": "Deleted the field"}),
                200,
            )
        else:
            logger.error(f"field with id {field_id} not found")
            _, rc, msg = "fail", 404, "field not found"
    except Exception as e:
        logger.error(
            f"error retrieving field with id {field_id}, err: {traceback.format_exc()}",
        )
        _, rc, msg = "fail", 500, str(e)
    finally:
        pass
    return err_response(msg, rc)


def clone_field(
    user,
    token_info,
    field_id,
    target_workspace_id,
    target_field_bundle_id,
    target_field_name=None,
):
    try:
        field = nosql_db.get_field_by_id(field_id)
        if not field:
            logger.error(f"field with id {field_id} not found")
            _, rc, msg = "fail", 404, "field not found"
            return err_response(msg, rc)
        # Check origin workspace permissions
        bundle_info = nosql_db.get_field_bundle_info(field.parent_bundle_id)
        user_json = token_info.get("user_obj", None) if token_info else None
        user_permission, _ws = nosql_db.get_user_permission(
            field.workspace_id,
            email=user,
            user_json=user_json,
        )

        if user_permission not in ["admin", "owner", "editor", "viewer"] or (
            user_permission == "viewer" and bundle_info.user_id != user_json["id"]
        ):
            err_str = (
                f"Not authorized to clone field from workspace {field.workspace_id}"
            )
            log_str = f"user {user} not authorized to clone field from workspace {field.workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        # Check target workspace permissions
        target_bundle_info = nosql_db.get_field_bundle_info(target_field_bundle_id)
        user_permission, _ws = nosql_db.get_user_permission(
            target_workspace_id,
            email=user,
            user_json=user_json,
        )

        if user_permission not in ["admin", "owner", "editor", "viewer"] or (
            user_permission == "viewer"
            and target_bundle_info.user_id != user_json["id"]
        ):
            err_str = (
                f"Not authorized to clone field to workspace {target_workspace_id}"
            )
            log_str = f"user {user} not authorized to clone field to workspace {target_workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        if target_field_name:
            field.name = target_field_name
        new_fields = create_new_fields_from_existing_fields(
            [field.to_dict()],
            target_bundle_info,
            user_json,
        )

        if new_fields:
            extract_field_bundle_from_workspace(
                workspace_id=target_workspace_id,
                field_bundle_id=target_field_bundle_id,
                overwrite_cache=list(new_fields.keys())[0],
                user=user,
                token_info=token_info,
                return_output=False,
            )

            return make_response(
                jsonify(
                    {
                        "field_ids": list(new_fields.keys()),
                        "status": "Field replicated successfully.",
                    },
                ),
                200,
            )
        else:
            err_str = f"Error in cloning {field_id} to {target_workspace_id}: {target_field_bundle_id}"
            logger.error(err_str)
            _, rc, msg = "fail", 500, err_str
    except Exception as e:
        logger.error(
            f"error cloning field with id {field_id}, err: {traceback.format_exc()}",
        )
        _, rc, msg = "fail", 500, str(e)
    finally:
        pass
    return err_response(msg, rc)


def get_field_details(
    user,
    token_info,
    field_bundle_id,
    field_ids=None,
    return_only_status=False,
):  # noqa: E501
    """Returns the field by given id

     # noqa: E501

    :param return_only_status:
    :param field_ids:
    :param field_bundle_id:
    :param user:
    :param token_info:

    :rtype: Field
    """
    try:
        user_json = token_info.get("user_obj", None) if token_info else None
        if not user_json:
            return unauthorized_response()
        bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
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

            projection = None
            if return_only_status:
                projection = {
                    "_id": 0,
                    "id": 1,
                    "name": 1,
                    "status": 1,
                }
            if not field_ids:
                fields = nosql_db.get_fields_in_bundle(
                    field_bundle_id,
                    projection=projection,
                )
            else:
                if set(field_ids) <= set(bundle_info.field_ids):
                    fields = nosql_db.get_field_by_ids(field_ids, projection=projection)
                else:
                    err_str = "All the fields provided are not part of the bundle_id"
                    log_str = f"All the fields provided {field_ids} are not part of the bundle_id {field_bundle_id}"
                    logger.info(log_str)
                    return err_response(err_str, 404)

            return make_response(jsonify(fields), 200)
        else:
            logger.error(f"field bundle with id {field_bundle_id} not found")
            _, rc, msg = "fail", 404, "bundle not found"
    except Exception as e:
        logger.error(
            f"error retrieving fields for bundle id {field_bundle_id}, err: {traceback.format_exc()}",
        )
        _, rc, msg = "fail", 500, str(e)
    finally:
        pass
    return err_response(msg, rc)


def dependent_field_setup(user, token_info, field, newly_created_field_id):
    # Meta field
    if field.options.get("deduct_from_file_meta", False) and field.options.get(
        "meta_param",
        "",
    ):
        extract_file_meta_dependent_workflow_fields(
            user,
            token_info,
            field.workspace_id,
            field.parent_bundle_id,
            [newly_created_field_id],
            field.options.get("meta_param", ""),
            check_permission=False,
        )
    elif field.options.get("deduct_from_fields", False) and field.options.get(
        "parent_fields",
        [],
    ):
        extract_fields_dependent_workflow_fields(
            user,
            token_info,
            field.workspace_id,
            field.parent_bundle_id,
            newly_created_field_id,
            field_options=field.options,
            check_permission=False,
        )
    else:
        logger.info(
            f"Strange!! "
            f"Nothing to do be done for dependent {field} .. {newly_created_field_id}..",
        )


def validate_dependent_field(f):
    ret_val = True
    err_str = ""
    if not f.options.get("deduct_from_file_meta", False) and not f.options.get(
        "deduct_from_fields",
        False,
    ):
        ret_val = False
        err_str = "Neither deduct_from_file_meta or deduct_from_fields is set"
    elif f.options.get("deduct_from_file_meta", False) and not f.options.get(
        "meta_param",
        "",
    ):
        ret_val = False
        meta_param = f.options.get("meta_param", "")
        err_str = (
            f"Invalid meta_param {meta_param} for workflow field dependent on file meta"
        )
    elif f.options.get("deduct_from_fields", False):
        if not f.options.get("parent_fields", []):
            ret_val = False
            err_str = "No parent fields mentioned"
        elif f.options.get("type", "") not in DEPENDENT_FIELD_ALLOWED_TYPES:
            ret_val = False
            f_type = f.options.get("type", "")
            err_str = f"Unsupported type {f_type} in dependent field creation"
        elif f.options.get("type", "") in DEPENDENT_FIELD_ALLOWED_TYPES:
            f_type = f.options.get("type", "")
            if f_type == CAST_FIELD_TYPE and not f.options.get("cast_options", {}):
                ret_val = False
                err_str = (
                    "No cast options provided for cast type dependent field creation"
                )
            if f_type == BOOLEAN_MULTI_CAST_FIELD_TYPE and (
                not f.options.get("cast_options", {}) or f.data_type != "list"
            ):
                ret_val = False
                main_err_str = (
                    "No cast options provided"
                    if not f.options.get("cast_options", {})
                    else "data type is not list"
                )
                err_str = f"{main_err_str} for boolean multi cast type dependent field creation"
            elif f_type == FORMULA_FIELD_TYPE:
                if not f.options.get("formula_options", {}):
                    ret_val = False
                    formula_options = f.options.get("formula_options", {})
                    err_str = f"invalid formula options {formula_options} in formula type dependent field creation"
                else:
                    formula_options = f.options.get("formula_options", {})
                    if not formula_options.get("formula_field_map", {}):
                        ret_val = False
                        formula_field_map = formula_options.get("formula_field_map", {})
                        err_str = (
                            f"invalid formula field map {formula_field_map} "
                            f"in formula type dependent field creation"
                        )
                    else:
                        formula_str = formula_options.get("formula_str", "")
                        ret_val, err_str = validate_formula(formula_str)

    return ret_val, err_str
