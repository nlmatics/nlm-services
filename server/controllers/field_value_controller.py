import logging
import traceback

import connexion
from flask import jsonify
from flask import make_response

from server import err_response
from server.models.field_value import FieldValue  # noqa: E501
from server.storage import nosql_db
from server.utils import graph_utils
from server.utils import str_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_field_value(user, token_info, body=None):  # noqa: E501
    """Add New User defined Field (no bundle)

     # noqa: E501

    :param user:
    :param token_info:
    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        user_json = token_info.get("user_obj", None)
        field_value = FieldValue.from_dict(connexion.request.get_json())  # noqa: E501
        bundle_info = nosql_db.get_field_bundle_info(field_value.field_bundle_id)
        user_permission, _ws = nosql_db.get_user_permission(
            field_value.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"] or (
            user_permission == "viewer" and bundle_info.user_id != user_json["id"]
        ):
            err_str = "Not authorized to create field value"
            log_str = f"user {user} not authorized to create field value in workspace {field_value.workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        created_time = str_utils.timestamp_as_str()
        field_value.created_on = created_time
        field_value.id = str_utils.generate_field_id(
            field_value.created_on + field_value.field_id + field_value.workspace_id,
        )
        field_value.active = True

        name = f"{user_json['first_name']} {user_json['last_name']}"

        existing_field_values = nosql_db.read_extracted_field(
            {"field_idx": field_value.field_id, "file_idx": field_value.doc_id},
            {"top_fact"},
        )
        previous_selected_row = None
        if len(existing_field_values) > 1:
            previous_selected_row = existing_field_values[0].get("top_fact", None)

        history = {
            "username": name,
            "edited_time": created_time,
            "previous": previous_selected_row,
            "modified": field_value.selected_row,
        }
        if not field_value.history:
            field_value.history = []
        field_value.history.append(history)
        # Retrieve document name if not received
        if not field_value.doc_name:
            filter_params = {
                "_id": 0,
                "name": 1,
            }
            document = nosql_db.get_document(
                field_value.workspace_id,
                field_value.doc_id,
                filter_params,
            )
            field_value.doc_name = document.name

        newly_created_field_id = nosql_db.create_field_value(field_value)

        if newly_created_field_id:  # TODO: test case needed
            field = nosql_db.get_field_by_id(field_value.field_id)
            if field and field.options and field.options.get("child_fields", []):
                # Update the child field values.
                child_fields = field.options.get("child_fields", [])
                for child_field_id in child_fields:
                    child_field = nosql_db.get_field_by_id(child_field_id)
                    if (
                        child_field
                        and child_field.is_entered_field
                        and child_field.is_dependent_field
                        and child_field.options
                    ):
                        logger.info(
                            f"Updating dependent field {child_field_id} for document {field_value.doc_id}",
                        )
                        nosql_db.create_fields_dependent_workflow_field_values(
                            child_field.workspace_id,
                            child_field.parent_bundle_id,
                            child_field_id,
                            child_field.options,
                            name,
                            created_time,
                            file_idx=field_value.doc_id,
                        )

            logger.info(f"new field value with id {newly_created_field_id} created")
            return make_response(jsonify(field_value), 200)
        else:
            logger.error(
                f"unknown error creating field value, got 'null' as id {newly_created_field_id}",
            )
            _, rc, msg = "fail", 500, "unknown status creating field"
    else:
        _, rc, msg = "fail", 422, "invalid json"
    return err_response(msg, rc)


def get_field_value(
    user,
    token_info,
    workspace_id: str,
    field_bundle_id: str,
    doc_id: str,
    field_id: str,
):
    try:
        bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
        if bundle_info.workspace_id != workspace_id:
            err_str = "Workspace ID mismatch "
            log_str = (
                f"Provided workspace ID {workspace_id} and retrieved workspace ID "
                f"{bundle_info.workspace_id} mismatch "
            )
            logger.info(log_str)
            return err_response(err_str, 403)
        user_permission, _ws = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None) if token_info else None,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = f"Not authorized to retrieve field value for {workspace_id}"
            log_str = (
                f"user {user} not authorized to retrieve field value for "
                f"workspace {bundle_info.workspace_id}"
            )
            logger.info(log_str)
            return err_response(err_str, 403)

        existing_field_values = nosql_db.read_extracted_field(
            {
                "field_idx": field_id,
                "file_idx": doc_id,
                "workspace_idx": workspace_id,
                "field_bundle_idx": field_bundle_id,
            },
            {
                "_id": 0,
                "topic_facts": 0,
            },
        )
        if existing_field_values and len(existing_field_values) == 1:
            return make_response(jsonify(existing_field_values[0]), 200)
        else:
            err_str = "No Value or More than 1 value retrieved "
            log_str = f"No Value or More than 1 value retrieved for {workspace_id}:{field_bundle_id}:{doc_id}:{field_id}"
            logger.info(log_str)
            return err_response(err_str, 403)
    except Exception as e:
        logger.error(
            f"error extracting values for field bundle {field_bundle_id} from document {doc_id}, "
            f"error: {traceback.format_exc()}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}))


def bulk_approve(
    user,
    token_info,
    field_id=None,
    field_bundle_id=None,
    workspace_id=None,
    doc_id=None,
    body=None,
):
    query = {}
    err_str = "Not authorized to bulk approve"
    if field_bundle_id:
        query["field_bundle_idx"] = field_bundle_id
    elif field_id:
        query["field_idx"] = field_id
    else:
        return err_response("Missing parameter for fieldId or fieldBundleId", 400)

    if workspace_id:
        query["workspace_idx"] = workspace_id
        user_permission, _ws = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None) if token_info else None,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            log_str = f"user {user} not authorized to bulk approve in workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)
    elif doc_id:
        query["file_idx"] = doc_id
        document = nosql_db.get_document_info_by_id(doc_id)
        user_permission, _ws = nosql_db.get_user_permission(
            document.workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None) if token_info else None,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            log_str = f"user {user} not authorized to bulk approve in workspace {document.workspace_id}"
            logger.error(log_str)
            return err_response(err_str, 403)
    else:
        return err_response("Missing parameter for docId or workspaceId", 400)

    modified_count = nosql_db.bulk_approve_field_value(query=query)
    return make_response(jsonify({"modified_count": modified_count}), 200)


def bulk_disapprove(
    user,
    token_info,
    field_id=None,
    field_bundle_id=None,
    workspace_id=None,
    doc_id=None,
    body=None,
):
    query = {}
    err_str = "Not authorized to bulk disapprove"
    if field_bundle_id:
        query["field_bundle_idx"] = field_bundle_id
    elif field_id:
        query["field_idx"] = field_id
    else:
        return err_response("Missing parameter for fieldId or fieldBundleId", 400)

    if workspace_id:
        query["workspace_idx"] = workspace_id
        user_permission, _ws = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None) if token_info else None,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            log_str = f"user {user} not authorized to bulk disapprove in workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)
    elif doc_id:
        query["file_idx"] = doc_id
        document = nosql_db.get_document_info_by_id(doc_id)
        user_permission, _ws = nosql_db.get_user_permission(
            document.workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None) if token_info else None,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            log_str = f"user {user} not authorized to bulk disapprove in workspace {document.workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)
    else:
        return err_response("Missing parameter for docId or workspaceId", 400)

    modified_count = nosql_db.bulk_disapprove_field_value(query=query)
    return make_response(jsonify({"modified_count": modified_count}), 200)


def delete_field_values_by_field_id(
    user,
    token_info,
    field_id,
    doc_id,
    field_bundle_id: str = None,
):  # noqa: E501
    """Add New User defined Field (no bundle)

     # noqa: E501

    :param doc_id:
    :param field_id:
    :param field_bundle_id:
    :param token_info:
    :param user:

    :rtype: IdWithMessage
    """
    try:
        field = nosql_db.get_field_by_id(field_id)
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
            err_str = "Not authorized to delete field value"
            log_str = f"user {user} not authorized to delete field value in workspace {field.workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        extracted_top_fact = nosql_db.delete_field_value(
            field_id,
            doc_id,
            field.workspace_id,
            field_bundle_idx=field_bundle_id,
        )

        logger.info(f"all overridden field values for field {field_id} deleted")
        field = nosql_db.get_field_by_id(field_id)
        if field and field.options and field.options.get("child_fields", []):
            # Update the child field values.
            child_fields = field.options.get("child_fields", [])
            created_time = str_utils.timestamp_as_str()
            name = f"{user_json['first_name']} {user_json['last_name']}"
            for child_field_id in child_fields:
                child_field = nosql_db.get_field_by_id(child_field_id)
                if (
                    child_field
                    and child_field.is_entered_field
                    and child_field.is_dependent_field
                    and child_field.options
                ):
                    logger.info(
                        f"Updating dependent field {child_field_id} for document {doc_id}",
                    )
                    nosql_db.create_fields_dependent_workflow_field_values(
                        child_field.workspace_id,
                        child_field.parent_bundle_id,
                        child_field_id,
                        child_field.options,
                        name,
                        created_time,
                        file_idx=doc_id,
                    )
        return make_response(
            jsonify({"top_fact": extracted_top_fact}),
            200,
        )
    except Exception as e:
        logger.error(
            f"error deleting field values for field with id {field_id}, err: {str(e)}",
        )
        status, rc, msg = "fail", 500, str(e)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_relation_edge_topic_facts(
    user,
    token_info,
    field_id,
    relation_head,
    relation_tail,
):
    try:
        field = nosql_db.get_field_by_id(field_id)
        user_json = token_info.get("user_obj", None) if token_info else None
        user_permission, _ws = nosql_db.get_user_permission(
            field.workspace_id,
            email=user,
            user_json=user_json,
        )
        print("user_permission: ", user_permission)
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to retrieve relation field tree"
            log_str = f"user {user} not authorized to retrieve relation field tree in workspace {field.workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        topic_facts = nosql_db.get_relation_edge_topic_facts(
            field_id,
            relation_head,
            relation_tail,
        )
        return make_response(
            jsonify(topic_facts),
            200,
        )
    except Exception as e:
        logger.error(
            f"error retrieving field topic facts for field with id {field_id}, relation_head {relation_head}, relation_tail: {relation_tail} err: {traceback.format_exc()}",
        )
        status, rc, msg = "fail", 500, str(e)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_relation_field_graph(user, token_info, field_id, refresh=False):
    try:
        field = nosql_db.get_field_by_id(field_id)
        user_json = token_info.get("user_obj", None) if token_info else None
        user_permission, _ws = nosql_db.get_user_permission(
            field.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to retrieve relation field graph"
            log_str = f"user {user} not authorized to retrieve relation field graph in workspace {field.workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        saved_graph = nosql_db.get_saved_graph(field.workspace_id, field_id)
        if saved_graph and "graph_json" in saved_graph and not refresh:
            logger.info(f"returning saved graph {field_id}")
            graph_json = saved_graph["graph_json"]
        else:
            logger.info(f"building relation graph for field {field_id}")
            existing_field_values = nosql_db.read_extracted_field(
                {"field_idx": field_id, "file_idx": "all_files"},
                {"_id": 0, "topic_facts": 1},
            )
            topic_facts = []
            for item in existing_field_values:
                topic_facts.extend(item.get("topic_facts", []))
            graph_json = graph_utils.get_relation_field_graph_json(field, topic_facts)
            nosql_db.save_graph(field.workspace_id, field_id, graph_json)
            logger.info(
                f"built graph for relation field {field_id} with {len(topic_facts)} facts",
            )
        return make_response(
            jsonify(graph_json),
            200,
        )
    except Exception as e:
        logger.error(
            f"error retrieving field graph for field with id {field_id}, err: {str(e)}",
        )
        status, rc, msg = "fail", 500, str(e)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_relation_field_tree(user, token_info, field_id, refresh=False):
    try:
        field = nosql_db.get_field_by_id(field_id)
        user_json = token_info.get("user_obj", None) if token_info else None
        user_permission, _ws = nosql_db.get_user_permission(
            field.workspace_id,
            email=user,
            user_json=user_json,
        )
        print("user_permission: ", user_permission)
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to retrieve relation field tree"
            log_str = f"user {user} not authorized to retrieve relation field tree in workspace {field.workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        saved_graph = nosql_db.get_saved_graph(field.workspace_id, field_id)
        if saved_graph and "tree_json" in saved_graph and not refresh:
            logger.info(f"returning saved tree {field_id}")
            tree_json = saved_graph["tree_json"]
        else:
            logger.info(f"building relation tree for field {field_id}")
            existing_field_values = nosql_db.read_extracted_field(
                {"field_idx": field_id, "file_idx": "all_files"},
                {"_id": 0, "topic_facts": 1},
            )
            topic_facts = []
            for item in existing_field_values:
                topic_facts.extend(item.get("topic_facts", []))
            tree_json = graph_utils.get_relation_field_tree_json(field, topic_facts)
            nosql_db.save_graph(
                field.workspace_id,
                field_id,
                tree_json,
                json_label="tree_json",
            )
            logger.info(
                f"built tree for relation field {field_id} with {len(topic_facts)} facts",
            )
        return make_response(
            jsonify(tree_json),
            200,
        )
    except Exception as e:
        logger.error(
            f"error retrieving field tree for field with id {field_id}, err: {traceback.format_exc()}",
        )
        status, rc, msg = "fail", 500, str(e)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


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
            err_str = "Not authorized to view field value"
            log_str = f"user {user} not authorized to view field value in workspace {field.workspace_id}"
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


def get_workspace_knowledge_graph(
    user,
    token_info,
    workspace_id,
    selected_node=None,
    refresh=False,
    depth=2,
):
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

        saved_graph = nosql_db.get_saved_graph(workspace_id, None)
        if saved_graph and not refresh:
            logger.info(
                f"returning saved knowledge graph {workspace_id} with depth {depth}",
            )
            graph_json = saved_graph["graph_json"]
        else:
            logger.info(f"rebuilding knowledge graph {workspace_id}")
            graph_json = graph_utils.get_knowledge_graph_json(workspace_id)
            nosql_db.save_graph(workspace_id, None, graph_json)

        if selected_node:
            result_json = graph_utils.get_knowledge_tree_json(
                graph_json,
                selected_node,
                depth=depth,
            )
        else:
            result_json = graph_json
        logger.info(
            f"returning knowledge graph for workspace {workspace_id}",
        )
        return make_response(
            jsonify(result_json),
            200,
        )
    except Exception as e:
        logger.error(
            f"error retrieving knowledge graph for workspace with id {workspace_id}, err: {str(e)}",
        )
        status, rc, msg = "fail", 500, str(e)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def autocomplete_relation_node(
    user,
    token_info,
    workspace_id,
    field_id=None,
    search_text="",
):
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
        search_text = graph_utils.normalize_node_text(search_text)
        matching_node_labels = nosql_db.autocomplete_relation_node(
            workspace_id,
            field_id,
            search_text,
        )

        return make_response(
            jsonify(matching_node_labels),
            200,
        )
    except Exception as e:
        logger.error(
            f"error retrieving knowledge graph for workspace with id {workspace_id}, err: {str(e)}",
        )
        status, rc, msg = "fail", 500, str(e)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def build_stats_table(
    user,
    token_info,
    workspace_id: str,
    field_bundle_id: str,
):
    try:
        bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
        if bundle_info.workspace_id != workspace_id:
            err_str = "Workspace ID mismatch "
            log_str = (
                f"Provided workspace ID {workspace_id} and retrieved workspace ID "
                f"{bundle_info.workspace_id} mismatch"
            )
            logger.info(log_str)
            return err_response(err_str, 403)
        user_permission, _ws = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None) if token_info else None,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = f"Not authorized to retrieve field value for {workspace_id}"
            log_str = (
                f"user {user} not authorized to retrieve field value for "
                f"workspace {bundle_info.workspace_id}"
            )
            logger.info(log_str)
            return err_response(err_str, 403)

        projection = {
            "_id": 0,
            "id": 1,
            "name": 1,
            "is_entered_field": 1,
        }
        fields = nosql_db.get_fields_in_bundle(
            field_bundle_id,
            projection=projection,
            return_dict=True,
        )
        non_entered_fields = {
            field["id"]: field["name"]
            for field in fields
            if not field["is_entered_field"]
        }

        stats = nosql_db.build_field_value_stats(
            workspace_id,
            field_bundle_id,
            list(non_entered_fields.keys()),
        )
        for col_stat in stats["colStats"]:
            if col_stat["_id"] in non_entered_fields:
                col_stat["fieldName"] = non_entered_fields[col_stat["_id"]]

        stats["nFieldsPerDocument"] = len(non_entered_fields)
        return make_response(jsonify(stats), 200)
    except Exception as e:
        logger.error(
            f"error constructing stats for field bundle {field_bundle_id}"
            f"error: {traceback.format_exc()}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}))
