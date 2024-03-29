import logging
import os
import tempfile

import connexion
from flask import jsonify
from flask import make_response
from nlm_ingestor.ingestor import data_loader
from nlm_utils.storage import file_storage
from nlm_utils.utils import ensure_bool
from werkzeug.utils import secure_filename

from server import err_response
from server import unauthorized_response
from server.controllers.document_controller import delete_document_by_id
from server.controllers.document_controller import upload_data_row_file
from server.controllers.field_bundle_controller import (
    replicate_field_bundles_between_workspaces,
)
from server.controllers.subscription_controller import create_stripe_subscription
from server.controllers.subscription_controller import DEFAULT_SUBSCRIPTION_TRIAL_DAYS
from server.models.field_bundle import FieldBundle
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.models.ignore_block import IgnoreBlock
from server.models.workspace import Workspace  # noqa: E501
from server.storage import nosql_db
from server.utils import str_utils
from server.utils.metric_utils import update_metric_data
from server.utils.notification_utils import send_workspace_delete_notification
from server.utils.notification_utils import send_workspace_update_notification
from server.utils.str_utils import timestamp_as_utc_str
from server.utils.indexer_utils.es_client import es_client

MODIFIABLE_WORKSPACE_SETTINGS = {
    "domain": "domain",
    "search_settings": "search_settings",
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
SEND_NOTIFICATIONS = ensure_bool(os.getenv("SEND_NOTIFICATIONS", False))
UPDATE_USAGE_METRICS = ensure_bool(os.getenv("UPDATE_USAGE_METRICS", False))
DEFAULT_WORKSPACE_DOMAIN = os.getenv("DEFAULT_WORKSPACE_DOMAIN", "general")


try:
    from server.utils.indexer_utils.indexer_ops import add_synonym_dictionary, update_document_meta
except ImportError:
    logger.error(
        "nlm-services-v2 is running without ingestor. Functions related to ingestion are limited.",
        exc_info=True,
    )


def create_new_workspace(body, user, token_info):  # noqa: E501
    """Creates a new workspace for a user

     # noqa: E501

    :param token_info:
    :param user:
    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        re = connexion.request.get_json()
        workspace = Workspace.from_dict({"name": re["name"]})  # noqa: E501
        user_obj = token_info.get("user_obj", None)
        if not user_obj:
            return unauthorized_response()
        workspace.user_id = workspace.user_id or user_obj["id"]
        if user_obj["id"] != workspace.user_id:
            err_str = "Not authorized to create a workspace"
            log_str = f"{user} not authorized to create workspace"
            logger.info(log_str)
            return err_response(err_str, 403)

        workspace.shared_with = re["sharedWith"] if "sharedWith" in re else []
        collaborators = re["collaborators"] if "collaborators" in re else {}
        # Default settings for search
        if (
            not workspace.settings.get("search_settings", {})
            or workspace.settings["search_settings"].get("table_search", None) is None
        ):
            workspace.settings["search_settings"] = {
                "table_search": False,
            }

        workspace.statistics = {
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
        status, rc, msg = create_workspace(user_obj, workspace, collaborators)
        if status != "fail":
            return make_response(
                jsonify(IdWithMessage(status, msg)),
                rc,
            )
    else:
        status, rc, msg = "fail", 422, "invalid json"
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def create_workspace(
    user_json,
    workspace,
    collaborators=None,
    create_default_field_bundle=True,
):  # noqa: E501
    """Creates a new workspace for a user
    :param user_json: User Profile JSON
    :param workspace: Workspace object
    :param collaborators: Collaborators if any
    :param create_default_field_bundle: Create default field bundle unless specified otherwise.

    :rtype: status (workspace_id, if successful), response_code, message.
    """
    collaborators = collaborators or {}
    workspace.id = str_utils.generate_workspace_id(
        workspace.user_id,
        workspace.name,
    )
    workspace.created_on = workspace.created_on or str_utils.timestamp_as_str()
    # Override the settings if we need to copy the application level settings.
    app_name = user_json.get("app_name", "")
    if app_name:
        # Retrieve the application specific settings.
        app_settings = nosql_db.get_application_settings(app_name)
        create_workspace_with_app_settings = app_settings.get("workspace", {}).get(
            "create_workspace_with_app_settings",
            False,
        )
        if create_workspace_with_app_settings:
            default_workspace_settings = app_settings.get("workspace", {}).get(
                "default_settings",
                {
                    "domain": DEFAULT_WORKSPACE_DOMAIN,
                    "search_settings": {
                        "table_search": False,
                    },
                },
            )
            index = default_workspace_settings.get("index_settings", {}).get(
                "index",
                "",
            )
            if index and index.endswith("_"):
                last_used_es_index = (
                    nosql_db.get_nlm_settings("last_used_es_index") or 0
                )
                default_workspace_settings["index_settings"]["index"] = index + str(
                    last_used_es_index,
                )
                max_indices_allowed = app_settings.get("workspace", {}).get(
                    "max_indices_to_create",
                    300,
                )
                nosql_db.update_nlm_settings(
                    "last_used_es_index",
                    (last_used_es_index + 1) % max_indices_allowed,
                )

            workspace.settings = default_workspace_settings

    logger.info(f"creating workspace {workspace}")
    workspace.active = True
    try:
        existing_workspaces = nosql_db.get_all_workspaces(user_id=user_json["id"])
        existing_ws_list = [(ws.name, ws.user_id) for ws in existing_workspaces]
        if (workspace.name, workspace.user_id) in existing_ws_list:
            status, rc, msg = "fail", 200, "workspace already exists"
        else:
            workspace_id = nosql_db.create_workspace(workspace)
            if collaborators:
                workspace.id = workspace_id
                workspace = _change_permission(
                    user_json["email_id"],
                    collaborators,
                    workspace,
                    new_workspace=True,
                )
                nosql_db.update_workspace(workspace_id, workspace)

            if create_default_field_bundle:
                # create default field bundle
                timestamp = str_utils.timestamp_as_str()
                bundle = {
                    "id": str_utils.generate_unique_fieldbundle_id(
                        workspace.user_id,
                        workspace_id,
                        "default",
                    ),
                    "cachedFile": None,
                    "userId": workspace.user_id,
                    "workspaceId": workspace_id,
                    "createdOn": timestamp,
                    "active": True,
                    "fieldIds": [],
                    "tags": None,
                    "bundleName": "Default Field Set",
                    "parentBundleId": None,
                    "bundleType": "DEFAULT",
                }
                bundle_id = nosql_db.create_field_bundle(FieldBundle.from_dict(bundle))
                logger.info(
                    f"Default field set for {workspace_id} created: {bundle_id}",
                )
            # Update or creates a preferred_workspace entry for the user.
            nosql_db.update_prefered_workspace(user_json["id"], workspace_id)

            if workspace_id:
                if UPDATE_USAGE_METRICS:
                    update_metric_data(user_json, [("num_workspaces", 1)])
                logger.info(f"workspace created with id: {workspace_id}")
                status, rc, msg = workspace_id, 200, "workspace created"
            else:
                logger.error("unable to create workspace, unknown error")
                status, rc, msg = "fail", 500, "unknown error"
    except Exception as e:
        logger.error(
            f"error creating workspace with id {workspace.id}, err: {str(e)}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, str(e)
    return status, rc, msg


def delete_workspace_by_id(
    workspace_id,
    user,
    token_info,
    permanent=False,
):  # noqa: E501
    """Deletes the workspace for given id

     # noqa: E501

    :param user:
    :param token_info:
    :param workspace_id:
    :type workspace_id: str
    :param permanent: Delete Permanently the WS.

    :rtype: IdWithMessage
    """
    try:
        workspace_obj = nosql_db.get_workspace_by_id(workspace_id)
        ws_shared_with = workspace_obj.shared_with
        ws_collaborators = workspace_obj.collaborators
        user_obj = token_info.get("user_obj", None)
        if workspace_obj:
            if (
                not user_obj
                or workspace_obj.user_id != user_obj["id"]
                and (user_obj["is_admin"] is None or user_obj["is_admin"] is False)
            ):
                logger.error(
                    f"{user} not authorized to delete workspace for user {workspace_obj.user_id}",
                )
                return make_response(
                    jsonify({"status": "fail", "reason": "unauthorized"}),
                    403,
                )
            # TODO: To allow deletion, we need to send notification to the other users.
            if SEND_NOTIFICATIONS:
                send_workspace_delete_notification(
                    user_obj,
                    workspace_obj,
                    ws_collaborators.keys(),
                )
            if len(ws_shared_with) != 0 and len(ws_collaborators) != 0:
                logger.error(
                    f"{workspace_obj.name} is shared with {len(ws_shared_with)} others. Cannot proceed with deletion",
                )
                return make_response(
                    jsonify(
                        {
                            "status": "fail",
                            "reason": f"{workspace_obj.name} is shared with {len(ws_shared_with)} others",
                        },
                    ),
                    403,
                )
        # ws_default = nosql_db.get_default_workspace_for_user_id(user_obj.id)
        # logger.info(f"workspace default is {ws_default}")
        # if nosql_db.get_prefered_workspace(user_obj.id):
        #     if nosql_db.get_prefered_workspace(user_obj.id).id == workspace_id:
        #         nosql_db.update_prefered_workspace(user_obj.id, ws_default.id)
        #         logger.info(
        #             f"prefered workspace for {user_obj.id} set to {ws_default.id}",
        #         )
        if permanent:
            # Retrieve the documents and delete them permanently
            doc_projection = {
                "_id": 0,
                "id": 1,
            }
            documents = nosql_db.get_folder_contents(
                workspace_id,
                "root",
                projection=doc_projection,
            )["documents"]
            metric_data = [
                ("num_docs", 0),
                ("num_pages", 0),
                ("doc_size", 0),
            ]
            for doc in documents:
                doc_metric = delete_document_by_id(
                    user,
                    token_info,
                    doc.id,
                    permanent=True,
                    update_metric=False,
                )
                for idx, (m_data, m1_data) in enumerate(zip(metric_data, doc_metric)):
                    metric_data[idx] = (m_data[0], m_data[1] + m1_data[1])

            # Retrieve Field bundles
            logger.info(f"Deleting Field bundles and fields for {workspace_id}")
            bundles = nosql_db.get_field_bundles_in_workspace(workspace_id)
            num_fields = 0
            for f_bundle in bundles:
                # Delete the fields
                field_ids = f_bundle.field_ids
                num_fields += len(field_ids)
                for field_id in field_ids:
                    nosql_db.delete_field_by_field_id(field_id, update_bundle=False)
                # Delete the field bundle
                nosql_db.delete_field_bundle(f_bundle.id)
            # Delete the history.
            for action in ["uploaded_document", "opened_document"]:
                nosql_db.delete_file_history_in_workspace(
                    user_obj["id"],
                    workspace_id,
                    action,
                )
            logger.info(f"Deleting ES index {workspace_id}")
            # Delete the ES Index
            es_client.delete_index(
                workspace_id,
                workspace_settings=workspace_obj.settings,
            )
            # Delete any left overs in Storage.
            file_storage.delete_files([f"{user_obj['id']}/{workspace_id}"])
            # Add the num_fields metric
            metric_data.append(("num_fields", -num_fields))
            metric_data.append(("num_workspaces", -1))
            if UPDATE_USAGE_METRICS:
                # Update the metrics
                update_metric_data(
                    user_obj,
                    metric_data,
                )
        idx = nosql_db.delete_workspace(workspace_id, permanent)
        if idx:
            logger.info(f"workspace with id {idx} deleted")
            return make_response(jsonify(IdWithMessage(idx, "workspace deleted")), 200)
        else:
            logger.error(f"unknown error deleting workspace with id {workspace_id}")
            status, rc, msg = "fail", 500, "result of workspace deletion unknown"
    except Exception as e:
        logger.error(f"error deleting workspace with id {workspace_id}, err: {str(e)}")
        status, rc, msg = "fail", 500, str(e)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def undo_delete_workspace(
    user,
    token_info,
    workspace_id,
):
    user_permission, _ws = nosql_db.get_user_permission(
        workspace_id,
        email=user,
        user_json=token_info.get("user_obj", None) if token_info else None,
    )
    if user_permission not in ["admin", "owner"]:
        err_str = "Not authorized to undo delete workspace"
        log_str = f"user {user} not authorized to undo delete workspace {workspace_id}"
        logger.info(log_str)
        return err_response(err_str, 403)

    nosql_db.undo_delete_workspace(workspace_id)
    return make_response(
        jsonify(IdWithMessage(workspace_id, "workspace restored")),
        200,
    )


def get_archived_workspace(user, token_info):
    user_id = token_info["user_obj"]["id"]
    return nosql_db.get_archived_workspaces(user_id)


def get_all_workspaces(
    user,
    token_info,
):  # noqa: E501
    """Returns list of all workspaces

     # noqa: E501


    :rtype: List[Workspace]
    """
    try:
        user_json = token_info.get("user_obj", None)
        if not user_json or not user_json.get("is_admin", False):
            err_str = "Not authorized"
            log_str = f"user {user} not authorized to retrieve all workspaces"
            logger.info(log_str)
            return err_response(err_str, 403)
        workspace_list = nosql_db.get_all_workspaces()
        logger.info(f"{len(workspace_list)} workspaces returned")
        return make_response(jsonify(workspace_list), 200)
    except Exception as e:
        logger.error(f"error retrieving workspaces, err: {str(e)}")
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def get_workspace_by_id(
    workspace_id,
    user,
    token_info,
    fetch_content=False,
):  # noqa: E501
    """Return workspace for given workspaceId

     # noqa: E501

    :param user:
    :param token_info:
    :param workspace_id:
    :type workspace_id: str

    :rtype: Workspace
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

        if workspace:
            workspace.subscribed_users = []
            logger.info(f"found workspace with id {workspace_id}")
            return make_response(jsonify(workspace), 200)
        else:
            logger.error(f"workspace with id {id} not found")
            return make_response(
                jsonify({"status": "fail", "reason": "workspace not found"}),
                404,
            )

    except Exception as e:
        logger.error(
            f"error retrieving workspace with id {workspace_id}, err: {str(e)}",
            exc_info=True,
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def get_workspaces_by_user_id(user_id, user, token_info):  # noqa: E501
    """Return list of workspace for user

     # noqa: E501

    :param user_id:
    :type user_id: str
    :param token_info

    :rtype: List[Workspace]
    """
    try:
        defaultUser = os.getenv("DEFAULT_USER", "default@nlmatics.com")
        # !!!!WARNING - DO NOT CHANGE THIS LINE WITHOUT REVIEW
        if (
            # user_id == "defaultUser"
            defaultUser
            and os.getenv("AUTH_PROVIDER", None) == "fakeauth"
        ):
            db_user = nosql_db.get_user_by_email(defaultUser)
            user_id = db_user.id
            user = defaultUser

        if token_info["user_obj"]["id"] != user_id:
            logger.error(
                f"{user} is not authorized to retrieve workspaces for user with id {user_id}",
            )
            return make_response(
                jsonify({"status": "fail", "reason": "unauthorized"}),
                404,
            )

        user_workspaces, ws_dict = nosql_db.get_workspaces_for_user(
            user,
            user_id,
            user_profile=token_info["user_obj"],
        )
        logger.info(f"{len(user_workspaces)} workspaces found for user {user_id}")
        return make_response(jsonify(ws_dict), 200)
    except Exception as e:
        logger.info(
            f"error retrieving workspaces for user with id {user_id}, err: {str(e)}",
            exc_info=True,
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def update_workspace_by_id(user, token_info, workspace_id):  # noqa: E501
    """Updates the workspace for given id

     # noqa: E501

    :param token_info:
    :param user:
    :param workspace_id:
    :type workspace_id: str

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        try:
            user_permission, _ws = nosql_db.get_user_permission(
                workspace_id,
                email=user,
                user_json=token_info.get("user_obj", None),
            )
            if user_permission not in ["admin", "owner", "editor"]:
                err_str = "Not authorized to update workspace"
                log_str = (
                    f"user {user} not authorized to update workspace {workspace_id}"
                )
                logger.info(log_str)
                return err_response(err_str, 403)

            body = connexion.request.get_json()
            orig_workspace = nosql_db.get_workspace_by_id(workspace_id)
            logger.info(f"received new workspace data {body}")
            # new_workspace.id = orig_workspace.idw2
            orig_workspace.name = body.get("name", "") or orig_workspace.name
            orig_workspace.shared_with = (
                body["sharedWith"] if "sharedWith" in body else []
            )
            collaborators = body.get("collaborators", {})
            orig_collaborators = orig_workspace.collaborators
            orig_workspace = _change_permission(
                user,
                collaborators,
                orig_workspace,
                user_permission=user_permission,
            )
            idx = nosql_db.update_workspace(workspace_id, orig_workspace)

            if idx:
                if SEND_NOTIFICATIONS:
                    # Send Notifications
                    send_workspace_update_notification(
                        token_info.get("user_obj", None),
                        orig_workspace,
                        orig_collaborators,
                        collaborators,
                    )
                logger.info(
                    f"workspace with id {workspace_id} updated to {orig_workspace}",
                )
                return make_response(
                    jsonify(IdWithMessage(workspace_id, "workspace updated")),
                )
            else:
                logger.error(
                    f"unknown status while updating workspace with id {workspace_id}",
                )
                status, rc, msg = "fail", 500, "unknown status"

        except Exception as e:
            logger.error(
                f"error updating workspace with id {workspace_id}, err: {str(e)}",
                exc_info=True,
            )
            status, rc, msg = "fail", 500, str(e)
    else:
        logger.error("invalid json in request")
        status, rc, msg = "fail", 422, "invalid json in request"
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def upload_data(
    user,
    token_info,
    workspace_id=None,
    file_name_column=1,
    title_start_column=1,
    title_end_column=3,
    file=None,
):
    try:
        user_obj = token_info.get("user_obj", None)
        if not user_obj:
            return unauthorized_response()
        workspace = nosql_db.get_workspace_by_id(workspace_id)
        if not workspace:
            logger.error(
                f"unknown status while updating workspace with id {workspace_id}",
            )
            status, rc, msg = "fail", 500, "workspace does not exist"
            return make_response(jsonify({"status": status, "reason": msg}), rc)
        user_id = workspace.user_id
        user_permission, _ws = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=user_obj,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to upload to workspace"
            log_str = (
                f"user {user} not authorized to upload to workspace {workspace_id}"
            )
            logger.info(log_str)
            return err_response(err_str, 403)

        # unauthorized = check_workspace_access(user, workspace_id)
        # if unauthorized:
        #     logger.error(f"{user} not authorized to upload to workspace {workspace_id}")
        #     status, rc, msg = "fail", 500, "unauthorized access"
        #     return make_response(jsonify({"status": status, "reason": msg}), rc)

        filename = secure_filename(file.filename)
        tempfile_handler, tmp_file = tempfile.mkstemp(filename)
        os.close(tempfile_handler)
        file.save(tmp_file)
        dl = data_loader.DataLoader(
            file_name=tmp_file,
            filename_col=file_name_column,
            title_col_range=[title_start_column, title_end_column],
        )
        if tmp_file and os.path.exists(tmp_file):
            os.unlink(tmp_file)

        for row_file_info in dl.data_row_file_infos:
            logger.info(f"uploading file: {row_file_info.title}")
            upload_data_row_file(user_id, workspace_id, filename, row_file_info)

    except Exception as e:
        logger.error(
            f"Error while adding data rows to workspace {workspace_id}, err: {str(e)}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, f"{e}"
        return make_response(jsonify({"status": status, "reason": msg}), rc)


def add_private_dictionary(
    user,
    token_info,
    workspace_id,
    file=None,
):
    try:
        user_permission, workspace = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None),
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to add dictionary to workspace"
            log_str = f"user {user} not authorized to add dictionary to workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        if not workspace:
            logger.error(
                f"unknown status while updating workspace with id {workspace_id}",
            )
            status, rc, msg = "fail", 500, "unknown status"

        # make an entry into db
        filename = secure_filename(file.filename)
        tempfile_handler, tmp_file = tempfile.mkstemp(filename)
        os.close(tempfile_handler)
        file.save(tmp_file)

        synonym_dictionary = {}

        with open(tmp_file) as f:
            for line in f:
                tokens = line.rstrip().split(",")
                if len(tokens) < 1:
                    continue
                elif tokens[0].startswith("#"):
                    continue
                for i in range(len(tokens)):
                    tokens[i] = tokens[i].strip()
                # tokens must be sorted to make sure replacement function works properly
                synonym_dictionary[tokens[0]] = sorted(tokens, reverse=True)

        if tmp_file and os.path.exists(tmp_file):
            os.unlink(tmp_file)

        add_synonym_dictionary(synonym_dictionary, workspace_idx=workspace_id)

        # this will override existing dictionary, we may want to introduce an parameter for update
        workspace.settings["private_dictionary"] = synonym_dictionary

        nosql_db.update_workspace(workspace_id, workspace)

        return make_response(jsonify(IdWithMessage(workspace_id, "dictionary added")))
    except Exception as e:
        logger.error(
            f"Error during adding dictionary to workspace {workspace_id}, err: {str(e)}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, f"{e}"
        return make_response(jsonify({"status": status, "reason": msg}), rc)


def remove_private_dictionary(
    user,
    token_info,
    workspace_id,
):
    try:
        user_permission, workspace = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None),
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to remove dictionary from workspace"
            log_str = f"user {user} not authorized to remove dictionary from workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        if not workspace:
            logger.error(
                f"unknown status while updating workspace with id {workspace_id}",
            )
            status, rc, msg = "fail", 500, "unknown status"
            return make_response(jsonify({"status": status, "reason": msg}), rc)

        synonym_dictionary = {}

        add_synonym_dictionary(synonym_dictionary, workspace_idx=workspace_id)

        workspace.settings.pop("private_dictionary", None)

        nosql_db.update_workspace(workspace_id, workspace)

        return make_response(jsonify(IdWithMessage(workspace_id, "dictionary removed")))
    except Exception as e:
        logger.error(
            f"Error during removal of dictionary from workspace {workspace_id}, err: {str(e)}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, f"{e}"
        return make_response(jsonify({"status": status, "reason": msg}), rc)


def create_ignore_block(
    user,
    token_info,
    workspace_id,
):

    if not connexion.request.is_json:
        _, rc, msg = "fail", 422, "invalid json"
        return err_response(msg, rc)

    try:

        user_permission, workspace = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None),
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to create ignore block"
            log_str = f"user {user} not authorized to create ignore block workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        ignore_block = IgnoreBlock.from_dict(connexion.request.get_json())  # noqa:

        if not workspace:
            logger.error(
                f"unknown status while updating workspace with id {workspace_id}",
            )
            status, rc, msg = "fail", 500, "unknown status"

        ignore_block = {
            "text": ignore_block.ignore_text,
            "ignore_all_after": ignore_block.ignore_all_after,
            "level": ignore_block.block_type,
        }

        settings = workspace.settings.get("ignore_block", [])
        if not isinstance(settings, list):
            settings = []
        settings.append(ignore_block)

        workspace.settings["ignore_block"] = settings

        nosql_db.update_workspace(workspace_id, workspace)

        # re_ingest_documents_in_workspace(user=None, workspace_id=workspace_id)

        return make_response(jsonify(IdWithMessage(workspace_id, "ignore block added")))
    except Exception as e:
        logger.error(
            f"Error during adding ignore block to workspace {workspace_id}, err: {str(e)}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, f"{e}"
        return make_response(jsonify({"status": status, "reason": msg}), rc)


def update_workspace_settings(
    user,
    token_info,
    workspace_id,
):

    if not connexion.request.is_json:
        logger.error("update_workspace_settings: invalid json in request")
        _, rc, msg = "fail", 422, "invalid json in request"
        return err_response(msg, rc)

    try:

        user_permission, workspace = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None),
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to update workspace settings"
            log_str = f"user {user} not authorized to update workspace settings for {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        if not workspace:
            logger.error(
                f"No workspace with id {workspace_id} found",
            )
            status, rc, msg = "fail", 500, "unknown workspace"
            return make_response(jsonify({"status": status, "reason": msg}), rc)

        json_req_body = connexion.request.get_json()
        data_to_set = False
        for k in json_req_body.keys():
            if k not in MODIFIABLE_WORKSPACE_SETTINGS:
                msg = f"Cannot modify {k} for workspace settings"
                logger.error(msg)
                return make_response(
                    jsonify({"status": "fail", "reason": msg}),
                    422,
                )
            else:
                workspace.settings[MODIFIABLE_WORKSPACE_SETTINGS[k]] = json_req_body[k]
                data_to_set = True

        if data_to_set:
            nosql_db.update_workspace(workspace_id, workspace)

        logger.info(
            f"Workspace with id {workspace_id} updated with set_data {workspace.settings}",
        )

        return make_response(
            jsonify(IdWithMessage(workspace_id, "Workspace settings updated.")),
        )
    except Exception as e:
        logger.error(
            f"Error during adding ignore block to workspace {workspace_id}, err: {str(e)}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, f"{e}"
        return make_response(jsonify({"status": status, "reason": msg}), rc)


def add_user_permission(user, workspace_id, permission, email):
    # get attributes and permissions
    workspace = nosql_db.get_workspace_by_id(workspace_id)
    current_user_permission = get_user_permission(user, workspace_id, user)
    target_user_permission = get_user_permission(user, workspace_id, email)

    # does not allow users to change their own permissions
    if user == email:
        raise Exception(
            f'User {user} cannot modify own permission in workspace "{workspace.name}"',
        )

    # check current user have enough permissions to add users
    if current_user_permission == "owner" or current_user_permission == "editor":
        # editors cannot modiy other editors permissions
        if current_user_permission == "editor" and target_user_permission == "editor":
            raise Exception(
                f'User {user} does not have permission to modify permissions of user {email} in workspace "{workspace.name}"',
            )
        # assign permission to a new user or update existing
        workspace.collaborators[email] = permission
        nosql_db.update_workspace(workspace_id, workspace)
        return make_response(
            jsonify({"user_email": email, "permission": permission}),
            200,
        )
    else:
        raise Exception(
            f'User {user} does not have permission in workspace "{workspace.name}"',
        )


def _change_permission(
    user,
    collaborators,
    workspace,
    new_workspace=False,
    user_permission=None,
):
    """

    :param user:
    :param collaborators:
    :param workspace:
    :param new_workspace:
    :param user_permission: If its not a new_workspace, pass the user_permission
    :return:
    """
    workspace_id = workspace.id
    user_permission = user_permission if not new_workspace else "owner"
    workspace.collaborators = {}
    for c in collaborators:
        email = c
        permission = collaborators[c]
        # does not allow users to change their own permissions
        # check current user have enough permissions to add users
        if user_permission in ["admin", "owner", "editor"]:
            # editors cannot modify other editors permissions
            if user_permission == "editor" and permission == "editor":
                err_str = (
                    "Not authorized to modify permission settings in this workspace"
                )
                log_str = f"user {user} not authorized to modify permission settings in {workspace_id}"
                logger.info(log_str)
                return err_response(err_str, 403)

            # assign permission to a new user or update existing
            workspace.collaborators[email] = permission
            # nosql_db.update_workspace(workspace_id, new_workspace)
            # return make_response(
            #     jsonify({"user_email": email, "permission": permission}),
            #     200,
            # )
        else:
            err_str = "Not authorized to modify permission settings in this workspace"
            log_str = f"user {user} not authorized to modify permission settings in {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)
    return workspace


def get_user_permission(user, workspace_id, email):
    # user not in the database
    user_obj = nosql_db.get_user_by_email(email)
    if not user_obj:
        return ""

    workspace = nosql_db.get_workspace_by_id(workspace_id)
    user_id = user_obj.id
    user_domain = email.split("@")[1]

    if user_id == workspace.user_id:
        return "owner"
    elif (
        user_id in workspace.shared_with
        or user_domain in workspace.shared_with
        or "*" in workspace.shared_with
    ):
        # old implementation support
        return "editor"
    elif (
        user_id in workspace.collaborators
        or user_domain in workspace.collaborators
        or "*" in workspace.collaborators
    ):
        # new implementation
        return workspace.collaborators[user_id]
    else:
        # user does not have permission in this workspace
        return ""


def subscribe_workspace(token_info, workspace_id):
    """
    Subscribes a user to a given Workspace ID
    :param token_info: Token info as passed from the security module
    :param workspace_id: Workspace ID to subscribe to
    :return: ID of the workspace
    """
    user_profile = token_info.get("user_obj", None)

    try:
        subscribed_workspaces = user_profile.get("subscribed_workspaces", [])
        if workspace_id not in subscribed_workspaces:
            # Check and validate the workspace presence.
            workspace = nosql_db.get_workspace_by_id(
                workspace_id,
                remove_private_data=False,
            )
            if not workspace:
                return make_response(
                    jsonify({"status": "fail", "reason": "Invalid workspace_id"}),
                    500,
                )
            # Stripe Configuration is present. Handle the subscription separately.
            if workspace.stripe_conf and user_profile.get("stripe_conf", {}).get(
                "stripe_customer_id",
                "",
            ):
                if workspace_id not in user_profile.get(
                    "subscribed_workspaces",
                    [],
                ) and workspace_id not in user_profile.get("restricted_workspaces", []):
                    stripe_product_plan_id = workspace.stripe_conf.get(
                        "stripe_product_plan_id",
                        "",
                    )
                    if stripe_product_plan_id:
                        trial_days = workspace.settings.get(
                            "default_trial_period_days",
                            DEFAULT_SUBSCRIPTION_TRIAL_DAYS,
                        )
                        metadata = {
                            "app_name": user_profile.get("app_name", ""),
                            "nlm_user_id": user_profile["id"],
                            "stripe_product_plan_id": stripe_product_plan_id,
                            "resource": "workspace",
                            "resource_id": workspace_id,
                        }
                        subscription_id = create_stripe_subscription(
                            user_profile.get("stripe_conf", {}).get(
                                "stripe_customer_id",
                                "",
                            ),
                            stripe_product_plan_id,
                            trial_days,
                            metadata=metadata,
                        )
                        if subscription_id:
                            # Add workspace id to subscribed workspaces.
                            subscribed_workspaces.append(workspace_id)
                            set_data = {
                                "subscribed_workspaces": subscribed_workspaces,
                            }
                            stripe_subscriptions = user_profile.get(
                                "stripe_conf",
                                {},
                            ).get("subscriptions", [])
                            found_subs = False
                            for subs in stripe_subscriptions:
                                if stripe_product_plan_id == subs.get(
                                    "stripe_product_plan_id",
                                    "",
                                ):
                                    found_subs = True
                                    break
                            if not found_subs:
                                stripe_subscriptions.append(
                                    {
                                        "stripe_product_plan_id": stripe_product_plan_id,
                                        "stripe_subscription_id": subscription_id,
                                        "stripe_resource_id": workspace_id,
                                        "status": "trialing",
                                        "end_date": timestamp_as_utc_str(trial_days),
                                    },
                                )
                                set_data[
                                    "stripe_conf.subscriptions"
                                ] = stripe_subscriptions
                            nosql_db.update_user(
                                set_data,
                                user_id=user_profile["id"],
                            )
                            return make_response(
                                jsonify(
                                    IdWithMessage(workspace_id, "Subscription success"),
                                ),
                                200,
                            )
                else:
                    err_str = "Already subscribed to the workspace."
                    if workspace_id in user_profile.get("restricted_workspaces", []):
                        err_str = f"Trial Period is over. Subscribe now to access - {workspace.name}"
                    logger.info(
                        f"Not updating subscription status {workspace_id}, err: {err_str}",
                    )
                    return make_response(
                        jsonify(IdWithMessage(workspace_id, err_str)),
                        200,
                    )

            workspace_updated = False
            # Update the workspace only if public sharing is enabled.
            if user_profile["id"] not in workspace.subscribed_users and (
                "*" in workspace.collaborators or "*" in workspace.shared_with
            ):
                # Update the workspace
                workspace.subscribed_users.append(user_profile["id"])
                nosql_db.update_workspace(workspace.id, workspace)
                workspace_updated = True
            if workspace_updated:
                # Update the User profile
                subscribed_workspaces.append(workspace_id)
                nosql_db.update_user(
                    {"subscribed_workspaces": subscribed_workspaces},
                    user_id=user_profile["id"],
                )
                return make_response(
                    jsonify(IdWithMessage(workspace_id, "Subscription success")),
                    200,
                )
            else:
                err_str = "either user is already added to the subscription list or workspace is not a public one"
                logger.info(
                    f"Not updating subscription status {workspace_id}, err: {err_str}",
                )
                return make_response(
                    jsonify({"status": "fail", "reason": err_str}),
                    500,
                )
        else:
            return make_response(
                jsonify(IdWithMessage("error", "workspace already subscribed")),
                200,
            )
    except Exception as e:
        logger.error(
            f"Error during subscribing to workspace {workspace_id}, err: {str(e)}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, f"{e}"
        return make_response(jsonify({"status": status, "reason": msg}), rc)


def unsubscribe_workspace(token_info, workspace_id):
    """
    Unsubscribes a user from a given Workspace
    :param token_info: Token info as passed from the security module
    :param workspace_id: Workspace ID to subscribe to
    :return: ID of the workspace
    """
    user_profile = token_info.get("user_obj", None)

    try:
        subscribed_workspaces = user_profile.get("subscribed_workspaces", [])
        if workspace_id in subscribed_workspaces:
            # Check and validate the workspace presence.
            workspace = nosql_db.get_workspace_by_id(
                workspace_id,
                remove_private_data=False,
            )
            if not workspace:
                return make_response(
                    jsonify({"status": "fail", "reason": "Invalid workspace_id"}),
                    500,
                )

            # Update the workspace only if public sharing is enabled.
            if user_profile["id"] in workspace.subscribed_users:
                # Update the workspace
                workspace.subscribed_users.remove(user_profile["id"])
                nosql_db.update_workspace(workspace.id, workspace)
                # Update the User profile
                nosql_db.update_user(
                    {
                        "subscribed_workspaces": subscribed_workspaces.remove(
                            workspace_id,
                        ),
                    },
                    user_id=user_profile["id"],
                )
                return make_response(
                    jsonify(IdWithMessage(workspace_id, "Unsubscribed successfully")),
                    200,
                )
            else:
                err_str = "subscription list of workspace does not have the user id"
                logger.info(
                    f"Not updating subscription status {workspace_id}, err: {err_str}",
                )
                return make_response(
                    jsonify({"status": "fail", "reason": err_str}),
                    500,
                )
        else:
            return make_response(
                jsonify(IdWithMessage("error", "workspace already unsubscribed")),
                200,
            )
    except Exception as e:
        logger.error(
            f"Error unsubscribing from workspace {workspace_id}, err: {str(e)}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, f"{e}"
        return make_response(jsonify({"status": status, "reason": msg}), rc)


def clone_workspace(token_info, workspace_id):
    """
    Clones a workspace
    :param token_info: Token info as passed from the security module
    :param workspace_id: Workspace ID which has to be cloned
    :return: ID of the workspace
    """
    user_json = token_info.get("user_obj", None)
    ws_name = ""
    shared_with = []
    collaborators = {}
    try:
        if connexion.request and connexion.request.is_json:
            req_json = connexion.request.get_json()
            ws_name = req_json.get("name", "")
            shared_with = req_json.get("sharedWith", [])
            collaborators = req_json.get("collaborators", {})
        user_permission, ws = nosql_db.get_user_permission(
            workspace_id,
            email=user_json["email_id"],
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to clone this workspace"
            log_str = f"{user_json['email_id']} not authorized to clone workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        ws_name = ws_name or ("Clone of " + ws.name)
        workspace = Workspace.from_dict({"name": ws_name})  # noqa: E501

        workspace.user_id = user_json["id"]
        workspace.shared_with = shared_with
        # Reset the statistics
        workspace.statistics = {
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
        status, rc, msg = create_workspace(
            user_json,
            workspace,
            collaborators,
            create_default_field_bundle=False,
        )
        if status == "fail":
            return make_response(jsonify({"status": status, "reason": msg}), rc)
        replicate_field_bundles_between_workspaces(
            ws.id,  # Source Workspace ID
            status,  # Return of create_workspace call will be workspace_id
            user_json,  # User Profile
        )
        return make_response(
            jsonify(IdWithMessage(status, "Cloning successful")),
            200,
        )
    except Exception as e:
        logger.error(
            f"Error during cloning workspace {workspace_id}, err: {str(e)}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, f"{e}"
        return make_response(jsonify({"status": status, "reason": msg}), rc)


def update_documents_meta_in_index(
    user,
    token_info,
    workspace_id,
):
    try:
        user_permission, workspace = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None),
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = (
                f"Not authorized to update document meta for workspace {workspace_id}"
            )
            log_str = f"user {user} not authorized to add dictionary to workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        if not workspace:
            logger.error(
                f"unknown status while updating workspace with id {workspace_id}",
            )
            status, rc, msg = "fail", 500, "unknown status"

        update_document_meta(workspace_idx=workspace_id)

        return make_response(
            jsonify(IdWithMessage(workspace_id, "Document meta updated for workspace")),
        )
    except Exception as e:
        logger.error(
            f"Error during updating document meta for workspace {workspace_id}, err: {str(e)}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, f"{e}"
        return make_response(jsonify({"status": status, "reason": msg}), rc)
