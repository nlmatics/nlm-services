import logging
import traceback

import connexion
from flask import jsonify
from flask import make_response

from server import err_response
from server import unauthorized_response
from server.models.document_folder import DocumentFolder
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.storage import nosql_db
from server.utils import str_utils


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def delete_document_folder(user, folder_id, recursive=None):  # noqa: E501
    """Delete an existing folder

     # noqa: E501

    :param folder_id:
    :type folder_id: str
    :param recursive:
    :type recursive: bool

    :rtype: IdWithMessage
    """
    raise NotImplementedError()


def get_folder_contents(
    user,
    token_info,
    workspace_id,
    folder_id="root",
    expand_all=True,
    doc_per_page=10000,
    offset=0,
    name_contains="",
    name_startswith="",
    sort_method=None,
    reverse_sort=None,
    filter_status="all",
    filter_date_from=None,
    filter_date_to=None,
    projection_params=None,
):  # noqa: E501
    """Returns document folder hierarchy

     # noqa: E501

    :param filter_date_to:
    :param filter_date_from:
    :param filter_status:
    :param reverse_sort:
    :param sort_method:
    :param name_startswith:
    :param name_contains:
    :param offset:
    :param doc_per_page:
    :param workspace_id:
    :param user:
    :param token_info:
    :param folder_id:
    :type folder_id: str
    :param expand_all:
    :type expand_all: bool
    :param projection_params:
    :type projection_params: list

    :rtype: List[Object]
    """
    try:

        user_obj = token_info.get("user_obj", None)
        if not user_obj:
            logger.error(f"cannot identify user {user}, action denied")
            return unauthorized_response()
        user_permission, _ws = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=user_obj,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to change permission"
            log_str = f"{user} not authorized to change permission in workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)
        ws = (
            nosql_db.get_default_workspace_for_user_id(
                user_obj["id"],
                remove_private_data=False,
            )
            if workspace_id == "default"
            else _ws
        )
        if not ws:
            logger.error(f"workspace with id {workspace_id} not found")
            status, rc, msg = "fail", 422, "invalid workspace"
            return make_response(jsonify({"status": status, "reason": msg}), rc)
        # Update or creates a preferred_workspace entry for the user.
        nosql_db.update_prefered_workspace(user_obj["id"], workspace_id)

        # TODO: we need more serious permission check here.
        # if ws and (ws.user_id != user_obj.id):
        #     logger.error(f"{user} not authorized to query folder contents")
        #     return make_response(
        #         jsonify({"status": "fail", "reason": "unauthorized"}), 401,
        #     )
        logger.info(
            f"Getting folder containers for folder {folder_id} in workspace {workspace_id}",
        )
        filter_struct = {}
        if filter_status in [
            "ingest_failed",
            "ready_for_ingestion",
            "ingest_inprogress",
            "ingest_ok",
        ]:
            filter_struct["status"] = filter_status
        # filter_date_to and filter_date_from are in epoch format.
        if filter_date_from:
            filter_struct["filter_date_from"] = filter_date_from
            filter_struct["filter_date_to"] = filter_date_to

        if not sort_method:
            sort_method = ws.settings.get("document_settings", {}).get(
                "default_sort_field",
                "name",
            )
        if reverse_sort is None:
            reverse_sort = ws.settings.get("document_settings", {}).get(
                "reverse_sort",
                False,
            )
        projection_params = projection_params or {}
        projection = None
        if projection_params:
            projection = {}
            for proj in projection_params:
                projection[proj] = 1
            projection["_id"] = 0

        folder_contents = _expand_folder_contents(
            ws.id,
            folder_id,
            doc_per_page,
            offset,
            name_contains,
            name_startswith,
            sort_method,
            reverse_sort,
            filter_struct=filter_struct,
            projection=projection,
        )
        if folder_id == "root":
            return make_response(
                jsonify(
                    {
                        "totalDocCount": folder_contents["totalDocCount"],
                        "documents": folder_contents["documents"],
                    },
                ),
                200,
            )
        else:
            return make_response(jsonify(folder_contents), 200)
    except Exception as e:
        logger.error(f"error getting folder contents, err: {traceback.format_exc()}")
        status, rc, msg = "fail", 500, str(e)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def _expand_folder_contents(
    workspace_id,
    folder_id,
    doc_per_page=10000,
    offset=0,
    name_contains="",
    name_startswith="",
    sort_method="name",
    reverse_sort=False,
    filter_struct=None,
    projection=None,
):
    logger.info(f"Getting children for folder id {folder_id}")
    content = nosql_db.get_folder_contents(
        workspace_id,
        folder_id,
        doc_per_page,
        offset,
        name_contains=name_contains,
        name_startswith=name_startswith,
        sort_method=sort_method,
        reverse_sort=reverse_sort,
        filter_struct=filter_struct,
        projection=projection,
    )
    return content


def update_document_folder(user, folder_id, new_name):  # noqa: E501
    """Updates name of an existing document folder

     # noqa: E501

    :param folder_id:
    :type folder_id: str
    :param new_name:
    :type new_name: str

    :rtype: IdWithMessage
    """
    raise NotImplementedError()


def create_folder(body=None, user=None):  # noqa: E501
    """Creates a new folder a user&#x27;s workspace

     # noqa: E501

    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        try:
            user_obj = nosql_db.get_user_by_email(user)
            if not user_obj:
                return unauthorized_response()
            folder = DocumentFolder.from_dict(
                connexion.request.get_json(),
            )  # noqa: E501
            if folder.workspace_id == "default":
                ws = nosql_db.get_default_workspace_for_user_id(user_obj.id)
            else:
                ws = nosql_db.get_workspace_by_id(folder.workspace_id)
            if ws and not nosql_db.is_user_email_matches_id(user, ws.user_id):
                err_str = "Not authorized to create folder in workspace"
                log_str = f"{user} not authorized to create folder in workspace"
                logger.info(log_str)
                return err_response(err_str, 403)
            if not ws:
                logger.error(f"workspace {folder.workspace_id} does not exists")
                status, rc, msg = "fail", 422, "invalid workspace"
            else:
                folder.workspace_id = (
                    ws.id
                )  # replace with 'true' workspace id if 'default' workspace is specified
                folder.parent_folder = folder.parent_folder or "root"
                if nosql_db.folder_by_name_exists(
                    folder.name,
                    ws.id,
                    folder.parent_folder,
                ):
                    logger.error(f"folder '{folder.name}' already exists")
                    return err_response(f"folder {folder.name} already exists", 400)
                folder.id = str_utils.generate_folder_id(
                    folder.name,
                    folder.workspace_id,
                    folder.parent_folder,
                )
                folder.created_on = folder.created_on or str_utils.timestamp_as_str()
                folder.is_deleted = False
                if folder.parent_folder != "root" and not nosql_db.folder_exists(
                    folder.workspace_id,
                    folder.parent_folder,
                ):
                    logger.error(
                        f"parent folder with id {folder.parent_folder} does not exists",
                    )
                    status, rc, msg = "fail", 422, "parent folder does not exists"
                else:
                    id = nosql_db.create_folder(folder)
                    if id:
                        logger.info(
                            f"folder {folder.name} created under workspace {folder.workspace_id}, parent folder {folder.parent_folder}",
                        )
                        return make_response(
                            jsonify(IdWithMessage(id, "folder created")),
                        )
                    else:
                        status, rc, msg = (
                            "fail",
                            500,
                            "folder creation failed with unknown status",
                        )
        except Exception as e:
            logger.error(f"error creating folder, err: {traceback.format_exc()}")
            status, rc, msg = "fail", 500, str(e)
    else:
        status, rc, msg = "fail", 422, "invalid json request"
    return make_response(jsonify({"status": status, "reason": msg}), rc)
