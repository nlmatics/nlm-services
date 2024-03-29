import logging
import os
import tempfile
import threading
import traceback
from urllib.parse import urlparse

import connexion
import magic
import validators
from flask import jsonify
from flask import make_response
from flask import send_from_directory
from server.utils.indexer_utils.indexer_ops import index_data_row_file
from server.utils.indexer_utils.indexer_ops import ingest_document
from server.utils.indexer_utils.indexer_ops import run_yolo_inference
from nlm_utils.rabbitmq import producer
from nlm_utils.storage import file_storage
from nlm_utils.utils.utils import ensure_bool
from werkzeug.utils import secure_filename

from server import err_response
from server import unauthorized_response
from server.controllers.extraction_controller import apply_template
from server.models.document import Document  # noqa: E501
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.models.rename_doc import RenameDoc  # noqa: E501
from server.storage import nosql_db as nosqldb
from server.utils import file_utils
from server.utils import str_utils
from server.utils.metric_utils import update_metric_data
from server.utils.notification_general import NotifyAction
from server.utils.notification_utils import send_document_notification
from server.utils.notification_utils import send_search_criteria_workflow_notification
from server.utils.tika_utils import get_page_thumbnail
from server.utils.tika_utils import get_page_thumbnail_by_doc_id
from server.utils.tika_utils import ocr_first_page
from server.utils.tika_utils import ocr_first_page_by_doc_id
from server.utils.indexer_utils.es_client import es_client


# import server.config as cfg

# Todo - this function should be refactored to a separate utility class

# from nlm_ingest_client.nlm_ingest_client import IngestClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MSG_UPLOAD_SUCCESSFUL = "upload successful"
SEND_NOTIFICATIONS = ensure_bool(os.getenv("SEND_NOTIFICATIONS", False))
UPDATE_USAGE_METRICS = ensure_bool(os.getenv("UPDATE_USAGE_METRICS", False))
PERFORM_TIKA_OCR = ensure_bool(os.environ.get("TIKA_OCR", False))


def delete_document_by_id(
    user,
    token_info,
    document_id,
    nosql_db=nosqldb,
    permanent=False,
    update_metric=True,
):  # noqa: E501
    """Delete an existing document

     # noqa: E501

    :param user: User email id
    :param token_info: Return object from the Authenticate function. (Token authentication)
    :param document_id:
    :type document_id: str
    :param permanent: Do we need to delete the document permanently?
    :param update_metric: Whether we want to update the metric or not.

    :param nosql_db:
    :type nosql_db: NoSqlDb

    :rtype: IdWithMessage
    """
    try:
        user_obj = token_info["user_obj"]
        if not user_obj:
            logger.error(f"invalid user {user}, action denied")
            return unauthorized_response()
        doc = nosql_db.get_document_info_by_id(
            document_id,
            return_dict=True,
            check_is_deleted=permanent,
        )
        if not doc:
            logger.error(f"document {document_id} not found")
            return err_response("document not found", 400)
        user_permission, ws = nosql_db.get_user_permission(
            doc["workspace_id"],
            email=user,
            user_json=user_obj,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to delete document"
            log_str = f"user {user} not authorized to delete document {document_id} from workspace {ws.id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        parse_and_render_only = (
            doc["parse_options"]["parse_and_render_only"]
            if doc.get("parse_options", None)
            else False
        )
        metric_data = []
        if doc["status"] == "ingest_ok":
            metric_data = [
                ("num_docs", -1),
                ("num_pages", -(doc.get("num_pages", 0) or 0)),
                ("doc_size", -(doc.get("file_size", 0) or 0)),
            ]
        if update_metric and UPDATE_USAGE_METRICS:
            update_metric_data(
                user_obj,
                metric_data,
            )
        statistics = ws.statistics or {}
        if not statistics.get("document", {}) or not statistics["document"].get(
            "total",
            0,
        ):
            statistics["document"] = {
                "total": 0,
            }
        else:
            if statistics["document"]["total"] > 1:
                statistics["document"]["total"] -= 1
            else:
                statistics["document"]["total"] = 0

        set_data = {
            "statistics": statistics,
        }
        nosql_db.update_workspace_data(ws.id, set_data)

        if permanent:
            # Delete the minio storage
            file_prefix_list = [
                doc.get("doc_location", ""),
                doc.get("rendered_file_location", ""),
                f"bbox/features/{document_id}",
            ]
            logger.info(f"Deleting files with prefix {file_prefix_list} from storage.")
            file_storage.delete_files(file_prefix_list)
        # Retrieve field bundles and fields for this document.
        bundles = nosql_db.get_field_bundles_in_workspace(doc["workspace_id"])
        for bundle in bundles:
            field_ids = bundle.field_ids
            for field_id in field_ids:
                # Delete field value for that specific doc_id
                nosql_db.delete_field_value(
                    field_id,
                    document_id,
                    doc["workspace_id"],
                    True,
                )
        # Delete bbox
        nosql_db.delete_bbox_by_doc_id(document_id)

        # remove file from db
        del_doc = nosql_db.delete_document(document_id, permanent)
        if SEND_NOTIFICATIONS:
            # Send notification
            send_document_notification(
                user_obj,
                ws,
                Document(**doc),
                NotifyAction.DOCUMENT_REMOVED_FROM_WORKSPACE,
            )
        if not parse_and_render_only:
            # remove file from ES index
            es_client.delete_from_index(
                document_id,
                ws.id,
                workspace_settings=ws.settings,
            )
            # remove file from mongo index
            nosql_db.remove_es_entry(document_id, ws.id)

        if del_doc:
            logger.info(f"document with id {document_id} deleted")
            if update_metric:
                return make_response(
                    jsonify(IdWithMessage(del_doc, "document deleted")),
                    200,
                )
            else:
                # Invocation from workspace controller delete.
                return metric_data
        else:
            logger.error(
                f"unknown error while deleting the document with id {document_id}",
            )
            status, rc, msg = "fail", 500, "unknown error while deleting the document"
    except Exception as e:
        logger.error(
            f"error deleting document with id {document_id}, err: {traceback.format_exc()}",
        )
        status, rc, msg = "fail", 500, str(e)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def download_document_by_id(
    document_id,
    workspace_id,
    render_format="original",
    nosql_db=nosqldb,
    file_storage=file_storage,
    user=None,
    token_info=None,
):  # noqa: E501
    """Download a document identified by docId

     # noqa: E501

    :param document_id:
    :type document_id: str

    :param db:
    :type db: NoSqlDb

    :param file_storage:
    :type file_storage: ObjectStore

    :rtype: str
    """
    try:
        document = nosql_db.get_document(workspace_id, document_id)
        user_permission, _ws = nosql_db.get_user_permission(
            document.workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None) if token_info else None,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to download document"
            log_str = f"user {user} not authorized to download document {document_id} from workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        logger.info(
            f"Attempting to retrieving document {document_id} in workspace {workspace_id} in format {render_format}",
        )
        # tmp_file = tempfile.mkstemp()[1]
        tempfile_handler, tempfile_location = tempfile.mkstemp()
        os.close(tempfile_handler)
        if document:
            doc_loc_to_serve = document.doc_location
            doc_mimetype = document.mime_type
            doc_name = document.name
            if render_format in ["html", "json", "xml"] or not doc_loc_to_serve:
                # document.rendered_file_location = document.doc_location
                logger.info("DOC CONTROLLER DOCUMENT")
                logger.info(document)
                logger.info(
                    f"document {document_id} requested in {render_format} format",
                )
                doc_loc_to_serve = document.rendered_file_location
                doc_mimetype = "text/html"
                if render_format == "json":
                    doc_mimetype = "application/json"
                    doc_loc_to_serve = document.rendered_json_file_location
                elif render_format == "xml":
                    doc_mimetype = "application/xml"
                    doc_loc_to_serve = document.rendered_json_file_location
                if not doc_name.endswith(f".{render_format}"):
                    doc_name += f".{render_format}"
                logger.info(f"DOC CONTROLLER: {doc_loc_to_serve}")
                if (
                    document.status != "ingest_ok"
                    or not doc_loc_to_serve
                    or not file_storage.document_exists(doc_loc_to_serve)
                ):
                    logger.error(
                        f"document {doc_loc_to_serve} requested in {render_format} format does not exists",
                    )
                    return make_response(
                        jsonify(
                            {
                                "status": "fail",
                                "reason": f"document not available in {render_format} format",
                            },
                        ),
                        403,
                    )
            # with open(tmp_file, "wb") as fh:
            #     logger.info(f"Writing file to {tmp_file}")
            #     file_storage.download_from_location(doc_loc_to_serve, fh)
            logger.info(f"Writing file to {tempfile_location} for doc_name {doc_name}")
            file_storage.download_from_location(doc_loc_to_serve, tempfile_location)
            logger.info("Sending response")
            if render_format == "xml":
                file_utils.convert_json_to_xml(tempfile_location)
            return send_from_directory(
                os.path.dirname(tempfile_location),
                os.path.basename(tempfile_location),
                mimetype=doc_mimetype,
                as_attachment=True,
                download_name=doc_name.replace("\n", ""),
            )
        else:
            status, rc, msg = "fail", 404, "document not found"

        if os.path.exists(tempfile_location):
            os.unlink(tempfile_location)
    except Exception as e:
        logger.error(f"error fetching document, err: {traceback.format_exc()}")
        status, rc, msg = "fail", 500, str(e)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_document_info_by_id(
    user,
    token_info,
    document_id,
    nosql_db=nosqldb,
):  # noqa: E501
    """Returns document information by id

     # noqa: E501
    :param user:
    :param token_info:
    :param document_id:
    :type document_id: str

    :rtype: Document
    """
    try:
        document = nosql_db.get_document_info_by_id(document_id)
        user_permission, _ws = nosql_db.get_user_permission(
            document.workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None) if token_info else None,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to retrieve document info"
            log_str = (
                f"user {user} not authorized to retrieve document info for {document_id} "
                f"from workspace {document.workspace_id}"
            )
            logger.info(log_str)
            return err_response(err_str, 403)

        return make_response(
            jsonify(document),
            200,
        )
    except Exception as e:
        logger.error(
            f"Error retrieving info for document with id {document_id}, err: {str(e)}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def get_document_info_by_source_url(
    user,
    token_info,
    url,
    nosql_db=nosqldb,
):  # noqa: E501
    """Returns document information by id

     # noqa: E501

    :param user:
    :param token_info:
    :param url:
    :type url: str

    :rtype: Document
    """
    try:
        logger.info(f"finding document by url: {url}")
        document = nosql_db.get_document_info_by_source_url(url)
        user_permission, _ws = nosql_db.get_user_permission(
            document.workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None) if token_info else None,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to retrieve document info"
            log_str = (
                f"user {user} not authorized to retrieve document info for {document.id} "
                f"from workspace {document.workspace_id}"
            )
            logger.info(log_str)
            return err_response(err_str, 403)

        return make_response(
            jsonify(document),
            200,
        )
    except Exception as e:
        logger.error(
            f"Error retrieving info for document with url {url}",
            exc_info=True,
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 200)


def get_document_key_info_by_id(
    user,
    token_info,
    document_id,
    nosql_db=nosqldb,
):  # noqa: E501
    """Returns document information by id

     # noqa: E501

    :param user:
    :param token_info: Return object from the Authenticate function. (Token authentication)
    :param document_id:
    :type document_id: str

    :rtype: Document
    """
    try:
        doc = nosql_db.get_document_info_by_id(document_id)
        user_permission, _ws = nosql_db.get_user_permission(
            doc.workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None),
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to retrieve document"
            log_str = (
                f"user {user} not authorized to retrieve document key_info {document_id} "
                f"from workspace {doc.workspace_id}"
            )
            logger.info(log_str)
            return err_response(err_str, 403)

        user_id = token_info["user_obj"]["id"]
        workspace_id = doc.workspace_id
        nosql_db.create_file_history(
            user_id,
            workspace_id,
            document_id,
            "opened_document",
        )
        return make_response(
            jsonify(nosql_db.get_document_key_info_by_id(document_id)),
            200,
        )
    except Exception as e:
        logger.error(
            f"Error retrieving key info for document with id {document_id}, err: {str(e)}",
            exc_info=True,
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def get_documents_in_workspace(
    user,
    token_info,
    workspace_id,
    doc_per_page=10000,
    offset=0,
    return_only_status=False,
    nosql_db=nosqldb,
    check_permission=True,
):  # noqa: E501
    """List all documents in the workspace

     # noqa: E501

    :param user:
    :param token_info:
    :param workspace_id:
    :type workspace_id: str
    :param return_only_status:
    :param offset:
    :param nosql_db:
    :param doc_per_page:
    :param check_permission: bool

    :rtype: List[Document]
    """
    try:
        projection = None
        do_sort = True
        if check_permission:
            user_permission, _ws = nosql_db.get_user_permission(
                workspace_id,
                email=user,
                user_json=token_info.get("user_obj", None),
            )
            if user_permission not in ["admin", "owner", "editor", "viewer"]:
                err_str = "Not authorized to view the workspace"
                log_str = f"user {user} not authorized to retrieve documents from workspace {workspace_id}"
                logger.info(log_str)
                return err_response(err_str, 403)
            if return_only_status:
                projection = {
                    "_id": 0,
                    "id": 1,
                    "status": 1,
                }
                do_sort = False

        return nosql_db.get_folder_contents(
            workspace_id,
            "root",
            doc_per_page,
            offset,
            projection=projection,
            do_sort=do_sort,
        )
    except Exception as e:
        logger.error(
            f"error retrieving documents from workspace, reason: {str(e)}",
            exc_info=True,
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def re_ingest_all_documents(user, token_info, nosql_db=nosqldb):
    user_permission, _ws = nosql_db.get_user_permission(
        "",
        email=user,
        user_json=token_info.get("user_obj", None),
    )
    if user_permission not in ["admin"]:
        err_str = "Not authorized to re-ingest"
        log_str = f"user {user} not authorized to re-ingest all documents"
        logger.info(log_str)
        return err_response(err_str, 403)

    logger.info("reingesting all documents")
    existing_workspaces = nosqldb.get_all_workspaces()
    existing_ws_list = [ws.id for ws in existing_workspaces]
    documents = []
    for ws_id in existing_ws_list:
        documents += get_documents_in_workspace(
            user,
            token_info,
            workspace_id=ws_id,
            check_permission=False,
        )["documents"]
        try:
            es_client.delete_index(ws_id, workspace_settings=_ws.settings)
        except Exception:
            pass

    logger.info(f"reingesting {len(documents)} documents")
    if len(documents) == 0:
        return make_response("No Documents to re-ingest", 400)
    else:
        logger.info(f"reingesting {len(documents)} documents")

        # delete ES index when re-ingesting
        # es_client.delete_index(workspace_idx=workspace_id)
        # re-ingest each document
        for doc in documents:
            try:
                # re-ingest
                re_ingest_single_document_in_workspace(
                    user="",
                    token_info={},
                    document_id=doc.id,
                )
            except Exception:
                logger.error(f"error ingesting document: {doc} ", exc_info=True)

    return make_response("re-ingestion finished", 200)


def _re_ingest_doc(
    user_obj,
    doc,
    apply_ocr=False,
    nosql_db=nosqldb,
):
    try:
        # re-ingest
        if PERFORM_TIKA_OCR:
            logger.info(f"re-ingesting {doc.name}")
            ocr_thread = threading.Thread(
                target=ocr_first_page_by_doc_id,
                args=(),
                kwargs={"doc": doc},
            )
            thumbnail_thread = threading.Thread(
                target=get_page_thumbnail_by_doc_id,
                args=(),
                kwargs={"doc": doc},
            )
            ocr_thread.start()
            thumbnail_thread.start()
        task_body = {
            "doc_id": doc.id,
            "workspace_idx": doc.workspace_id,
            "user_obj": user_obj,
            "re_ingest": True,
        }
        if apply_ocr:
            task_body["apply_ocr"] = True
        task = nosql_db.insert_task(
            user_obj["id"],
            "ingestion",
            task_body,
        )
        nosql_db.set_document_status(doc.id, "ready_for_ingestion")

        # send task to rabbitmq producer
        res = producer.send(task)
        if res:
            logger.info(f"Document {doc.id} queued")
            return {
                "status": "queued",
                "task": {
                    "_id": task.get("_id", ""),
                },
                "detail": "re-ingestion queued",
            }
        else:
            ingest_document(
                doc,
                user_profile=user_obj,
                re_ingest=True,
                parse_options={"apply_ocr": True} if apply_ocr else {},
            )
            return (
                {
                    "doc_id": doc.id,
                    "status": "Ingest Success",
                },
            )
    except Exception as e:
        logger.info(f"error ingesting document: {doc} ", exc_info=True)
        logger.info(e)
        return None


def re_ingest_documents_in_workspace(
    user,
    token_info,
    workspace_id,
    failed_docs=False,
    apply_ocr=False,
    nosql_db=nosqldb,
):
    user_permission, _ws = nosql_db.get_user_permission(
        workspace_id,
        email=user,
        user_json=token_info.get("user_obj", None),
    )
    if user_permission not in ["admin", "owner", "editor"]:
        err_str = "Not authorized to re-ingest"
        log_str = f"user {user} not authorized to re-ingest documents in workspace {workspace_id}"
        logger.info(log_str)
        return err_response(err_str, 403)

    logger.info(f"re-ingesting documents in workspace {workspace_id}")
    # Initialize variables
    do_total_doc_count = True
    total_doc_count = 0
    skip_offset = -1
    docs_per_page = -1
    document_status = []

    opt_query_params = {}
    if failed_docs:
        opt_query_params["status"] = "ingest_failed"

    while skip_offset <= total_doc_count:
        if skip_offset == -1:
            skip_offset = 0
            docs_per_page = 10000
        result = nosql_db.get_docs_in_workspace(
            workspace_id,
            do_total_doc_count,
            "root",
            docs_per_page,
            skip_offset,
            opt_query_params=opt_query_params,
        )
        # First invocation will have do_total_doc_count == True
        if do_total_doc_count:
            total_doc_count = result["totalDocCount"]
            if not total_doc_count:
                logger.info(
                    f"No documents to re-ingest in {workspace_id} with QueryParams: {opt_query_params}",
                )
                return make_response("No Documents to re-ingest", 400)
            do_total_doc_count = False

            if not failed_docs and not apply_ocr:
                # delete ES index when re-ingesting entire workspace
                try:
                    es_client.delete_index(
                        workspace_id,
                        workspace_settings=_ws.settings,
                    )
                    logger.info("deleting index")
                except Exception as e:
                    logger.error("failed to delete index")
                    logger.error(e)

        documents = result["documents"]
        logger.info(
            f"re-ingesting {len(documents)} documents from workspace {workspace_id}",
        )
        # re-ingest each document
        for doc in documents:
            ret_status = _re_ingest_doc(
                token_info["user_obj"],
                doc,
                apply_ocr,
            )
            if ret_status:
                document_status.append(ret_status)

        skip_offset += docs_per_page

    if len(document_status) > 0:
        return make_response(jsonify(document_status), 200)
    else:
        return make_response("re-ingestion failed", 500)


def re_ingest_single_document_in_workspace(
    user,
    token_info,
    document_id,
    apply_ocr=False,
    nosql_db=nosqldb,
):
    logger.info(f"re-ingesting document {document_id}")
    doc = nosql_db.get_document_info_by_id(document_id)
    user_permission, _ws = nosql_db.get_user_permission(
        doc.workspace_id,
        email=user,
        user_json=token_info.get("user_obj", None),
    )
    if user_permission not in ["admin", "owner", "editor"]:
        err_str = "Not authorized to re-ingest the document"
        log_str = f"user {user} not authorized to re-ingest document {document_id} in workspace {doc.workspace_id}"
        logger.info(log_str)
        return err_response(err_str, 403)

    ret_status = _re_ingest_doc(
        token_info["user_obj"],
        doc,
        apply_ocr,
    )
    if ret_status:
        return make_response(
            jsonify(ret_status),
            200,
        )
    return make_response("re-ingestion failed", 500)


def update_document_by_id(
    user,
    token_info,
    document_id,
    nosql_db=nosqldb,
):

    if not connexion.request.is_json:
        logger.error("update_workspace_settings: invalid json in request")
        _, rc, msg = "fail", 422, "invalid json in request"
        return err_response(msg, rc)

    try:

        # check if the document with the document id exists
        doc_info = nosql_db.get_document_info_by_id(document_id)
        user_permission, _ws = nosql_db.get_user_permission(
            doc_info.workspace_id,
            user_json=token_info.get("user_obj", None),
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to modify document"
            log_str = f"user {user} not authorized to modify document {document_id}"
            logger.info(log_str)
            return err_response(err_str, 403)
        if doc_info:
            data_to_set = connexion.request.get_json()
            if data_to_set:
                nosql_db.set_document_info(document_id, data_to_set)

            logger.info(
                f"Document with id {document_id} updated with set_data {data_to_set}",
            )

        return make_response(
            jsonify(IdWithMessage(document_id, "Document updated.")),
        )
    except Exception as e:
        logger.error(
            f"Error during adding ignore block to workspace {document_id}, err: {str(e)}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, f"{e}"
        return make_response(jsonify({"status": status, "reason": msg}), rc)


def modify_document_by_id(
    user,
    token_info,
    document_id,
    file=None,
    nosql_db=nosqldb,
    file_storage=file_storage,
):  # noqa: E501
    """Upload a new file to replace the document with id

     # noqa: E501
    :param user:
    :param token_info:
    :param document_id:
    :type document_id: str
    :param file:
    :type file: strstr

    :rtype: IdWithMessage
    """
    try:
        # check if the document with the document id exists
        doc_info = nosql_db.get_document_info_by_id(document_id)
        user_permission, _ws = nosql_db.get_user_permission(
            doc_info.workspace_id,
            user_json=token_info.get("user_obj", None),
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to modify document"
            log_str = f"user {user} not authorized to modify document {document_id}"
            logger.info(log_str)
            return err_response(err_str, 403)
        if doc_info:
            if not file:
                return make_response(
                    jsonify({"status": "fail", "reason": "file argument empty"}),
                    500,
                )
            filename = secure_filename(file.filename)
            _, file_extension = os.path.splitext(file.filename)

            tempfile_handler, tmp_file = tempfile.mkstemp(suffix=file_extension)
            os.close(tempfile_handler)
            # populate tmp_file

            with open(tmp_file, "wb") as fp:
                fp.write(file.read())
            # extract file properties
            props = _extract_file_properties(tmp_file)

            workspace_id = doc_info.workspace_id
            user_id = doc_info.user_id
            folder_id = doc_info.parent_folder
            mime_type = doc_info.mime_type

            # if exists, upload the file to object store at the same location
            doc_location = _upload_file_to_store(
                store=file_storage,
                tmp_file=tmp_file,
                user_id=user_id,
                workspace_id=workspace_id,
                folder_id=folder_id,
                doc_id=document_id,
                mime_type=mime_type,
            )

            doc_info.doc_location = doc_location
            doc_info.created_on = props["createdOn"]
            doc_info.mime_type = props["mimeType"]
            doc_info.file_size = props["fileSize"]
            doc_info.checksum = props["checksum"]
            doc_info.name = filename
            doc_info.update = True

            # data = doc_info.to_dict()
            logger.info(f"uploading document: {filename}")
            # _ = ingest_document(data)
            task = nosql_db.insert_task(
                token_info["user_obj"]["id"],
                "ingestion",
                {
                    "doc_id": document_id,
                    "workspace_idx": workspace_id,
                    "user_obj": token_info.get("user_obj", None),
                    "re_ingest": False,
                    "notify_action": NotifyAction.DOCUMENT_UPDATED_INPLACE,
                },
            )

            # send task to rabbitmq producer
            res = producer.send(task)
            if res:
                logger.info("Document queued")
            else:
                ingest_document(
                    doc_info,
                    user_profile=token_info.get("user_obj", None),
                    re_ingest=False,
                )
                logger.info("Document successfully uploaded")
                sc_workflows = nosql_db.get_search_criteria_workflows(
                    workspace_id=doc_info.workspace_id,
                )
                workspace = nosql_db.get_workspace_by_id(doc_info.workspace_id)
                for sc_workflow in sc_workflows:
                    if sc_workflow.actions and sc_workflow.search_criteria:
                        facts = apply_template(
                            workspace_idx=doc_info.workspace_id,
                            file_idx=doc_info.id,
                            ad_hoc=True,
                            **sc_workflow.search_criteria.to_dict(),
                        )
                        if len(facts) > 0:
                            send_search_criteria_workflow_notification(
                                workspace,
                                doc_info,
                                facts,
                                sc_workflow,
                            )

            # doc_info.name = new_doc_info.title
            # data['update'] = True
            # logging.info(f"msg2{data}")
            # ingestor.submit_document_for_ingestion(json.dumps(doc_info.to_dict(), default=str).encode('utf-8'))
            # overwrite the database entry
            if nosql_db.update_document(document_id, doc_info):
                if SEND_NOTIFICATIONS:
                    send_document_notification(
                        token_info["user_obj"],
                        workspace_id,
                        doc_info,
                        NotifyAction.DOCUMENT_UPDATED_INPLACE,
                    )
                return make_response(
                    jsonify(
                        IdWithMessage(document_id, "document updated successfully"),
                    ),
                )
            else:
                return make_response(
                    jsonify({"status": "fail", "reason": "unable to update document"}),
                    500,
                )

    except Exception as e:
        # logging.info(f"error3{e} {traceback.format_exc()}")
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)
    finally:
        if tmp_file and os.path.exists(tmp_file):
            os.unlink(tmp_file)


def rename_document_by_id(
    user,
    token_info,
    document_id,
    body=None,
    nosql_db=nosqldb,
):  # noqa: E501

    try:
        doc_info = nosql_db.get_document_info_by_id(document_id)
        user_permission, _ws = nosql_db.get_user_permission(
            doc_info.workspace_id,
            user_json=token_info.get("user_obj", None),
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to rename document"
            log_str = f"user {user} not authorized to rename document {document_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        if connexion.request.is_json:
            doc_update = RenameDoc.from_dict(connexion.request.get_json())  # noqa: E501

        new_name = doc_update.new_name
        if not new_name:
            err_str = "Cannot rename to empty string"
            logger.info(err_str)
            return err_response(err_str, 400)

        if nosql_db.rename_document(document_id, new_name):
            nosql_db.update_file_name_in_field_value(
                doc_info.workspace_id,
                document_id,
                new_name,
            )
            doc_info = nosql_db.get_document_info_by_id(document_id)

            return doc_info
        else:
            return make_response(
                jsonify({"status": "fail", "reason": "unable to rename document"}),
                500,
            )
    except Exception as e:
        # logging.info(f"error3{e} {traceback.format_exc()}")
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def upload_data_row_file(
    user_id,
    workspace_id,
    source_file_name,
    row_file_info,
):
    # row_filename = secure_filename(row_file_info.filename)
    tempfile_handler, tmp_file = tempfile.mkstemp(suffix="html")
    os.close(tempfile_handler)
    with open(tmp_file, "w") as f:
        f.write(row_file_info.html_str)
    # calculate the file properties
    props = _extract_file_properties(tmp_file)
    # doc_id = str_utils.generate_doc_id(file.filename)
    doc_id = str_utils.generate_unique_document_id(
        row_file_info.filename,
        props["checksum"],
        props["fileSize"],
    )
    doc_loc = _upload_file_to_store(
        file_storage,
        tmp_file,
        user_id,
        workspace_id,
        "root",
        doc_id,
        props["mimeType"],
    )
    logger.info(f"document stored as {doc_loc}")

    doc_details = {
        **props,
        "id": doc_id,
        "workspaceId": workspace_id,
        "parentFolder": "root",
        "docLocation": None,
        "userId": user_id,
        "name": row_file_info.filename,
        "sourceUrl": "xls upload from: " + source_file_name,
        "status": "ready_for_ingestion",
    }  # TODO: replace status strings with a enums
    doc = Document.from_dict(doc_details)
    logger.info(f"uploading document: {row_file_info.filename}")
    if nosqldb.create_document(doc):
        logger.info("document successfully uploaded")
        index_data_row_file(doc_id, row_file_info)
        return make_response(jsonify(IdWithMessage(doc_id, MSG_UPLOAD_SUCCESSFUL)))


def upload_document(
    user,
    token_info,
    workspace_id,
    file=None,
    folder_id="root",
    nosql_db=nosqldb,
    action="upload",
    apply_ocr=False,
    file_storage=file_storage,
    return_raw=False,
    parent_task=None,
    file_meta=None,
):  # noqa: E501
    """Uploads a new document to a workspace

     # noqa: E501


    :param file_meta:
    :param workspace_id:
    :type workspace_id: str
    :param file:
    :param folder_id:
    :type folder_id: str
    :param file_storage:
    :param nosql_db:

    :rtype: IdWithMessage
    """
    # check if the workspace exists
    tmp_file = None
    try:
        user_obj = token_info.get("user_obj", None) if token_info else None
        if not user_obj:
            user_obj = nosql_db.get_user_by_email(user).to_dict()
            if not user_obj:
                return unauthorized_response()

        user_permission, _ws = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=user_obj,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to upload documents"
            log_str = f"user {user} not authorized to upload documents in workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        ws = (
            nosqldb.get_default_workspace_for_user_id(user_obj["id"])
            if workspace_id == "default"
            else _ws
        )
        if not ws:
            logger.error(f"workspace with id {workspace_id} does not exists")
            status, rc, msg = "fail", 422, "workspace does not exists"
        else:
            user_id = user_obj["id"]
            # check if the folder exists in the workspace
            if folder_id != "root" and not nosql_db.folder_exists(
                workspace_id,
                folder_id,
            ):
                logger.error(f"folder with id {folder_id} does not exists")
                status, rc, msg = "fail", 422, "folder does not exists"
            else:
                workspace_id = ws.id
                # get all docs in folder location

                projection = {
                    "meta": 0,
                    "blocks": 0,
                    "key_info": 0,
                }
                for doc in nosql_db.get_documents_by_name(
                    file.filename,
                    workspace_id,
                    folder_id,
                    projection,
                ):
                    if doc.status == "ingest_ok" and doc.mime_type not in {
                        "text/x-markdown",
                        "text/markdown",
                    }:
                        if action == "pass":
                            return (
                                make_response(
                                    jsonify(IdWithMessage(doc.id, "passed")),
                                )
                                if not return_raw
                                else None
                            )
                        elif action != "reingest":
                            log_data = (
                                f"document {file.filename} already exists, "
                                f"did you mean to update an existing document?"
                            )
                            logger.info(log_data)
                            return (
                                err_response(
                                    log_data,
                                    400,
                                    doc_id=doc.id,
                                )
                                if not return_raw
                                else None
                            )

                    logging.info(f"re-ingesting id: {doc.id}, name: {doc.name}")

                    # get bad document entry
                    document = nosql_db.get_document_info_by_id(doc.id)

                    # re-upload raw document
                    filename = secure_filename(file.filename)

                    tempfile_handler, tmp_file = tempfile.mkstemp()
                    os.close(tempfile_handler)

                    file.save(tmp_file)
                    doc_location = _upload_file_to_store(
                        file_storage,
                        tmp_file,
                        document.user_id,
                        document.workspace_id,
                        document.parent_folder,
                        document.id,
                        document.mime_type,
                    )
                    logging.info(f"Updated {doc_location}")
                    document.status = "ready_for_ingestion"
                    # reupdate tika file
                    document.update = True
                    task_body = {
                        "doc_id": doc.id,
                        "workspace_idx": doc.workspace_id,
                        "user_obj": user_obj,
                        "re_ingest": True,
                        "parent_task": parent_task,
                    }
                    if apply_ocr:
                        task_body["apply_ocr"] = True

                    task = nosql_db.insert_task(
                        user_obj["id"],
                        "ingestion",
                        task_body,
                    )

                    # send task to rabbitmq producer
                    res = producer.send(task)
                    if res:
                        logger.info("Document queued")
                        return (
                            make_response(
                                jsonify(
                                    {
                                        "id": document.id,
                                        "status": "queued",
                                        "task": {
                                            "_id": task.get("_id", ""),
                                        },
                                        "detail": "Document queued",
                                    },
                                ),
                                200,
                            )
                            if not return_raw
                            else document.id
                        )
                    else:
                        ingest_document(
                            doc,
                            user_profile=user_obj,
                            re_ingest=True,
                            parse_options={"apply_ocr": True} if apply_ocr else {},
                        )
                        logger.info("Document successfully uploaded")

                        return (
                            make_response(
                                jsonify(IdWithMessage(document.id, "re-ingesting")),
                            )
                            if not return_raw
                            else document.id
                        )

                # save the incoming file to a temporary location
                filename = secure_filename(file.filename)
                _, file_extension = os.path.splitext(file.filename)

                tempfile_handler, tmp_file = tempfile.mkstemp(suffix=file_extension)
                os.close(tempfile_handler)

                file.save(tmp_file)

                # calculate the file properties
                props = _extract_file_properties(tmp_file)
                # doc_id = str_utils.generate_doc_id(file.filename)
                doc_id = str_utils.generate_unique_document_id(
                    file.filename,
                    props["checksum"],
                    props["fileSize"],
                )
                doc_loc = _upload_file_to_store(
                    file_storage,
                    tmp_file,
                    user_id,
                    workspace_id,
                    folder_id,
                    doc_id,
                    props["mimeType"],
                )
                logger.info(f"document stored as {doc_loc}")
                source_url = file.filename if validators.url(file.filename) else ""
                # make an entry into db
                doc_details = {
                    **props,
                    "id": doc_id,
                    "workspaceId": workspace_id,
                    "parentFolder": folder_id,
                    "docLocation": doc_loc,
                    "userId": user_id,
                    "name": file.filename,
                    "sourceUrl": source_url,
                    "status": "ready_for_ingestion",
                }  # TODO: replace status strings with a enums
                if file_meta:
                    doc_details["meta"] = file_meta
                doc = Document.from_dict(doc_details)
                logger.info(f"uploading document: {filename}")
                # ingest_document(doc)
                # ingestor.submit_document_for_ingestion(json.dumps(doc.to_dict(), default=str).encode('utf-8'))
                if nosql_db.create_document(doc):
                    if PERFORM_TIKA_OCR:
                        ocr_thread = threading.Thread(
                            target=ocr_first_page,
                            args=(),
                            kwargs={"raw_file": tmp_file, "doc": doc},
                        )
                        thumbnail_thread = threading.Thread(
                            target=get_page_thumbnail,
                            args=(),
                            kwargs={"raw_file": tmp_file, "doc": doc},
                        )
                        ocr_thread.start()
                        thumbnail_thread.start()

                    task_body = {
                        "doc_id": doc_id,
                        "workspace_idx": workspace_id,
                        "user_obj": user_obj,
                        "re_ingest": False,
                        "notify_action": NotifyAction.DOCUMENT_ADDED_TO_WORKSPACE,
                    }
                    statistics = ws.statistics or {}
                    if not statistics.get("document", {}) or not statistics[
                        "document"
                    ].get("total", 0):
                        statistics["document"] = {
                            "total": 1,
                        }
                    else:
                        statistics["document"]["total"] += 1

                    set_data = {
                        "statistics": statistics,
                    }
                    nosql_db.update_workspace_data(ws.id, set_data)

                    if apply_ocr:
                        task_body["apply_ocr"] = True

                    task = nosql_db.insert_task(
                        user_id,
                        "ingestion",
                        task_body,
                    )

                    # send task to rabbitmq producer
                    res = producer.send(task)
                    if res:
                        logger.info("Document queued")
                        return (
                            make_response(
                                jsonify(
                                    {
                                        "id": doc_id,
                                        "status": "queued",
                                        "task": {
                                            "_id": task.get("_id", ""),
                                        },
                                        "detail": "Document queued",
                                    },
                                ),
                                200,
                            )
                            if not return_raw
                            else doc_id
                        )
                    else:
                        ingest_document(
                            doc,
                            user_profile=user_obj,
                            re_ingest=False,
                            parse_options={"apply_ocr": True} if apply_ocr else {},
                        )
                        if SEND_NOTIFICATIONS:
                            sc_workflows = nosql_db.get_search_criteria_workflows(
                                workspace_id=doc.workspace_id,
                            )
                            workspace = nosql_db.get_workspace_by_id(doc.workspace_id)
                            for sc_workflow in sc_workflows:
                                if sc_workflow.actions and sc_workflow.search_criteria:
                                    facts = apply_template(
                                        workspace_idx=doc.workspace_id,
                                        file_idx=doc.id,
                                        ad_hoc=True,
                                        **sc_workflow.search_criteria.to_dict(),
                                    )
                                    if len(facts) > 0:
                                        send_search_criteria_workflow_notification(
                                            workspace,
                                            doc,
                                            facts,
                                            sc_workflow,
                                        )

                            # Retrieve the status
                            doc = nosql_db.db["document"].find_one({"id": doc.id})
                            if doc and doc["status"] == "ingest_ok":
                                send_document_notification(
                                    user_obj,
                                    ws,
                                    Document(**doc),
                                    NotifyAction.DOCUMENT_ADDED_TO_WORKSPACE,
                                )
                        logger.info("Document successfully uploaded")

                        return (
                            make_response(
                                jsonify(IdWithMessage(doc_id, MSG_UPLOAD_SUCCESSFUL)),
                            )
                            if not return_raw
                            else doc_id
                        )
                else:
                    logger.error("upload status unknown")
                    status, rc, msg = "fail", 500, "unknown upload status"
    except Exception as e:
        logger.error(
            f"error uploading file, stacktrace: {traceback.format_exc()}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, str(e)

    finally:
        if tmp_file and os.path.exists(tmp_file):
            os.unlink(tmp_file)
    return (
        make_response(jsonify({"status": status, "reason": msg}), rc)
        if not return_raw
        else status
    )


def _upload_file_to_store(
    store,
    tmp_file,
    user_id,
    workspace_id,
    folder_id,
    doc_id,
    mime_type,
):
    dest_blob_name = os.path.join(user_id, workspace_id, folder_id, doc_id)
    return store.upload_blob(tmp_file, dest_blob_name, mime_type)


def _extract_file_properties(filepath):
    if filepath.endswith(".md"):
        mime_type = "text/x-markdown"
    elif filepath.endswith(".html"):
        mime_type = "text/html"
    elif filepath.endswith(".pdf"):
        mime_type = "application/pdf"
    elif filepath.endswith(".xml"):
        mime_type = "text/xml"
    else:
        mime_type = magic.from_file(filepath, mime=True)
    file_size = os.path.getsize(filepath)
    checksum = file_utils.get_file_sha256(filepath)
    creation_date = str_utils.timestamp_as_str()
    return {
        "fileSize": file_size,
        "mimeType": mime_type,
        "checksum": checksum,
        "createdOn": creation_date,
        "isDeleted": False,
    }


def upload_by_url(user, token_info, workspace_id, nosql_db=nosqldb):
    user_permission, _ws = nosql_db.get_user_permission(
        workspace_id,
        email=user,
        user_json=token_info.get("user_obj", None),
    )
    if user_permission not in ["admin", "owner", "editor"]:
        err_str = "Not authorized to upload documents"
        log_str = f"user {user} not authorized to upload documents in workspace {workspace_id}"
        logger.info(log_str)
        return err_response(err_str, 403)

    body = connexion.request.get_json()
    # some common request headers included
    request_headers = {
        "Accept-Encoding": "gzip, deflate, sdch",
        "Accept-Language": "en-US,en;q=0.8",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    }
    for header in body["request_headers"]:
        request_headers[header["request_header"]] = header["request_value"]

    user_id = token_info["user_obj"]["id"]
    parsed_url = urlparse(body["url"])
    root_domain = (
        body["root_domain"]
        if "root_domain" in body
        else parsed_url.scheme + "://" + parsed_url.netloc
    )
    allowed_domain = (
        body["allowed_domain"] if "allowed_domain" in body else parsed_url.netloc
    )
    task_body = {
        "user": user,
        "user_id": user_id,
        "user_obj": token_info.get("user_obj", None),
        "workspace_idx": workspace_id,
        "url": body["url"],
        "html_tag": body["html_selector"]["html_tag"],
        "html_selector": {
            body["html_selector"]["html_name"]: body["html_selector"]["html_value"],
        }
        if "html_name" in body["html_selector"]
        and "html_value" in body["html_selector"]
        else None,
        "upload_pdf": body["upload_pdf"],
        "pdf_only": body["pdf_only"],
        "titles": body["title_selector"],
        "headless_broswer": body["use_headless"],
        "max_depth": body["crawl_depth"],
        "allowed_domain": allowed_domain,
        "root_domain": root_domain,
        "request_headers": request_headers,
        "cookie_string": body["cookie_string"],
        "bearer_token": body["bearer_token"],
        "start_depth": 0,
        "used_links": [],
    }
    # print("task body is ....", task_body)
    task = nosqldb.insert_task(
        user_id,
        "html_crawling",
        task_body,
    )
    res = producer.send(task)
    if not res:
        # raise RuntimeError("can not send task to queue")
        from worker.html_crawl_task import crawl # uncomment to test without messaging
        crawl({"body":task_body})


def upload_and_parse_document(
    token_info,
    file=None,
    render_format: str = "all",
    start_page: int = 0,
    end_page: int = -1,
    folder_id="root",
):
    """
    Uploads a new document and initiates a task to perform ingestion.

    :param token_info: Token info object after the authentication of access_token
    :param file: Binary file to be parsed
    :param render_format: 'all'(both html and json) or 'html' or 'json'
    :param start_page: Page range to parse (Start index)
    :param end_page: Page range to parse (End index)
    :param folder_id: Root by default

    :rtype: IdWithMessage
    """
    tmp_file = None
    workspace_id = "default"  # By default send it to default_workspace
    try:
        user = token_info["user_obj"]
        if not user.get("m2m_email", ""):
            return unauthorized_response(
                "Developer Account not activated. Please contact support.",
            )
        parse_options = {
            "parse_and_render_only": True,
            "render_format": render_format,
            "parse_pages": ()
            if start_page == 0 and end_page == -1
            else (start_page, end_page),
        }
        # save the incoming file to a temporary location
        filename = secure_filename(file.filename)
        _, file_extension = os.path.splitext(file.filename)
        tempfile_handler, tmp_file = tempfile.mkstemp(suffix=file_extension)
        os.close(tempfile_handler)
        file.save(tmp_file)
        # calculate the file properties
        props = _extract_file_properties(tmp_file)
        doc_id = str_utils.generate_unique_document_id(
            file.filename,
            props["checksum"],
            props["fileSize"],
        )
        doc_loc = _upload_file_to_store(
            file_storage,
            tmp_file,
            user["id"],
            workspace_id,
            folder_id,
            doc_id,
            props["mimeType"],
        )
        logger.info(f"document stored as {doc_loc}")
        source_url = file.filename if validators.url(file.filename) else ""
        # make an entry into db
        doc_details = {
            **props,
            "id": doc_id,
            "workspaceId": workspace_id,
            "parentFolder": folder_id,
            "docLocation": doc_loc,
            "userId": user["id"],
            "name": file.filename,
            "sourceUrl": source_url,
            "status": "ready_for_ingestion",
        }  # TODO: replace status strings with a enums
        doc = Document.from_dict(doc_details)
        doc_json = doc.to_dict()
        doc_json["parse_options"] = parse_options
        logger.info(f"uploading document: {filename}")
        if nosqldb.create_document(doc, doc_json):
            if PERFORM_TIKA_OCR:
                ocr_thread = threading.Thread(
                    target=ocr_first_page,
                    args=(),
                    kwargs={"raw_file": tmp_file, "doc": doc},
                )
                thumbnail_thread = threading.Thread(
                    target=get_page_thumbnail,
                    args=(),
                    kwargs={"raw_file": tmp_file, "doc": doc},
                )
                ocr_thread.start()
                thumbnail_thread.start()

            task = nosqldb.insert_task(
                user["id"],
                "ingestion",
                {
                    "doc_id": doc_id,
                    "workspace_idx": workspace_id,
                    "user_obj": user,
                    "re_ingest": False,
                },
            )

            # send task to rabbitmq producer
            res = producer.send(task)
            if res:
                logger.info("Document queued")
                return make_response(
                    jsonify(
                        {
                            "id": doc_id,
                            "status": "queued",
                            "task": {
                                "_id": task.get("_id", ""),
                            },
                            "detail": "Document queued",
                        },
                    ),
                    200,
                )
            else:
                nosqldb.delete_task(task["_id"])
                ingest_document(
                    doc,
                    rerun_extraction=False,
                    parse_options=parse_options,
                    user_profile=user,
                    re_ingest=False,
                )
                logger.info("Document successfully uploaded")

                return make_response(
                    jsonify(IdWithMessage(doc_id, MSG_UPLOAD_SUCCESSFUL)),
                )
        else:
            logger.error("upload status unknown")
            status, rc, msg = "fail", 500, "unknown upload status"
    except Exception as e:
        logger.error(
            f"error uploading file, stacktrace: {traceback.format_exc()}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, str(e)

    finally:
        if tmp_file and os.path.exists(tmp_file):
            os.unlink(tmp_file)
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_ml_bbox(
    user,
    token_info,
    document_id,
    page_idx=-1,
):
    user_obj = token_info["user_obj"]
    if not user_obj:
        logger.error(f"invalid user {user}, action denied")
        return unauthorized_response()

    doc = nosqldb.get_document_info_by_id(
        document_id,
        return_dict=True,
    )
    if not doc:
        logger.error(f"document {document_id} not found")
        return err_response("document not found", 400)
    user_permission, ws = nosqldb.get_user_permission(
        doc["workspace_id"],
        email=user,
        user_json=user_obj,
    )
    if user_permission not in ["admin", "owner", "editor"]:
        err_str = "Not authorized"
        log_str = f"user {user} not authorized to retrieve ML Boxes for {document_id}"
        logger.info(log_str)
        return err_response(err_str, 403)

    ml_bbox = nosqldb.get_inference_bbox(document_id, page_idx)
    return make_response(jsonify({"ml_bbox": ml_bbox}), 200)


def run_ml_bbox(user, token_info, document_id):

    user_obj = token_info["user_obj"]
    if not user_obj:
        logger.error(f"invalid user {user}, action denied")
        return unauthorized_response()

    doc = nosqldb.get_document_info_by_id(
        document_id,
        return_dict=True,
    )
    if not doc:
        logger.error(f"document {document_id} not found")
        return err_response("document not found", 400)
    user_permission, ws = nosqldb.get_user_permission(
        doc["workspace_id"],
        email=user,
        user_json=user_obj,
    )
    if user_permission not in ["admin", "owner", "editor"]:
        err_str = "Not authorized"
        log_str = f"user {user} not authorized to run ML Boxes for {document_id}"
        logger.info(log_str)
        return err_response(err_str, 403)

    user_id = token_info["user_obj"]["id"]
    run_yolo_inference(user_id, document_id)


def get_doc_status_in_workspace(
    user,
    token_info,
    workspace_id,
    nosql_db=nosqldb,
):  # noqa: E501
    """Delete an existing document

     # noqa: E501

    :param user: User email id
    :param token_info: Return object from the Authenticate function. (Token authentication)
    :param workspace_id: Workspace ID
    :param nosql_db:

    :rtype: IdWithMessage
    """
    try:
        user_obj = token_info["user_obj"]
        if not user_obj:
            logger.error(f"invalid user {user}, action denied")
            return unauthorized_response()

        user_permission, ws = nosql_db.get_user_permission(
            workspace_id,
            email=user,
            user_json=user_obj,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to access status of documents"
            log_str = f"User {user} not authorized to access status of documents from workspace {workspace_id}"
            logger.info(log_str)
            return err_response(err_str, 403)

        docs_status = nosqldb.get_status_of_docs_in(workspace_id)
        return make_response(jsonify(docs_status), 200)

    except Exception as e:
        logger.error(
            f"error accessing status of documents from workspace {workspace_id}, err: {traceback.format_exc()}",
        )
        status, rc, msg = "fail", 500, str(e)
        return make_response(jsonify({"status": status, "reason": msg}), rc)
