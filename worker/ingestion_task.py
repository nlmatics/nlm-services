import logging
import os
from threading import Thread

from server.utils.indexer_utils.indexer_ops import ingest_document
from nlm_utils.utils import ensure_bool

from .base_task import BaseTask
from server.controllers.extraction_controller import apply_template
from server.controllers.extraction_controller import parse_grid_query
from server.models.document import Document
from server.storage import nosql_db
from server.tika_daemon_check import check_tika
from server.utils.notification_utils import send_document_notification
from server.utils.notification_utils import send_search_criteria_workflow_notification

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
SEND_NOTIFICATIONS = ensure_bool(os.getenv("SEND_NOTIFICATIONS", False))


def start_tika():
    tika_thread = Thread(target=check_tika)
    tika_thread.daemon = True
    tika_thread.start()


class IngestionTask(BaseTask):
    task_name = "ingestion"

    def run(self):
        # get the arguments from Thread
        (task,) = self._args
        exception_queue = self._kwargs.get("exception_queue")
        task_body = task["body"]

        query = {
            "id": task_body["doc_id"],
            "workspace_id": task_body["workspace_idx"],
        }
        apply_ocr = task_body.get("apply_ocr", False)

        doc = nosql_db.db["document"].find_one(query)
        parse_options = doc.get("parse_options", None)

        if apply_ocr:
            if parse_options is None:
                parse_options = {}
            parse_options["apply_ocr"] = True

        doc = Document(**doc)
        try:
            ingest_document(
                doc,
                parse_options=parse_options,
                user_profile=task_body.get("user_obj", None),
                re_ingest=task_body.get("re_ingest", False),
            )
            logger.info("document successfully uploaded")
            notify_action = task_body.get("notify_action", None)
            if task_body.get("parent_task", "") != "html_crawling":
                # Send notification if not a re-ingest
                if (
                    notify_action
                    and SEND_NOTIFICATIONS
                    and not task_body.get("re_ingest", False)
                ):
                    notify_doc = nosql_db.db["document"].find_one(
                        {"id": task_body["doc_id"]},
                    )
                    if notify_doc and notify_doc["status"] == "ingest_ok":
                        # Only send notifications to the collaborators if the status is "ingest_ok"
                        workspace = nosql_db.get_workspace_by_id(
                            notify_doc["workspace_id"],
                        )
                        send_document_notification(
                            task_body.get("user_obj", None),
                            workspace,
                            Document(**notify_doc),
                            notify_action,
                        )
                sc_workflows = nosql_db.get_search_criteria_workflows(
                    workspace_id=doc.workspace_id,
                )
                workspace = nosql_db.get_workspace_by_id(doc.workspace_id)
                for sc_workflow in sc_workflows:
                    if sc_workflow.actions and sc_workflow.search_criteria:
                        file_filter_struct = {}
                        if sc_workflow.search_criteria.field_filter is not None:
                            start_row = 0
                            end_row = 5000
                            grid_query = {
                                "startRow": start_row,
                                "endRow": end_row,
                                "filterModel": sc_workflow.search_criteria.field_filter.get(
                                    "filter_model",
                                    {},
                                ),
                            }

                            (
                                limit,
                                skip,
                                sort_tuple_list,
                                filter_dict,
                                group_by_list,
                                value_aggregate_list,
                                review_status_filter_dict,
                            ) = parse_grid_query(grid_query, None)

                            logger.info(f"limit: {limit}")
                            logger.info(f"skip: {skip}")
                            logger.info(f"filter_dict: {filter_dict}")

                            if filter_dict:
                                file_filter_struct = (
                                    nosql_db.retrieve_grid_data_from_field_values(
                                        doc.workspace_id,
                                        sc_workflow.search_criteria.field_filter.get(
                                            "field_bundle_id",
                                            {},
                                        ),
                                        file_ids=[doc.id],
                                        limit=limit,
                                        skip=skip,
                                        sort_tuple_list=sort_tuple_list,
                                        filter_dict=filter_dict,
                                        group_by_list=group_by_list,
                                        value_aggregate_list=value_aggregate_list,
                                        return_only_file_ids=True,
                                    )
                                )
                                if not file_filter_struct.get(
                                    "results",
                                    [],
                                ) or not file_filter_struct.get(
                                    "totalMatchCount",
                                    0,
                                ):
                                    err_str = "No files matching the field filter"
                                    log_str = f"{err_str} {sc_workflow.search_criteria.field_filter}"
                                    logger.info(log_str)
                                    file_filter_struct = {}

                        facts = apply_template(
                            workspace_idx=doc.workspace_id,
                            file_idx=doc.id,
                            ad_hoc=True,
                            **sc_workflow.search_criteria.to_dict(),
                            file_filter_struct=file_filter_struct,
                        )
                        logger.info(
                            f"Matched facts for Search Criteria Workflow: {len(facts)}",
                        )
                        if len(facts) > 0:
                            send_search_criteria_workflow_notification(
                                workspace,
                                doc,
                                sc_workflow,
                                facts,
                                filter_match=True if file_filter_struct else False,
                            )
                        elif file_filter_struct:
                            send_search_criteria_workflow_notification(
                                workspace,
                                doc,
                                sc_workflow,
                                facts=[],
                                filter_match=True,
                            )

        except Exception as e:
            self.logger.error(e, exc_info=True)
            if exception_queue:
                exception_queue.put(e)
