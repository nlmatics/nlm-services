import logging

from .base_task import BaseTask
from server.controllers.extraction_controller import apply_template
from server.storage import nosql_db
from server.utils import str_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ExtractionTask(BaseTask):
    task_name = "extraction"

    def run(self):
        # get the arguments from Thread
        (task,) = self._args
        task_body = task["body"]
        exception_queue = self._kwargs.get("exception_queue")

        try:

            field = nosql_db.get_field_by_id(task_body["override_topic"])

            nosql_db.update_field_extraction_status(
                field_idx=field.id,
                action="extracting",
            )
            if task_body.get("is_dependent_field", False):
                if task_body.get("doc_meta_param", None):
                    nosql_db.create_workflow_fields_from_doc_meta(
                        task_body["workspace_idx"],
                        task_body["field_bundle_idx"],
                        task_body["override_topic"],
                        task_body["doc_meta_param"],
                        task_body["name"],
                        task_body["created_time"],
                    )
                elif task_body.get("field_options", {}):
                    nosql_db.create_fields_dependent_workflow_field_values(
                        task_body["workspace_idx"],
                        task_body["field_bundle_idx"],
                        task_body["override_topic"],
                        task_body["field_options"],
                        task_body["name"],
                        task_body["created_time"],
                    )
            else:
                apply_template(**task_body)

            nosql_db.update_field_extraction_status(
                field_idx=field.id,
                action="batch_done",
                doc_per_page=task_body["doc_per_page"],
            )
            # Perform dependent field update.
            if field.options and field.options.get("child_fields", []):
                status_field_ref = nosql_db.get_field_status(
                    task_body["override_topic"],
                )
                if status_field_ref and (
                    status_field_ref.get("status", {}).get("progress", "") == "done"
                    or (
                        status_field_ref.get("status", {}).get("done", 0)
                        and status_field_ref.get("status", {}).get("done", 0)
                        >= status_field_ref.get("status", {}).get("total", 0)
                    )
                ):
                    current_time = str_utils.timestamp_as_str()
                    for child_field_id in field.options.get("child_fields", []):
                        child_field = nosql_db.get_field_by_id(child_field_id)
                        logger.info(
                            f"Extracting dependent field {child_field_id} with options {child_field.options}"
                            f" for workspace {child_field.workspace_id}",
                        )
                        nosql_db.create_fields_dependent_workflow_field_values(
                            child_field.workspace_id,
                            child_field.parent_bundle_id,
                            child_field_id,
                            child_field.options,
                            "extraction_task",
                            current_time,
                        )

        except Exception as e:
            self.logger.error(e, exc_info=True)
            if exception_queue:
                exception_queue.put(e)
