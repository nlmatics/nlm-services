import logging
import os

from nlm_utils.model_client import YoloClient

from .base_task import BaseTask
from server.storage import nosql_db


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class YoloTask(BaseTask):
    task_name = "yolo"

    def run(self):
        # get the arguments from Thread
        (task,) = self._args
        exception_queue = self._kwargs.get("exception_queue")
        task_body = task["body"]

        try:
            yolo = YoloClient(
                url=os.getenv(
                    "IMAGE_MODEL_SERVER_URL"                ),
            )
            pages = yolo(task_body["doc_id"])

            nosql_db.save_inference_doc(task_body["doc_id"], pages)
            logger.info("yolo inference saved in the database")

        except Exception as e:
            self.logger.error(e, exc_info=True)
            if exception_queue:
                exception_queue.put(e)
