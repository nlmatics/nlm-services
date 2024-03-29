import logging
import os

from nlm_utils.model_client import ClassificationClient

from .base_task import BaseTask
from server.storage import nosql_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ActiveLearningTask(BaseTask):
    task_name = "active_learning"

    def run(self):
        # get the arguments from Thread
        (task,) = self._args
        exception_queue = self._kwargs.get("exception_queue")
        task_body = task["body"]

        clients = task_body["clients"]
        prepared_samples = task_body["prepared_samples"]
        ids = task_body["ids"]

        qnli_client = (
            ClassificationClient(
                model="roberta",
                task="qnli",
                url=os.getenv("MODEL_SERVER_URL"),
            )
            if "qnli" in clients
            else None
        )

        boolq_client = (
            ClassificationClient(
                model="roberta",
                task="boolq",
                url=os.getenv("BOOLQ_MODEL_SERVER_URL", os.getenv("MODEL_SERVER_URL")),
            )
            if "boolq" in clients
            else None
        )

        phrase_qa_client = (
            ClassificationClient(
                model="roberta",
                task="roberta-phraseqa",
                url=os.getenv("MODEL_SERVER_URL"),
            )
            if "phraseqa" in clients
            else None
        )

        qa_client = (
            ClassificationClient(
                model="roberta",
                task="roberta-qa",
                url=os.getenv("QA_MODEL_SERVER_URL", os.getenv("MODEL_SERVER_URL")),
            )
            if "qa" in clients
            else None
        )

        try:
            nosql_db.update_saved_search_status(ids, "training")

            update_workers = True
            if boolq_client and len(prepared_samples["boolq"]["questions"]):
                logger.info("training boolq")
                boolq_client.active_learning(
                    questions=prepared_samples["boolq"]["questions"],
                    sentences=prepared_samples["boolq"]["sentences"],
                    labels=prepared_samples["boolq"]["labels"],
                    update_workers=update_workers,
                )
                logger.info("training boolq complete")

            if qnli_client and len(prepared_samples["qnli"]["questions"]):
                logger.info("training qnli")
                qnli_client.active_learning(
                    questions=prepared_samples["qnli"]["questions"],
                    sentences=prepared_samples["qnli"]["sentences"],
                    labels=prepared_samples["qnli"]["labels"],
                    update_workers=update_workers,
                )
                logger.info("training qnli complete")

            if qa_client and len(prepared_samples["qa"]["questions"]):
                logger.info("training qa")
                qa_client.active_learning(
                    questions=prepared_samples["qa"]["questions"],
                    sentences=prepared_samples["qa"]["sentences"],
                    answers=prepared_samples["qa"]["answers"],
                    update_workers=update_workers,
                )
                logger.info("training qa complete")

            if phrase_qa_client and len(prepared_samples["phraseqa"]["questions"]):
                logger.info("training phraseqa")
                phrase_qa_client.active_learning(
                    questions=prepared_samples["phraseqa"]["questions"],
                    sentences=prepared_samples["phraseqa"]["sentences"],
                    answers=prepared_samples["phraseqa"]["answers"],
                    update_workers=update_workers,
                )
                logger.info("training phraseqa complete")

            nosql_db.update_saved_search_status(ids, "trained")

        except Exception as e:
            nosql_db.update_saved_search_status(ids, "failed")
            self.logger.error(e, exc_info=True)
            if exception_queue:
                exception_queue.put(e)
