#!/usr/bin/env python
import json
import logging
import os
import queue
import traceback

import pika
from bson.objectid import ObjectId

from server.storage import nosql_db
from worker import ActiveLearningTask
from worker import BaseTask
from worker import ExtractionTask
from worker import HTMLCrawlTask
from worker import IngestionTask
from worker import YoloTask
from worker.ingestion_task import start_tika


def set_task_status(_id, status, detail=None):
    nosql_db.db["task"].find_one_and_update(
        {"_id": ObjectId(_id)},
        {"$set": {"status": status, "detail": detail}},
    )


LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logging.getLogger("pika").setLevel(logging.ERROR)
MAX_CONNECTION_RETRY = 5


class NLMWorker:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # to hold running exception
        self.exception_queue = queue.Queue()

        rabbit_host = os.getenv("NLM_MQ_HOST", "localhost")
        rabbit_username = os.getenv("NLM_MQ_USERNAME", "")
        rabbit_password = os.getenv("NLM_MQ_PASSWORD", "")
        if rabbit_username and rabbit_password:
            credentials = pika.PlainCredentials(rabbit_username, rabbit_password)
            parameters = pika.ConnectionParameters(
                rabbit_host,
                5672,
                "nlm",
                credentials,
            )
        else:
            parameters = pika.ConnectionParameters(
                host=rabbit_host,
                port=5672,
            )

        self.params = parameters

        self.connection = pika.BlockingConnection(parameters)

        self.channel = self.connection.channel()

        self.channel.queue_declare(queue="task_queue", durable=True)

        self.tasks = {}

        self.conn_retry_count = 0

    def add_task(self, task_class: BaseTask):
        # create an instance of the class
        if task_class.task_name in self.tasks:
            raise ValueError("task {task_class} already exists")
        self.tasks[task_class.task_name] = task_class

    def run_server(self):
        self.logger.info(f"worker registered with tasks: {self.tasks}")

        def callback(ch, method, properties, body):

            task = json.loads(body)
            self.logger.info(f"receive a task for {task['task_name']}")

            if task["task_name"] not in self.tasks:
                set_task_status(
                    task["_id"],
                    "failed",
                    f"task {task['task_name']} not found",
                )
            else:
                task_class = self.tasks[task["task_name"]]
                thread = task_class(
                    args=(task,),
                    kwargs={"exception_queue": self.exception_queue},
                )
                # daemon the working thread
                thread.daemon = True
                thread.start()
                # send heartbeat to rabbit if thread is alive
                while thread.is_alive():
                    self.connection.process_data_events()
                    # block thread process for 5 second
                    thread.join(timeout=5)

                # checking for exception in the working thread
                try:
                    exception = self.exception_queue.get(block=False)
                except queue.Empty:
                    # empty queue, task finished without exception
                    self.logger.info(f"task {task['task_name']} completed")
                    set_task_status(task["_id"], "completed")
                else:
                    # exception found, mark task as failed
                    self.logger.info(
                        f"task {task['task_name']} failed. \n{task}\n{exception}",
                    )
                    set_task_status(task["_id"], "failed", str(exception))

            # send ack to ribbit
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue="task_queue", on_message_callback=callback)
        self.logger.info("worker is ready for task.")
        try:
            self.channel.start_consuming()
        except Exception as e:
            self.logger.critical(
                f"Worker error {str(e)}, err: {traceback.format_exc()}",
            )
            self.channel.stop_consuming()
            self.connection.close()
            if self.conn_retry_count <= MAX_CONNECTION_RETRY:
                self.conn_retry_count += 1
                self.connection = pika.BlockingConnection(self.params)
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue="task_queue", durable=True)
                self.run_server()


if __name__ == "__main__":
    worker = NLMWorker()

    start_tika()

    # check for allowed tasks
    tasks_from_env = os.environ.get(
        "TASKS",
        "IngestionTask HTMLCrawlTask ExtractionTask YoloTask ActiveLearningTask",
    ).split()

    if "SecRssTask" in tasks_from_env:
        from worker.sec_rss_task import SecRssTask

        SecRssTask().run()
    if "IngestionTask" in tasks_from_env:
        worker.add_task(IngestionTask)
    if "HTMLCrawlTask" in tasks_from_env:
        worker.add_task(HTMLCrawlTask)
    if "ExtractionTask" in tasks_from_env:
        worker.add_task(ExtractionTask)
    if "YoloTask" in tasks_from_env:
        worker.add_task(YoloTask)
    if "ActiveLearningTask" in tasks_from_env:
        worker.add_task(ActiveLearningTask)

    worker.run_server()
