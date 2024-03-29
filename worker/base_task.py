import logging
from threading import Thread


class BaseTask(Thread):
    task_name = "base_task"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def run(self, *args, **kwargs):
        raise NotImplementedError
