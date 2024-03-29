from .active_learning_task import ActiveLearningTask
from .base_task import BaseTask
from .extraction_task import ExtractionTask
from .html_crawl_task import HTMLCrawlTask
from .ingestion_task import IngestionTask
from .yolo_task import YoloTask

__all__ = (
    "BaseTask",
    "ExtractionTask",
    "HTMLCrawlTask",
    "IngestionTask",
    "YoloTask",
    "ActiveLearningTask",
)
