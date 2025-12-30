from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from src.utils.spark_manager import SparkManager

class BaseTransformer(ABC):
    def __init__(self) -> None:
        # On précise que self.spark est une SparkSession
        self.spark: SparkSession = SparkManager.get_session()

    @abstractmethod
    def run(self) -> None:
        pass

    def log_progress(self, message: str) -> None:
        print(f"[PIPELINE STEP] : {message}")