
from abc import ABC, abstractmethod
from src.utils.spark_manager import SparkManager

class BaseTransformer(ABC):
    def __init__(self):
        self.spark = SparkManager.get_session()

    @abstractmethod
    def run(self):
        """
        Toutes les classes qui héritent de BaseTransformer 
        DOIVENT implémenter cette méthode.
        """
        pass

    def log_progress(self, message: str):
        """Une méthode commune à tous les transformers."""
        print(f"[PIPELINE STEP] : {message}")