from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

class SparkManager:
    """Gère la session Spark de manière centralisée."""
    
    @staticmethod
    def get_session(app_name="YGO_Project"):
        builder = (
            SparkSession.builder
                .appName(app_name)
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
                .config("spark.ui.showConsoleProgress", "true")
                .master("local[*]") # Utilise tous les coeurs de ton Ryzen 7
        )

        return configure_spark_with_delta_pip(
            builder, 
            extra_packages=["io.delta:delta-spark_2.12:3.0.0"]
        ).getOrCreate()