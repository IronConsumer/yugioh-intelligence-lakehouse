from pyspark.sql import functions as F
from src.utils.spark_manager import SparkManager
from pyspark.sql import functions as F
from src.transformation.base_transformer import BaseTransformer
from src.config.schema import CardColumns as Col

class SilverTransformation(BaseTransformer):
    def run(self):
        """Exécute le pipeline de transformation de la couche Bronze vers la couche Silver.
        
        Cette méthode charge les données brutes au format Delta depuis la zone Bronze, 
        applique un schéma typé (casting, extraction de prix imbriqués) et sauvegarde 
        le résultat nettoyé dans la zone Silver en mode overwrite.

        Args:
            self: Instance de la classe contenant la SparkSession et les utilitaires de log.

        Returns:
            None
            
        Raises:
            AnalysisException: Si le chemin source Bronze ou les colonnes spécifiées sont absents.
        """
        
        self.log_progress("Bronze -> Silver (Nettoyage)")
        
        df = self.spark.read.format("delta").load("data/bronze/cards")

        df_silver = df.select(
            F.col(Col.ID).cast("int"),
            F.col(Col.NAME),
            F.col(Col.TYPE),
            F.col(Col.RAW_PRICES).getItem(0).getItem("cardmarket_price")
                .cast("decimal(10,2)").alias(Col.PRICE_CM),
            F.col(Col.RAW_PRICES).getItem(0).getItem("tcgplayer_price")
                .cast("decimal(10,2)").alias(Col.PRICE_TCG),
            F.col(Col.INGESTED_AT).cast("timestamp")
        )

        df_silver.write.format("delta").mode("overwrite").save("data/silver/cards")

        self.log_progress(f"Succès : {df_silver.count()} lignes écrites dans Silver.")
        
if __name__ == "__main__":
    transformer = SilverTransformation()
    transformer.run()