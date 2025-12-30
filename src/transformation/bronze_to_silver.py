from pyspark.sql import functions as F
from src.utils.spark_manager import SparkManager

class SilverTransformation:
    def __init__(self):
        self.spark = SparkManager.get_session()

    def run(self):
        print("🥈 Transformation Bronze vers Silver en cours...")
        
        df_bronze = self.spark.read.format("delta").load("data/bronze/cards")

        df_silver = df_bronze.select(
            F.col("id").cast("int"),
            F.col("name"),
            F.col("type"),
            F.col("race"),
            F.col("archetype"),
            F.col("card_prices").getItem(0).getItem("cardmarket_price").cast("decimal(10,2)").alias("price_cm"),
            F.col("card_prices").getItem(0).getItem("tcgplayer_price").cast("decimal(10,2)").alias("price_tcg"),
            F.col("ingested_at").cast("timestamp")
        )

        df_silver = df_silver.dropDuplicates(["id", "name"])

        print("💾 Sauvegarde dans la couche Silver...")
        df_silver.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save("data/silver/cards")

        print(f"✅ Couche Silver prête : {df_silver.count()} cartes traitées.")

if __name__ == "__main__":
    transformer = SilverTransformation()
    transformer.run()