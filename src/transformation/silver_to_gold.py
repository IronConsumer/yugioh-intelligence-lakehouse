from pyspark.sql import functions as F
from src.utils.spark_manager import SparkManager

class GoldTransformation:
    def __init__(self):
        self.spark = SparkManager.get_session()

    def run(self):
        print("🥇 Création de la vue Gold: Arbitrage des prix...")
        
        df_silver = self.spark.read.format("delta").load("data/silver/cards")
        df_gold = df_silver.withColumn(
            "price_diff", F.abs(F.col("price_tcg") - F.col("price_cm"))
        ).withColumn(
            "price_gap_pct", 
            (F.col("price_diff") / F.col("price_cm")) * 100
        )

        df_gold = df_gold.filter(
            (F.col("price_cm") > 1) & (F.col("price_gap_pct") > 20)
        )

        df_gold = df_gold.select(
            "name",
            "type",
            "archetype",
            "price_cm",
            "price_tcg",
            F.round("price_diff", 2).alias("price_diff"),
            F.round("price_gap_pct", 1).alias("price_gap_pct")
        ).orderBy(F.desc("price_diff"))

        print("💾 Sauvegarde dans la couche Gold...")
        df_gold.write.format("delta") \
            .mode("overwrite") \
            .save("data/gold/arbitrage_opportunities")

        print(f"✅ Table Gold créée avec {df_gold.count()} opportunités détectées.")

if __name__ == "__main__":
    transformer = GoldTransformation()
    transformer.run()