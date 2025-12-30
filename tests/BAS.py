# ############################################################################################################################# 
# region Import

from src.utils.spark_manager import SparkManager
from pyspark.sql import DataFrame


spark = SparkManager.get_session()
df : DataFrame = spark.read.format("delta").load("data/bronze/cards")
df.show(20,False)

df = spark.read.format("delta").load("data/silver/cards").show(20,False)

import duckdb

duckdb.sql("SELECT * FROM 'data/bronze/cards/*.parquet' LIMIT 5").show(100,False)
print("Spark est opérationnel sur ton setup !")

# endregion
# #############################################################################################################################
 


# ############################################################################################################################# 
# region Silver Transfo

# 1. Lecture de la couche Silver
df_silver = self.spark.read.format("delta").load("data/silver/cards")

# 2. Calcul des indicateurs de prix (Arbitrage)
# On calcule la différence et le pourcentage d'écart
df_gold = df_silver.withColumn(
    "price_diff", F.abs(F.col("price_tcg") - F.col("price_cm"))
).withColumn(
    "price_gap_pct", 
    (F.col("price_diff") / F.col("price_cm")) * 100
)

# 3. Filtrage : On ne garde que les opportunités sérieuses
# - Prix min 1€ (pour éviter les divisions par zéro ou cartes sans valeur)
# - Un écart de plus de 20%
df_gold = df_gold.filter(
    (F.col("price_cm") > 1) & (F.col("price_gap_pct") > 20)
)

# 4. Sélection finale des colonnes pour le "Métier"
df_gold = df_gold.select(
    "name",
    "type",
    "archetype",
    "price_cm",
    "price_tcg",
    F.round("price_diff", 2).alias("price_diff"),
    F.round("price_gap_pct", 1).alias("price_gap_pct")
).orderBy(F.desc("price_diff"))

# 5. Écriture en Delta Gold
print("💾 Sauvegarde dans la couche Gold...")
df_gold.write.format("delta") \
    .mode("overwrite") \
    .save("data/gold/arbitrage_opportunities")

print(f"✅ Table Gold créée avec {df_gold.count()} opportunités détectées.")
# endregion
# #############################################################################################################################