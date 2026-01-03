# ############################################################################################################################# 
# region Import

# from src.utils.spark_manager import SparkManager
# from src.utils.tools import showout, restart, clear

from dev import *
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField

# endregion
# #############################################################################################################################
 
# ############################################################################################################################# 
# region check bronze


def cols(*structs: StructField) -> list:
    """Transforme une série de noms de colonnes en une liste d'objets Column Spark.
    
    Args:
        *names (str): Noms des colonnes à transformer.

    Returns:
        list: Liste contenant les objets F.col() correspondants.
    """
    return list(map(lambda struct : F.col(struct.name) , structs))

from src.config.schema import BronzeColumn as BRZ
# spark = SparkManager.get_session()
df_bronze : DataFrame = spark.read.format("delta").load("data/bronze/cards")

df_bronze.printSchema()

key_map = [
    # "card_sets",
    # "card_images", # ALL NULL
    "card_prices",
    "typeline", # type des monstre
    "linkmarkers", # pos link
    "banlist_info",
]


sets_keys = (
    df_bronze
        # .select(F.explode("linkmarkers").alias("linkmarkers"))
        # .select(F.map_keys("typeline_info"))
        .distinct()
)
sets_keys.show(truncate=False)


showout(
    df_bronze
        .filter(F.col('banlist_info').isNotNull())
        .select(
            BRZ.id.name, BRZ.name.name,
            BRZ.banlist_info.name
            # F.explode("typeline").alias("image_info"),
            # F.map_keys("image_info")
        )
)

df = (
    df_bronze
        .withColumn("set", F.explode(F.col("card_sets")))
        .select(
            *cols(
            BRZ.id,
            BRZ.name,
            BRZ.type,
            BRZ.race,
            BRZ.attribute,
            BRZ.level),
            F.col("set")[BRZ.set_code.name].alias(BRZ.set_code.name),
            F.col("set")[BRZ.set_name.name].alias(BRZ.set_name.name),
            F.col("set")[BRZ.set_rarity.name].alias(BRZ.set_rarity.name),
            F.col("set")[BRZ.set_rarity_code.name].alias(BRZ.set_rarity_code.name),
            F.col("set")[BRZ.set_price.name].alias(BRZ.set_price.name),
        )
        .drop("set")
        .filter(F.col(BRZ.id.name)==86066372)

)
showout(df,'output_exemple_set_info')


agg_calc = [
    F.count('*').alias('nb_rarity'),
    F.count_distinct(BRZ.set_name.name).alias('nb_extension')
]

(
    df
    .groupBy('set_rarity')
    .agg(*agg_calc)
    .orderBy(F.col('nb_rarity').desc())
    .show(100,False)
)

(
    df
    .filter(F.col('set_rarity')=='Platinum Secret Rare')
    .select('set_name')
    .distinct()
    .show(200,False)
)

(
    df
    .filter(
        (F.col(BRZ.set_rarity.name).isNotNull()) 
        # (F.col(BRZ.set_rarity.name)=="Quarter Century Secret Rare") 
        & 
        (
            (F.col(BRZ.set_rarity_code.name).isNull()) 
            | (F.col(BRZ.set_rarity_code.name)==" " )
        )
    )
    # .select(BRZ.set_rarity_code.name).distinct()
    # .show(truncate=False)
    .count()
)


(
    df
    .select("set_rarity_code","set_rarity")
    .distinct()
    .show(truncate=False)
)


# endregion
# #############################################################################################################################

# ############################################################################################################################# 
# region Read Collection


from src.logic.collection_manager import ingest_user_collection, apply_rarity_hierarchy, get_passion_sort_view

df_base = ingest_user_collection("data/20260102_collection_ygo.csv", spark)

df_with_rank = apply_rarity_hierarchy(df_base, spark)

df_final = get_passion_sort_view(df_with_rank)

showout(df_final, "plan_rangement_passion")

# endregion
# #############################################################################################################################

