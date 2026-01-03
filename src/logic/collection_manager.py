from pyspark.sql import functions as F, SparkSession
from pyspark.sql import DataFrame

def get_rarity_reference(spark):
    """
    Crée le référentiel de hiérarchie des raretés.
    Plus le rang est bas (1), plus la carte est 'prestigieuse'.
    """
    data = [
        ("Quarter Century Secret Rare", 1, "(QCSR)"),
        ("Starlight Rare", 2, "SLR"),
        ("Ultimate Rare", 3, "UTI"),
        ("Collector's Rare", 4, "CR"),
        ("Secret Rare", 5, "SCR"),
        ("Ultra Rare", 6, "UR"),
        ("Super Rare", 7, "SR"),
        ("Rare", 8, "R"),
        ("Common", 9, "C")
    ]
    return spark.createDataFrame(data, ["rarity_name", "rarity_rank", "rarity_short"])

def apply_rarity_hierarchy(df_collection: DataFrame, spark):
    """
    Joint la collection utilisateur avec le référentiel des raretés.
    """
    df_ref = get_rarity_reference(spark)
    
    return (
        df_collection
            .join(df_ref, df_collection.cardrarity == df_ref.rarity_name, "left")
            .fillna({"rarity_rank": 99, "rarity_short": "N/A"})
            .drop("rarity_name")
    )

def get_passion_sort_view(df_normalized: DataFrame):
    """
    Vue 'Passion' : Trié par Extension (set_code) puis par prestige (rarity_rank).
    """
    return (
        df_normalized
            .orderBy(
                F.col("cardset").asc(),
                F.col("rarity_rank").asc(),
                F.col("cardname").asc()
            )
            .select(
                "rarity_rank",
                "cardrarity",
                "cardset",
                "set_code",
                "cardname",
                "cardq"
            )
    )

def ingest_user_collection(csv_path: str, spark_session : SparkSession ):
    df_raw = spark_session.read.option("header", "true").csv(csv_path)
    df_collection = (
        df_raw
        .withColumnRenamed("cardid", "id")
        .withColumnRenamed("cardcode", "set_code")
        .withColumn("cardrarity", 
            F.when(F.col("cardrarity") == "Quarter Century Secret Rare", "(QCSR)")
             .otherwise(F.col("cardrarity"))
        )
        .withColumn("sku", F.concat_ws("_", F.col("id"), F.col("set_code"), F.col("cardrarity")))
        .withColumn("last_inventory_update", F.current_timestamp())
    )
    
    return df_collection

def apply_storage_rules(df_ranked: DataFrame):
    """
    Définit l'emplacement physique de la carte selon sa rareté et sa quantité.
    """
    return (
        df_ranked
        .withColumn("storage_type", 
            F.when(F.col("rarity_rank") <= 4, "CLASSEUR_PRESTIGE")
             .when(F.col("rarity_rank").between(5, 7), "CLASSEUR_TOP_TIER")
             .otherwise("BOITE_VRAC")
        )
        .withColumn("storage_address", 
            F.concat(
                F.col("storage_type"),
                F.lit(" | "),
                # Pour les classeurs, on range par set, pour le vrac par type/attribut
                F.when(F.col("storage_type").contains("CLASSEUR"), F.col("cardset"))
                 .otherwise(F.lit("STOCK_GENERAL"))
            )
        )
        # On calcule aussi si on a un playset complet (3 cartes ou +)
        .withColumn("is_playset", F.col("cardq") >= 3)
    )