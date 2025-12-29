from src.utils.spark_manager import SparkManager
from pyspark.sql import DataFrame
 

spark = SparkManager.get_session()
df : DataFrame = spark.read.format("delta").load("data/bronze/cards")
df.show(20,False)


import duckdb

duckdb.sql("SELECT * FROM 'data/bronze/cards/*.parquet' LIMIT 5").show(100,False)
print("Spark est opérationnel sur ton setup !")

