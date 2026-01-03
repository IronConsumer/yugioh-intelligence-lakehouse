# ############################################################################################################################# 
# region Import

from src.utils.spark_manager import SparkManager
from src.utils.tools import showout, restart, clear
from pyspark.sql import DataFrame
# endregion
# #############################################################################################################################
 
# ############################################################################################################################# 
# region check bronze

spark = SparkManager.get_session()
df : DataFrame = spark.read.format("delta").load("data/bronze/cards")
df.show(20,False)

showout(df)

# endregion
# #############################################################################################################################
