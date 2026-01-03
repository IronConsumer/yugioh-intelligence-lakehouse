# dev.py
import sys
import os

sys.path.append(os.getcwd())

from src.utils.tools import showout, clear, restart
from src.utils.spark_manager import SparkManager
from src.config.schema import CardColumns

spark = SparkManager.get_session()

print("\n🚀 Environnement de Dev prêt !")
print("""
      Outils disponibles : 
      \t - spark,
      \t - CardColumns,
      \t - restart(),
      \t - clear(),
      \t - showout(df, 'nom')""")