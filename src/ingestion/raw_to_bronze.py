import requests
from src.utils.spark_manager import SparkManager
from datetime import datetime
from pyspark.sql.functions import lit

class YgoIngestion:
    API_URL = "https://db.ygoprodeck.com/api/v7/cardinfo.php"

    def __init__(self):
        self.spark = SparkManager.get_session()

    def fetch_data(self):
        print("🛰️ Récupération des données depuis l'API...")
        response = requests.get(self.API_URL)
        response.raise_for_status()
        return response.json()["data"]

    def run(self):
        raw_data = self.fetch_data()
        
        ingestion_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        df = self.spark.createDataFrame(raw_data)
        
        df = df.withColumn("ingested_at", lit(ingestion_date))

        print("💾 Sauvegarde dans la couche Bronze (Delta)...")
        df.write.format("delta") \
            .mode("overwrite") \
            .save("data/bronze/cards")
        
        print(f"✅ Ingestion terminée ! {df.count()} cartes sauvegardées.")

if __name__ == "__main__":
    ingestor = YgoIngestion()
    ingestor.run()