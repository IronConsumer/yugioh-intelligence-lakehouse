import sys
from src.ingestion.raw_to_bronze import YgoIngestion
from src.transformation.bronze_to_silver import SilverTransformation
from src.transformation.silver_to_gold import GoldTransformation

def main():
    try:
        print("🚀 Démarrage du Pipeline Lakehouse Yu-Gi-Oh...")
        
        ingestor = YgoIngestion()
        ingestor.run()
        
        silver = SilverTransformation()
        silver.run()
        
        gold = GoldTransformation()
        gold.run()
        
        print("✨ Pipeline terminé avec succès !")
        
    except Exception as e:
        print(f"💥 Erreur critique durant le pipeline : {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()