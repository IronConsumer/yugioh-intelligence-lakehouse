from src.utils.logger import get_logger
from src.ingestion.raw_to_bronze import YgoIngestion
from src.transformation.bronze_to_silver import SilverTransformation
from src.transformation.silver_to_gold import GoldTransformation
from src.quality.gold_checks import GoldQualityChecker

# On initialise le logger pour le point d'entrée
logger = get_logger("PIPELINE_MAIN")

def main() -> None:
    try:
        logger.info("🚀 Démarrage du cycle complet de données Lakehouse")
        
        logger.info("--- ÉTAPE 1 : INGESTION ---")
        YgoIngestion().run()
        
        logger.info("--- ÉTAPE 2 : SILVER TRANSFORMATION ---")
        SilverTransformation().run()
        
        logger.info("--- ÉTAPE 3 : GOLD TRANSFORMATION ---")
        GoldTransformation().run()
        
        logger.info("--- ÉTAPE 4 : DATA QUALITY CHECKS ---")
        quality_ok = GoldQualityChecker().run_checks()
        
        if quality_ok:
            logger.info("🏁 PIPELINE COMPLET ET VALIDÉ !")
        else:
            logger.warning("🏁 Pipeline terminé mais avec des alertes qualité.")

        logger.info("✨ Pipeline exécuté avec succès. Données disponibles dans data/gold/")

    except Exception as e:
        logger.error(f"💥 Échec du pipeline : {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()