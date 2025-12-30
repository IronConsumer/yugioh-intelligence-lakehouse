from pyspark.sql import functions as F, DataFrame
from src.utils.spark_manager import SparkManager
from src.config.schema import CardColumns as Col
from src.utils.logger import get_logger

logger = get_logger("DATA_QUALITY")

class GoldQualityChecker:
    def __init__(self) -> None:
        self.spark = SparkManager.get_session()

    def _check_volume(self, df: DataFrame) -> bool:
        """Vérifie si le volume de données est suffisant (seuil minimal de 10).
        
        Args:
            df (DataFrame): Le DataFrame Gold à analyser.
            
        Returns:
            bool: True si le test passe, False sinon.
        """
        count = df.count()
        if count < 10:
            logger.warning(f"⚠️ Volume anormalement faible : seulement {count} cartes trouvées.")
            return False
        return True

    def _check_price_consistency(self, df: DataFrame) -> bool:
        """Vérifie l'absence de prix nuls ou négatifs.
        
        Args:
            df (DataFrame): Le DataFrame Gold à analyser.
            
        Returns:
            bool: True si tous les prix sont valides, False sinon.
        """
        zero_prices = (
            df.filter(
                (F.col(Col.PRICE_CM) <= 0) | 
                (F.col(Col.PRICE_TCG) <= 0)
            ).count()
        )
        if zero_prices > 0:
            logger.error(f"❌ {zero_prices} cartes ont un prix nul ou négatif !")
            return False
        return True

    def _check_business_logic(self, df: DataFrame) -> bool:
        """Valide la cohérence mathématique entre les colonnes de prix et leur différence.
        
        Args:
            df (DataFrame): Le DataFrame Gold à analyser.
            
        Returns:
            bool: True si les calculs sont exacts, False sinon.
        """
        bad_math = (
            df.withColumn(
                "calc_diff", 
                F.round(F.abs(F.col(Col.PRICE_TCG) - F.col(Col.PRICE_CM)), 2))
              .filter("calc_diff != price_diff")
              .count()
        )
        
        if bad_math > 0:
            logger.error(f"❌ Erreur de calcul détectée sur {bad_math} lignes !")
            return False
        return True

    def run_checks(self) -> bool:
        """Exécute l'ensemble des tests de qualité sur la couche Gold.
        
        Cette méthode centralise les appels aux différents checks de validation 
        (Volume, Cohérence, Logique métier) sur les opportunités d'arbitrage.

        Returns:
            bool: True si tous les tests sont validés, False si au moins un test échoue.
        """
        logger.info("🧪 Démarrage des tests de qualité sur la couche Gold...")
        
        df_gold = (
            self.spark.read
                .format("delta")
                .load("data/gold/arbitrage_opportunities")
        )

        # Exécution des tests et agrégation des résultats
        results = [
            self._check_volume(df_gold),
            self._check_price_consistency(df_gold),
            self._check_business_logic(df_gold)
        ]

        has_error = not all(results)

        if not has_error:
            logger.info("✅ Tous les tests de qualité sont passés avec succès !")
        
        return not has_error

if __name__ == "__main__":
    checker = GoldQualityChecker()
    checker.run_checks()

