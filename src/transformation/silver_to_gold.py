from pyspark.sql import functions as F
from src.transformation.base_transformer import BaseTransformer
from src.config.schema import CardColumns as Col

class GoldTransformation(BaseTransformer):
    """
    Transforme la couche Silver en couche Gold pour identifier 
    les opportunités d'arbitrage de prix.
    """


    def run(self): 
        """Exécute le pipeline de transformation de la couche Silver vers la couche Gold.
        
        Cette méthode calcule des indicateurs d'intelligence métier pour identifier des 
        opportunités d'arbitrage de prix entre Cardmarket et TCGPlayer. Elle filtre 
        les résultats pour ne conserver que les écarts significatifs et sauvegarde 
        la table finale pour l'exploitation analytique.

        Calculs effectués :
            - PRICE_DIFF : Différence absolue entre PRICE_TCG et PRICE_CM.
            - PRICE_GAP_PCT : Écart en pourcentage par rapport au prix Cardmarket.

        Filtres appliqués :
            - Prix Cardmarket > 1 unit.
            - Écart de prix (gap) > 20%.

        Args:
            self: Instance de la classe contenant la SparkSession et les utilitaires de log.

        Returns:
            None
            
        Raises:
            AnalysisException: Si le chemin Silver est inaccessible ou si les colonnes requises manquent.
        """
        self.log_progress("Silver -> Gold (Intelligence Métier)")
        
        df_silver = self.spark.read.format("delta").load("data/silver/cards")

        df_gold = ( 
            df_silver
                .withColumn(
                    Col.PRICE_DIFF, 
                    F.abs(F.col(Col.PRICE_TCG) - F.col(Col.PRICE_CM)))
                .withColumn(
                    Col.PRICE_GAP_PCT, 
                    (F.col(Col.PRICE_DIFF) / F.col(Col.PRICE_CM)) * 100)
        )

        df_gold = df_gold.filter(
            (F.col(Col.PRICE_CM) > 1) & (F.col(Col.PRICE_GAP_PCT) > 20)
        )

        df_gold = df_gold.select(
            Col.NAME,
            Col.TYPE,
            Col.ARCHETYPE,
            Col.PRICE_CM,
            Col.PRICE_TCG,
            F.round(Col.PRICE_DIFF, 2).alias(Col.PRICE_DIFF),
            F.round(Col.PRICE_GAP_PCT, 1).alias(Col.PRICE_GAP_PCT)
        ).orderBy(F.desc(Col.PRICE_DIFF))

        output_path = "data/gold/arbitrage_opportunities"
        df_gold.write.format("delta") \
            .mode("overwrite") \
            .save(output_path)

        self.log_progress(f"Table Gold générée avec {df_gold.count()} lignes.")

if __name__ == "__main__":
    transformer = GoldTransformation()
    transformer.run()