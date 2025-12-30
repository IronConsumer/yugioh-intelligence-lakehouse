from dataclasses import dataclass

@dataclass(frozen=True)
class CardColumns:
    # Colonnes Source / Bronze
    ID = "id"
    NAME = "name"
    TYPE = "type"
    RACE = "race"
    ARCHETYPE = "archetype"
    RAW_PRICES = "card_prices"
    
    # Colonnes Silver
    PRICE_CM = "price_cm"
    PRICE_TCG = "price_tcg"
    INGESTED_AT = "ingested_at"
    
    # Colonnes Gold
    PRICE_DIFF = "price_diff"
    PRICE_GAP_PCT = "price_gap_pct"