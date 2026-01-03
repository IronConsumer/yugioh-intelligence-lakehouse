from dataclasses import dataclass
from pyspark.sql.types import *


# ############################################################################################################################# 
# region Bronze columns
@dataclass(frozen=True)
class BronzeColumn:
    archetype              : StructField =StructField('archetype',             StringType(), True)
    card_images            : StructField =StructField('card_images',           ArrayType(MapType(StringType(), LongType(), True), True), True)
    card_prices            : StructField =StructField('card_prices',           ArrayType(MapType(StringType(), StringType(), True), True), True)
    card_sets              : StructField =StructField('card_sets',             ArrayType(MapType(StringType(), StringType(), True), True), True)
    desc                   : StructField =StructField('desc',                  StringType(), True)
    frameType              : StructField =StructField('frameType',             StringType(), True)
    humanReadableCardType  : StructField =StructField('humanReadableCardType', StringType(), True)
    id                     : StructField =StructField('id',                    LongType(), True)
    name                   : StructField =StructField('name',                  StringType(), True)
    race                   : StructField =StructField('race',                  StringType(), True)
    type                   : StructField =StructField('type',                  StringType(), True)
    ygoprodeck_url         : StructField =StructField('ygoprodeck_url',        StringType(), True)
    atk                    : StructField =StructField('atk',                   LongType(), True)
    attribute              : StructField =StructField('attribute',             StringType(), True)
    defence                : StructField =StructField('def',                   LongType(), True)
    level                  : StructField =StructField('level',                 LongType(), True)
    typeline               : StructField =StructField('typeline',              ArrayType(StringType(), True), True)
    linkmarkers            : StructField =StructField('linkmarkers',           ArrayType(StringType(), True), True)
    linkval                : StructField =StructField('linkval',               LongType(), True)
    monster_desc           : StructField =StructField('monster_desc',          StringType(), True)
    pend_desc              : StructField =StructField('pend_desc',             StringType(), True)
    scale                  : StructField =StructField('scale',                 LongType(), True)
    banlist_info           : StructField =StructField('banlist_info',          MapType(StringType(), StringType(), True), True)
    ingested_at            : StructField =StructField('ingested_at',           StringType(), True)

    # ############################################################################################################################# 
    # region card_sets ARRAY
    set_rarity       : StructField = StructField("set_rarity",      StringType(), True)
    set_code         : StructField = StructField("set_code",        StringType(), True)
    set_name         : StructField = StructField("set_name",        StringType(), True)
    set_rarity_code  : StructField = StructField("set_rarity_code", StringType(), True)
    set_price        : StructField = StructField("set_price",       StringType(), True)
    # endregion
    # #############################################################################################################################
   
    # ############################################################################################################################# 
    # region card_price
    coolstuffinc_price : StructField = StructField("coolstuffinc_price", DecimalType(10, 2), True)
    amazon_price       : StructField = StructField("amazon_price",       DecimalType(10, 2), True)
    tcgplayer_price    : StructField = StructField("tcgplayer_price",    DecimalType(10, 2), True)
    ebay_price         : StructField = StructField("ebay_price",         DecimalType(10, 2), True)
    cardmarket_price   : StructField = StructField("cardmarket_price",   DecimalType(10, 2), True)
    # endregion
    # #############################################################################################################################

    # ############################################################################################################################# 
    # region ban list key
    ban_tcg = "ban_tcg"
    ban_ocg = "ban_ocg"
    ban_goat = "ban_goat"    
    # endregion
    # #############################################################################################################################    
    
         
# endregion
# #############################################################################################################################


# ############################################################################################################################# 
# region CardColumns
@dataclass(frozen=True)
class CardColumns:
    
    # Colonnes Silver
    PRICE_CM = "price_cm"
    PRICE_TCG = "price_tcg"
    INGESTED_AT = "ingested_at"
    
    # Colonnes Gold
    PRICE_DIFF = "price_diff"
    PRICE_GAP_PCT = "price_gap_pct"    
# endregion
# #############################################################################################################################
