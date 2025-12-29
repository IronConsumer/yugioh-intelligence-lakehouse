from pydantic import BaseModel, Field
from typing import List, Optional

class CardPrice(BaseModel):
    cardmarket_price: str
    tcgplayer_price: str
    ebay_price: str

class Card(BaseModel):
    id: int
    name: str
    type: str
    desc: str
    race: Optional[str] = None
    archetype: Optional[str] = None
    card_prices: List[CardPrice]