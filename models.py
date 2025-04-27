import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Union
from uuid import UUID

# Import Pydantic V2 components
from pydantic import BaseModel, Field, ConfigDict # Use ConfigDict

logger = logging.getLogger(__name__)

# Helper function for timezone-aware UTC now
def tz_aware_utc_now():
    return datetime.now(timezone.utc)

# --- Core Database Mapped Models ---

class Token(BaseModel):
    """Represents a tracked SPL token."""
    # Add Optional ID if mapping from DB
    # id: Optional[UUID] = None
    mint_address: str = Field(description="Unique Solana mint address")
    name: Optional[str] = Field(default=None, description="Token name (e.g., USDC)")
    symbol: Optional[str] = Field(default=None, description="Token symbol (e.g., $USDC)")
    decimals: Optional[int] = Field(default=None, description="Token decimals for conversion")
    total_supply: Optional[str] = Field(default=None, description="Total token supply (raw string)")
    logo_uri: Optional[str] = Field(default=None, description="URI for the token's logo")
    platform: str = Field(default="solana", description="Blockchain platform")
    created_at: datetime = Field(default_factory=tz_aware_utc_now)
    updated_at: Optional[datetime] = Field(default=None)

    # --- Pydantic V2 Configuration ---
    model_config = ConfigDict(
        from_attributes=True, # Replaces orm_mode
        populate_by_name=True # Optional: Allows matching DB column names to different field names if aliased
    )

class Metrics(BaseModel):
    """Time-series metrics for a token."""
    # id: Optional[UUID] = None
    mint_address: str
    timestamp: datetime = Field(default_factory=tz_aware_utc_now)
    price_usd: Optional[float] = Field(default=None)
    fdv_usd: Optional[float] = Field(default=None)
    liquidity_usd: Optional[float] = Field(default=None)
    volume_1h: Optional[float] = Field(default=None)
    volume_24h: Optional[float] = Field(default=None)
    percent_change_1h: Optional[float] = Field(default=None)
    percent_change_24h: Optional[float] = Field(default=None)

    model_config = ConfigDict(from_attributes=True)

class Subscription(BaseModel):
    """Links a user chat to a tracked token."""
    # id: Optional[UUID] = None
    chat_id: int = Field(description="Telegram user/chat ID")
    mint_address: str = Field(description="Mint address of the token being tracked")
    settings: Dict[str, Any] = Field(default_factory=dict, description="User settings")
    created_at: datetime = Field(default_factory=tz_aware_utc_now)
    updated_at: Optional[datetime] = Field(default=None)

    model_config = ConfigDict(from_attributes=True)

class Holder(BaseModel):
    """Represents holder data fetched (likely not directly from DB with orm_mode)."""
    # Note: orm_mode less useful here as this is usually built from API data
    # id: Optional[UUID] = None
    mint_address: str # Needed for context maybe
    wallet_address: str = Field(description="Holder's wallet address")
    balance: float = Field(description="Decimal token amount held")
    percentage: float = Field(description="Percentage of total supply held")
    # Removed timestamp/rank - often calculated contextually

    # No DB config needed if built from API data primarily
    # model_config = ConfigDict(from_attributes=True)


class TokenSnapshot(BaseModel):
    """Aggregated data model for bot display (not usually from DB ORM)."""
    mint_address: str
    name: Optional[str] = None
    symbol: Optional[str] = None
    price_usd: Optional[float] = None
    fdv_usd: Optional[float] = None
    liquidity_usd: Optional[float] = None
    volume_1h: Optional[float] = None
    volume_24h: Optional[float] = None
    percent_change_1h: Optional[float] = None
    percent_change_24h: Optional[float] = None
    top_holders: List[Holder] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=tz_aware_utc_now)
    error: Optional[str] = Field(default=None)


class WalletState(BaseModel):
     """State for a classified wallet/token pair (maps to DB)."""
     # id: Optional[UUID] = None
     mint_address: str
     wallet_address: str
     classification: str
     initial_raw_balance: str
     current_status: str
     last_status_update: datetime
     created_at: datetime

     model_config = ConfigDict(from_attributes=True)

# --- Models for TIS Views ---
class WalletCategoryStats(BaseModel):
     count: int = 0
     statuses: Dict[str, int] = Field(default_factory=lambda: {"GREEN": 0, "YELLOW": 0, "RED": 0})
     current_holding_perc_supply: Optional[float] = None
     initial_buy_perc_supply: Optional[float] = None
     realized_profit_sol: Optional[float] = None

class TISAnalysisSummary(BaseModel):
     mint_address: str
     token_name: Optional[str] = None
     token_symbol: Optional[str] = None
     analysis_timestamp: datetime
     team: WalletCategoryStats = Field(default_factory=WalletCategoryStats)
     insider: WalletCategoryStats = Field(default_factory=WalletCategoryStats)
     sniper: WalletCategoryStats = Field(default_factory=WalletCategoryStats)
     creator: WalletCategoryStats = Field(default_factory=WalletCategoryStats) # Add creator
     error: Optional[str] = None

class WalletInfo(BaseModel):
     address: str
     status: str # GREEN, YELLOW, RED

class TISCategoryDetail(BaseModel):
     stats: WalletCategoryStats
     wallets: List[WalletInfo] = Field(default_factory=list)
     error: Optional[str] = None