"""
Utility functions for processing Dune API data
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from data.models import Metrics, Holder

logger = logging.getLogger(__name__)


def process_dune_holders(mint_address: str, holders_data: List[Dict[str, Any]]) -> List[Holder]:
    """
    Process token holders data from Dune API
    
    Args:
        mint_address: Token mint address
        holders_data: Holders data from Dune API
        
    Returns:
        List of Holder models
    """
    holders = []
    
    try:
        for i, holder_data in enumerate(holders_data):
            holder = Holder(
                mint_address=mint_address,
                wallet_address=holder_data.get("wallet_address", ""),
                balance=float(holder_data.get("balance", 0)),
                percentage=float(holder_data.get("percentage", 0)),
                rank=i + 1,
                timestamp=datetime.utcnow()
            )
            holders.append(holder)
    except Exception as e:
        logger.error(f"Error processing Dune holders data: {e}")
    
    return holders


def process_dune_volume(mint_address: str, volume_data: Dict[str, Any]) -> Optional[Metrics]:
    """
    Process token volume data from Dune API
    
    Args:
        mint_address: Token mint address
        volume_data: Volume data from Dune API
        
    Returns:
        Metrics model or None if not enough data
    """
    try:
        # This is a simplified implementation
        # In a real-world scenario, you would need more data to fill all metrics fields
        
        return Metrics(
            mint_address=mint_address,
            timestamp=datetime.utcnow(),
            price_usd=None,  # Would need price data
            fdv_usd=None,    # Would need supply data
            liquidity_usd=None,  # Would need liquidity data
            volume_1h=volume_data.get("volume_1h", None),
            volume_24h=volume_data.get("volume", None),
            percent_change_1h=volume_data.get("percent_change_1h", None),
            percent_change_24h=volume_data.get("percent_change", None)
        )
    except Exception as e:
        logger.error(f"Error processing Dune volume data: {e}")
        return None


def format_first_buyers(first_buyers_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Format first buyers data for API response
    
    Args:
        first_buyers_data: First buyers data from Dune API
        
    Returns:
        Formatted first buyers data
    """
    formatted_data = []
    
    try:
        for buyer in first_buyers_data:
            formatted_buyer = {
                "wallet_address": buyer.get("wallet_address", ""),
                "amount": float(buyer.get("amount", 0)),
                "timestamp": buyer.get("timestamp", ""),
                "transaction_id": buyer.get("transaction_id", "")
            }
            formatted_data.append(formatted_buyer)
    except Exception as e:
        logger.error(f"Error formatting first buyers data: {e}")
    
    return formatted_data


def format_top_traders(top_traders_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Format top traders data for API response
    
    Args:
        top_traders_data: Top traders data from Dune API
        
    Returns:
        Formatted top traders data
    """
    formatted_data = []
    
    try:
        for trader in top_traders_data:
            formatted_trader = {
                "wallet_address": trader.get("wallet_address", ""),
                "buy_volume": float(trader.get("buy_volume", 0)),
                "sell_volume": float(trader.get("sell_volume", 0)),
                "net_volume": float(trader.get("net_volume", 0)),
                "trade_count": int(trader.get("trade_count", 0))
            }
            formatted_data.append(formatted_trader)
    except Exception as e:
        logger.error(f"Error formatting top traders data: {e}")
    
    return formatted_data
