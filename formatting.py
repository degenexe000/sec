import logging
from typing import Optional, Union # Union needed for type hints like float | int | str

logger = logging.getLogger(__name__)

def shorten_address(address: Optional[str], chars: int = 5) -> str:
    """
    Shortens a Solana address (or any string) clearly.
    e.g., shorten_address("ABCDEFGHIJKLMN", 5) -> "ABCDE...KLMN"
    Returns "N/A" on None input, original if too short or invalid length.
    """
    if address is None:
        return "N/A"
    if not isinstance(address, str):
         try: address = str(address) # Try converting just in case
         except Exception: return "Invalid Address Type"
    if chars <= 0:
         logger.warning(f"shorten_address called with invalid chars length: {chars}")
         return address # Return original if length invalid

    if len(address) <= chars * 2:
        return address # Return original if it's already short enough

    return f"{address[:chars]}...{address[-chars:]}"


def format_large_currency(value: Optional[Union[float, int, str]], default: str = "N/A") -> str:
    """
    Formats large numbers into $K, $M, $B currency formats.
    Handles None and non-numeric input gracefully.
    """
    if value is None:
        return default
    try:
        # Use float for comparison, allows for decimals in input strings
        value_f = float(value)
    except (TypeError, ValueError):
        logger.debug(f"Could not convert value '{value}' to float for large currency format.")
        return default # Cannot convert

    if value_f == 0:
        return "$0.00" # Explicit zero format

    abs_value = abs(value_f) # Use absolute value for threshold checks

    if abs_value >= 1_000_000_000:
        # Show 2 decimal places for Billions
        return f"${value_f / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        # Show 2 decimal places for Millions
        return f"${value_f / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        # Show 1 decimal place for Thousands
        return f"${value_f / 1_000:.1f}K"
    # Default to 2 decimal places for values under 1000
    return f"${value_f:,.2f}"


def format_price(value: Optional[Union[float, int, str]], default: str = "N/A") -> str:
    """
    Formats price, showing variable precision for very small values. Handles None.
    """
    if value is None:
        return default
    try:
        value_f = float(value)
    except (ValueError, TypeError):
        logger.debug(f"Could not convert value '{value}' to float for price format.")
        return default # Cannot convert

    if value_f == 0:
        return "$0.00" # Show zero price clearly
    elif 0 < abs(value_f) < 0.00000001: # 10 decimal places for very low value tokens
        return f"${value_f:.10f}"
    elif 0 < abs(value_f) < 0.00001: # 8 decimal places
        return f"${value_f:.8f}"
    elif 0 < abs(value_f) < 0.01: # 6 decimal places
        return f"${value_f:.6f}"
    else:
        # Default to 4 decimal places for prices >= $0.01
        return f"${value_f:.4f}"


def format_percentage(value: Optional[Union[float, int, str]], default: str = "N/A") -> str:
    """
    Formats number into percentage string with sign (+/-) and 2 decimal places.
    Handles None and non-numeric input.
    """
    if value is None:
        return default
    try:
        value_f = float(value)
    except (ValueError, TypeError):
        logger.debug(f"Could not convert value '{value}' to float for percentage format.")
        return default # Cannot convert

    # Use :+.2f format specifier to automatically handle sign and decimals
    return f"{value_f:+.2f}%"