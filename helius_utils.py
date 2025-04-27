# utils/processing.py (or integrate into respective services)
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation # Use Decimal for potential supply calc precision

# Import base models for type hinting return values
from data.models import Token, Holder # Metrics might not be directly processed here

logger = logging.getLogger(__name__)

# --- Processing Helius DAS getAsset response ---

def process_helius_das_metadata(mint_address: str, das_asset_data: Dict[str, Any]) -> Optional[Token]:
    """
    Processes the Helius DAS 'getAsset' response to extract token metadata.
    Returns a Pydantic Token model or None on critical failure.

    Args:
        mint_address: The requested mint address (for validation).
        das_asset_data: The raw dictionary response from HeliusAPI.get_token_metadata_das.

    Returns:
        A Pydantic Token object or None.
    """
    if not isinstance(das_asset_data, dict):
        logger.error(f"process_helius_das_metadata: Input data is not a dictionary for {mint_address}")
        return None

    logger.debug(f"Processing Helius DAS metadata for {mint_address}")
    try:
        # --- Adapt Key Access based on ACTUAL Helius DAS Response Structure ---
        interface_type = das_asset_data.get("interface") # e.g., "FungibleToken", "Custom" etc. VERIFY!
        token_info = das_asset_data.get("token_info", {}) # Found under content -> metadata OR spl20/token_info? VERIFY!
        content = das_asset_data.get("content", {})
        metadata_section = content.get("metadata", {})
        files = content.get("files", [])

        # Core fields needed for Athena
        fetched_mint = das_asset_data.get("id") # Verify key for mint address in response
        if fetched_mint != mint_address:
            logger.warning(f"Mismatched mint address in DAS response! Expected {mint_address}, got {fetched_mint}")
            # Continue carefully or return None?

        decimals = token_info.get("decimals")
        supply_str = token_info.get("supply")

        if decimals is None or supply_str is None:
            logger.warning(f"Essential metadata (decimals/supply) missing in DAS response for {mint_address}")
            # Decide if partial metadata is acceptable
            # return None # Fail strict

        # Create the Pydantic model instance
        token = Token(
            mint_address=mint_address, # Use original request mint
            name=metadata_section.get("name"),
            symbol=metadata_section.get("symbol"),
            decimals=int(decimals) if decimals is not None else None, # Convert safely
            total_supply=str(supply_str) if supply_str is not None else None, # Store as string
            logo_uri=files[0].get("uri") if files and isinstance(files[0], dict) else metadata_section.get("image") or None, # Find logo URI
            # created_at should be from DB table ideally
        )
        return token

    except (TypeError, ValueError, KeyError, Exception) as e:
        logger.exception(f"Error processing Helius DAS metadata for {mint_address}: {e}", exc_info=True)
        return None


# --- Processing Helius RPC getTokenAccounts response ---

def process_helius_token_holders(
    mint_address: str,
    helius_holder_accounts: List[Dict[str, Any]],
    decimals: Optional[int], # Required for decimal balance calculation
    total_supply_decimal: Optional[float] # Required for percentage
) -> List[Holder]:
    """
    Processes the raw token account list from Helius RPC 'getTokenAccounts'.
    Calculates decimal balances and percentages. Aggregates balance per owner.

    Args:
        mint_address: The token mint address these holders belong to.
        helius_holder_accounts: List of account dicts from Helius API response.
        decimals: The token's decimals (REQUIRED for calculations).
        total_supply_decimal: The token's total supply adjusted for decimals (REQUIRED for %).

    Returns:
        List of Pydantic Holder models, sorted by balance descending.
    """
    if decimals is None or total_supply_decimal is None or total_supply_decimal <= 0:
        logger.error(f"Cannot process holders for {mint_address}: Missing decimals ({decimals}) or invalid total supply ({total_supply_decimal}).")
        return []

    logger.debug(f"Processing {len(helius_holder_accounts)} raw holder accounts for {mint_address}")
    owner_balances_raw: Dict[str, int] = {}
    processed_holders: List[Holder] = []

    # 1. Aggregate RAW balances per unique owner
    for account in helius_holder_accounts:
        try:
            owner = account.get("owner")
            # Check different places amount might be (depends on standard RPC vs enhanced Helius format)
            amount_str = account.get("amount") \
                      or account.get("account", {}).get("data", {}).get("parsed", {}).get("info", {}).get("tokenAmount", {}).get("amount")

            if owner and amount_str is not None:
                 raw_amount = int(amount_str)
                 if raw_amount > 0: # Only include holders with non-zero balance
                      owner_balances_raw[owner] = owner_balances_raw.get(owner, 0) + raw_amount
            else:
                # Log if owner or amount missing from a raw account entry
                logger.debug(f"Skipping holder record with missing owner/amount: {account}")

        except (ValueError, TypeError, KeyError) as e:
             logger.warning(f"Skipping holder record due to parsing error: {e}. Record: {account}")
             continue

    if not owner_balances_raw:
        logger.info(f"No valid holders with balance found after aggregation for {mint_address}")
        return []

    # 2. Calculate decimal balance and percentage for each unique owner
    for owner, raw_balance in owner_balances_raw.items():
        try:
            balance_decimal = float(Decimal(str(raw_balance)) / (Decimal(10) ** decimals))
            percentage = (balance_decimal / total_supply_decimal) * 100 if total_supply_decimal > 0 else 0.0

            holder = Holder(
                mint_address=mint_address,
                wallet_address=owner,
                balance=balance_decimal, # Store calculated decimal amount
                percentage=percentage,
                # Rank is determined after sorting
                # timestamp=datetime.now(timezone.utc) # Timestamp represents when processed
            )
            processed_holders.append(holder)
        except (InvalidOperation, TypeError, ValueError, Exception) as e:
            logger.error(f"Error calculating balance/percentage for owner {owner}, raw: {raw_balance}, dec: {decimals}, supply: {total_supply_decimal}. Error: {e}")

    # 3. Sort by balance descending
    processed_holders.sort(key=lambda h: h.balance, reverse=True)

    # 4. Assign Rank (Optional, can be done just before display)
    # for i, holder in enumerate(processed_holders):
    #     holder.rank = i + 1

    logger.info(f"Processed {len(processed_holders)} unique holders for {mint_address}")
    return processed_holders


# --- Removed extract_metrics_from_events ---
# Reason: Metrics (Price, Volume, Liquidity, FDV) should come from dedicated sources
# like Solscan /token/price, /token/markets, or Birdeye/Raydium APIs, or Helius getAsset
# not derived solely from event logs which is inaccurate and complex.


# --- Removed process_webhook_event ---
# Reason: Switched architecture to WebSockets. If webhooks were used, this function
# would need robust parsing based on Helius's specific webhook payload structure.