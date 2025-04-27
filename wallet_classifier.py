import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Set

# Assuming Settings are globally imported if needed, otherwise pass via __init__
# from config.settings import settings
# DB likely only needed if caching results here instead of Redis, remove if unused
# from data.db import Database
from data.helius_api import HeliusAPI, HeliusAPIError # Import API and specific error

logger = logging.getLogger(__name__)

# --- Configuration Thresholds (Consider getting from settings) ---
TEAM_HOLDING_PERCENT_THRESHOLD = 5.0      # % of total supply
INSIDER_WINDOW_MINUTES = 10
SNIPER_WINDOW_SECONDS = 10

# Maximum number of holders to process for team classification (performance limit)
MAX_HOLDERS_TO_PROCESS_FOR_TEAM = 5000 # Fetch max 5 pages of 1000

class TransactionDetector:
    """
    Analyzes historical data to classify wallets associated with a token launch
    as potentially Team, Insider, or Sniper based on heuristics.
    NOTE: Needs implementation for timestamp/transaction fetching.
    Does NOT store state (G/Y/R) - that belongs elsewhere (e.g., WalletAnalyzer).
    """

    def __init__(self, helius_api: HeliusAPI): # Simplified dependencies for just classification
        """
        Initialize the detector.

        Args:
            helius_api: An initialized HeliusAPI client instance.
        """
        self.helius_api = helius_api
        # Removed settings, db if not directly used by this class's core function
        logger.info("TransactionDetector (Wallet Classifier) Initialized.")


    # --- Placeholder Methods (REQUIRE IMPLEMENTATION) ---

    async def get_dex_listing_timestamp(self, mint_address: str) -> Optional[datetime]:
        """
        Placeholder: Needs implementation to find the first DEX listing time.
        This is CRITICAL for accurate Insider/Sniper detection.
        Sources: DexScreener API, Birdeye API, parse earliest Helius transaction history.
        Must return a timezone-aware UTC datetime object.
        """
        logger.warning(f"ACTION NEEDED: get_dex_listing_timestamp for {mint_address} must be implemented.")
        # Example: Return time 1 hour ago for testing, replace this!
        # return datetime.now(timezone.utc) - timedelta(hours=1)
        return None # Return None if lookup fails

    async def get_transactions_around_listing(self,
                                             mint_address: str,
                                             listing_time: datetime,
                                             window_minutes_after: int = INSIDER_WINDOW_MINUTES + 5 # Fetch slightly longer window
                                             ) -> List[Dict[str, Any]]:
        """
        Placeholder: Fetch and parse relevant transaction details around the listing time.
        Should return a list of dictionaries, each containing *at least*:
         - 'signature': str
         - 'timestamp': datetime (timezone-aware UTC)
         - 'is_buy': bool (True if wallet acquired the target token)
         - 'wallet': str (Address of the account acquiring/disposing the token in this event)
        Needs implementation using Helius/Solscan APIs and TransactionParser.
        """
        logger.warning(f"ACTION NEEDED: get_transactions_around_listing for {mint_address} must be implemented.")
        # Example using Helius (Conceptual):
        # 1. Get signatures around listing_time +/- window using getSignaturesForAddress (limit, before/until)
        # 2. For each signature, get details using getTransaction.
        # 3. Use TransactionParser to extract swap/transfer info into the required format.
        return [] # Return empty list until implemented

    # --- Classification Logic ---

    async def classify_wallets(self, mint_address: str) -> Dict[str, Any]:
        """
        Classifies wallets for a given token.

        Returns:
            Dict: {"team": [], "insider": [], "sniper": [], "error": Optional[str]}
        """
        # Initialize result structure with error key for explicit failure indication
        classifications: Dict[str, Any] = {"team": [], "insider": [], "sniper": [], "error": None}
        # Set to track wallets already classified to prevent assigning multiple roles
        classified_wallets: Set[str] = set()
        logger.info(f"Starting wallet classification for token: {mint_address}")

        # --- 1. Fetch Metadata (Essential for calculations) ---
        metadata: Optional[Dict[str, Any]] = None
        decimals: Optional[int] = None
        total_supply_decimal: float = 0.0
        try:
            metadata = await self.helius_api.get_token_metadata_das(mint_address) # Primary source
            if not metadata or metadata.get("decimals") is None or metadata.get("total_supply") is None:
                msg = "Missing or incomplete core metadata (decimals/supply)"
                logger.error(f"Classification failed for {mint_address}: {msg}. Metadata: {metadata}")
                classifications["error"] = msg
                return classifications # Cannot proceed without core metadata

            decimals = int(metadata["decimals"])
            total_supply_raw = int(metadata["total_supply"])
            if decimals >= 0: # Basic sanity check for decimals
                 total_supply_decimal = total_supply_raw / (10 ** decimals)
            if total_supply_decimal <= 0:
                 logger.warning(f"Total supply reported as zero or invalid for {mint_address}. Team classification may be skipped.")
            logger.debug(f"Metadata for {mint_address}: Decimals={decimals}, Supply={total_supply_decimal:.2f}")

        except HeliusAPIError as e:
             logger.error(f"Helius API error fetching metadata for {mint_address}: {e}")
             classifications["error"] = f"Metadata fetch failed (Helius): {e}"
             return classifications
        except (ValueError, TypeError, Exception) as e:
             logger.exception(f"Error processing metadata for {mint_address}: {e}", exc_info=True)
             classifications["error"] = f"Metadata processing error: {e}"
             return classifications

        # --- 2. Identify Potential Team Wallets (Based on Holdings) ---
        if total_supply_decimal > 0: # Only check if supply is valid
            try:
                 # Fetch holders - implement pagination logic within get_token_holders_paginated
                 # Limit pages fetched for performance in classification phase
                 holders_raw = await self.helius_api.get_token_holders_paginated(
                     mint_address, max_pages=MAX_HOLDERS_TO_PROCESS_FOR_TEAM // 1000 + 1
                 )
                 if holders_raw is None: # Check for None indicating API error
                     logger.warning(f"Failed to fetch holder data for {mint_address}, skipping team classification.")
                 elif not holders_raw: # Empty list is valid, just means no holders found
                      logger.info(f"No holders found via Helius for {mint_address}, skipping team classification.")
                 else:
                      # Aggregate balances per owner
                      owner_balances_decimal: Dict[str, float] = {}
                      for acc in holders_raw:
                           owner = acc.get("owner")
                           raw_amount = int(acc.get("amount", 0))
                           if owner and raw_amount > 0 and decimals is not None:
                                balance_decimal = raw_amount / (10 ** decimals)
                                owner_balances_decimal[owner] = owner_balances_decimal.get(owner, 0.0) + balance_decimal

                      # Apply threshold
                      team_wallets_found = []
                      for owner, balance in owner_balances_decimal.items():
                            percentage = (balance / total_supply_decimal) * 100
                            if percentage >= TEAM_HOLDING_PERCENT_THRESHOLD:
                                 team_wallets_found.append(owner)
                                 classified_wallets.add(owner)
                      classifications["team"] = team_wallets_found
                      logger.info(f"Identified {len(team_wallets_found)} potential team wallets (holding >={TEAM_HOLDING_PERCENT_THRESHOLD}%) for {mint_address}.")

            except HeliusAPIError as e:
                  logger.error(f"Helius API error during team wallet identification for {mint_address}: {e}")
                  # Continue to other classifications if possible
            except Exception as e:
                  logger.exception(f"Unexpected error identifying team wallets for {mint_address}: {e}", exc_info=True)


        # --- 3. Identify Potential Insiders & Snipers (Based on Transaction Timing) ---
        try:
             listing_time: Optional[datetime] = await self.get_dex_listing_timestamp(mint_address)

             if not listing_time:
                 logger.warning(f"Cannot identify insiders/snipers for {mint_address}: DEX listing time unavailable.")
             else:
                 # Ensure listing time is UTC aware
                 if listing_time.tzinfo is None:
                     listing_time = listing_time.replace(tzinfo=timezone.utc)

                 # Fetch relevant transactions (IMPLEMENT THIS METHOD)
                 transactions = await self.get_transactions_around_listing(mint_address, listing_time)

                 if not transactions:
                      logger.info(f"No relevant transactions found around listing for {mint_address}. Cannot identify insiders/snipers.")
                 else:
                      sniper_window_end = listing_time + timedelta(seconds=SNIPER_WINDOW_SECONDS)
                      insider_window_end = listing_time + timedelta(minutes=INSIDER_WINDOW_MINUTES)

                      sniper_candidates: Set[str] = set()
                      insider_candidates: Set[str] = set()

                      for tx in transactions:
                           # Requires 'timestamp', 'wallet', and 'is_buy' from get_transactions_around_listing
                           tx_timestamp = tx.get("timestamp")
                           tx_wallet = tx.get("wallet")
                           is_buy = tx.get("is_buy")

                           # Basic validation of required fields from parser
                           if not isinstance(tx_timestamp, datetime) or not tx_wallet or not isinstance(is_buy, bool):
                                logger.debug(f"Skipping transaction due to missing/invalid fields: {tx.get('signature')}")
                                continue

                           # Ensure comparison is timezone aware
                           if tx_timestamp.tzinfo is None: tx_timestamp = tx_timestamp.replace(tzinfo=timezone.utc)

                           # Check timing windows only for BUYS and wallets not already classified
                           if is_buy and tx_wallet not in classified_wallets:
                               # Sniper Check
                               if listing_time < tx_timestamp <= sniper_window_end:
                                     sniper_candidates.add(tx_wallet)
                               # Insider Check (mutually exclusive with sniper for this analysis)
                               elif sniper_window_end < tx_timestamp <= insider_window_end:
                                     insider_candidates.add(tx_wallet)

                      # Assign lists, ensuring sniper/insider candidates are not already 'team'
                      classifications["sniper"] = list(sniper_candidates - classified_wallets)
                      classified_wallets.update(classifications["sniper"]) # Add snipers to classified set

                      classifications["insider"] = list(insider_candidates - classified_wallets) # Only add if not already team/sniper
                      # Note: classified_wallets already includes team and sniper now

                      logger.info(f"Identified {len(classifications['sniper'])} potential snipers and {len(classifications['insider'])} potential insiders for {mint_address}.")

        except Exception as e:
             logger.exception(f"Error during insider/sniper identification for {mint_address}: {e}", exc_info=True)
             # Log error but return classifications found so far
             if classifications["error"] is None: # Only add error if none exists yet
                 classifications["error"] = "Partial failure during insider/sniper classification."


        logger.info(f"Classification finished for {mint_address}. Results: "
                    f"Team={len(classifications['team'])}, "
                    f"Insider={len(classifications['insider'])}, "
                    f"Sniper={len(classifications['sniper'])}. "
                    f"Error='{classifications.get('error')}'")
        return classifications

# --- End of Class ---