# data/wallet_analyzer.py
import logging
import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Any, Tuple, Union

# Core dependencies
from config.settings import settings
from data.db import Database
from data.helius_api import HeliusAPI, HeliusAPIError
from data.solscan_api import SolscanAPI, SolscanAPIError
# Import parser and its potential error
from data.transaction_parser import TransactionParser, TransactionParsingError
from data.notification_system import NotificationSystem
from data.free_access import FreeAccessManager

# Pydantic model and formatting utils
from data.models import WalletState # Ensure this model is defined
from utils.formatting import shorten_address, format_large_currency

# Assume async redis via db.py or direct import
import redis.asyncio as aioredis # Or import redis for sync version
# Ensure correct import for your chosen Redis client in db.py

# Supabase/PostgREST error imports (match actual exception types if possible)
try:
     from postgrest.exceptions import APIError as PostgrestAPIError
except ImportError:
     PostgrestAPIError = Exception

logger = logging.getLogger(__name__)

# --- Constants ---
# ... (CLASSIFIED_WALLET_CACHE_PREFIX, PROCESSED_REALTIME_TX_PREFIX from Response #111) ...
CLASSIFIED_WALLET_CACHE_TTL = settings.classification_cache_ttl_seconds
PROCESSED_TX_TTL = settings.processed_tx_ttl_seconds
TEAM_HOLDING_PERCENT_THRESHOLD = 5.0 # Configurable?
INSIDER_WINDOW_MINUTES = 10
SNIPER_WINDOW_SECONDS = 15
MAX_HOLDERS_FOR_TEAM_CLASSIFICATION = 5000
KNOWN_PROGRAM_IDS = { # From Response #111
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
    "11111111111111111111111111111111", "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
    "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s", "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY",
    "SMPLecH534NA9acpos4G6x7uf3LWbCAwZQE9e8ZekMu", "Safe11111111111111111111111111111111111111",
}

class WalletAnalyzer:
    """
    Core analysis engine: Classifies wallets (Creator, Team, Insider, Sniper),
    stores initial state, processes real-time transactions to track state (G/Y/R),
    and triggers appropriate notifications. Uses Helius/Solscan and Redis/Supabase.
    """

    def __init__(self,
                 db: Database,
                 helius_api: HeliusAPI,
                 solscan_api: SolscanAPI,
                 transaction_parser: TransactionParser,
                 notification_system: NotificationSystem,
                 free_access_manager: FreeAccessManager):
        self.settings = settings
        self.db = db
        self.helius_api = helius_api
        self.solscan_api = solscan_api
        self.parser = transaction_parser
        self.notification_system = notification_system
        self.free_access_manager = free_access_manager
        # Get sync clients via db for use with asyncio.to_thread
        self.sync_redis_client = db.connect_redis() # Assumes db.py provides sync client
        self.sync_supabase_client = db.get_supabase_sync_client()
        logger.info("WalletAnalyzer Initialized.")

    # --- Redis/DB Helpers ---
    async def _redis_get(self, key: str) -> Optional[str]: # Using sync via thread
        try: return await asyncio.to_thread(self.sync_redis_client.get, key)
        except Exception as e: logger.exception(f"Redis GET Error {key}", exc_info=False); return None # Less noisy log

    async def _redis_setex(self, key: str, ttl: int, value: str): # Using sync via thread
        try: await asyncio.to_thread(self.sync_redis_client.setex, key, ttl, value)
        except Exception as e: logger.exception(f"Redis SETEX Error {key}", exc_info=False)

    async def _redis_exists(self, key: str) -> bool: # Using sync via thread
        try: return await asyncio.to_thread(self.sync_redis_client.exists, key) > 0 # type: ignore
        except Exception as e: logger.exception(f"Redis EXISTS Error {key}", exc_info=False); return False

    async def _execute_supabase_query(self, query_builder): # Using sync via thread
        try:
            target_func = getattr(query_builder, 'execute')
            response = await asyncio.to_thread(target_func)
            error = getattr(response, 'error', None)
            if error:
                 # Extract message detail if possible from PostgREST error format
                 err_detail = error.get("message", str(error)) if isinstance(error, dict) else str(error)
                 logger.error(f"Supabase Query Error: {err_detail}")
                 return {"data": None, "error": error, "message": err_detail}
            return {"data": getattr(response, 'data', None), "count": getattr(response, 'count', None), "error": None}
        except Exception as e:
             logger.exception(f"Supabase thread execution error: {e}", exc_info=True)
             return {"data": None, "error": str(e), "message": f"Internal error during DB query: {str(e)[:100]}"}

    # --- Placeholder Methods ---
    async def get_dex_listing_timestamp(self, mint_address: str) -> Optional[datetime]:
        # --- !!! MUST BE IMPLEMENTED using Birdeye/DexScreener/etc. !!! ---
        logger.warning(f"ACTION REQUIRED: get_dex_listing_timestamp({mint_address}) not implemented.")
        return None # Cannot proceed reliably without this

    async def get_transactions_around_listing(self, mint_address: str, listing_time: datetime) -> Optional[List[Dict[str, Any]]]:
        """Fetches/parses transactions relevant for INSIDER/SNIPER check."""
        logger.warning(f"ACTION REQUIRED: get_transactions_around_listing({mint_address}) not implemented.")
        # --- MUST BE IMPLEMENTED ---
        # 1. Determine fetch window (e.g., listing_time to listing_time + ~15 mins)
        # 2. Fetch signatures (Helius/Solscan /account/transactions for mint or Raydium etc.)
        # 3. Fetch details (Helius/Solscan get_transaction_detail or actions)
        # 4. Parse using self.parser.parse_transaction_for_event or parse_solscan_actions
        # 5. Return required format: [{'timestamp': dt, 'wallet': str (BUYER!), 'is_buy': True}, ...] or None on failure
        return None # Indicate failure/not implemented


    # --- Wallet Classification ---
    async def get_classified_wallets(self, mint_address: str, force_recalculate: bool = False) -> Optional[Dict[str, Any]]:
        # ... (Implementation from Response #111 using cache, _calculate_classifications, etc.) ...
        pass # Uses code from #111

    async def _calculate_classifications(self, mint_address: str) -> Dict[str, Any]:
        """Calculates Creator, Team, Insider, Sniper classifications."""
        classifications: Dict[str, Any] = {"creator": [], "team": [], "insider": [], "sniper": [], "error": None}
        classified_wallets: Set[str] = set()

        # 1. Fetch & Process Metadata (includes Authority checks)
        metadata: Optional[Dict[str, Any]] = None
        decimals: Optional[int] = None
        total_supply_decimal: float = 0.0
        try:
            metadata = await self._get_metadata(mint_address) # Use helper for fallback logic
            if not metadata or metadata.get("decimals") is None or metadata.get("total_supply") is None:
                raise ValueError("Core metadata (decimals/supply) could not be fetched.")
            decimals = int(metadata["decimals"])
            total_supply_raw = int(metadata["total_supply"])
            if decimals < 0: raise ValueError("Invalid negative decimals.")
            if total_supply_raw < 0: raise ValueError("Invalid negative total supply.")
            total_supply_decimal = total_supply_raw / (10 ** decimals) if decimals >= 0 else float(total_supply_raw)

            # Classify Creator
            creator_wallets = set()
            mint_authority = metadata.get("mintAuthority")
            freeze_authority = metadata.get("freezeAuthority")
            for auth in [mint_authority, freeze_authority]:
                 if auth and isinstance(auth, str) and len(auth) > 30 and auth not in KNOWN_PROGRAM_IDS:
                       creator_wallets.add(auth)
            classifications["creator"] = list(creator_wallets)
            classified_wallets.update(creator_wallets)
            logger.info(f"Found {len(creator_wallets)} creators for {mint_address}.")

        except Exception as e:
             logger.error(f"Classification stopped for {mint_address}: Metadata Error: {e}")
             classifications["error"] = f"Metadata Error: {str(e)[:100]}"
             return classifications

        # 2. Identify Team Wallets (Using Fix 1 logic)
        team_wallets_found = []
        if total_supply_decimal > 0:
            try:
                 holders_raw = await self.helius_api.get_token_holders_paginated(
                     mint_address, max_pages=(MAX_HOLDERS_FOR_TEAM_CLASSIFICATION // 1000 + 1)
                 )
                 if holders_raw is None:
                     logger.warning(f"Team check skipped: Helius holder fetch failed {mint_address}")
                 elif holders_raw:
                     owner_raw_balances = {} # Aggregate RAW amounts first
                     for holder in holders_raw:
                          owner = holder.get("owner")
                          try: raw_amount = int(holder.get("amount", 0)) # Ensure amount is int
                          except (TypeError, ValueError): continue # Skip if amount invalid
                          if owner and raw_amount > 0:
                              owner_raw_balances[owner] = owner_raw_balances.get(owner, 0) + raw_amount
                      # Calculate percentages
                      for owner, total_raw in owner_raw_balances.items():
                            balance_decimal = total_raw / (10 ** decimals)
                            percentage = (balance_decimal / total_supply_decimal) * 100
                            if (percentage >= TEAM_HOLDING_PERCENT_THRESHOLD and
                                owner not in classified_wallets and # Exclude Creator
                                owner not in KNOWN_PROGRAM_IDS):     # Exclude Programs
                                team_wallets_found.append(owner)
                                classified_wallets.add(owner) # Mark as classified
                 logger.info(f"Found {len(team_wallets_found)} potential team wallets for {mint_address}.")
            except Exception as e:
                 logger.exception(f"Error during team classification for {mint_address}", exc_info=False) # Log less detail
        else:
             logger.warning(f"Team classification skipped due to zero/invalid supply: {mint_address}")
        classifications["team"] = team_wallets_found

        # 3. Identify Insiders & Snipers (Using Fix 2 structure)
        try:
            listing_time = await self.get_dex_listing_timestamp(mint_address) # Needs implementation
            if not listing_time:
                 logger.warning(f"Skipping Insider/Sniper: Listing time unknown for {mint_address}")
            else:
                 transactions = await self.get_transactions_around_listing(mint_address, listing_time) # Needs implementation
                 if transactions is None: logger.warning(f"Skipping Insider/Sniper: Tx fetch failed for {mint_address}")
                 elif not transactions: logger.info(f"No initial txns found for Insider/Sniper: {mint_address}")
                 else:
                     # Process transactions for timing heuristics
                     # ... (Logic from Response #95 to check time windows and populate sniper/insider lists) ...
                     # Ensure check: `if is_buy and tx_wallet not in classified_wallets:`
                     logger.info(f"Found {len(classifications['sniper'])} snipers, {len(classifications['insider'])} insiders.")
        except Exception as e:
            logger.exception(f"Error during Insider/Sniper classification: {e}", exc_info=False)
            if classifications["error"] is None: classifications["error"] = "Partial failure on Sniper/Insider check"

        # 4. Trigger background tasks to store initial state for *all* newly classified wallets
        store_tasks = []
        all_classified_list = classifications["creator"] + classifications["team"] + classifications["insider"] + classifications["sniper"]
        unique_newly_classified = set(all_classified_list)

        for wallet in unique_newly_classified:
            # Find which category it belongs to (prioritize?)
            # Simple first-match (Creator > Team > Sniper > Insider)
            cls_type = "sniper" if wallet in classifications["sniper"] \
                 else "insider" if wallet in classifications["insider"] \
                 else "team" if wallet in classifications["team"] \
                 else "creator" if wallet in classifications["creator"] \
                 else None
            if cls_type:
                 # Launch task, don't wait
                 store_tasks.append(asyncio.create_task(self.store_initial_wallet_state(mint_address, wallet, cls_type)))
        if store_tasks:
             logger.info(f"Launched {len(store_tasks)} tasks to store initial wallet states for {mint_address}")


        if classifications.get("error") is None: classifications.pop("error", None)
        return classifications

    # --- Store Initial State (Integrate Fix 4) ---
    async def store_initial_wallet_state(self, mint_address: str, wallet_address: str, classification: str):
        """Stores initial balance and GREEN state if not already present."""
        logger.debug(f"Store Init State Check: Wallet={wallet_address}, Mint={mint_address}")
        try:
            if await self._check_state_exists(mint_address, wallet_address):
                logger.debug(f"State already exists for {wallet_address}/{mint_address}")
                return

            # Fetch RAW balance using helper
            initial_raw_balance = await self._fetch_wallet_token_balance_raw(wallet_address, mint_address)
            if initial_raw_balance is None:
                logger.error(f"Failed initial balance fetch for {wallet_address}/{mint_address}. Cannot store state.")
                return
            if initial_raw_balance <= 0:
                 logger.info(f"Skipping state store, initial balance <= 0 for {wallet_address}/{mint_address}")
                 return

            insert_data = {
                "mint_address": mint_address,
                "wallet_address": wallet_address,
                "classification": classification,
                "initial_raw_balance": str(initial_raw_balance),
                "current_status": "GREEN",
                "last_status_update": datetime.now(timezone.utc).isoformat()
            }
            # Use DB Helper for Insert
            query = self.sync_supabase_client.table("classified_wallet_token_states").insert(insert_data, upsert=False) # Explicitly prevent upsert on this first write maybe
            result = await self._execute_supabase_query(query)

            if result and result.get("error"):
                 # Log Supabase errors (e.g., maybe constraint violation if race condition despite check)
                 logger.error(f"DB insert failed for initial state {wallet_address}/{mint_address}: {result.get('message')}")
            elif result:
                 logger.info(f"Stored initial GREEN state [{classification}] for {wallet_address}/{mint_address}")

        except Exception as e:
            logger.exception(f"Unexpected error storing initial state for {wallet_address}/{mint_address}", exc_info=True)

    async def _check_state_exists(self, mint: str, wallet: str) -> bool:
         """ Checks DB if state record exists. """
         query = self.sync_supabase_client.table("classified_wallet_token_states").select("id", count="exact").eq("mint_address", mint).eq("wallet_address", wallet).limit(1)
         res = await self._execute_supabase_query(query)
         return res and not res.get("error") and res.get("count", 0) > 0

    # --- Real-Time Processing (Integrate Fix 3 - Basic Alert only) ---
    # This version implements the SIMPLER real-time handling from Fix #3,
    # which only sends an alert about the action, it does NOT do G/Y/R tracking.
    # Choose this OR the more complex stateful version from Response #111 / #95.
    async def process_realtime_transaction_simple_alert(self, tx_data_from_wss: Dict[str, Any]):
        """
        Processes RT transaction data to trigger alerts based on *immediate action*,
        NOT for G/Y/R state tracking.
        """
        signature = "(sig_missing)"
        try:
            signature = tx_data_from_wss.get("transaction", {}).get("signatures", [None])[0]
            if not signature: return

            processed_key = f"{PROCESSED_REALTIME_TX_PREFIX}{signature}"
            if await self._redis_exists(processed_key): return
            await self._redis_setex(processed_key, PROCESSED_TX_TTL, "processed")

            # 1. Parse to get essential event details (e.g., who did what with which token)
            # Using simplified parser concept from Fix 3 - Needs Implementation!
            # parsed_event = await asyncio.to_thread(self.parser.parse_transaction_for_event, tx_data_from_wss)
            logger.warning(f"ACTION NEEDED: Simple Real-Time: parse_transaction_for_event needs implementation to find involved mint, wallet, action {signature}")
            # --- Placeholder Structure ---
            # Assumes parser can return:
            # involved_mint = "MINT_ADDRESS_XYZ..."
            # wallet_actions = {"WALLET_ADDR_1": "buy", "WALLET_ADDR_2": "sell"}
            involved_mint = self.parser.extract_mint_from_tx(tx_data_from_wss) # Needs impl
            wallet_actions = self.parser.detect_wallet_actions(tx_data_from_wss) # Needs impl
            if not involved_mint or not wallet_actions: return
            # --- End Placeholder ---


            # 2. Get classifications for this specific token
            classifications = await self.get_classified_wallets(involved_mint) # Uses cache
            if not classifications: return # Cannot alert if we don't know classifications

            wallet_type_map = {w: t for t, wl in classifications.items() for w in wl if isinstance(wl, list)}

            # 3. Check if any action involves a classified wallet
            alerts_to_queue = []
            for wallet, action in wallet_actions.items():
                 classification_type = wallet_type_map.get(wallet)
                 if classification_type: # This wallet is classified!
                     logger.info(f"ALERT DETECTED: Classified [{classification_type}] wallet {wallet} action '{action}' on {involved_mint} in tx {signature[:10]}")

                     # 4. Get Subscribers & Queue Notification
                     subscribers = await self.free_access_manager.get_subscribers_for_token(involved_mint)
                     if subscribers:
                          # Format the simple alert based on Fix 3 example
                          alert_content = (
                              f"🚨 **{classification_type.capitalize()} Activity Alert** 🚨\n\n"
                              f"Wallet: `{shorten_address(wallet)}` ({classification_type.capitalize()})\n"
                              f"Action: *{action.upper()}*\n"
                              f"Token: `{involved_mint}`\n"
                              f"Tx: `{signature[:10]}...` ([Scan](https://solscan.io/tx/{signature}))"
                          )
                          for chat_id in subscribers:
                               alerts_to_queue.append({
                                   "chat_id": chat_id, "token_address": involved_mint, "type": f"{classification_type}_activity_alert",
                                   "content": alert_content, "parse_mode": "Markdown" # Use MarkdownV1
                               })
                     else: logger.info("Activity detected but no subscribers.")


            # Queue all generated alerts
            if alerts_to_queue:
                 logger.info(f"Queueing {len(alerts_to_queue)} activity alerts for tx {signature[:10]}")
                 for alert in alerts_to_queue:
                     await self.notification_system.queue_notification(alert)

        except TransactionParsingError as e:
            logger.error(f"Failed to parse TX {signature} for RT Alert: {e}")
            # Don't retry parsing usually, mark processed? Or let Redis key expire? Mark error maybe.
            await self._redis_setex(processed_key, 60, f"parse_error: {e}")
        except Exception as e:
            logger.exception(f"CRITICAL Error processing RT Transaction {signature} (Simple Alert): {e}", exc_info=True)


    # --- REMOVED State Update logic for G/Y/R - Replaced by process_realtime_transaction_simple_alert above ---

    # --- Helpers (Keep as needed for DB state & balance fetching if G/Y/R is re-enabled) ---
    async def _get_metadata(self, mint_address: str) -> Optional[Dict[str, Any]]: pass # Needs implementation
    async def _get_wallet_token_states(self, pairs: List[Tuple[str,str]]) -> Dict: pass # Needs implementation
    async def _fetch_wallet_token_balance_raw(self, wallet: str, mint:str) -> Optional[int]: pass # Needs implementation

    # --- TIS View Methods (Need Implementation using DB + _get_metadata etc) ---
    async def get_category_summary_stats(self, mint_address: str) -> Dict[str, Any]: return {"error": "Not implemented"}
    async def get_wallets_in_category_with_state(self, mint_address: str, category: str) -> Dict[str, Any]: return {"error": "Not implemented"}
    async def get_wallet_token_interaction_details(self, mint_address: str, wallet_address: str) -> Dict[str, Any]: return {"error": "Not implemented"}