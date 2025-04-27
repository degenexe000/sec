import logging
import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Any

# Assume necessary API/DB clients and services are imported
from data.helius_api import HeliusAPI, HeliusAPIError
from data.solscan_api import SolscanAPI, SolscanAPIError # Or whichever transaction source is used
from data.transaction_parser import TransactionParser
from config.settings import settings
from data.db import Database
from data.notification_system import NotificationSystem
from data.free_access import FreeAccessManager
import redis # For typing redis connection from pool

logger = logging.getLogger(__name__)

# --- Constants ---
CLASSIFIED_WALLET_CACHE_PREFIX = "classified_wallets:"
PROCESSED_SELL_ALERT_TX_PREFIX = "processed_sell_alert_tx:" # Specific prefix for this service
LAST_PROCESSED_MONITOR_SIG_PREFIX = "last_processed_monitor_sig:"

CLASSIFIED_WALLET_CACHE_TTL_SECONDS = settings.classification_cache_ttl_seconds
PROCESSED_TX_TTL_SECONDS = settings.processed_tx_ttl_seconds


class MonitoringService:
    """
    Performs background monitoring: fetches recent transactions for tracked tokens,
    checks for sells by classified wallets, and triggers notifications.
    Relies on classifications potentially being updated elsewhere.
    """

    def __init__(self,
                 db: Database,
                 helius_api: HeliusAPI, # Needed for tx fetching
                 solscan_api: SolscanAPI, # Needed? Or just use Helius?
                 transaction_parser: TransactionParser, # To parse fetched txs
                 notification_system: NotificationSystem, # To send alerts
                 free_access_manager: FreeAccessManager): # To get subs
        self.settings = settings # Access global settings
        self.db = db
        self.helius_api = helius_api
        self.solscan_api = solscan_api # Optional dependency?
        self.parser = transaction_parser
        self.notification_system = notification_system
        self.free_access_manager = free_access_manager
        # Get the sync redis client for use with asyncio.to_thread
        self.sync_redis_client: redis.Redis = self.db.connect_redis()
        # Get sync supabase client
        self.sync_supabase_client = self.db.get_supabase_sync_client()
        logger.info("MonitoringService Initialized.")

    # --- Redis Helpers (Using sync client with asyncio.to_thread) ---
    async def _redis_get(self, key: str) -> Optional[str]:
         try: return await asyncio.to_thread(self.sync_redis_client.get, key)
         except Exception as e: logger.exception(f"Redis GET Error - Key: {key}", exc_info=True); return None

    async def _redis_setex(self, key: str, ttl: int, value: str):
         try: await asyncio.to_thread(self.sync_redis_client.setex, key, ttl, value)
         except Exception as e: logger.exception(f"Redis SETEX Error - Key: {key}", exc_info=True)

    async def _redis_set(self, key: str, value: str):
         try: await asyncio.to_thread(self.sync_redis_client.set, key, value)
         except Exception as e: logger.exception(f"Redis SET Error - Key: {key}", exc_info=True)

    async def _redis_exists(self, key: str) -> bool:
        try: return await asyncio.to_thread(self.sync_redis_client.exists, key) > 0 # type: ignore # exists returns int
        except Exception as e: logger.exception(f"Redis EXISTS Error - Key: {key}", exc_info=True); return False

    # --- Supabase Helper ---
    async def _execute_supabase_query(self, query_builder):
        """Executes sync Supabase query via thread with detailed logging."""
        try:
            target_func = getattr(query_builder, 'execute')
            response = await asyncio.to_thread(target_func)
            if hasattr(response, 'error') and response.error:
                 logger.error(f"Supabase Query Error: {response.error}")
                 # Wrap error info consistently if possible based on supabase-py structure
                 return {"data": None, "error": response.error, "message": response.error.get("message", "Supabase query failed")}
            # Assume success structure
            return {"data": getattr(response, 'data', None), "count": getattr(response, 'count', None), "error": None}
        except Exception as e:
            logger.exception(f"Unexpected Supabase execution error: {e}", exc_info=True)
            return {"data": None, "error": str(e), "message": f"Internal error during DB query: {str(e)[:100]}"}

    # --- Core Logic ---

    async def get_classified_wallets_cached(self, mint_address: str) -> Optional[Dict[str, List[str]]]:
        """Gets classified wallets from Redis cache. Does NOT calculate if miss."""
        cache_key = f"{CLASSIFIED_WALLET_CACHE_PREFIX}{mint_address}"
        try:
            cached_data = await self._redis_get(cache_key)
            if cached_data:
                logger.debug(f"Cache hit for classified wallets: {mint_address}")
                return json.loads(cached_data)
            else:
                 logger.info(f"Cache miss for classified wallets: {mint_address}. Relies on external update.")
                 return None # Indicate cache miss, this service doesn't recalculate
        except json.JSONDecodeError:
             logger.error(f"Failed to decode cached classification JSON for {mint_address}. Cache invalid.")
             return None
        except Exception as e: # Catch Redis errors
             logger.error(f"Failed to get classifications from cache for {mint_address}: {e}")
             return None


    async def fetch_and_parse_recent_transactions(self,
                                                 mint_address: str,
                                                 last_processed_sig: Optional[str]
                                                 ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Fetches recent transaction signatures via Helius, gets details, parses them,
        and returns a list of potentially relevant parsed events and the newest signature seen.
        """
        logger.debug(f"Fetching transactions for {mint_address} until sig: {last_processed_sig}")
        parsed_events = []
        newest_sig_in_batch = None

        try:
            # 1. Fetch Signatures using HeliusAPI
            signatures_info = await self.helius_api.get_signatures_for_address(
                account_address=mint_address,
                limit=50, # Fetch a reasonable batch size
                until=last_processed_sig # Fetch transactions NEWER than the last one processed
            )

            if signatures_info is None: # API Error occurred
                 logger.error(f"Failed to fetch signatures for {mint_address}. Aborting fetch.")
                 return [], last_processed_sig # Return empty, keep old marker

            if not signatures_info:
                 logger.debug(f"No new transaction signatures found for {mint_address} since {last_processed_sig}")
                 return [], last_processed_sig # No new txns, keep old marker


            logger.info(f"Fetched {len(signatures_info)} new signatures for {mint_address}. Getting details...")
            # Store the signature of the LATEST transaction in this batch
            newest_sig_in_batch = signatures_info[0].get('signature') # Signatures are newest first

            # 2. Fetch Details Concurrently (Optional optimization)
            # Be mindful of API rate limits if fetching many details!
            fetch_tasks = [self.helius_api.get_transaction_details(sig_info['signature']) for sig_info in signatures_info if sig_info.get('signature')]
            transaction_details_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            # 3. Parse Transactions
            for i, detail_result in enumerate(transaction_details_results):
                 sig_info = signatures_info[i] # Corresponding signature info
                 if isinstance(detail_result, Exception) or detail_result is None:
                      logger.warning(f"Failed to get/parse tx detail for sig {sig_info.get('signature')}: {detail_result}")
                      continue

                 # Check for transaction error within Helius/RPC response before parsing
                 if detail_result.get("meta", {}).get("err"):
                      logger.debug(f"Skipping failed transaction {sig_info.get('signature')}")
                      continue

                 # Use the Transaction Parser
                 # parsed = self.parser.parse_transaction_for_event(detail_result) # Use your main parser
                 parsed = await asyncio.to_thread(self.parser.parse_transaction_for_event, detail_result) # If parser becomes complex/sync

                 if parsed and not parsed.get("error"): # If parser returns usable data
                       parsed_events.append(parsed)
                       # logger.debug(f"Successfully parsed event type {parsed.get('type')} from tx {parsed.get('signature')}")

        except Exception as e:
             logger.exception(f"Error during fetch/parse transaction cycle for {mint_address}: {e}", exc_info=True)
             # Don't update the last_processed_sig on error, might miss txs
             return [], last_processed_sig

        logger.debug(f"Returning {len(parsed_events)} parsed events. Newest sig: {newest_sig_in_batch}")
        # Return parsed events AND the signature of the latest transaction successfully processed in this batch
        return parsed_events, newest_sig_in_batch or last_processed_sig


    async def run_monitoring_cycle(self):
        """Performs one cycle of monitoring tracked tokens for classified wallet sells."""
        logger.info("Starting classified wallet sell monitoring cycle...")
        start_time = asyncio.get_event_loop().time()
        processed_count = 0
        alert_count = 0

        try:
            # More efficient: get unique mints directly
            tracked_mints = await self.free_access_manager.get_all_unique_tracked_mints()
            if not tracked_mints:
                 logger.info("Monitoring cycle: No tokens currently being tracked by any user.")
                 return

            logger.info(f"Monitoring cycle: Checking {len(tracked_mints)} unique tracked tokens...")

            # Consider asyncio.gather to check multiple tokens concurrently, but be mindful of API rate limits
            tasks = [self.monitor_token_sells(mint) for mint in tracked_mints]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results to count alerts/errors
            for i, result in enumerate(results):
                 mint = tracked_mints[i]
                 if isinstance(result, Exception):
                      logger.error(f"Monitoring failed for token {mint} during gather: {result}")
                 elif isinstance(result, int): # monitor_token_sells returns count of alerts sent
                      alert_count += result
                      processed_count += 1

        except Exception as e:
            logger.exception(f"CRITICAL error in monitoring cycle gather loop: {e}", exc_info=True)
        finally:
            duration = asyncio.get_event_loop().time() - start_time
            logger.info(f"Monitoring cycle finished. Duration: {duration:.2f}s. Tokens Processed: {processed_count}/{len(tracked_mints) if 'tracked_mints' in locals() else 'N/A'}. Alerts Queued: {alert_count}")

    async def monitor_token_sells(self, mint_address: str) -> int:
        """Monitors sells for a single token and returns number of alerts queued."""
        alerts_queued = 0
        logger.debug(f"Starting sell monitor for token: {mint_address}")
        try:
            # 1. Get current classifications (expecting potential None if uncached/error)
            classified = await self.get_classified_wallets_cached(mint_address)
            if not classified: return 0 # Can't monitor without classifications

            all_classified_wallets: Set[str] = set().union(*(v for k,v in classified.items() if isinstance(v, list)))
            if not all_classified_wallets: return 0 # No wallets actually classified

            # Create reverse map for easy type lookup
            wallet_type_map = {w: t for t, wl in classified.items() for w in wl}

            # 2. Get last processed marker from Redis
            last_sig_key = f"{LAST_PROCESSED_MONITOR_SIG_PREFIX}{mint_address}"
            last_processed_sig = await self._redis_get(last_sig_key)

            # 3. Fetch and Parse NEW Transactions SINCE the marker
            parsed_events, newest_sig = await self.fetch_and_parse_recent_transactions(mint_address, last_processed_sig)

            if not parsed_events:
                # If no new events BUT we got a new latest signature, update marker anyway
                if newest_sig and newest_sig != last_processed_sig:
                     logger.debug(f"Updating marker for {mint_address} to {newest_sig} (no relevant events found).")
                     await self._redis_set(last_sig_key, newest_sig)
                return 0 # No relevant parsed events

            # 4. Check each relevant event for sells by classified wallets
            for event in parsed_events:
                try:
                    event_sig = event.get("signature")
                    if not event_sig: continue

                    # 5. De-duplicate based on this alert service's specific check
                    processed_key = f"{PROCESSED_SELL_ALERT_TX_PREFIX}{event_sig}"
                    if await self._redis_exists(processed_key):
                        logger.debug(f"Sell alert for tx {event_sig} already processed. Skipping.")
                        continue

                    # Determine relevant wallets & mints based on parser output
                    # Example assumes parser gives clear seller & token sold
                    seller = event.get("seller_wallet") # Check exact key from your parser
                    token_sold_mint = event.get("token_sold_mint") # Check exact key

                    # Only proceed if it involves the token we're monitoring and has a seller
                    if seller and token_sold_mint == mint_address:
                         # 6. Check if seller is classified for THIS token
                         if seller in all_classified_wallets:
                             classification_type = wallet_type_map.get(seller, "Classified")

                             logger.info(f"ALERT DETECTED: Classified [{classification_type}] wallet {seller} sold {mint_address} in tx {event_sig}")

                             # 7. Find users tracking this token
                             # Should be optimized using FreeAccessManager cache maybe
                             subscribers = await self.free_access_manager.get_subscribers_for_token(mint_address)

                             if subscribers:
                                 # 8. Format & Queue Alert (Use dedicated formatter if logic complex)
                                 # Assume event dict has necessary details like amount
                                 amount = event.get('amount_sold', 0.0) # Get parsed sell amount
                                 # formatting utils removed, implement formatting here or use utils
                                 amt_str = f"{float(amount):,.2f}" if isinstance(amount, (float, int)) else str(amount)
                                 alert_content = (
                                      f"🚨 **Sell Alert: {classification_type.capitalize()}** 🚨\n\n"
                                      f"Token: `{mint_address}`\n" # Consider adding name/$symbol
                                      f"Wallet: `{shorten_address(seller)}` ({classification_type.capitalize()})\n"
                                      f"Sold: ~{amt_str} tokens\n" # Add $value if available
                                      f"Tx: `{event_sig[:10]}...` ([Scan](https://solscan.io/tx/{event_sig}))" # Example with link
                                 )
                                 for chat_id in subscribers:
                                      notification = {
                                          "chat_id": chat_id, "token_address": mint_address,
                                          "type": f"{classification_type}_sell_alert",
                                          "content": alert_content, "data": event # Include event data
                                      }
                                      await self.notification_system.queue_notification(notification)
                                 alerts_queued += len(subscribers)
                                 logger.info(f"Queued {len(subscribers)} sell alerts for tx {event_sig}")
                             else:
                                 logger.warning(f"Sell by {seller} ({classification_type}) detected for {mint_address} but no subscribers found.")

                             # 9. Mark as processed ONLY AFTER queuing alerts successfully
                             await self._redis_setex(processed_key, PROCESSED_TX_TTL_SECONDS, "processed")

                except Exception as inner_e:
                    logger.exception(f"Error processing single parsed event {event.get('signature')} for {mint_address}: {inner_e}", exc_info=True)


            # 10. Update latest processed signature marker in Redis if processing was successful
            if newest_sig and newest_sig != last_processed_sig:
                await self._redis_set(last_sig_key, newest_sig)
                logger.info(f"Updated last processed signature for {mint_address} monitoring to {newest_sig}")

        except Exception as e:
            logger.exception(f"CRITICAL error during monitoring sells for token {mint_address}: {e}", exc_info=True)
            # Return 0 alerts queued on major error for this token
            return 0

        return alerts_queued # Return count of alerts queued in this run for this token