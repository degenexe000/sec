# bot/service.py
import logging
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

# Import project components
from config.settings import settings # Use configured singleton
from data.db import Database
from data.helius_api import HeliusAPI
from data.solscan_api import SolscanAPI # Added
from data.wallet_analyzer import WalletAnalyzer # Replaced transaction_detector
from data.notification_system import NotificationSystem
from data.free_access import FreeAccessManager
# Import formatting utilities
from utils.formatting import format_large_currency, format_price, format_percentage, shorten_address

logger = logging.getLogger(__name__)

class TelegramBotService:
    """
    Service layer for Athena Bot. Orchestrates interactions between bot commands,
    data sources (Helius, Solscan, DB), analysis (WalletAnalyzer), and notifications.
    """

    def __init__(self,
                 # Correct dependencies for the advanced architecture
                 db: Database,
                 helius_api: HeliusAPI,
                 solscan_api: SolscanAPI,
                 wallet_analyzer: WalletAnalyzer,
                 notification_system: NotificationSystem,
                 free_access_manager: FreeAccessManager):
        # Store injected instances
        self.settings = settings # Use global settings instance
        self.db = db
        self.helius_api = helius_api
        self.solscan_api = solscan_api
        self.wallet_analyzer = wallet_analyzer
        self.notification_system = notification_system
        self.free_access_manager = free_access_manager
        logger.info("TelegramBotService initialized with all dependencies.")

    async def validate_token(self, mint_address: str) -> bool:
        """
        Validate a token mint address using primarily Solscan metadata endpoint.
        Falls back to Helius if needed and configured.
        """
        logger.info(f"VALIDATE: Attempting validation for: {mint_address}")
        try:
            # Primary Check: Solscan
            logger.debug(f"VALIDATE: Calling Solscan /token/meta for {mint_address}")
            meta_solscan = await self.solscan_api.get_token_meta(mint_address)
            # Check if the response is valid and maybe contains a key piece like 'symbol'
            # Solscan might return {} or specific error structures even on 200 for unknown.
            # Check for a specific field indicating validity. Adjust check as needed.
            if isinstance(meta_solscan, dict) and meta_solscan.get("symbol"):
                logger.info(f"VALIDATE: SUCCESS - Found metadata via Solscan for {mint_address}.")
                return True
            else:
                 logger.warning(f"VALIDATE: No valid metadata from Solscan for {mint_address}. Response: {str(meta_solscan)[:200]}...")
                 # Optional: Fallback to Helius DAS
                 # logger.debug(f"VALIDATE: Falling back to Helius DAS for {mint_address}")
                 # metadata_helius = await self.helius_api.get_token_metadata_das(mint_address)
                 # if metadata_helius and metadata_helius.get("name"):
                 #     logger.info(f"VALIDATE: SUCCESS - Found metadata via Helius DAS for {mint_address}.")
                 #     return True
                 # else:
                 #     logger.warning(f"VALIDATE: FAILED - No valid metadata found via Solscan or Helius for {mint_address}.")
                 #     return False
                 return False # Failed validation if primary fails

        except Exception as e:
            logger.exception(f"VALIDATE: ERROR - Exception during validation API call for {mint_address}", exc_info=True)
            return False

    async def subscribe_user(self, chat_id: int, mint_address: str) -> Dict[str, Any]:
        """Subscribes user via FreeAccessManager, returns status dict."""
        try:
            result = await self.free_access_manager.track_token(chat_id, mint_address)
            logger.info(f"subscribe_user called track_token for {chat_id}/{mint_address}, got: {result.get('status')}")
            status = result.get("status")
            # Trigger background analysis/caching on successful *new* track or even if already tracking
            if status in ["tracking", "already_tracking"]:
                 # Don't wait for this, let it run in the background
                 asyncio.create_task(self.wallet_analyzer.get_classified_wallets(mint_address, force_recalculate=False))
            return result # Return the full result dict from track_token
        except Exception as e:
             logger.exception(f"Error calling free_access_manager.track_token: {e}", exc_info=True)
             return {"status": "error", "message": f"Internal error during subscription: {str(e)[:100]}"}


    async def unsubscribe_user(self, chat_id: int, mint_address: str) -> bool:
        """Unsubscribes user via FreeAccessManager."""
        try:
            result = await self.free_access_manager.stop_tracking_token(chat_id, mint_address)
            logger.info(f"unsubscribe_user called stop_tracking_token for {chat_id}/{mint_address}, got: {result.get('status')}")
            return result.get("status") == "stopped" # Return True only if successfully stopped
        except Exception as e:
             logger.exception(f"Error calling free_access_manager.stop_tracking_token: {e}", exc_info=True)
             return False

    async def get_user_subscriptions(self, chat_id: int) -> List[Dict[str, Any]]:
        """Gets subscriptions, potentially enriching with names later."""
        logger.info(f"Getting subscriptions for chat_id {chat_id}")
        try:
            # FreeAccessManager returns subs joined with basic token data from DB
            subs = await self.free_access_manager.get_user_tracked_tokens(chat_id)
            # Could enhance here by fetching missing names via API if needed, but keep simple for now
            return subs
        except Exception as e:
            logger.exception(f"Error calling get_user_tracked_tokens for {chat_id}: {e}", exc_info=True)
            return []

    async def get_token_snapshot(self, mint_address: str) -> Dict[str, Any]:
        """Fetches snapshot using Solscan (meta, price) & Helius (holders)."""
        logger.info(f"Fetching snapshot for {mint_address}")
        snapshot = {"mint_address": mint_address, "timestamp": datetime.now(timezone.utc), "error": None}
        # Store intermediate results for FDV calc
        metadata_res = None
        price_res = None
        try:
             # Fetch primary data in parallel
             tasks = {
                 "metadata": asyncio.create_task(self.solscan_api.get_token_meta(mint_address)),
                 "price": asyncio.create_task(self.solscan_api.get_token_price(mint_address)),
                 "holders": asyncio.create_task(self.helius_api.get_top_holders(mint_address, top_n=10))
                 # TODO: Add Task for Volume/Liquidity (e.g., Solscan /token/markets? Birdeye?)
             }
             await asyncio.gather(*tasks.values(), return_exceptions=True)

             # --- Process results ---
             # Metadata
             metadata_res = tasks["metadata"].result()
             if isinstance(metadata_res, Exception) or not isinstance(metadata_res, dict):
                  logger.warning(f"Snapshot: Solscan metadata failed for {mint_address}: {metadata_res}")
                  snapshot['name'], snapshot['symbol'] = "Unknown (Meta Failed)", ""
             else:
                  # Parse Solscan meta response (VERIFY actual field names from docs)
                  snapshot['name'] = metadata_res.get("name", "Unknown")
                  snapshot['symbol'] = metadata_res.get("symbol", "")
                  snapshot['_decimals'] = metadata_res.get("decimals") # Needed for FDV/holders
                  snapshot['_total_supply'] = metadata_res.get("supply") or metadata_res.get("tokenInfo", {}).get("supply") # Find supply key

             # Price
             price_res = tasks["price"].result()
             if isinstance(price_res, Exception) or price_res is None:
                  logger.warning(f"Snapshot: Solscan price failed for {mint_address}: {price_res}")
                  snapshot['price_usd'] = None
             else:
                  snapshot['price_usd'] = price_res # Assumes get_token_price returns float or None

             # Holders
             holders_res = tasks["holders"].result()
             if isinstance(holders_res, Exception) or holders_res is None:
                  logger.warning(f"Snapshot: Helius Top Holders failed for {mint_address}: {holders_res}")
                  snapshot['top_holders'] = []
             else:
                  snapshot['top_holders'] = holders_res

             # TODO: Fetch and process Volume/Liquidity results if task was added
             snapshot['liquidity_usd'] = 0 # Placeholder
             snapshot['volume_24h'] = 0 # Placeholder
             snapshot['percent_change_1h'] = 0 # Placeholder
             snapshot['percent_change_24h'] = 0 # Placeholder

             # Calculate FDV
             snapshot['fdv_usd'] = None
             if snapshot['price_usd'] is not None and snapshot.get('_total_supply') and snapshot.get('_decimals') is not None:
                  try:
                       price = float(snapshot['price_usd'])
                       supply_raw = int(snapshot['_total_supply'])
                       decimals = int(snapshot['_decimals'])
                       if decimals >= 0:
                            supply_decimal = supply_raw / (10**decimals)
                            snapshot['fdv_usd'] = price * supply_decimal
                  except Exception as fdv_e:
                       logger.warning(f"Could not calculate FDV for {mint_address}: {fdv_e}")

             # Clean up internal keys
             snapshot.pop('_decimals', None)
             snapshot.pop('_total_supply', None)


        except Exception as e:
             logger.exception(f"Error assembling snapshot for {mint_address}: {e}", exc_info=True)
             snapshot['error'] = f"Snapshot generation error: {str(e)[:100]}"

        return snapshot


    # --- Snapshot Formatting ---
    def format_snapshot_message(self, snapshot: Dict[str, Any]) -> str:
         # Use the enhanced version from Response #71 which handles None, uses utils
         # ... (Code from Response #71, using format_price, format_large_currency, etc.) ...
         # Ensure keys match snapshot dict above ('liquidity_usd', 'volume_24h', 'percent_change_24h', etc.)
        if not snapshot: return "❌ Error: Snapshot data is empty." # Added safety check
        if snapshot.get("error"):
             mint_val = snapshot.get('mint_address', 'unknown address')
             mint_display = f"`{mint_val}`" if mint_val != 'unknown address' else mint_val
             # Using MARKDOWN_V2 escaping: Needs \ before chars like ._{}()[]!#+-=`|
             return f"❌ Failed to load snapshot data for {mint_display}\.\n_Error: {str(snapshot.get('error', 'Unknown error')).replace('.', '\.')}_"


        # ... rest of the implementation from #71 / #73 adjusting keys if necessary ...
        # Using MarkdownV2 requires escaping special characters: ._{}()[]!#+-=`|
        # This makes formatting complex, stick to MarkdownV1 if simpler unless V2 needed.
        # Reverting to Markdown V1 for simplicity in this example. Change parse_mode if needed.

        mint_address = snapshot.get('mint_address', 'N/A')
        name = snapshot.get('name', 'Unknown Token')
        symbol = snapshot.get('symbol', '')
        price_usd = snapshot.get('price_usd')
        liquidity_usd = snapshot.get('liquidity_usd') # Currently Placeholder
        volume_24h = snapshot.get('volume_24h') # Currently Placeholder
        percent_change_1h = snapshot.get('percent_change_1h') # Placeholder
        percent_change_24h = snapshot.get('percent_change_24h') # Placeholder
        fdv_usd = snapshot.get('fdv_usd') # Calculated above
        top_holders = snapshot.get("top_holders", [])

        ticker = f"${symbol}" if symbol and symbol != "?" else ""
        title = f"*{name}{f' ({ticker})' if ticker else ''}*" # Wrap in markdown bold/italic

        mint_line = f"`{mint_address}`" if mint_address != 'N/A' else "N/A"

        price_line = format_price(price_usd, default="N/A") # Use N/A default maybe?
        liquidity_line = format_large_currency(liquidity_usd, default="N/A")
        volume_24h_line = format_large_currency(volume_24h, default="N/A")
        fdv_line = format_large_currency(fdv_usd, default="N/A")

        change_1h_val = format_percentage(percent_change_1h, default="N/A")
        change_1h_emoji = ""
        if isinstance(percent_change_1h, (float, int)):
            if percent_change_1h > 1: change_1h_emoji = "📈 "
            elif percent_change_1h < -1: change_1h_emoji = "📉 "

        change_24h_val = format_percentage(percent_change_24h, default="N/A")
        change_24h_emoji = "🟢" if isinstance(percent_change_24h, (float, int)) and percent_change_24h >= 0 else "🔴"

        holders_summary = "N/A"
        details_hint = ""
        if top_holders:
             try:
                 valid_percs = [h.get('percentage', 0) for h in top_holders[:5] if isinstance(h.get('percentage'), (float, int))]
                 top_5_perc = sum(valid_percs)
                 holders_summary = f"Top 5 hold {top_5_perc:.2f}%"
                 details_hint = "_(Tap 'Top Holders' for list)_" # This button removed in new flow
             except Exception as e: holders_summary = "Error"


        message = "\n".join(filter(None, [
             f"📊 *Token Snapshot:* {title}",
             f"\nMint: {mint_line}",
             f"\n💰 Price: {price_line}",
             f"💎 FDV: {fdv_line}",
             f"💧 Liquidity: {liquidity_line}",
             f"📈 24h Volume: {volume_24h_line}",
             "\n*Changes:*",
             f"{change_1h_emoji}1h: {change_1h_val}",
             f"{change_24h_emoji} 24h: {change_24h_val}",
             # Removed holder summary as dedicated buttons aren't primary now
             # f"\n👥 Top Holders: {holders_summary}",
             # details_hint,
             f"\n_Updated: Just Now_ ⏱️"
        ]))

        return message


    # --- TIS Drilldown Methods ---
    async def get_tis_summary(self, mint_address: str) -> Dict[str, Any]:
        """Pass-through to WalletAnalyzer for TIS summary data."""
        logger.info(f"SERVICE: Getting TIS Summary for {mint_address}")
        # WalletAnalyzer returns dict with error key on failure
        return await self.wallet_analyzer.get_category_summary_stats(mint_address)

    def format_tis_summary_message(self, summary_data: Dict[str, Any], mint_address: str) -> str:
        """Formats the TIS summary view."""
        if not summary_data or summary_data.get("error"):
            return f"⚠️ Could not load TIS Analysis for `{mint_address}`.\nError: {summary_data.get('error', 'Unknown')}"

        token_name = summary_data.get("token_name", "Token")
        token_symbol = summary_data.get("token_symbol", "")
        ticker = f"${symbol}" if token_symbol else ""
        title = f"*{token_name}{f' ({ticker})' if ticker else ''}*"

        lines = [
            f"🔬 *T.I.S Analysis Summary* 🔬\n",
            f"Token: {title}",
            f"Mint: `{mint_address}`\n",
            "--- Summary ---",
        ]

        categories = {"team": "👥 Team", "insider": "🕵️ Insiders", "sniper": "🎯 Snipers"}
        for key, display_name in categories.items():
            data = summary_data.get(key, {"count": 0, "statuses": {}})
            count = data.get("count", 0)
            statuses = data.get("statuses", {})
            g, y, r = statuses.get("GREEN", 0), statuses.get("YELLOW", 0), statuses.get("RED", 0)
            lines.append(f"{display_name}: {count} wallets (🟢{g} 🟡{y} 🔴{r})")

        lines.append("\n_Tap a category below for wallet list._")
        return "\n".join(lines)

    async def get_detailed_category_view(self, mint_address: str, category: str) -> Dict[str, Any]:
        """Pass-through to WalletAnalyzer for detailed category data."""
        logger.info(f"SERVICE: Getting Detailed Category View for {category}/{mint_address}")
        # WalletAnalyzer returns dict with error key on failure
        return await self.wallet_analyzer.get_wallets_in_category_with_state(mint_address, category)

    def format_detailed_category_message(self, detailed_data: Dict[str, Any], mint_address: str) -> str:
        """Formats the header for the detailed category view."""
        if not detailed_data or detailed_data.get("error"):
             return f"⚠️ Error loading details for category view: {detailed_data.get('error', 'Unknown')}"

        stats = detailed_data.get('stats', {})
        category = stats.get('category', 'Unknown')
        count = stats.get('count', 0)
        statuses = stats.get('statuses', {'GREEN': 0, 'YELLOW': 0, 'RED': 0})
        current_perc_supply = stats.get('current_holding_perc_supply') # Might be None

        title_map = {"team": "👥 Team", "insider": "🕵️ Insiders", "sniper": "🎯 Snipers"}
        view_title = title_map.get(category.lower(), f"{category.capitalize()}") + " Wallet Details"

        message_lines = [
             f"*{view_title}*\n",
             f"Token: `{mint_address}`", # Can add Name/$Ticker if passed in stats
             f"\n--- Category Stats ---",
             f"Total Wallets: {count}",
        ]
        if current_perc_supply is not None:
             message_lines.append(f"Current Holdings: {current_perc_supply:.2f}% of supply")
        else:
             message_lines.append("Current Holdings: N/A")

        status_line = f"Status Breakdown: 🟢{statuses.get('GREEN', 0)} 🟡{statuses.get('YELLOW', 0)} 🔴{statuses.get('RED', 0)}"
        message_lines.append(status_line)
        message_lines.append("\n--- Wallets ---")
        message_lines.append("_(Tap wallet below to view on Solscan)_ 👇")

        return "\n".join(message_lines)

    # Placeholder formatting for removed features - can be deleted
    def format_first_buyers_message(self, data: Any) -> str: return "Feature Not Implemented."
    def format_top_traders_message(self, data: Any) -> str: return "Feature Not Implemented."
    def format_health_score_message(self, data: Any) -> str: return "Feature Not Implemented."
    # ... etc for other old formatters ...