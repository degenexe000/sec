# data/websocket_manager.py
import websockets
import websockets.exceptions
import json
import asyncio
import logging
from typing import Optional, Dict, List, Any, Set, Union
from datetime import datetime, timezone
import ssl # Keep for potential context config

# Use the central settings instance
from config.settings import settings
# Import needed services/components via TYPE_CHECKING if causes cycles
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from data.wallet_analyzer import WalletAnalyzer
    from data.free_access import FreeAccessManager

logger = logging.getLogger(__name__)

# --- Constants from Settings or Defaults ---
PING_INTERVAL_SECONDS = 30  # Recommended based on Helius examples
PING_TIMEOUT_SECONDS = 20  # Timeout for pong response
RECEIVE_TIMEOUT_SECONDS = PING_INTERVAL_SECONDS * 2 + 10 # Must be > ping interval+timeout
WEBSOCKET_RECONNECT_DELAY = settings.websocket_reconnect_delay
MAX_ACCOUNTS_PER_SUBSCRIPTION = 45000 # Based on Helius Docs for accountInclude

class WebSocketError(Exception):
    """Custom exception for WebSocket manager errors."""
    pass

class WebSocketManager:
    """
    Manages persistent WebSocket connection to Helius Atlas for real-time events.
    Uses Helius enhanced `transactionSubscribe` with `accountInclude` filter.
    Handles subscriptions dynamically, dispatches full transaction data.
    Includes keep-alive pings and auto-reconnect.
    """

    def __init__(self,
                 wallet_analyzer: 'WalletAnalyzer',
                 free_access_manager: 'FreeAccessManager' # Inject FreeAccessManager
                ):
        if not settings.helius_api_key:
            raise ValueError("Helius API key required for WebSocket (HELIUS_API_KEY).")

        # --- Correct Helius Atlas WSS URL ---
        self.wss_url = f"wss://atlas-mainnet.helius-rpc.com/?api-key={settings.helius_api_key}"
        self.wallet_analyzer = wallet_analyzer
        self.free_access_manager = free_access_manager # Store dependency

        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self._listen_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._main_loop_task: Optional[asyncio.Task] = None # Track the main reconnect loop
        self._connection_lock = asyncio.Lock() # Prevent concurrent connect/resubscribe

        # --- Refined Subscription State ---
        # What addresses do we INTEND to monitor?
        self.intended_accounts_to_monitor: Set[str] = set()
        # Details of the currently ACTIVE Helius transaction subscription
        self.helius_subscription_details: Dict[str, Any] = {
             "helius_id": None,          # The ID Helius assigned to the sub
             "local_request_id": None, # The last local ID we used to request it
             "active_filter_accounts": set() # The set of accounts CURRENTLY filtered by active sub
        }

        logger.info(f"WebSocketManager Initialized for Helius Atlas WSS.")

    async def connect(self) -> bool:
        """Establishes WebSocket connection."""
        async with self._connection_lock: # Prevent multiple threads trying to connect
             if self.websocket and self.websocket.open: return True # Already connected

             logger.info(f"Attempting WebSocket connection to Helius Atlas...")
             # Clear old state before attempting connect
             self.helius_subscription_details["helius_id"] = None
             self.helius_subscription_details["local_request_id"] = None
             self.helius_subscription_details["active_filter_accounts"] = set()

             try:
                 # Connect without internal ping management, use custom loop
                 self.websocket = await websockets.connect(
                      self.rpc_wss_url,
                      open_timeout=25 # Increase open timeout slightly
                 )
                 logger.info("Atlas WebSocket connected successfully.")
                 # Attempt to establish or update the main subscription immediately
                 await self._update_helius_subscription()
                 # Start background tasks needed for a healthy connection
                 self._start_background_tasks()
                 return True
             except Exception as e:
                  logger.error(f"WebSocket connection failed: {type(e).__name__}: {e}")
                  self.websocket = None
                  return False

    # --- Background Tasks (Ping / Listen) ---
    async def _ping_loop(self):
        """Sends pings periodically via the library's method."""
        logger.debug("Starting WebSocket ping loop.")
        while self.running and self.websocket and self.websocket.open:
            try:
                pong_waiter = await self.websocket.ping()
                await asyncio.wait_for(pong_waiter, timeout=PING_TIMEOUT_SECONDS)
                logger.debug("Ping -> Pong received.")
                await asyncio.sleep(PING_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                logger.warning(f"WebSocket pong not received in {PING_TIMEOUT_SECONDS}s. Closing connection.")
                await self._close_ws_safely(code=1001, reason="Ping Timeout")
                break # Exit loop, will trigger reconnect
            except websockets.exceptions.ConnectionClosed:
                logger.info("WebSocket connection closed during ping.")
                break
            except Exception as e: # Catch errors during ping send/wait
                 logger.exception(f"Error in ping loop: {e}", exc_info=True)
                 # If ping fails drastically, close and reconnect might be best
                 if self.websocket and self.websocket.open:
                     await self._close_ws_safely(code=1011, reason="Ping Loop Error")
                 break # Exit loop, trigger reconnect
        logger.debug("Exiting WebSocket ping loop.")

    async def _listen(self):
        """Listens for messages and dispatches for handling."""
        if not self.websocket or not self.websocket.open: return
        logger.info("Starting WebSocket listener loop...")
        while self.running and self.websocket and self.websocket.open:
            try:
                message = await asyncio.wait_for(self.websocket.recv(), timeout=RECEIVE_TIMEOUT_SECONDS)
                # Launch handling in a separate task to avoid blocking the listen loop
                asyncio.create_task(self._handle_raw_message(message))
            except asyncio.TimeoutError:
                 logger.warning(f"WebSocket receive timeout after {RECEIVE_TIMEOUT_SECONDS}s. Assume stale connection, closing.")
                 await self._close_ws_safely(code=1001, reason="Receive Timeout")
                 break # Exit loop, trigger reconnect
            except websockets.exceptions.ConnectionClosed:
                 logger.info("WebSocket connection closed during listen.")
                 break
            except Exception as e:
                 logger.exception(f"Error receiving/dispatching message in listener loop: {e}", exc_info=True)
                 await asyncio.sleep(1) # Avoid tight loops on persistent errors
        logger.info("WebSocket listener loop finished.")
        await self._close_ws_safely() # Ensure clean close if loop exits unexpectedly


    async def _handle_raw_message(self, message: Union[str, bytes]):
        """Parse JSON and route message to RPC handler."""
        # ... (Same safe JSON parsing as Response #101) ...
        try: data = json.loads(message)
        except Exception: logger.warning(...); return
        await self._handle_incoming_rpc_message(data)


    async def _handle_incoming_rpc_message(self, data: Dict):
        """Parses Helius Atlas JSON-RPC messages."""
        # ... (Same parsing logic as Response #101) ...
        # Key adjustments:
        if "method" in data and "params" in data: # Notification
            # ... (parse method, params, helius_sub_id, result) ...
            if method == "transactionNotification":
                # Check if it's for our *active* subscription
                if helius_sub_id == self.helius_subscription_details["helius_id"]:
                    tx_data = result.get("transaction")
                    signature = result.get("signature") or tx_data.get("transaction", {}).get("signatures", [None])[0]
                    if tx_data and signature:
                        logger.info(f"Dispatching tx data for sig: {signature[:10]}...")
                        asyncio.create_task(self.wallet_analyzer.process_realtime_transaction(tx_data))
                    # ... (Log warning if missing data) ...
                # else: Ignore notification if not for current sub_id

            # ... (Handle other notification methods like accountNotification) ...

        elif "result" in data and "id" in data: # Response to our request
            request_id = data["id"] # Our local ID, e.g., "sub_tx_main_..."
            helius_sub_id = data["result"] # Helius assigned ID
            # --- Confirmation Handling ---
            # Check if this confirms the request we last sent for the main transaction subscription
            if request_id == self.helius_subscription_details["local_request_id"] and isinstance(helius_sub_id, (int, str)):
                logger.info(f"CONFIRMED Helius transaction subscription! Helius ID: {helius_sub_id} (for Request ID: {request_id})")
                self.helius_subscription_details["helius_id"] = helius_sub_id
                # Now active_filter_accounts reflects what Helius confirmed is being watched
            elif "unsub_tx_" in str(request_id) and helius_sub_id is True:
                 logger.info(f"Unsubscribe confirmed by Helius for Request ID {request_id}")
                 # If the unsub was for the *main* sub id, ensure local state reflects it
                 helius_id_unsubbed = request_id.split("_")[-1] # Hacky way to get ID from unsub request
                 if helius_id_unsubbed == str(self.helius_subscription_details.get("helius_id")):
                      self.helius_subscription_details = { "helius_id": None, "local_request_id": None, "active_filter_accounts": set()}
                      logger.info("Cleared local state for main transaction subscription.")
            else:
                 logger.info(f"Received miscellaneous RPC response: ID='{request_id}', Result='{data['result']}'")

        # ... (Handle JSON-RPC error responses) ...


    # --- Background Task Helpers ---
    def _start_background_tasks(self):
         """Start listener and ping loops if not running."""
         # Ensure only one instance runs using the task attribute check
         if not self._listen_task or self._listen_task.done():
             self._listen_task = asyncio.create_task(self._listen(), name="WSSListenTask")
         if not self._ping_task or self._ping_task.done():
              self._ping_task = asyncio.create_task(self._ping_loop(), name="WSSPingTask")


    async def _stop_background_tasks(self):
         """Stop listener and ping tasks safely."""
         # ... (Logic from Response #101 to cancel and gather _listen_task, _ping_task) ...


    async def _close_ws_safely(self, code=1000, reason=""):
         """Close WebSocket connection safely."""
         # ... (Logic from Response #101 using self.websocket.close()) ...


    # --- Public Control Methods ---
    async def start(self):
        """Starts the WebSocket Manager's main connection loop."""
        if self.running: return
        logger.info("Starting WebSocket Manager Service...")
        self.running = True
        # Load initial intended subscriptions BEFORE starting loop
        await self._load_initial_intended_subscriptions()
        self._main_loop_task = asyncio.create_task(self._run_with_reconnect(), name="WSSMainLoop")

    async def stop(self):
        """Stops the WebSocket Manager gracefully."""
        if not self.running: return
        logger.info("Stopping WebSocket Manager Service...")
        self.running = False # Signal loops to stop
        await self._close_ws_safely(reason="Application Shutdown") # Close connection immediately
        # Cancel the main reconnect loop task itself
        if self._main_loop_task and not self._main_loop_task.done():
             self._main_loop_task.cancel()
             try: await self._main_loop_task
             except asyncio.CancelledError: pass
        # Stop/cleanup background tasks (listen/ping are stopped within reconnect loop usually)
        await self._stop_background_tasks()
        logger.info("WebSocket Manager Service stopped.")


    async def _run_with_reconnect(self):
        """Main internal loop handling connections, listeners, and reconnections."""
        # ... (Logic from Response #101 - Connect, wait for listen task, sleep on disconnect) ...


    # --- Subscription Management Logic ---
    async def _load_initial_intended_subscriptions(self):
         """Load tracked accounts from persistence at startup."""
         logger.info("Loading initial accounts to monitor...")
         try:
             # Use FreeAccessManager to get unique mints/wallets currently tracked
             # Decision: Monitor Mints or Classified Wallets? Start with Mints.
             mints_to_track = await self.free_access_manager.get_all_unique_tracked_mints()
             self.intended_accounts_to_monitor = set(mints_to_track)
             logger.info(f"Loaded {len(self.intended_accounts_to_monitor)} unique mints for initial monitoring.")
             # TODO: Load classified wallets separately if needed for different filters?
         except Exception as e:
             logger.exception("Failed to load initial subscriptions", exc_info=True)


    async def _update_helius_subscription(self):
        """Compares intended accounts with current sub and sends update request if needed."""
        if not self.websocket or not self.websocket.open:
             logger.warning("Cannot update Helius subscription: WebSocket not connected.")
             return

        current_intended = self.intended_accounts_to_monitor # Set of addresses
        currently_active = self.helius_subscription_details["active_filter_accounts"] # Set of addresses

        if current_intended == currently_active:
             logger.debug("No changes needed for Helius transaction subscription.")
             return

        if not current_intended:
             # Intention is to monitor nothing, unsubscribe if needed
             logger.info("Intended monitoring list is empty. Unsubscribing from Helius transactions.")
             if self.helius_subscription_details["helius_id"]:
                  await self.unsubscribe_current_transaction_subscription()
             # Update local state immediately
             self.helius_subscription_details["active_filter_accounts"] = set()
             return

        # Intention is to monitor a non-empty list
        new_account_list = list(current_intended)
        if len(new_account_list) > MAX_ACCOUNTS_PER_SUBSCRIPTION:
             logger.warning(f"Desired accounts ({len(new_account_list)}) exceeds limit. Truncating to {MAX_ACCOUNTS_PER_SUBSCRIPTION}.")
             new_account_list = new_account_list[:MAX_ACCOUNTS_PER_SUBSCRIPTION]


        # Send new subscribe request (Helius likely handles replacing the old one if same type/filter root?)
        # Generate new local request ID
        local_sub_id = f"sub_tx_main_{int(datetime.now(timezone.utc).timestamp())}"
        # Use Helius Enhanced transactionSubscribe Payload
        params = [
            { "accountInclude": new_account_list, "failed": False, "vote": False },
            { "commitment": "confirmed", "encoding": "jsonParsed", "transactionDetails": "full", "maxSupportedTransactionVersion": 0 }
        ]
        payload = {"jsonrpc": "2.0", "id": local_sub_id, "method": "transactionSubscribe", "params": params}

        logger.info(f"Sending updated transactionSubscribe (ID: {local_sub_id}) for {len(new_account_list)} accounts...")
        # Unsubscribe *before* sending new sub? Or let Helius handle override?
        # Let's assume Helius handles override; otherwise, need unsubscribe logic here.
        # if self.helius_subscription_details["helius_id"]: await self.unsubscribe_current_transaction_subscription() # Optional Unsub first

        self.helius_subscription_details["local_request_id"] = local_sub_id
        self.helius_subscription_details["active_filter_accounts"] = set(new_account_list) # Optimistically update local state

        try:
             await self.websocket.send(json.dumps(payload))
             logger.debug("Updated transactionSubscribe request sent.")
             # Helius ID will be updated via _handle_incoming_rpc_message confirmation
        except Exception as e:
             logger.exception(f"Failed to send updated transactionSubscribe", exc_info=True)
             # Revert optimistic update? State might be inconsistent until next reconnect/resubscribe.
             self.helius_subscription_details["active_filter_accounts"] = currently_active # Revert maybe?


    async def _resubscribe_all_intended(self):
        """ Central function to ensure current subscription matches intended state. """
        logger.info("Triggering Helius subscription update/resubscription check...")
        await self._update_helius_subscription()


    async def unsubscribe_current_transaction_subscription(self):
        """Unsubscribes from the current main Helius transaction subscription."""
        if not self.websocket or not self.websocket.open: return False
        helius_id = self.helius_subscription_details.get("helius_id")
        if not helius_id: logger.info("No active Helius tx sub ID to unsubscribe."); return False

        local_unsub_id = f"unsub_tx_{helius_id}_{int(datetime.now(timezone.utc).timestamp())}"
        payload = {
            "jsonrpc": "2.0", "id": local_unsub_id,
            "method": "transactionUnsubscribe", # Needs verification from Helius Docs
            "params": [helius_id]
        }
        logger.info(f"Sending transactionUnsubscribe request for Helius ID: {helius_id}")
        try:
            await self.websocket.send(json.dumps(payload))
            # Clear local state ONLY when CONFIRMED via response handler (_handle_incoming_rpc_message)
            # Do NOT clear self.intended_subscriptions here
            return True
        except Exception as e:
            logger.exception(f"Failed to send transactionUnsubscribe", exc_info=True)
            return False

    # --- Public Methods to Modify Monitored Accounts ---
    def add_account_to_monitor(self, account_address: str):
        """Adds account & triggers subscription update."""
        if account_address not in self.intended_accounts_to_monitor:
            logger.info(f"Adding account to WSS monitor list: {account_address}")
            self.intended_accounts_to_monitor.add(account_address)
            if self.running: asyncio.create_task(self._update_helius_subscription())

    def remove_account_to_monitor(self, account_address: str):
        """Removes account & triggers subscription update."""
        if account_address in self.intended_accounts_to_monitor:
            logger.info(f"Removing account from WSS monitor list: {account_address}")
            self.intended_accounts_to_monitor.discard(account_address)
            if self.running: asyncio.create_task(self._update_helius_subscription())

    # --- Add methods for other sub types (e.g., accountSubscribe) if needed ---