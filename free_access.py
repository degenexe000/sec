import logging
import asyncio
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

# Import settings and DB access
from config.settings import settings # Use global settings
from data.db import Database

# Import exception types if known (e.g., from previous logs)
# This might vary based on supabase-py version interacting with postgrest-py
try:
     # For supabase-py V1/V2 using postgrest-py
     from postgrest.exceptions import APIError as PostgrestAPIError
except ImportError:
     # Fallback or define based on observed errors
     PostgrestAPIError = Exception # Define as base Exception if import fails


logger = logging.getLogger(__name__)


class FreeAccessManager:
    """
    Manages user registration and token tracking (subscriptions),
    enforcing free access for all features. Uses Supabase backend.
    Designed for an asynchronous environment.
    """

    def __init__(self, db: Database): # Settings can be accessed globally via imported 'settings'
        """
        Initialize free access manager.

        Args:
            db: Initialized Database connection manager instance.
        """
        self.settings = settings # Store settings if needed, or access global directly
        self.db = db
        # Get the SYNC client, calls will be wrapped
        self.supabase = db.get_supabase_sync_client()
        logger.info("FreeAccessManager initialized.")


    async def _execute_supabase_query(self, query_builder) -> Dict[str, Any]:
        """Helper to execute sync Supabase queries in a thread with error handling."""
        try:
            # response = await asyncio.to_thread(query_builder.execute) # standard call
            # Need to access the execute method correctly within the thread
            target_func = getattr(query_builder, 'execute')
            response = await asyncio.to_thread(target_func)

            # Basic response check (adapt based on supabase-py version)
            # V1 often returned { data: [...], error: None, count: ... } on success
            # V2 might be slightly different or raise exceptions directly more often
            if hasattr(response, 'error') and response.error:
                logger.error(f"Supabase API Error during execute: {response.error}")
                # Re-raise as a standard exception? Or return error dict?
                # For simplicity, return dict matching previous pattern.
                return {"error": response.error, "message": response.error.get("message", "Unknown Supabase error")}
            # Success case - V1/V2 usually have .data attribute
            elif hasattr(response, 'data'):
                # Return structure compatible with original code expectations
                # Include count if present (useful for select/delete maybe)
                 return {"data": response.data, "count": getattr(response, 'count', None), "error": None, "status_code": 200 }
            else:
                # Unexpected response format
                 logger.error(f"Unexpected Supabase response format: {response}")
                 return {"error": "Unexpected Supabase response format", "data": None}

        except PostgrestAPIError as api_error: # Catch specific PostgREST error
             logger.error(f"Supabase Postgrest API Error: {api_error.message} (Code: {api_error.code}) Details: {api_error.details} Hint: {api_error.hint}")
             return {"status": "error", "message": api_error.message or "Supabase API Error"}
        except Exception as e:
             logger.exception(f"Unhandled error executing Supabase query via thread: {e}", exc_info=True)
             # Wrap exception info into the return dict
             return {"status": "error", "message": f"Internal error: {str(e)[:100]}"}


    async def register_user(self, chat_id: int, username: Optional[str] = None) -> Dict[str, Any]:
        """
        Registers a new user or updates last_active if existing.
        Returns standardized status dict.
        """
        logger.info(f"Attempting to register/update user: chat_id={chat_id}, username={username}")
        try:
            # 1. Check if user exists
            select_query = self.supabase.table("users").select("chat_id").eq("chat_id", chat_id).limit(1)
            check_result = await self._execute_supabase_query(select_query)

            if check_result.get("error"): # Handle error during check
                return {"status": "error", "chat_id": chat_id, "message": check_result.get("message", "DB Error checking user")}

            if check_result.get("data"):
                # User exists -> Update last_active
                logger.debug(f"User {chat_id} exists. Updating last_active.")
                update_query = self.supabase.table("users") \
                    .update({"last_active": datetime.now(timezone.utc).isoformat()}) \
                    .eq("chat_id", chat_id)
                update_result = await self._execute_supabase_query(update_query)

                if update_result.get("error"): # Log update failure but return 'existing'
                     logger.error(f"Failed to update last_active for user {chat_id}: {update_result.get('message')}")

                return {"status": "existing", "chat_id": chat_id, "message": "User already registered"}

            else:
                # User doesn't exist -> Create new user
                logger.info(f"Registering new user: {chat_id}")
                # Prepare user data exactly as expected by DB schema
                # Make sure JSONB fields are valid JSON/dicts
                user_data = {
                    "chat_id": chat_id,
                    "username": username,
                    "access_level": "free",
                    "features_enabled": json.dumps({ # Ensure this is serialized if column is JSONB and client doesn't auto-convert
                        "token_tracking": True, "price_alerts": True, "transaction_monitoring": True,
                        "advanced_analytics": True, "team_detection": True, "insider_detection": True,
                        "sniper_detection": True
                    }),
                    "settings": json.dumps({ # Ensure this is serialized
                        "notification_frequency": "realtime", "min_transaction_amount": 0,
                        "alert_types": ["price", "liquidity", "team", "insider", "sniper", "status_change"] # Added status change
                    }),
                    "last_active": datetime.now(timezone.utc).isoformat() # Add initial last_active
                    # created_at has default NOW()
                }

                insert_query = self.supabase.table("users").insert(user_data)
                insert_result = await self._execute_supabase_query(insert_query)

                if insert_result.get("error"):
                     logger.error(f"Failed to insert new user {chat_id}: {insert_result.get('message')}")
                     return {"status": "error", "chat_id": chat_id, "message": insert_result.get("message", "DB error registering user")}
                else:
                    logger.info(f"Successfully registered user {chat_id}")
                    return {"status": "registered", "chat_id": chat_id, "message": "User registered with free access"}

        except Exception as e: # Catch-all for unexpected issues
             logger.exception(f"Unexpected error in register_user for chat_id {chat_id}", exc_info=True)
             return {"status": "error", "chat_id": chat_id, "message": f"Internal server error: {str(e)[:100]}"}


    async def get_user_access(self, chat_id: int) -> Dict[str, Any]:
        """
        Gets user access info, ensuring user exists (registers if not)
        and that all features are marked enabled (free access enforcement).
        Returns standardized status dict.
        """
        logger.debug(f"Getting user access info for chat_id {chat_id}")
        try:
            # Fetch user data
            select_query = self.supabase.table("users").select("*").eq("chat_id", chat_id).limit(1)
            response = await self._execute_supabase_query(select_query)

            if response.get("error"):
                 # Propagate the error message
                 return {"status": "error", "chat_id": chat_id, "message": response.get("message", "DB error fetching user access")}

            if not response.get("data"):
                logger.info(f"User {chat_id} not found, attempting registration.")
                # Register if doesn't exist (register_user returns status dict)
                # Passing username=None initially, might get updated elsewhere
                return await self.register_user(chat_id, username=None)

            # User exists
            user_data = response["data"][0]
            logger.debug(f"User {chat_id} found. Checking/updating features.")

            # Check if features need updating to enforce free access
            needs_update = False
            # Safely parse JSONB fields - assumes they are stored as valid JSON strings or dicts by supabase-py
            current_features = user_data.get("features_enabled", {})
            if isinstance(current_features, str): # Handle case where it might be a string from DB
                try: current_features = json.loads(current_features)
                except json.JSONDecodeError: current_features = {}
            if not isinstance(current_features, dict): current_features = {} # Fallback if parsing failed or bad type

            all_features_list = [ # Keep this list updated
                "token_tracking", "price_alerts", "transaction_monitoring",
                "advanced_analytics", "team_detection", "insider_detection",
                "sniper_detection"
            ]
            for feature in all_features_list:
                 if not current_features.get(feature): # If feature missing or False
                      current_features[feature] = True
                      needs_update = True

            if user_data.get("access_level") != "free":
                 needs_update = True

            # Perform update only if necessary
            if needs_update:
                logger.info(f"Updating user {chat_id} to ensure free access/all features.")
                update_data = {
                     "access_level": "free",
                     "features_enabled": json.dumps(current_features), # Serialize back to JSON if needed
                     "last_active": datetime.now(timezone.utc).isoformat()
                }
                update_query = self.supabase.table("users").update(update_data).eq("chat_id", chat_id)
                update_result = await self._execute_supabase_query(update_query)
                if update_result.get("error"):
                      logger.error(f"Failed to update features/status for user {chat_id}: {update_result.get('message')}")
                      # Non-fatal? User access might be inconsistent now.

            # Return current (potentially updated) state
            return {
                "status": "active", # Assuming existing user means active
                "chat_id": chat_id,
                "access_level": "free", # Return enforced level
                "features_enabled": current_features, # Return potentially updated dict
                "settings": json.loads(user_data.get("settings", '{}')) if isinstance(user_data.get("settings"), str) else user_data.get("settings", {}),
                "message": "User has free access"
            }

        except Exception as e:
             logger.exception(f"Unexpected error in get_user_access for {chat_id}", exc_info=True)
             return {"status": "error", "chat_id": chat_id, "message": f"Internal error getting user access: {str(e)[:100]}"}


    async def track_token(self, chat_id: int, token_address: str) -> Dict[str, Any]:
        """Tracks a token after ensuring user exists. Handles token/subscription inserts."""
        logger.info(f"Attempting to track token {token_address} for chat {chat_id}")
        try:
            # 1. Ensure user exists and access is OK
            user_access = await self.get_user_access(chat_id)
            if user_access.get("status") in ["error"]:
                logger.warning(f"Cannot track token for {chat_id}: User access check failed ({user_access.get('message')})")
                return user_access # Propagate user access error

            # 2. Ensure token exists in 'tokens' table (for FK constraint)
            # This check & insert might be prone to race conditions in high concurrency
            # Consider using SQL 'INSERT ... ON CONFLICT DO NOTHING' if using asyncpg directly.
            token_query = self.supabase.table("tokens").select("mint_address", count="exact").eq("mint_address", token_address)
            token_check = await self._execute_supabase_query(token_query)

            # Check results based on how your client returns count or data presence
            token_exists = False
            if token_check.get("error"):
                 logger.error(f"DB error checking token {token_address}: {token_check.get('message')}")
                 # Decide whether to proceed or return error - let's try inserting
            elif token_check.get("count", 0) > 0 or token_check.get("data"): # Check count or data presence
                 token_exists = True

            if not token_exists:
                logger.info(f"Token {token_address} not in DB, inserting...")
                token_insert_data = {"mint_address": token_address} # Add name later if known
                token_insert_query = self.supabase.table("tokens").insert(token_insert_data) # Consider ON CONFLICT clause
                token_insert_result = await self._execute_supabase_query(token_insert_query)
                if token_insert_result.get("error"):
                     # Failed to insert token, FK violation likely on subscription insert
                     logger.error(f"Failed to insert token {token_address}: {token_insert_result.get('message')}")
                     return {"status": "error", "chat_id": chat_id, "token_address": token_address, "message": token_insert_result.get("message", "DB Error saving token")}


            # 3. Check if subscription already exists
            sub_query = self.supabase.table("subscriptions") \
                 .select("id", count="exact") \
                 .eq("chat_id", chat_id) \
                 .eq("mint_address", token_address)
            sub_check = await self._execute_supabase_query(sub_query)

            if sub_check.get("error"):
                logger.error(f"DB error checking subscription for {chat_id}/{token_address}: {sub_check.get('message')}")
                # Continue and try to insert, relying on UNIQUE constraint? Risky. Better return error.
                return {"status": "error", "chat_id": chat_id, "token_address": token_address, "message": sub_check.get("message", "DB error checking subscription")}

            if sub_check.get("count", 0) > 0 or sub_check.get("data"):
                 logger.info(f"User {chat_id} already tracking {token_address}.")
                 return {"status": "already_tracking", "chat_id": chat_id, "token_address": token_address, "message": "Token already tracked"}

            # 4. Insert new subscription
            logger.info(f"Adding new subscription for {chat_id} / {token_address}")
            subscription_data = {
                 "chat_id": chat_id,
                 "mint_address": token_address,
                 "settings": json.dumps({ # Serialize default settings
                      "notification_types": ["status_change", "price_change", "liquidity_change"], # Focused default alerts
                      "min_transaction_amount_usd": 500, # Example default whale alert threshold
                      "price_alert_threshold_percent": 10, # Example threshold
                      "liquidity_alert_threshold_percent": 15
                 })
             }
            sub_insert_query = self.supabase.table("subscriptions").insert(subscription_data)
            sub_insert_result = await self._execute_supabase_query(sub_insert_query)

            if sub_insert_result.get("error"):
                 logger.error(f"Failed to insert subscription for {chat_id}/{token_address}: {sub_insert_result.get('message')}")
                 return {"status": "error", "chat_id": chat_id, "token_address": token_address, "message": sub_insert_result.get("message", "DB Error saving subscription")}

            logger.info(f"Successfully tracked token {token_address} for chat {chat_id}")
            return {"status": "tracking", "chat_id": chat_id, "token_address": token_address, "message": "Token tracking started"}

        except Exception as e:
             # Catch-all for any unexpected failure in the sequence
             logger.exception(f"CRITICAL failure in track_token {chat_id}/{token_address}", exc_info=True)
             return {"status": "error", "chat_id": chat_id, "token_address": token_address, "message": f"Internal error tracking token: {str(e)[:100]}"}


    async def get_user_tracked_tokens(self, chat_id: int) -> List[Dict[str, Any]]:
        """Gets list of subscriptions for a user, joined with token info."""
        logger.info(f"Getting tracked tokens for chat_id {chat_id}")
        try:
            # User access check can optionally be skipped if commands always ensure user exists
            # await self.get_user_access(chat_id)

            query = self.supabase.table("subscriptions").select("*, tokens(*)").eq("chat_id", chat_id) # Assumes FK setup allows join syntax
            response = await self._execute_supabase_query(query)

            if response.get("error"):
                logger.error(f"Error fetching tracked tokens for {chat_id}: {response.get('message')}")
                return []

            # Return the data list directly
            return response.get("data", [])

        except Exception as e:
            logger.exception(f"Unexpected error in get_user_tracked_tokens for {chat_id}", exc_info=True)
            return []


    async def stop_tracking_token(self, chat_id: int, token_address: str) -> Dict[str, Any]:
        """Removes a token subscription for a user."""
        logger.info(f"Attempting to stop tracking {token_address} for {chat_id}")
        try:
            # Delete directly, Supabase handles case where row doesn't exist gracefully (returns count 0?)
            delete_query = self.supabase.table("subscriptions") \
                .delete() \
                .eq("chat_id", chat_id) \
                .eq("mint_address", token_address)
            delete_result = await self._execute_supabase_query(delete_query)

            if delete_result.get("error"):
                 logger.error(f"Error deleting subscription for {chat_id}/{token_address}: {delete_result.get('message')}")
                 return {"status": "error", "chat_id": chat_id, "token_address": token_address, "message": delete_result.get('message', "DB error removing track")}

            # Check if deletion actually occurred (adapt based on what supabase-py execute returns for delete)
            # Often it returns the deleted data or just confirms success with data=[] and no error
            # A count attribute might exist on some versions
            rows_deleted = delete_result.get("count", 1 if not delete_result.get("error") and not delete_result.get("data") else 0) # Guesswork here
            if rows_deleted or (not delete_result.get("error") and not delete_result.get("data")): # Check if *something* indicates success
                logger.info(f"Successfully stopped tracking {token_address} for {chat_id}")
                return {"status": "stopped", "chat_id": chat_id, "token_address": token_address, "message": "Token tracking stopped"}
            else:
                 logger.info(f"Token {token_address} was likely not being tracked by {chat_id} (no rows deleted).")
                 return {"status": "not_tracking", "chat_id": chat_id, "token_address": token_address, "message": "Token was not being tracked"}

        except Exception as e:
            logger.exception(f"Unexpected error in stop_tracking_token {chat_id}/{token_address}", exc_info=True)
            return {"status": "error", "chat_id": chat_id, "token_address": token_address, "message": f"Internal error stopping track: {str(e)[:100]}"}

    async def get_subscribers_for_token(self, mint_address: str) -> List[int]:
        """Gets list of chat_ids tracking a specific token."""
        logger.debug(f"Getting subscribers for {mint_address}")
        chat_ids: List[int] = []
        try:
            query = self.supabase.table("subscriptions").select("chat_id").eq("mint_address", mint_address)
            response = await self._execute_supabase_query(query)
            if response.get("data"):
                chat_ids = [item['chat_id'] for item in response["data"] if 'chat_id' in item]
            logger.debug(f"Found {len(chat_ids)} subscribers for {mint_address}")
        except Exception as e:
            logger.exception(f"Error getting subscribers for {mint_address}", exc_info=True)
        return chat_ids


    async def get_all_active_subscriptions(self) -> List[Dict[str, Any]]:
        """ Gets ALL subscription records. Use with caution - needs pagination for scale."""
        logger.warning("Fetching ALL subscriptions - Consider pagination for large scale!")
        try:
             query = self.supabase.table("subscriptions").select("chat_id, mint_address, settings").limit(10000) # Add safety limit
             response = await self._execute_supabase_query(query)
             return response.get("data", [])
        except Exception as e:
            logger.exception(f"Error getting all subscriptions: {e}", exc_info=True)
            return []

    async def get_all_unique_tracked_mints(self) -> List[str]:
        """Gets unique list of mint addresses tracked by anyone."""
        logger.info("Fetching unique tracked mints...")
        try:
            query = self.supabase.table("subscriptions").select("mint_address") # Only select mint
            response = await self._execute_supabase_query(query)
            if response.get("data"):
                # Use set comprehension for efficiency
                unique_mints = {item['mint_address'] for item in response["data"] if item and 'mint_address' in item}
                logger.info(f"Found {len(unique_mints)} unique tracked mints.")
                return list(unique_mints)
            return []
        except Exception as e:
            logger.exception("Error getting unique tracked mints", exc_info=True)
            return []