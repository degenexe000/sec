import logging
import json
import asyncio
from typing import Any, Dict, List, Optional, Union

import aiohttp

# Use the central settings instance
from config.settings import settings

logger = logging.getLogger(__name__)

# Custom Exception for Helius Specific Errors
class HeliusAPIError(Exception):
    """Custom exception for Helius API related errors."""
    def __init__(self, message, status_code=None, response_data=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data

class HeliusAPI:
    """
    Optimized and Robust Helius API client supporting DAS and RPC methods via a shared session.
    Uses API Key in URL for RPC/DAS over RPC, check docs for REST auth if needed.
    """

    def __init__(self, session: aiohttp.ClientSession):
        """
        Initialize Helius API client.

        Args:
            session: An active shared aiohttp.ClientSession for making requests.
        """
        if not settings.helius_api_key:
            raise ValueError("Helius API key is required but not configured (HELIUS_API_KEY).")

        self.api_key = settings.helius_api_key
        # --- CORRECTED Endpoints using API Key in URL ---
        # Base URL for RPC methods (getTokenAccounts, getTransaction, etc.) AND DAS over RPC (getAsset)
        # CRITICAL: Verify mainnet RPC URL from Helius Dashboard
        self.rpc_url = f"https://mainnet.helius-rpc.com/?api-key={self.api_key}"
        # Base URL for potential REST API calls (e.g., V0 webhooks IF still used) - Verify this URL!
        self.rest_base_url = "https://api.helius.xyz"

        self.session = session
        # Headers for RPC are standard JSON
        self.rpc_headers = {"Content-Type": "application/json"}
        # Headers for REST V0 used Bearer token (check if API key is used differently)
        # If needed for REST: self.rest_headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        logger.info("HeliusAPI Initialized.")

    # --- RPC Request Helper ---
    async def _rpc_request(self, method: str, params: Optional[Union[Dict, List]] = None) -> Dict[str, Any]:
        """Helper for JSON-RPC requests to the Helius RPC endpoint."""
        payload = { "jsonrpc": "2.0", "id": "athena-helius-rpc", "method": method }
        if params is not None: payload["params"] = params

        logger.debug(f"Helius RPC Request: Method={method}, Params={params}")
        try:
            async with self.session.post(self.rpc_url, headers=self.rpc_headers, json=payload) as response:
                response_data = await response.json() # Assume JSON even on error
                status_code = response.status

                if status_code != 200 or "error" in response_data:
                    error_info = response_data.get("error", {})
                    error_code = error_info.get("code", status_code)
                    error_message = error_info.get("message", f"HTTP Error {status_code}")
                    logger.error(f"Helius RPC error [{error_code}] for method '{method}': {error_message}. Response: {response_data}")
                    raise HeliusAPIError(f"RPC Error {error_code}: {error_message}", status_code=status_code, response_data=response_data)

                if "result" not in response_data:
                    logger.error(f"Helius RPC response missing 'result' key for method '{method}'. Response: {response_data}")
                    raise HeliusAPIError("RPC Response Invalid: Missing 'result'")

                logger.debug(f"Helius RPC Success for method '{method}'")
                return response_data["result"]

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.exception(f"Network/ClientError during Helius RPC '{method}': {e}", exc_info=True)
            raise HeliusAPIError(f"Network/Client error: {e}") from e
        except json.JSONDecodeError as e:
             logger.exception(f"JSON Decode Error during Helius RPC '{method}': {e}", exc_info=True)
             raise HeliusAPIError(f"Invalid JSON response: {e}") from e
        except Exception as e:
            # Catch potential attribute errors from bad responses etc.
            logger.exception(f"Unexpected error during Helius RPC '{method}': {e}", exc_info=True)
            raise HeliusAPIError(f"Unexpected RPC error: {e}") from e


    # --- REST Request Helper (Example for V0 Webhooks - Verify URL/Auth) ---
    # async def _rest_request(self, http_method: str, path: str, ...) -> ...
    # Uses self.rest_base_url and correct headers for that specific API (e.g., V0 Auth)
    # Implementation similar to SolscanAPI _request but uses REST URL and Helius V0 auth


    # --- Public API Methods ---

    async def get_token_metadata_das(self, mint_address: str) -> Optional[Dict[str, Any]]:
        """
        Fetches token metadata using the Helius DAS getAsset method via RPC Interface.
        CRITICAL: Relies on assumptions about DAS method name and response structure. VERIFY DOCS.
        """
        logger.info(f"Fetching Helius DAS metadata for {mint_address}")
        try:
            # DAS 'getAsset' via RPC typically requires 'id' param
            params = {"id": mint_address} # Assuming 'id' param - VERIFY WITH HELIUS DAS DOCS
            # TODO: Add optional params? e.g., {'displayOptions': {'showFungible': True}} if needed
            das_asset_data = await self._rpc_request("getAsset", params=params) # Method name is "getAsset"? VERIFY

            # --- TODO: PARSE ACTUAL getAsset RESPONSE ---
            # Response structure needs verification from Helius DAS docs. Below is EXAMPLE.
            if not isinstance(das_asset_data, dict):
                 logger.error(f"Helius DAS getAsset returned non-dict for {mint_address}: {das_asset_data}")
                 return None

            content = das_asset_data.get("content", {})
            metadata_section = content.get("metadata", {}) # Standard metaplex loc?
            token_info = das_asset_data.get("token_info", {}) # Location of decimals/supply for SPL via DAS? VERIFY!

            # Try extracting core fields - adapt keys based on verified DAS response
            metadata = {
                "name": metadata_section.get("name"),
                "symbol": metadata_section.get("symbol"),
                "decimals": token_info.get("decimals"),
                "total_supply": token_info.get("supply"),
                "uri": content.get("json_uri"), # URI to off-chain JSON
                # Add more fields if needed: description, image, attributes, standard, mutable etc.
            }

            # Basic validation - return None if core info missing
            if metadata["decimals"] is None or metadata["total_supply"] is None:
                logger.warning(f"Missing decimals or supply in Helius DAS response for {mint_address}. Decimals: {metadata['decimals']}, Supply: {metadata['total_supply']}")
                # Optionally return partial data or None? Let's return partial for now.
                # return None # Stricter - requires core info

            logger.info(f"Successfully parsed Helius DAS metadata for {mint_address}: {metadata.get('name')} ({metadata.get('symbol')})")
            return metadata

        except HeliusAPIError as e:
            # RPC request error already logged in helper
            logger.error(f"Helius API error fetching DAS metadata for {mint_address}: {e.args[0]}")
            return None
        except Exception as e:
            logger.exception(f"Unexpected error processing DAS metadata for {mint_address}", exc_info=True)
            return None


    async def get_token_holders_paginated(self,
                                        mint_address: str,
                                        limit_per_page: int = 1000, # Helius default/max? Check docs.
                                        max_pages: int = 100 # Safety limit: fetch max 100k holders initially
                                        ) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches token holder accounts using RPC getTokenAccounts with pagination.

        Returns: List of raw token account dicts (contains owner, amount), or None on failure.
        """
        all_token_accounts = []
        page = 1 # Use page number instead of cursor if RPC uses it? CHECK DOCS!
        # Helius enhanced RPC likely uses page/limit for tokenAccounts, NOT cursor. VERIFY.
        params: Dict[str, Any] = {
            "mint": mint_address,
            "limit": limit_per_page,
            # Check Helius specific options - maybe just use `page`?
            "options": {"showZeroBalance": False}, # Keep ignoring zero balances
            "page": page
        }

        logger.info(f"Fetching Helius holders for {mint_address} (page {page})")

        try:
            while page <= max_pages:
                params["page"] = page # Update page number
                result = await self._rpc_request("getTokenAccounts", params=params) # Use correct method name? Verify Helius specific.

                # Parse result - Helius enhanced RPC might have different structure. VERIFY!
                # Standard RPC: result={'token_accounts': [...], 'cursor': ... OR total/page info }
                # Example Check for Helius Structure:
                if not result or not isinstance(result.get("token_accounts"), list):
                     logger.warning(f"No 'token_accounts' list found in Helius getTokenAccounts page {page} for {mint_address}. Result: {result}")
                     break # Stop pagination

                fetched_accounts = result["token_accounts"]
                if not fetched_accounts:
                    logger.info(f"Reached end of holders for {mint_address} on page {page}.")
                    break # No more accounts on this page

                all_token_accounts.extend(fetched_accounts)
                logger.info(f"Fetched {len(fetched_accounts)} more holder accounts for {mint_address} (page {page}, total {len(all_token_accounts)})")

                # Check for pagination - Does Helius return a cursor or just rely on page increment?
                # If no cursor/next page indicator, we have to stop or guess max pages.
                # Assume for now we just increment page number up to max_pages
                page += 1
                # Optional small delay if hitting rate limits
                # await asyncio.sleep(0.1)

            if page > max_pages:
                logger.warning(f"Reached max_pages ({max_pages}) limit fetching Helius holders for {mint_address}. Results might be incomplete.")

            return all_token_accounts

        except HeliusAPIError as e:
             logger.error(f"Helius API error during get_token_holders_paginated for {mint_address} on page {page}: {e}")
             return None # Return None to indicate overall fetch failure
        except Exception as e:
            logger.exception(f"Unexpected error in get_token_holders_paginated {mint_address}", exc_info=True)
            return None


    async def get_signatures_for_address(self,
                                          account_address: str,
                                          limit: int = 100,
                                          before: Optional[str] = None,
                                          until: Optional[str] = None
                                          ) -> Optional[List[Dict[str, Any]]]:
        """Fetches transaction signatures involving an address using RPC."""
        # Correct parameters based on standard RPC documentation
        params = [ account_address ]
        options = { "limit": limit }
        if before: options["before"] = before
        if until: options["until"] = until
        params.append(options)

        try:
             result = await self._rpc_request("getSignaturesForAddress", params=params)
             # Result is directly the list of signature info objects
             if isinstance(result, list):
                  logger.debug(f"Fetched {len(result)} signatures for {account_address} (until={until})")
                  return result
             else:
                  logger.error(f"getSignaturesForAddress returned non-list for {account_address}: {result}")
                  return None
        except HeliusAPIError as e:
             logger.error(f"Helius API error getting signatures for address {account_address}: {e}")
             return None
        except Exception as e:
             logger.exception(f"Unexpected error in get_signatures_for_address {account_address}", exc_info=True)
             return None


    async def get_transaction_details(self, signature: str, max_supported_version: int = 0) -> Optional[Dict[str, Any]]:
        """Fetches full details for a transaction using RPC."""
        logger.debug(f"Fetching transaction details for signature: {signature}")
        try:
             params = [
                 signature,
                 {
                     "encoding": "jsonParsed", # Essential for easier parsing
                     "commitment": "confirmed",
                     "maxSupportedTransactionVersion": max_supported_version
                 }
             ]
             result = await self._rpc_request("getTransaction", params=params)
             # getTransaction can return None if not found/confirmed
             if result is None:
                  logger.warning(f"getTransaction returned null for {signature}")
             return result
        except HeliusAPIError as e:
             logger.error(f"Helius API error getting tx details for {signature}: {e}")
             return None
        except Exception as e:
             logger.exception(f"Unexpected error fetching tx details for {signature}", exc_info=True)
             return None


    # --- Optional Combined Holder Fetch (keep or remove based on preference) ---
    async def get_top_holders(self, mint_address: str, top_n: int = 10) -> Optional[List[Dict[str, Any]]]:
         """Combines metadata and holder fetching for processed top N holders."""
         # ... (Implementation from Response #46, using self.get_token_metadata_das and self.get_token_holders_paginated) ...
         pass # Ensure methods called are updated per above


    # --- Deprecated/Replaced methods from original snippet - REMOVE ---
    # async def get_token_metadata(...) -> Removed, use get_token_metadata_das
    # async def get_token_balances(...) -> Removed, use get_token_holders_paginated
    # async def get_token_events(...) -> Removed, better handled via WSS or getSignatures+getTransaction
    # async def create_webhook(...) -> Removed (using WSS architecture) or use _rest_request based on V0 docs if kept
    # async def delete_webhook(...) -> Removed or use _rest_request