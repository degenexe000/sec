import logging
import asyncio
import json
from typing import Any, Dict, List, Optional, Union

import aiohttp

# Use the central settings instance for API Key
from config.settings import settings

logger = logging.getLogger(__name__)

# Constants for Retries
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY_SECONDS = 1.5
DEFAULT_RETRY_BACKOFF_FACTOR = 1.5
DEFAULT_REQUEST_TIMEOUT_SECONDS = 15 # Default timeout for API calls

# Status codes typically safe to retry on
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# Custom Exception
class SolscanAPIError(Exception):
    """Custom exception for Solscan Pro API v2 errors."""
    def __init__(self, message, status_code: Optional[int] = None, response_data: Optional[Any] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data
    def __str__(self):
        return f"{super().__str__()} (Status: {self.status_code})"


class SolscanAPI:
    """
    Optimized and Robust client for the Solscan Pro V2 API.
    Uses a shared aiohttp session, correct 'token' auth header, timeouts, and basic retries.
    """

    def __init__(self, session: aiohttp.ClientSession):
        self.base_url = "https://pro-api.solscan.io/v2.0"
        if not settings.solscan_api_key:
             raise ValueError("Solscan API key not configured (SOLSCAN_API_KEY).")
        self.headers = {
            "accept": "application/json",
            "token": settings.solscan_api_key
        }
        self.session = session
        logger.info("SolscanAPI (V2) Initialized with token auth.")

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        retries: int = DEFAULT_RETRY_ATTEMPTS,
        timeout: int = DEFAULT_REQUEST_TIMEOUT_SECONDS
    ) -> Optional[Any]:
        """
        Helper function for making requests to the Solscan API with retries and timeouts.
        """
        url = f"{self.base_url}{endpoint}"
        request_kwargs = {
            "headers": self.headers,
            "params": params,
            "json": json_data,
            "timeout": aiohttp.ClientTimeout(total=timeout) # Add timeout
        }
        request_kwargs = {k: v for k, v in request_kwargs.items() if v is not None}

        last_exception = None
        current_delay = DEFAULT_RETRY_DELAY_SECONDS

        for attempt in range(retries + 1):
            log_prefix = f"Solscan Req Attempt {attempt+1}/{retries+1}: {method} {url}"
            logger.debug(f"{log_prefix} Params: {params} Body: {json_data}")
            try:
                async with self.session.request(method, url, **request_kwargs) as response:
                    status_code = response.status
                    response_body_text = await response.text() # Read body once

                    if status_code == 200:
                        if 'application/json' in response.content_type.lower():
                             try:
                                 data = json.loads(response_body_text)
                                 logger.debug(f"Solscan Response OK for {endpoint}. Content sample: {response_body_text[:150]}...")
                                 return data # Success!
                             except json.JSONDecodeError as json_err:
                                  logger.error(f"{log_prefix} - Invalid JSON response despite 200 OK. Error: {json_err}. Content: {response_body_text[:300]}...")
                                  last_exception = SolscanAPIError(f"Invalid JSON (Status 200): {json_err}", status_code=status_code)
                                  # Don't retry invalid JSON from 200 OK immediately
                                  break # Exit retry loop
                        else:
                            logger.info(f"{log_prefix} - Non-JSON success response ({response.content_type}). Body: {response_body_text[:200]}")
                            return {"success": True} # Indicate non-JSON success

                    else: # Handle non-200 status codes
                        # Try to parse error message
                        error_detail = f"Status {status_code}"
                        try:
                             error_data = json.loads(response_body_text)
                             error_detail = error_data.get("errorMessage") or error_data.get("error_message") or error_data.get("message") or str(error_data)
                        except json.JSONDecodeError:
                             error_detail = response_body_text # Fallback to raw text

                        logger.warning(f"{log_prefix} - Failed with status {status_code}: {error_detail[:200]}...")
                        last_exception = SolscanAPIError(f"API Error: {error_detail}", status_code=status_code, response_data=error_data if 'error_data' in locals() else response_body_text)

                        # Decide if we should retry based on status code
                        if status_code in RETRY_STATUS_CODES and attempt < retries:
                             logger.info(f"Retrying request in {current_delay:.1f}s...")
                             await asyncio.sleep(current_delay)
                             current_delay *= DEFAULT_RETRY_BACKOFF_FACTOR # Exponential backoff
                             continue # Go to next attempt
                        else:
                             logger.error(f"Solscan API request failed permanently [{status_code}] for {endpoint}: {error_detail}")
                             break # Exit retry loop for non-retryable status or max retries


            except (aiohttp.ClientError, asyncio.TimeoutError) as e: # Catch network/timeout errors
                logger.warning(f"{log_prefix} - Network/Timeout error: {e}")
                last_exception = SolscanAPIError(f"Network/Timeout error: {e}")
                if attempt < retries:
                     logger.info(f"Retrying request in {current_delay:.1f}s...")
                     await asyncio.sleep(current_delay)
                     current_delay *= DEFAULT_RETRY_BACKOFF_FACTOR
                     continue # Go to next attempt
                else:
                    logger.error(f"Solscan request failed permanently after {retries+1} attempts due to network/timeout errors: {e}")
                    break # Exit retry loop

            except Exception as e: # Catch any other unexpected errors
                 logger.exception(f"{log_prefix} - Unexpected error: {e}", exc_info=True)
                 last_exception = SolscanAPIError(f"Unexpected error: {e}")
                 break # Exit loop on unexpected internal error

        # If loop finishes without returning success, raise the last known exception
        if last_exception:
            raise last_exception
        else:
            # Should not be reached if loop exits normally without success/exception
            logger.error(f"Solscan _request exited loop unexpectedly for {endpoint}")
            raise SolscanAPIError("Unknown error in request helper")

    # --- Endpoint Methods ---

    async def get_token_meta(self, mint_address: str) -> Optional[Dict[str, Any]]:
        """Gets metadata for a single token using /v2.0/token/meta."""
        endpoint = "/token/meta"
        params = {"address": mint_address} # <-- VERIFY Parameter Name ('address' or 'token_address')
        logger.info(f"Fetching Solscan metadata for {mint_address}")
        try:
            response = await self._request("GET", endpoint, params=params)
            # Add basic type check for safety before returning
            return response if isinstance(response, dict) else None
        except SolscanAPIError as e:
             logger.error(f"Failed Solscan get_token_meta API call for {mint_address}: {e}")
             return None

    async def get_token_price(self, mint_address: str) -> Optional[float]:
         """Gets the aggregated price for a token using /v2.0/token/price."""
         endpoint = "/token/price"
         params = {"address": mint_address} # <-- VERIFY Parameter Name
         logger.info(f"Fetching Solscan price for {mint_address}")
         try:
             result = await self._request("GET", endpoint, params=params)
             # --- Needs verification based on ACTUAL Solscan price response ---
             price = None
             if isinstance(result, dict):
                  if "priceUsdt" in result: price = result["priceUsdt"]
                  elif mint_address in result: price = result[mint_address]
                  elif isinstance(result.get("data"), dict) and "priceUsdt" in result["data"]: price = result["data"]["priceUsdt"]

             if price is not None:
                  try: return float(price)
                  except (ValueError, TypeError): logger.error(f"Could not convert Solscan price '{price}' for {mint_address}")
             else:
                  logger.warning(f"Price key/format not found in Solscan price response for {mint_address}: {str(result)[:200]}")
             return None
         except SolscanAPIError as e:
              logger.error(f"Failed Solscan get_token_price API call for {mint_address}: {e}")
              return None

    async def get_token_holders(self, mint_address: str, page: int = 1, page_size: int = 50) -> Optional[Dict[str, Any]]:
         """Gets token holders using /v2.0/token/holders. Returns full response dict."""
         endpoint = "/token/holders"
         params = {"address": mint_address, "page": page, "limit": page_size } # <-- VERIFY Param Names ('limit' vs 'page_size'?)
         logger.info(f"Fetching Solscan holders for {mint_address}, page {page}")
         try:
              # Response likely contains { 'total': N, 'holders': [...] } or similar - VERIFY
              response = await self._request("GET", endpoint, params=params)
              # Basic validation
              if isinstance(response, dict) and 'holders' in response:
                   return response
              logger.warning(f"Unexpected holder response format from Solscan for {mint_address}: {str(response)[:200]}")
              return None
         except SolscanAPIError as e:
              logger.error(f"Failed Solscan get_token_holders API call for {mint_address}: {e}")
              return None

    async def get_account_transactions(self, wallet_address: str, limit: int = 50, before: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
         """Gets tx signatures for a wallet using /v2.0/account/transactions."""
         endpoint = "/account/transactions"
         params = {"account": wallet_address, "limit": limit} # <-- VERIFY 'account' param name
         if before: params["before"] = before # <-- VERIFY 'before' param name
         logger.info(f"Fetching Solscan account txns for {wallet_address}")
         try:
              # Expects list or dict containing list - VERIFY!
              result = await self._request("GET", endpoint, params=params)
              if isinstance(result, list): return result
              elif isinstance(result, dict) and isinstance(result.get('data'), list): return result['data'] # Example wrapping
              logger.warning(f"Unexpected tx list format from Solscan account txns for {wallet_address}: {type(result)}")
              return None
         except SolscanAPIError as e:
              logger.error(f"Failed Solscan get_account_transactions API call for {wallet_address}: {e}")
              return None

    async def get_transaction_detail(self, signature: str) -> Optional[Dict[str, Any]]:
         """Gets detailed info for a tx using /v2.0/transaction/detail."""
         endpoint = "/transaction/detail"
         params = {"tx": signature} # <-- VERIFY 'tx' param name
         logger.info(f"Fetching Solscan tx detail for {signature[:10]}...")
         try:
              response = await self._request("GET", endpoint, params=params)
              return response if isinstance(response, dict) else None
         except SolscanAPIError as e:
              logger.error(f"Failed Solscan get_transaction_detail API call for {signature[:10]}...: {e}")
              return None

    async def get_transaction_actions(self, signature: str) -> Optional[List[Dict[str, Any]]]:
         """Gets parsed actions using /v2.0/transaction/actions."""
         endpoint = "/transaction/actions"
         params = {"tx": signature} # <-- VERIFY 'tx' param name
         logger.info(f"Fetching Solscan transaction actions for {signature[:10]}...")
         try:
             result = await self._request("GET", endpoint, params=params)
             # Assume list or dict with list - VERIFY!
             actions = None
             if isinstance(result, list): actions = result
             elif isinstance(result, dict) and isinstance(result.get('data'), list): actions = result['data']

             if actions is not None: logger.info(f"Received {len(actions)} actions from Solscan for {signature[:10]}")
             else: logger.warning(f"Unexpected structure from Solscan /transaction/actions for {signature[:10]}: {str(result)[:200]}")
             return actions # Return list or None
         except SolscanAPIError as e:
              logger.error(f"Failed Solscan get_transaction_actions API call for {signature[:10]}...: {e}")
              return None