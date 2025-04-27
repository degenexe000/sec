import logging
import asyncio
from typing import Dict, Any, List, Optional

# Import the actual API client classes for type hinting
from data.helius_api import HeliusAPI, HeliusAPIError
from data.solscan_api import SolscanAPI, SolscanAPIError
from data.dune_api import DuneAPI # Keep if Dune logic belongs here (unlikely)
# Import models if needed for type hinting results
from data.models import Holder # Example

logger = logging.getLogger(__name__)

class APIManager:
    """
    Refactored API Manager - Aggregates calls to specialized API clients.
    Receives already initialized API client instances.

    *** ARCHITECTURAL NOTE ***
    In the current advanced bot design using direct dependency injection into
    services like TelegramBotService and WalletAnalyzer, this APIManager class
    is likely REDUNDANT and can probably be removed entirely to simplify the architecture.
    Its methods often duplicate logic better handled by the specific service using the data.
    """

    def __init__(self,
                 helius_api: HeliusAPI,
                 solscan_api: SolscanAPI,
                 dune_api: DuneAPI,
                 # Add other initialized clients if absolutely needed by this manager:
                 # raydium_api: RaydiumV3API,
                 ):
        """
        Initialize API Manager with EXISTING, INITIALIZED client instances.

        Args:
            helius_api: Initialized HeliusAPI client.
            solscan_api: Initialized SolscanAPI client.
            dune_api: Initialized DuneAPI client.
        """
        logger.warning("APIManager instantiated. Note: This class is likely redundant and can be removed.")
        self.helius_api = helius_api
        self.solscan_api = solscan_api
        self.dune_api = dune_api
        # self.raydium_api = raydium_api # If passed in

    async def get_snapshot_components(self, token_address: str) -> Dict[str, Any]:
        """
        Example method to fetch discrete data parts needed for a snapshot.
        NOTE: This exact logic is better placed directly within
              TelegramBotService.get_token_snapshot for cleaner architecture.

        Args:
            token_address: The token mint address.

        Returns:
            A dictionary containing fetched components ('metadata', 'price', 'holders',
            'market_data', 'error') or None for components that failed. Includes an
            overall 'error' key if a major issue occurred.
        """
        results: Dict[str, Any] = {
            'metadata': None,
            'price': None,
            'holders': None,
            'market_data': None, # Placeholder for Volume/Liquidity
            'error': None # Overall error message
        }
        logger.debug(f"APIManager: Gathering snapshot components for {token_address}")

        try:
             # --- Define Tasks ---
             # Use specific clients injected during __init__
             # Prefer Solscan for meta/price based on previous decision
             meta_task = asyncio.create_task(self.solscan_api.get_token_meta(token_address))
             price_task = asyncio.create_task(self.solscan_api.get_token_price(token_address))
             # Use Helius for most reliable on-chain holders
             holders_task = asyncio.create_task(self.helius_api.get_top_holders(token_address, top_n=10))
             # TODO: Add task for Volume/Liquidity (e.g., Solscan /token/markets or Birdeye)
             # market_task = asyncio.create_task(self.solscan_api.get_token_markets(token_address))

             # --- Await Tasks ---
             # Wait for all primary tasks, allowing individual failures
             gathered_results = await asyncio.gather(
                  meta_task,
                  price_task,
                  holders_task,
                  # market_task, # Add when implemented
                  return_exceptions=True # Capture exceptions instead of crashing gather
             )

             # Unpack results carefully, checking for exceptions
             metadata, price, holders = gathered_results[0:3]
             # market_data = gathered_results[3] # If implemented

             # --- Process Results ---
             if isinstance(metadata, Exception) or metadata is None:
                  logger.warning(f"APIManager snapshot: Failed to get metadata for {token_address}: {metadata}")
                  # Keep results['metadata'] as None
             elif isinstance(metadata, dict):
                   results['metadata'] = metadata
             else:
                  logger.warning(f"APIManager snapshot: Unexpected metadata type for {token_address}: {type(metadata)}")


             if isinstance(price, Exception) or price is None:
                   logger.warning(f"APIManager snapshot: Failed to get price for {token_address}: {price}")
                   # Keep results['price'] as None
             elif isinstance(price, (float, int)):
                   results['price'] = float(price)
             else:
                   logger.warning(f"APIManager snapshot: Unexpected price type for {token_address}: {type(price)}")

             if isinstance(holders, Exception) or holders is None:
                   logger.warning(f"APIManager snapshot: Failed to get holders for {token_address}: {holders}")
                   # Keep results['holders'] as None
             elif isinstance(holders, list):
                   results['holders'] = holders
             else:
                    logger.warning(f"APIManager snapshot: Unexpected holders type for {token_address}: {type(holders)}")


             # TODO: Process market_data result when implemented

        except Exception as e:
             # Catch any unexpected error during the gather/unpacking process
             logger.exception(f"APIManager: Critical error gathering snapshot components for {token_address}", exc_info=True)
             results['error'] = f"Failed to gather components: {str(e)[:100]}"

        logger.debug(f"APIManager: Finished gathering components for {token_address}. Price found: {results['price'] is not None}")
        return results

    # Remove other methods that relied on old structures (e.g., Moralis) or
    # that duplicate logic now better handled in other services.
    # Only keep methods here if APIManager performs a *unique*, complex aggregation
    # or orchestration role not suitable for BotService or WalletAnalyzer.