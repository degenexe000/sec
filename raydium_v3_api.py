import logging
import aiohttp
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class RaydiumAPIError(Exception):
    """Custom exception for Raydium V3 API errors."""
    pass

class RaydiumV3API:
    """Client for Raydium V3 REST API interactions."""

    def __init__(self, session: aiohttp.ClientSession):
        """
        Args:
            session: An existing aiohttp ClientSession for HTTP requests.
        """
        self.session = session
        self.base_url = "https://api.raydium.io"

    async def get_prices(self, mint_addresses: List[str]) -> Optional[Dict[str, float]]:
        """
        Fetch USD prices for given list of mint addresses using Raydium V3 `/main/mint/price` endpoint.

        Args:
            mint_addresses: List of token mint addresses.

        Returns:
            Mapping of mint address to price, or None on error.
        """
        if not mint_addresses:
            return {}
        url = f"{self.base_url}/main/mint/price"
        params = {"mints": ",".join(mint_addresses)}
        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"Raydium V3 API get_prices failed [{resp.status}]: {text}")
                    raise RaydiumAPIError(f"HTTP {resp.status}")
                data = await resp.json()
                # Data expected as { mint_address: price }
                prices: Dict[str, float] = {}
                for mint in mint_addresses:
                    price = data.get(mint)
                    if price is not None:
                        try:
                            prices[mint] = float(price)
                        except (TypeError, ValueError):
                            logger.warning(f"Invalid price format for {mint}: {price}")
                return prices
        except Exception as e:
            logger.exception(f"Error fetching prices from Raydium V3 API: {e}")
            return None
