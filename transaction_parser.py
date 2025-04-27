import logging
from typing import Dict, Optional, Any, List, Set, Tuple
from datetime import datetime, timezone

# Consider importing decimal for precise balance math if needed later
# from decimal import Decimal

logger = logging.getLogger(__name__)

# Define Program IDs constants (can be moved to settings or a dedicated config)
# --- TODO: Verify Program IDs for relevant DEXes/Protocols ---
PROGRAM_ID_RAYDIUM_V4 = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
PROGRAM_ID_ORCA_WHIRLPOOL = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
PROGRAM_ID_SYSTEM = "11111111111111111111111111111111"
PROGRAM_ID_TOKEN = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
# Add others as needed...

class TransactionParsingError(Exception):
    """Custom exception for errors during transaction parsing."""
    pass

class TransactionParser:
    """
    Parses detailed Solana transaction data (from Helius/Solscan getTransaction)
    or potentially structured actions (from Solscan /transaction/actions)
    to extract meaningful events like swaps and transfers for analysis.

    Relies HEAVILY on parsing based on known program instruction layouts OR
    accurately calculating diffs from pre/post token balances.
    """

    def __init__(self):
        # Can add caches or program-specific decoders here if needed later
        logger.info("TransactionParser initialized.")

    def _resolve_account_key(self, index: int, account_keys: List[str]) -> Optional[str]:
        """Safely get account public key from index."""
        if 0 <= index < len(account_keys):
            return account_keys[index]
        logger.warning(f"Account index {index} out of bounds (num keys: {len(account_keys)})")
        return None

    def _calculate_balance_changes(self, meta: Dict[str, Any], account_keys: List[str]) -> List[Dict[str, Any]]:
        """
        Calculates token balance changes using preTokenBalances and postTokenBalances.
        This is often the most reliable general method if detailed instruction parsing fails.

        Returns: List of {'wallet': str, 'mint': str, 'raw_change': int}
        """
        changes: Dict[Tuple[str, str], int] = {} # Key: (owner, mint), Value: raw_change
        pre_balances = meta.get("preTokenBalances") or []
        post_balances = meta.get("postTokenBalances") or []

        # Map account index to owner for easier lookup if structure demands it
        # Sometimes owner is directly in pre/post, sometimes only accountIndex
        # Let's assume structure like Solscan/Helius might provide (VERIFY):
        # bal = {"accountIndex": X, "mint": "...", "owner": "...", "uiTokenAmount": {"amount": "...", ...}}

        balances_by_idx_mint = {}
        for bal_list in [pre_balances, post_balances]:
             for bal in bal_list:
                 idx = bal.get("accountIndex")
                 mint = bal.get("mint")
                 owner = bal.get("owner")
                 raw_amount = int(bal.get("uiTokenAmount", {}).get("amount", 0))
                 if idx is not None and mint is not None and owner is not None:
                     # If owner not present, might need to find owner of accountIndex via main account keys if it's a token account? complex.
                      key = (owner, mint)
                      balances_by_idx_mint[(idx, mint)] = (key, raw_amount) # Store mapping and amount

        # Calculate delta from post balances
        for bal in post_balances:
             idx = bal.get("accountIndex")
             mint = bal.get("mint")
             if idx is None or mint is None: continue
             key_amount = balances_by_idx_mint.get((idx, mint))
             if not key_amount: continue
             key, post_amount = key_amount

             pre_key_amount = balances_by_idx_mint.get((idx, mint)) # Fetch again for pre-amount context
             pre_amount = pre_key_amount[1] if pre_key_amount and (idx, mint) in pre_balances else 0 # Find corresponding pre_balance amount

             # Need refinement - this mapping logic is complex and depends heavily
             # on actual balance report structures and if they always pair pre/post
             # A simpler approach: Diff balances directly using owner+mint as key
             # Rebuilding simple approach:
             pre_map = {(b.get('owner'), b.get('mint')): int(b.get('uiTokenAmount',{}).get('amount',0)) for b in pre_balances if b and b.get('owner') and b.get('mint')}
             post_map = {(b.get('owner'), b.get('mint')): int(b.get('uiTokenAmount',{}).get('amount',0)) for b in post_balances if b and b.get('owner') and b.get('mint')}

             all_keys = set(pre_map.keys()) | set(post_map.keys())

             for owner, mint in all_keys:
                  pre_amt = pre_map.get((owner, mint), 0)
                  post_amt = post_map.get((owner, mint), 0)
                  delta = post_amt - pre_amt
                  if delta != 0:
                      changes[(owner, mint)] = delta # Will sum if somehow owner/mint appears multiple times

        # Convert to desired output format
        token_changes = [
            {"wallet": owner, "mint": mint, "raw_change": change}
            for (owner, mint), change in changes.items()
        ]
        logger.debug(f"Calculated {len(token_changes)} balance changes from pre/post.")
        return token_changes


    # --- PRIMARY PARSING METHOD using Full Transaction Detail ---
    def parse_transaction_for_event(self, tx_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Top-level parsing function using Helius/Solscan getTransaction response.
        Attempts to identify swaps or significant transfers. Relies heavily on balance diffs.

        Returns: Structured event dict or None.
        """
        signature = "(unknown)" # Extract early for logging
        try:
            # Basic validation of input
            if not tx_data or not isinstance(tx_data, dict): raise TransactionParsingError("Input tx_data is not a valid dictionary.")
            if tx_data.get("meta", {}).get("err"):
                # logger.debug(f"Skipping failed transaction: {tx_data.get('transaction',{}).get('signatures',[None])[0]}")
                return None # Ignore failed transactions

            signature = tx_data.get("transaction", {}).get("signatures", [None])[0]
            block_time_unix = tx_data.get("blockTime")
            if not signature or block_time_unix is None: # blockTime can be 0 sometimes, check None explicitly
                 raise TransactionParsingError("Missing signature or blockTime.")

            timestamp = datetime.fromtimestamp(block_time_unix, tz=timezone.utc)
            meta = tx_data.get("meta", {})
            # account_keys = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])

            event = {
                "type": "UNKNOWN", # Default type
                "timestamp": timestamp,
                "signature": signature,
                "involved_wallets": [], # Use list for deterministic output
                "token_changes": [], # List of {'wallet': str, 'mint': str, 'raw_change': int}
                # Add more event-specific fields later: e.g., dex_program, token_in/out for SWAP
                "error": None
            }

            # --- Core Logic: Use balance changes as primary source ---
            account_keys_list = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
            token_changes = self._calculate_balance_changes(meta, account_keys_list)
            event["token_changes"] = token_changes
            involved_wallets = list({change['wallet'] for change in token_changes})
            event["involved_wallets"] = involved_wallets

            # --- TODO: Enhance with Instruction/Log Parsing for Context ---
            # If balance changes detected, try to parse instructions/logs to determine *why*
            # - Was it a known DEX swap? -> Set event['type'] = "SWAP", add details
            # - Was it a simple SPL Transfer? -> Set event['type'] = "TRANSFER"
            # - Was it Mint/Burn?
            # This requires detailed implementation for each program ID in PROGRAM_IDS_TO_PARSE
            # Example Placeholder Logic:
            # program_interactions = self._find_program_interactions(tx_data) # Placeholder
            # if PROGRAM_ID_RAYDIUM_V4 in program_interactions: event['type'] = "SWAP"
            # elif PROGRAM_ID_TOKEN in program_interactions: # Simple transfer?
            #     if len(token_changes) == 2 and abs(token_changes[0]['raw_change']) == abs(token_changes[1]['raw_change']):
            #          event['type'] = "TRANSFER"


            # If nothing else identified, but changes occurred, label as general "ACTIVITY" or keep "UNKNOWN"
            if event["type"] == "UNKNOWN" and event["token_changes"]:
                event["type"] = "TOKEN_ACTIVITY" # General term if parsing incomplete


            if not event["token_changes"] and not event["involved_wallets"]:
                 # Transaction had no apparent SPL token balance impact (e.g., only SOL transfer, program interaction)
                 # logger.debug(f"No relevant token activity found parsing tx {signature}")
                 return None # Ignore if no relevant changes detected by balance diff

            logger.debug(f"Parsed tx {signature}: Type={event['type']}, Wallets={len(event['involved_wallets'])}, Changes={len(event['token_changes'])}")
            return event

        except Exception as e:
             logger.exception(f"CRITICAL Error parsing transaction {signature}: {e}", exc_info=True)
             # Return an event with error marked, or just None? Let's return error dict
             return { "signature": signature, "error": f"Parsing failed: {str(e)[:100]}" }


    # --- Parsing based on Solscan /transaction/actions (if verified available & useful) ---
    def parse_solscan_actions(self,
                              actions: Optional[List[Dict[str, Any]]],
                              signature: str # Needed for output context
                              ) -> Optional[Dict[str, Any]]:
        """
        Parses the structured 'actions' array possibly returned by Solscan
        to generate the standardized event dictionary.

        CRITICAL: Needs implementation based on ACTUAL Solscan action payload structure.
        """
        logger.debug(f"Attempting to parse Solscan actions for {signature}")
        if not actions or not isinstance(actions, list):
             logger.warning(f"No valid actions list provided by Solscan for {signature}. Falling back.")
             return None

        # TODO: Implement parsing logic based on Solscan's specific action format
        # Iterate `actions`:
        # Look for actions where `action['type']` is 'swap', 'transfer', 'mintTo', 'burn', etc.
        # Extract timestamp (if present), wallets (buyer/seller/from/to), mints, amounts
        # Calculate raw_change if necessary
        # Aggregate into the standard event dictionary format used by `parse_transaction_for_event`

        logger.warning(f"Solscan action parsing (parse_solscan_actions) is NOT IMPLEMENTED for sig {signature}")
        # Return None to indicate this pathway isn't implemented yet.
        return None