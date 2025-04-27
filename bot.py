# bot/bot.py
import logging
import asyncio
import re
import random
from typing import Dict, Any, List, Optional

# Import necessary libraries (ensure these are installed)
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    ApplicationBuilder, # Use ApplicationBuilder for v20+
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest
from telegram.constants import ParseMode # Import ParseMode

# Import key components (ensure correct paths relative to project structure)
from config.settings import settings # Use the singleton settings instance
from data.db import Database
from data.helius_api import HeliusAPI
from data.solscan_api import SolscanAPI # Added for dependency injection typing hint
from data.wallet_analyzer import WalletAnalyzer
from data.notification_system import NotificationSystem
from data.free_access import FreeAccessManager
from bot.service import TelegramBotService
from utils.formatting import shorten_address # Keep this util

logger = logging.getLogger(__name__)

# --- Define Callback data prefixes ---
# Keep only relevant prefixes for the current UI flow
CB_TRACK_TIS = "tis_sum:" # Callback for showing TIS Summary View
CB_VIEW_TEAM = "v_team:"  # Callback for showing Team Details
CB_VIEW_INSIDER = "v_ins:" # Callback for showing Insider Details
CB_VIEW_SNIPER = "v_snip:" # Callback for showing Sniper Details
CB_WALLET_DETAILS = "w_det:" # Callback for showing Single Wallet Details
CB_STOP_TRACK = "stop_trk:" # Callback for Stop Tracking button
CB_REFRESH = "refresh:"     # Callback for Refresh button (goes back to snapshot)

# Regex to detect if a message IS a Solana address (adjust if needed)
# Use ^ and $ for full match, remove for partial match within text
SOLANA_ADDRESS_REGEX = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

# --- Randomized Welcome Messages (Curated list) ---
WELCOME_MESSAGES = [
    # Examples - use list from #65 or #67 or customize here
    ("Heyyy 😉 Ready for some Solana data plays? Athena's here. **Paste that token address below 👇** Let's see what's cooking!",
     "_(Pssst... `/help` still works if you're feeling traditional.)_"),
    ("GM! ☀️ Ready to degen? **Drop the contract address here 👇**. I'll pull the stats. NFA!",
     "_(Boring commands list: `/help`)_"),
    ("Athena online! ✨ Skip `/track`. Real ones **paste the address right here 👇.** LFG!",
     "_(Need instructions? Try `/help`.)_"),
    ("WAGMI? Or NGMI? 🤔 Let's find out! **Paste that Solana contract address here.**",
     "_(Full command list via `/help`.)_"),
    ("Alright chart-watcher, let's get to it. **Paste that address 👇.** What's the target?",
     "_(`/help` for options.)_")
]

# --- Helper function to create the main keyboard ---
def create_main_keyboard(mint_address: str) -> InlineKeyboardMarkup:
    """Creates the simplified inline keyboard after tracking."""
    keyboard = [
        [InlineKeyboardButton("📊 TRACK T.I.S 📊", callback_data=f"{CB_TRACK_TIS}{mint_address}")],
        [
            InlineKeyboardButton("🛑 Stop Track", callback_data=f"{CB_STOP_TRACK}{mint_address}"),
            InlineKeyboardButton("🔄 Refresh", callback_data=f"{CB_REFRESH}{mint_address}")
        ],
    ]
    return InlineKeyboardMarkup(keyboard)

# --- Bot Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /start command with a random welcome message focused on pasting."""
    logger.info(f"FUNC_CALL: Entering start_command for chat_id {update.effective_chat.id}")
    try:
        if not WELCOME_MESSAGES:
             logger.error("WELCOME_MESSAGES list is empty!")
             await update.message.reply_text("Hello! Welcome to Athena Bot.") # Fallback
             return

        chosen_parts = random.choice(WELCOME_MESSAGES) # Now chooses a tuple if structure used
        chosen_message = "\n\n".join(chosen_parts) if isinstance(chosen_parts, tuple) else chosen_parts

        logger.debug(f"DEBUG: Chosen welcome message: {chosen_message[:80]}...")
        await update.message.reply_text(chosen_message, parse_mode=ParseMode.MARKDOWN_V2) # Use MARKDOWN_V2 for more robust parsing
        logger.info(f"SUCCESS: Sent welcome message to {update.effective_chat.id}")
    except Exception as e:
        logger.exception(f"ERROR: Exception inside start_command for chat {update.effective_chat.id}", exc_info=True)
        try: await update.message.reply_text("😬 Oops! Couldn't fetch my special greeting.")
        except Exception as send_e: logger.error(f"ERROR: Could not send error reply in start_command: {send_e}")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /help command - Updated for focused features."""
    help_text = (
        "🔍 *Athena Bot Commands* 🔍\n\n"
        "`/start` \- Get a fresh greeting\n"
        "`/help` \- Show this message\n"
        "`/track <address>` \- Track a new token (or just paste the address!)\n"
        "`/untrack <address>` \- Stop tracking a token\n"
        "`/list` \- See tokens you're currently tracking\n\n"
        "💡 **How to Use:**\n"
        "1️⃣ Paste a Solana token address directly into the chat\.\n"
        "2️⃣ I'll show a quick snapshot & track it\.\n"
        "3️⃣ Tap '📊 TRACK T\.I\.S 📊' to analyze Team, Insider, and Sniper wallets\.\n"
        "4️⃣ Explore the categories and specific wallets from there\."
        # escape special characters for MarkdownV2: ., -, _, *, [, ], (, ), ~, `, >, #, +, =, |, {, }, !
    )
    await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN_V2)


async def track_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /track command - Explicit tracking trigger."""
    if not context.args or len(context.args) != 1:
        # Provide example in backticks for easy copy
        await update.message.reply_text(
            "❌ Needs one argument\!\nExample: `/track EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZkFGCpx`",
             parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    mint_address = context.args[0].strip() # Strip potential whitespace
    chat_id = update.effective_chat.id
    logger.info(f"TRACK_COMMAND: Received for {mint_address} from {chat_id}")

    # Access Bot Service
    bot_service: Optional[TelegramBotService] = context.bot_data.get("bot_service")
    if not bot_service: logger.error("CRITICAL: Bot service missing in track_command context"); return

    # 1. Validate Format Locally (Quick check)
    if not SOLANA_ADDRESS_REGEX.fullmatch(mint_address):
         await update.message.reply_text(
             f"❌ Address format looks wrong for:\n`{mint_address}`\nCheck for typos\!",
             parse_mode=ParseMode.MARKDOWN_V2
         )
         return

    # 2. (Optional but recommended) Deeper validation via Service (API call)
    is_valid = await bot_service.validate_token(mint_address) # Assumes this uses APIs now
    if not is_valid:
         await update.message.reply_text(
             f"❌ Invalid or Unknown Token:\n`{mint_address}`\nDouble\-check the address on Solscan \(Mainnet\)\.",
             parse_mode=ParseMode.MARKDOWN_V2
         )
         return

    # 3. Send Loading message (similar to auto-detect)
    try:
         loading_message = await update.message.reply_text(
            f"✅ Got it\! Tracking `{mint_address}`\.\.\.\nFetching data now...",
             parse_mode=ParseMode.MARKDOWN_V2
         )
    except Exception as e:
         logger.error(f"Failed to send loading message for track command: {e}")
         # If sending fails, just proceed maybe? Or abort? Abort is safer.
         return

    # 4. Subscribe User (handles DB, etc.)
    try:
         subscribe_result = await bot_service.subscribe_user(chat_id, mint_address)
         status = subscribe_result.get("status") if isinstance(subscribe_result, dict) else None
    except Exception as e:
        logger.exception("Error during subscribe_user call in track_command", exc_info=True)
        status = "error" # Assume error if exception occurs
        error_msg = f"Internal error during subscription: {str(e)[:100]}"
    # Check result
    if status not in ["tracking", "already_tracking"]:
         error_msg = subscribe_result.get("message", "Subscription failed.") if isinstance(subscribe_result, dict) else "Subscription call failed."
         logger.error(f"Subscription failed via /track for {chat_id}/{mint_address}: {error_msg}")
         await context.bot.edit_message_text(
             chat_id=chat_id, message_id=loading_message.message_id,
             text=f"❌ Failed to track `{mint_address}`\.\n_Reason: {error_msg[:100]}_\nPlease try again later\.",
              parse_mode=ParseMode.MARKDOWN_V2
         )
         return

    # 5. Fetch and Display Snapshot (editing loading message)
    reply_markup = create_main_keyboard(mint_address)
    snapshot = await bot_service.get_token_snapshot(mint_address)

    if snapshot and not snapshot.get("error"):
        message_text = bot_service.format_snapshot_message(snapshot)
        await context.bot.edit_message_text(
             chat_id=chat_id, message_id=loading_message.message_id, text=message_text,
             reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True
        )
    else:
         error_detail = snapshot.get("error", "Could not retrieve snapshot.") if snapshot else "Snapshot data was empty."
         await context.bot.edit_message_text(
             chat_id=chat_id, message_id=loading_message.message_id,
             text=f"✅ Now Tracking: `{mint_address}`\n\n⚠️ Couldn't load initial stats \({error_detail[:100]}\.\.\.\)\. Use buttons below\.",
             reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2,
         )


# --- handle_potential_address (Auto-detect, KEEP from Response #87) ---
async def handle_potential_address(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
     # Use the enhanced version from Response #87 which includes good logging and error handling,
     # and uses create_main_keyboard()
     # ... (Code from Response #87 here) ...


# --- untrack_command & list_command (Mostly same, use backticks for mint) ---
async def untrack_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # ... (logic as before, but format messages with `mint_address`)
    if not context.args: #...
         return
    mint_address = context.args[0].strip()
    chat_id = update.effective_chat.id
    bot_service = context.bot_data.get("bot_service") # ...
    success = await bot_service.unsubscribe_user(chat_id, mint_address)
    if success:
        await update.message.reply_text(f"✅ Stopped tracking `{mint_address}`\.", parse_mode=ParseMode.MARKDOWN_V2)
    else:
         await update.message.reply_text(f"❌ Couldn't stop tracking `{mint_address}`\. Maybe not tracked\?", parse_mode=ParseMode.MARKDOWN_V2)

async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    bot_service = context.bot_data.get("bot_service") # ...
    subscriptions = await bot_service.get_user_subscriptions(chat_id) # Assuming this method exists
    if not subscriptions: #...
        return
    message = "🔍 *Your Tracked Tokens*\n\n"
    if isinstance(subscriptions, list):
        for i, sub in enumerate(subscriptions):
            mint_address = sub.get("mint_address")
            # Fetch name if possible, otherwise show unknown
            name = sub.get("tokens", {}).get("name") if sub.get("tokens") else "Unknown" # Based on sample data format
            if mint_address:
                message += f"{i+1}\. *{name}*\n   Mint: `{mint_address}`\n\n" # Use MarkdownV2
    else:
        message += "_Could not retrieve list\._"

    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN_V2)

# --- button_callback (Rewritten for TIS Drilldown) ---
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles inline keyboard button clicks for TIS drilldown, refresh, stop."""
    query = update.callback_query
    await query.answer() # Acknowledge button press

    callback_data = query.data
    logger.info(f"CALLBACK: Received '{callback_data}' from {query.from_user.id}")

    bot_service: Optional[TelegramBotService] = context.bot_data.get("bot_service")
    if not bot_service: logger.error("CRITICAL: Bot service missing in button_callback"); return

    # Extract prefix and value
    prefix, value = callback_data.split(":", 1) if ":" in callback_data else (None, None)
    if not prefix: logger.warning("Callback data missing separator ':'"); return

    try:
        # --- Refresh: Shows initial snapshot view ---
        if prefix == CB_REFRESH.rstrip(":"):
            mint_address = value
            await query.edit_message_text(f"🔄 Refreshing snapshot for `{mint_address}`\.\.\.", parse_mode=ParseMode.MARKDOWN_V2)
            snapshot = await bot_service.get_token_snapshot(mint_address)
            message = bot_service.format_snapshot_message(snapshot) # Uses improved formatter
            reply_markup = create_main_keyboard(mint_address)
            await query.edit_message_text(message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)

        # --- Stop Track ---
        elif prefix == CB_STOP_TRACK.rstrip(":"):
            mint_address = value
            success = await bot_service.unsubscribe_user(query.from_user.id, mint_address)
            if success:
                 await query.edit_message_text(f"🛑 Stopped tracking `{mint_address}`\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=None) # Remove buttons
            else:
                 await query.edit_message_text(f"❌ Couldn't stop tracking `{mint_address}`\. Maybe already stopped\?", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=query.message.reply_markup)

        # --- Show TIS Summary View ---
        elif prefix == CB_TRACK_TIS.rstrip(":"):
            mint_address = value
            await query.edit_message_text(f"🔬 Analyzing Team/Insider/Sniper Summary for `{mint_address}`\.\.\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=None)
            summary_data = await bot_service.get_tis_summary(mint_address) # New service method
            summary_message = bot_service.format_tis_summary_message(summary_data, mint_address) # New formatting method

            # Create keyboard for category drilldown
            tis_keyboard_rows = [
                 [ # One row for the category buttons
                      InlineKeyboardButton(f"👥 Team ({summary_data.get('team',{}).get('count', 0)})", callback_data=f"{CB_VIEW_TEAM}{mint_address}"),
                      InlineKeyboardButton(f"🕵️ Insiders ({summary_data.get('insider',{}).get('count', 0)})", callback_data=f"{CB_VIEW_INSIDER}{mint_address}"),
                      InlineKeyboardButton(f"🎯 Snipers ({summary_data.get('sniper',{}).get('count', 0)})", callback_data=f"{CB_VIEW_SNIPER}{mint_address}"),
                  ],
                  [InlineKeyboardButton("⬅️ Back to Snapshot", callback_data=f"{CB_REFRESH}{mint_address}")] # Back button uses Refresh action
             ]
            reply_markup_tis = InlineKeyboardMarkup(tis_keyboard_rows)
            await query.edit_message_text(summary_message, reply_markup=reply_markup_tis, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)


        # --- Show Detailed Category View (Team/Insider/Sniper) ---
        elif prefix in [CB_VIEW_TEAM.rstrip(":"), CB_VIEW_INSIDER.rstrip(":"), CB_VIEW_SNIPER.rstrip(":")]:
             mint_address = value
             category = prefix.split("_")[1] # Extract 'team', 'insider', or 'sniper'
             await query.edit_message_text(f"⏳ Loading {category.capitalize()} details for `{mint_address}`\.\.\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=None)

             detailed_data = await bot_service.get_detailed_category_view(mint_address, category) # New service method
             category_message = bot_service.format_detailed_category_message(detailed_data, mint_address) # New formatting method

             # Create keyboard with wallet buttons (URL buttons pointing to Solscan)
             wallet_keyboard_rows = []
             wallets_to_show = detailed_data.get('wallets', [])[:20] # Limit number shown
             for wallet_info in wallets_to_show:
                  address = wallet_info.get('address')
                  status = wallet_info.get('status', 'UNKNOWN')
                  if not address: continue

                  status_emoji = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}.get(status, "⚪️")
                  short_addr = shorten_address(address, 5) # Use util
                  solscan_url = f"https://solscan.io/account/{address}" # Direct link to Solscan

                  wallet_keyboard_rows.append(
                      [InlineKeyboardButton(f"{status_emoji} {short_addr}", url=solscan_url)] # Use URL button
                  )

             # Add Back button to TIS Summary
             CB_BACK_TO_TIS_SUM = f"{CB_TRACK_TIS}{mint_address}" # Use summary callback data
             wallet_keyboard_rows.append([InlineKeyboardButton("⬅️ Back to TIS Summary", callback_data=CB_BACK_TO_TIS_SUM)])

             reply_markup_wallets = InlineKeyboardMarkup(wallet_keyboard_rows)

             await query.edit_message_text(
                  category_message,
                  reply_markup=reply_markup_wallets,
                  parse_mode=ParseMode.MARKDOWN_V2,
                  disable_web_page_preview=True # Disable previews for cleaner look
             )

        # --- Wallet Details Button Clicked (Placeholder for potential future view) ---
        elif prefix == CB_WALLET_DETAILS.rstrip(":"):
              # Logic to show detailed wallet interactions - This wasn't implemented
              # in previous service layer example but structure is here.
              # Could show P/L, total buy/sell etc from get_and_format_wallet_token_details
              await query.answer("Individual wallet detail view not yet implemented.", show_alert=True)


        else:
            logger.warning(f"Received unknown callback prefix: {prefix}")
            await query.answer("Sorry, that button seems outdated.", show_alert=True)


    except Exception as e:
         logger.exception(f"Error in button_callback handling '{callback_data}'", exc_info=True)
         try: # Try to tell user something went wrong
             await query.edit_message_text("❌ Oops! An error occurred while processing that button. Please try 'Refresh'.")
         except Exception as report_e:
             logger.error(f"Failed to send error message in button_callback: {report_e}")

# --- start_bot Function ---
async def start_bot(
    settings: Settings,
    db: Database,
    notification_system: NotificationSystem,
    free_access_manager: FreeAccessManager,
    bot_service: TelegramBotService, # Pass the correctly initialized service
    # Add other initialized services IF needed DIRECTLY by bot logic (less common)
    helius_api: HeliusAPI,
    wallet_analyzer: WalletAnalyzer,
    solscan_api: SolscanAPI, # If needed directly (unlikely)
):
    """Initialize and run the Telegram bot application."""
    logger.info("Starting Telegram bot application setup")

    application = ApplicationBuilder() \
        .token(settings.telegram_bot_token) \
        .request(HTTPXRequest(connect_timeout=30, read_timeout=30)) \
        .concurrent_updates(True) \
        .build()

    # --- Store shared bot service in context ---
    application.bot_data["bot_service"] = bot_service

    # Inject bot instance into notification system
    if notification_system:
         notification_system.bot = application.bot
         logger.info("Injected bot instance into NotificationSystem.")

    # --- Register Handlers ---
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("track", track_command))
    application.add_handler(CommandHandler("untrack", untrack_command))
    application.add_handler(CommandHandler("list", list_command))
    application.add_handler(CallbackQueryHandler(button_callback)) # Single handler for all buttons
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_potential_address))

    # --- Start Application ---
    try:
        await application.initialize()
        await application.start()
        # Start polling for updates AFTER bot starts processing
        await application.updater.start_polling(drop_pending_updates=True) # Drop old updates on start
        logger.info("Bot started successfully and polling for updates.")

        # Keep application running using the robust wait logic
        # The 'application.idle()' check might be specific to older ways/setups
        # Running forever until manually stopped or signal received is typical
        await asyncio.Event().wait() # Simplest way to run forever until cancelled

    except Exception as e:
        logger.exception("Critical error during bot application startup or polling.", exc_info=True)
    finally:
        logger.info("Attempting to stop bot application...")
        if application.updater and application.updater.is_running:
            await application.updater.stop()
        if application.running:
             await application.stop()
        logger.info("Telegram bot application stopped.")