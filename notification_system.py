import logging
import json
import asyncio
from typing import Dict, Any, Optional, Union
from datetime import datetime, timezone

# Telegram imports for type hinting and error handling
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError, Forbidden, BadRequest, NetworkError

# Import base dependencies
from config.settings import settings # Use global settings
from data.db import Database

# Supabase/PostgREST error imports (match actual exception types if possible)
try:
     from postgrest.exceptions import APIError as PostgrestAPIError
except ImportError:
     PostgrestAPIError = Exception

logger = logging.getLogger(__name__)


class NotificationSystem:
    """
    Handles queuing and sending notifications via Telegram using an asyncio Queue.
    Also logs sent notifications to the database.
    Receives notifications as dictionaries with 'chat_id', 'content', 'type', etc.
    """

    def __init__(self, db: Database):
        """
        Initialize notification system.

        Args:
            db: Initialized Database connection manager instance.
        """
        self.settings = settings # Store global settings if needed (e.g., for delay)
        self.db = db
        # Get the synchronous Supabase client instance for logging
        self.supabase_sync_client = db.get_supabase_sync_client()
        # Redis client not directly used in this simplified version
        # self.redis = db.get_redis_connection() # Or db.connect_redis() if still sync

        self.notification_queue = asyncio.Queue(maxsize=5000) # Add max size for backpressure
        self.running = False
        self.processor_task: Optional[asyncio.Task] = None
        self.bot: Optional[Bot] = None # Telegram Bot instance, injected after Bot application creation

    async def start(self):
        """Start the notification system background processing task."""
        if not self.running:
            self.running = True
            # Start the queue processing coroutine as a task
            self.processor_task = asyncio.create_task(self._process_notification_queue())
            logger.info("Notification system processor task started.")

    async def stop(self):
        """Signal the notification system to stop processing and shut down."""
        if self.running:
            logger.info("Notification system stopping...")
            self.running = False
            # Put a sentinel value in the queue to unblock the processor
            await self.notification_queue.put(None)
            # Wait for the processor task to finish if it's running
            if self.processor_task:
                try:
                    await asyncio.wait_for(self.processor_task, timeout=10.0) # Wait max 10s
                except asyncio.TimeoutError:
                    logger.warning("Notification processor task did not finish promptly during stop.")
                    self.processor_task.cancel() # Force cancel if it times out
                    try: await self.processor_task
                    except asyncio.CancelledError: pass
                except Exception as e:
                    logger.exception(f"Error awaiting notification processor stop: {e}", exc_info=True)
            logger.info("Notification system stopped.")

    async def _process_notification_queue(self):
        """Coroutine that continuously processes notifications from the queue."""
        logger.info("Notification queue processor loop started.")
        while self.running:
            try:
                # Wait indefinitely for an item from the queue
                notification = await self.notification_queue.get()

                # Check for sentinel value to stop processing
                if notification is None:
                    logger.info("Received sentinel value, exiting notification processor loop.")
                    break # Exit the loop

                logger.debug(f"Processing notification: Type='{notification.get('type')}', ChatID='{notification.get('chat_id')}'")
                await self.send_telegram_notification(notification)
                self.notification_queue.task_done() # Mark task as done for queue management

                # Apply a small delay to avoid hitting Telegram rate limits too hard
                await asyncio.sleep(self.settings.notification_send_delay)

            except asyncio.CancelledError:
                logger.info("Notification queue processing task cancelled.")
                break # Exit loop if cancelled
            except Exception as e:
                # Log unexpected errors in the loop, but continue processing
                logger.exception(f"Error processing notification queue item: {e}", exc_info=True)
                # Avoid tight loop on persistent error by sleeping longer
                await asyncio.sleep(5)

        logger.info("Notification queue processor loop finished.")
        # Optional: Process remaining items if needed? Typically no, relies on queue size/startup persistence.

    async def send_telegram_notification(self, notification: Dict[str, Any]):
        """
        Attempts to send a formatted notification via the injected Telegram bot instance.

        Args:
            notification: Dictionary with at least 'chat_id' and 'content'.
                           Optionally: 'parse_mode', 'reply_markup', etc.
        """
        if not self.bot:
            logger.error("Bot instance is not available in NotificationSystem. Cannot send Telegram message.")
            return

        chat_id = notification.get("chat_id")
        content = notification.get("content")
        parse_mode = notification.get("parse_mode", ParseMode.MARKDOWN) # Default to Markdown v1 (less strict)
        reply_markup = notification.get("reply_markup") # Can be None or InlineKeyboardMarkup
        disable_preview = notification.get("disable_web_page_preview", True)

        if not chat_id or not content:
            logger.warning(f"Attempted to send invalid notification (missing chat_id or content). Skipping. Data: {str(notification)[:200]}")
            return

        try:
            # Send the message using the Bot instance
            await self.bot.send_message(
                chat_id=chat_id,
                text=content,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                disable_web_page_preview=disable_preview
            )
            logger.info(f"Successfully sent notification type '{notification.get('type', 'N/A')}' to chat_id {chat_id}")

            # Log to DB after successful send (or attempt) - fire and forget optional
            asyncio.create_task(self._log_notification_to_db(notification))

        except Forbidden:
            logger.warning(f"SEND FAILED (Forbidden): Bot may be blocked by user or kicked from chat {chat_id}.")
            # TODO: Optional - Add logic here to mark the user/subscription as inactive in your DB
        except BadRequest as e:
            logger.error(f"SEND FAILED (Bad Request) for chat {chat_id}: {e}. Message content (first 100): '{content[:100]}...' Check formatting or chat ID.")
            # Often due to malformed Markdown V2, or chat_id not found
        except NetworkError as e:
            logger.error(f"SEND FAILED (Network Error) for chat {chat_id}: {e}. Will likely be retried if queued again.")
        except TelegramError as e:
            # Catch other specific telegram errors
            logger.error(f"SEND FAILED (Telegram API Error) for chat {chat_id}: {e}")
        except Exception as e:
            # Catch unexpected errors during sending
            logger.exception(f"SEND FAILED (Unexpected Error) sending Telegram notification to {chat_id}", exc_info=True)

    async def _log_notification_to_db(self, notification: Dict[str, Any]):
        """Logs notification details to the Supabase database (asynchronously)."""
        logger.debug(f"Logging notification to DB: Type='{notification.get('type')}', ChatID='{notification.get('chat_id')}'")
        try:
            insert_data = {
                "chat_id": notification.get("chat_id"),
                "token_address": notification.get("token_address"), # Can be None
                "notification_type": notification.get("type"),
                "content": notification.get("content"), # Store formatted content?
                # Store potentially large data field, ensure DB column type is JSONB
                "data": json.dumps(notification.get("data")) if notification.get("data") is not None else None
            }
            # Validate required fields for logging
            if not insert_data["chat_id"] or not insert_data["notification_type"]:
                 logger.warning(f"Skipped logging notification due to missing required fields: {insert_data}")
                 return

            # Use helper method to execute query wrapped in thread
            log_query = self.supabase_sync_client.table("notifications").insert(insert_data)
            result = await self.db._execute_supabase_query(log_query) # Use helper from db.py

            # Check result from helper for errors
            if result.get("error"):
                 logger.error(f"Failed to log notification to Supabase DB. Error: {result.get('message', result.get('error'))}")
            else:
                 logger.debug(f"Notification successfully logged to database.")

        except Exception as e:
            # Catch errors during data prep or the execute call
            logger.exception(f"Error preparing or logging notification to Supabase DB", exc_info=True)


    async def queue_notification(self, notification: Dict[str, Any]):
        """
        Asynchronously puts a notification dictionary onto the queue.

        Args:
            notification: Notification data dictionary. Must contain 'chat_id' and 'content'.
        """
        if not isinstance(notification, dict):
            logger.error(f"Invalid data type passed to queue_notification: {type(notification)}")
            return
        if not self.running:
            logger.warning("Notification system is stopped. Cannot queue notification.")
            # Optionally raise error or return failure status
            return

        try:
            # Add timestamp if not present for logging/ordering
            if 'queued_at' not in notification:
                notification['queued_at'] = datetime.now(timezone.utc).isoformat()

            # Check queue size if it becomes an issue
            if self.notification_queue.qsize() > (self.notification_queue.maxsize * 0.9):
                logger.warning(f"Notification queue is over 90% full (size: {self.notification_queue.qsize()})!")

            await self.notification_queue.put(notification)
            logger.debug(f"Queued notification: Type={notification.get('type')}, ChatID={notification.get('chat_id')}")

        except Exception as e:
            logger.exception("Failed to queue notification", exc_info=True)