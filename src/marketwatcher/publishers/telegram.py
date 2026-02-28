"""Telegram Bot API publisher.

Handles:
- Bot authentication
- Message sending with HTML parse mode
- Chat info retrieval
- Error handling
"""

import httpx
from dataclasses import dataclass
from typing import Any

from marketwatcher.logging_config import get_logger

logger = get_logger("telegram")


@dataclass
class BotInfo:
    """Telegram bot information."""

    id: int
    is_bot: bool
    first_name: str
    username: str
    can_join_groups: bool
    can_read_all_group_messages: bool
    supports_inline_queries: bool


@dataclass
class ChatInfo:
    """Telegram chat information."""

    id: int
    type: str
    title: str | None = None
    username: str | None = None
    first_name: str | None = None
    last_name: str | None = None


@dataclass
class MessageResult:
    """Result of sending a message."""

    message_id: int
    date: int
    chat: dict


class TelegramPublisher:
    """Telegram Bot API client."""

    def __init__(self, bot_token: str):
        """Initialize publisher with bot token."""
        self.bot_token = bot_token
        self.base_url = f"https://api.telegram.org/bot{bot_token}"

    @property
    def client(self) -> httpx.Client:
        """Get HTTP client."""
        if not hasattr(self, "_client"):
            self._client = httpx.Client(timeout=30.0)
        return self._client

    def _request(self, method: str, data: dict | None = None) -> dict:
        """Make API request."""
        url = f"{self.base_url}/{method}"

        try:
            response = self.client.post(url, json=data)
            response.raise_for_status()
            result = response.json()

            if not result.get("ok"):
                error_desc = result.get("description", "Unknown error")
                raise TelegramError(f"API error: {error_desc}")

            return result.get("result", {})

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error: {e}")
            raise TelegramError(f"HTTP {e.response.status_code}: {e.response.text}")
        except httpx.TimeoutException:
            logger.error("Request timed out")
            raise TelegramError("Request timed out")

    def get_me(self) -> BotInfo:
        """Get bot information."""
        result = self._request("getMe")

        return BotInfo(
            id=result["id"],
            is_bot=result["is_bot"],
            first_name=result["first_name"],
            username=result["username"],
            can_join_groups=result.get("can_join_groups", False),
            can_read_all_group_messages=result.get("can_read_all_group_messages", False),
            supports_inline_queries=result.get("supports_inline_queries", False),
        )

    def get_chat(self, chat_id: str) -> ChatInfo:
        """Get chat information."""
        result = self._request("getChat", {"chat_id": chat_id})

        return ChatInfo(
            id=result["id"],
            type=result["type"],
            title=result.get("title"),
            username=result.get("username"),
            first_name=result.get("first_name"),
            last_name=result.get("last_name"),
        )

    def send_message(
        self,
        chat_id: str,
        text: str,
        parse_mode: str = "HTML",
        disable_web_page_preview: bool = True,
    ) -> MessageResult:
        """Send a message to a chat.

        Args:
            chat_id: Target chat ID or username
            text: Message text
            parse_mode: Parse mode (HTML or MarkdownV2)
            disable_web_page_preview: Disable link previews

        Returns:
            MessageResult with message info
        """
        data = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": disable_web_page_preview,
        }

        result = self._request("sendMessage", data)

        return MessageResult(
            message_id=result["message_id"],
            date=result["date"],
            chat=result["chat"],
        )

    def edit_message(
        self,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str = "HTML",
    ) -> dict:
        """Edit an existing message.

        Args:
            chat_id: Chat ID
            message_id: Message ID to edit
            text: New text
            parse_mode: Parse mode

        Returns:
            Updated message
        """
        data = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": parse_mode,
        }

        return self._request("editMessageText", data)

    def close(self) -> None:
        """Close HTTP client."""
        if hasattr(self, "_client"):
            self._client.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


class TelegramError(Exception):
    """Telegram API error."""
    pass
