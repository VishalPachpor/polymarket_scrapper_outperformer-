from __future__ import annotations

import json
import logging
import os
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org"


class TelegramAlerter:
    def __init__(self, *, bot_token: str | None = None, chat_id: str | None = None) -> None:
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
        self.enabled = bool(self.bot_token and self.chat_id)
        if not self.enabled:
            logger.warning(
                "TELEGRAM_DISABLED reason=missing_credentials "
                "(set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to enable)"
            )

    def send(self, text: str, *, copy_callback_data: str | None = None) -> bool:
        if not self.enabled:
            logger.info("TELEGRAM_NOOP text=%s", text[:120])
            return False
        url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/sendMessage"
        payload: dict[str, object] = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if copy_callback_data:
            payload["reply_markup"] = {
                "inline_keyboard": [[
                    {"text": "Copy Trade", "callback_data": copy_callback_data},
                    {"text": "Skip", "callback_data": "skip"},
                ]]
            }
        body = json.dumps(payload).encode("utf-8")
        req = Request(
            url,
            data=body,
            headers={"Content-Type": "application/json", "User-Agent": "tip-copy-trader/0.1"},
            method="POST",
        )
        try:
            with urlopen(req, timeout=5) as resp:
                if resp.status >= 400:
                    logger.error("TELEGRAM_HTTP_FAIL status=%s", resp.status)
                    return False
            return True
        except HTTPError as exc:
            logger.error("TELEGRAM_HTTP_ERROR status=%s reason=%s", exc.code, exc.reason)
            return False
        except URLError as exc:
            logger.error("TELEGRAM_NET_ERROR reason=%s", exc.reason)
            return False

    def market_link(self, slug: str) -> str:
        return f"https://polymarket.com/event/{quote(slug)}"

    def format_trade_alert(
        self, trade: dict, watched: dict, position: dict | None = None
    ) -> tuple[str, str]:
        """Return (html_text, callback_data) for a single watched-wallet trade.

        `position` (optional) is a dict with: open_shares, open_cost_usdc,
        avg_buy_price, realized_pnl_usdc, this_trade_pnl_usdc, trade_count.
        """
        side = (trade.get("side") or "").upper()
        size = float(trade.get("size") or 0)
        price = float(trade.get("price") or 0)
        pseudo = watched.get("pseudonym") or "wallet"
        slug = trade.get("slug") or trade.get("eventSlug") or ""
        title = trade.get("title") or ""
        outcome = trade.get("outcome") or ""
        notional = size * price
        side_emoji = "🟢 BUY" if side == "BUY" else "🔴 SELL"

        lines = [
            f"<b>{pseudo}</b>  {side_emoji}  <b>{outcome}</b>",
            f"size <b>{size:,.0f}</b>  @  ${price:.4f}   →   ${notional:,.0f}",
        ]

        if position is not None:
            shares = float(position.get("open_shares") or 0)
            cost = float(position.get("open_cost_usdc") or 0)
            avg = float(position.get("avg_buy_price") or 0)
            realized = float(position.get("realized_pnl_usdc") or 0)
            this_pnl = float(position.get("this_trade_pnl_usdc") or 0)
            tcount = int(position.get("trade_count") or 0)

            if side == "SELL" and abs(this_pnl) > 0.01:
                pnl_emoji = "✅" if this_pnl >= 0 else "❌"
                lines.append(f"this close: {pnl_emoji} ${this_pnl:+,.0f}")
            if shares > 0:
                lines.append(f"position: {shares:,.0f} sh @ avg ${avg:.4f}  (cost ${cost:,.0f})")
            else:
                lines.append("position: <i>flat</i>")
            realized_emoji = "📈" if realized >= 0 else "📉"
            lines.append(f"market PnL: {realized_emoji} ${realized:+,.0f}  ·  trades: {tcount}")

        # Wallet-level lifetime stats (static, refreshed periodically)
        lifetime = float(watched.get("lifetime_pnl_usdc") or 0)
        win_rate = float(watched.get("lifetime_win_rate") or 0)
        matches = int(watched.get("lifetime_matches") or 0)
        if lifetime > 0 or matches > 0:
            stat_bits = [f"lifetime ${lifetime:+,.0f}"]
            if matches > 0:
                stat_bits.append(f"{win_rate:.0%} W in {matches} matches")
            lines.append("<i>" + "  ·  ".join(stat_bits) + "</i>")

        lines.append(f"<i>{title[:80]}</i>")
        lines.append(f"<a href=\"{self.market_link(slug)}\">open market</a>")
        text = "\n".join(lines)

        tx = trade.get("transactionHash") or ""
        short_tx = tx[2:18] if tx.startswith("0x") else tx[:16]
        callback_data = f"copy:{short_tx}" if short_tx else "copy:unknown"
        return text, callback_data
