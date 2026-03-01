#!/usr/bin/env python3
"""
Solana Paper Trading Bot - Maestro Style UI
Clean version - no emoji syntax issues
"""

import logging
import os
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
import httpx

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
STARTING_BALANCE = 5_000.0
DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens/{}"
PRICE_CHECK_INTERVAL = 30

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Storage ───────────────────────────────────────────────────────────────────
users: dict = {}
trade_log: dict = {}
pending: dict = {}   # pending[user_id] = {"action": ..., ...}


# ── DexScreener ───────────────────────────────────────────────────────────────
async def get_token(contract: str) -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(DEXSCREENER_API.format(contract))
            data = r.json()
        pairs = [p for p in (data.get("pairs") or []) if p.get("chainId") == "solana"]
        if not pairs:
            return None
        best = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        price = float(best.get("priceUsd") or 0)
        if not price:
            return None
        mc = float(best.get("marketCap") or best.get("fdv") or 0)
        liq = float(best.get("liquidity", {}).get("usd", 0) or 0)
        liq_pct = (liq / mc * 100) if mc > 0 else 0
        return {
            "symbol":   best.get("baseToken", {}).get("symbol", "???"),
            "name":     best.get("baseToken", {}).get("name", "Unknown"),
            "price":    price,
            "change":   best.get("priceChange", {}).get("h24", 0),
            "volume":   float(best.get("volume", {}).get("h24", 0) or 0),
            "liq":      liq,
            "liq_pct":  round(liq_pct, 2),
            "mc":       mc,
            "dex":      best.get("dexId", "unknown"),
        }
    except Exception as e:
        logger.error(f"DexScreener: {e}")
        return None


# ── Helpers ───────────────────────────────────────────────────────────────────
def get_user(uid: int, uname: str) -> dict:
    if uid not in users:
        users[uid] = {
            "username":      uname or f"User{uid}",
            "balance":       STARTING_BALANCE,
            "holdings":      {},
            "realized_pnl":  0.0,
            "joined_at":     datetime.now(),
            "preset_buy":    None,
            "preset_sell":   None,
            "risk_pct":      None,
            "max_positions": None,
            "daily_limit":   None,
            "daily_trades":  0,
            "last_day":      None,
            "planned":       0,
            "impulse":       0,
            "followed":      0,
            "broken":        0,
            "streak":        0,
            "best_streak":   0,
        }
        trade_log[uid] = []
    return users[uid]


def money(n: float) -> str:
    if abs(n) >= 1_000_000_000: return f"${n/1_000_000_000:.2f}B"
    if abs(n) >= 1_000_000:     return f"${n/1_000_000:.2f}M"
    if abs(n) >= 1_000:         return f"${n:,.2f}"
    return f"${n:.6f}".rstrip("0").rstrip(".")


def mc_str(n: float) -> str:
    if n >= 1_000_000_000: return f"${n/1_000_000_000:.2f}B"
    if n >= 1_000_000:     return f"${n/1_000_000:.2f}M"
    if n >= 1_000:         return f"${n/1_000:.0f}K"
    return f"${n:.0f}"


def pnl(n: float) -> str:
    return f"[+{money(n)}]" if n >= 0 else f"[-{money(abs(n))}]"


def check_daily(d: dict) -> bool:
    today = datetime.now().date()
    if d["last_day"] != today:
        d["daily_trades"] = 0
        d["last_day"] = today
    lim = d.get("daily_limit")
    return not (lim and d["daily_trades"] >= lim)


def do_sell_core(ud: dict, uid: int, contract: str, usd: float, price: float, reason: str = "manual") -> dict:
    h = ud["holdings"][contract]
    tokens = usd / price
    ratio = tokens / h["amount"]
    cost = h["total_invested"] * ratio
    realized = usd - cost
    ud["realized_pnl"] += realized
    ud["balance"] += usd
    h["amount"] -= tokens
    h["total_invested"] = max(0, h["total_invested"] - cost)
    hold_h = (datetime.now() - h.get("bought_at", datetime.now())).total_seconds() / 3600
    closed = False
    if h["amount"] < 0.000001:
        trade_log.setdefault(uid, []).append({
            "symbol": h["symbol"], "contract": contract,
            "invested": h["total_invested"] + cost,
            "returned": usd, "realized_pnl": realized,
            "x": price / h["avg_price"] if h["avg_price"] > 0 else 0,
            "hold_h": round(hold_h, 1), "reason": reason,
            "closed_at": datetime.now(), "journal": h.get("journal", ""),
            "planned": h.get("planned", True),
            "followed_plan": h.get("followed_plan", None),
        })
        del ud["holdings"][contract]
        closed = True
    return {"received": usd, "tokens": tokens, "realized": realized,
            "closed": closed, "hold_h": round(hold_h, 1)}


async def portfolio_value(ud: dict) -> tuple[float, float]:
    tv, tc = 0.0, 0.0
    for c, h in ud["holdings"].items():
        info = await get_token(c)
        if info:
            tv += h["amount"] * info["price"]
            tc += h["total_invested"]
    return tv, tv - tc


# ── Token Card (Maestro style) ────────────────────────────────────────────────
def token_card(info: dict, contract: str, ud: dict) -> str:
    change = info["change"]
    direction = "+" if float(change or 0) >= 0 else ""
    held = contract in ud["holdings"]
    held_line = ""
    if held:
        h = ud["holdings"][contract]
        cv = h["amount"] * info["price"]
        cx = info["price"] / h["avg_price"] if h["avg_price"] > 0 else 0
        ppnl = cv - h["total_invested"]
        held_line = (
            f"\n"
            f"-- YOUR POSITION --\n"
            f"Value: {money(cv)}  |  {cx:.2f}x\n"
            f"PnL: {pnl(ppnl)}"
        )
    warn = "\n!! LOW LIQUIDITY - HIGH RISK !!" if info["liq"] < 50_000 else ""
    return (
        f"*{info['name']} (${info['symbol']})*\n"
        f"`{contract}`\n"
        f"\n"
        f"MC: *{mc_str(info['mc'])}*  |  Price: *{money(info['price'])}*\n"
        f"Liquidity: *{money(info['liq'])}* ({info['liq_pct']}%)\n"
        f"24h Vol: *{money(info['volume'])}*  |  Change: *{direction}{change}%*\n"
        f"DEX: {info['dex']}"
        f"{held_line}"
        f"{warn}"
    )


def buy_keyboard(contract: str, ud: dict) -> InlineKeyboardMarkup:
    pb = ud.get("preset_buy")
    preset_label = f"Buy ${pb:.0f} [PRESET]" if pb else "Set Preset Buy"
    held = contract in ud["holdings"]
    rows = []
    if held:
        rows.append([
            InlineKeyboardButton("Refresh", callback_data=f"refresh_{contract}"),
            InlineKeyboardButton("Go to Sell", callback_data=f"sell_token_{contract}"),
        ])
    else:
        rows.append([
            InlineKeyboardButton("Refresh", callback_data=f"refresh_{contract}"),
        ])
    rows.append([InlineKeyboardButton(preset_label, callback_data=f"buypreset_{contract}")])
    rows.append([
        InlineKeyboardButton("$50",   callback_data=f"buyamt_50_{contract}"),
        InlineKeyboardButton("$100",  callback_data=f"buyamt_100_{contract}"),
        InlineKeyboardButton("$250",  callback_data=f"buyamt_250_{contract}"),
    ])
    rows.append([
        InlineKeyboardButton("$500",  callback_data=f"buyamt_500_{contract}"),
        InlineKeyboardButton("$1000", callback_data=f"buyamt_1000_{contract}"),
        InlineKeyboardButton("Custom", callback_data=f"buycustom_{contract}"),
    ])
    if held:
        h = ud["holdings"][contract]
        as_targets = h.get("auto_sells", [])
        sl = h.get("stop_loss_pct")
        as_label = "Auto-Sell [SET]" if as_targets else "Auto-Sell"
        sl_label  = f"Stop Loss [{sl}%]" if sl else "Stop Loss"
        rows.append([
            InlineKeyboardButton(as_label, callback_data=f"as_menu_{contract}"),
            InlineKeyboardButton(sl_label, callback_data=f"sl_menu_{contract}"),
        ])
        rows.append([
            InlineKeyboardButton("Journal", callback_data=f"journal_{contract}"),
            InlineKeyboardButton("View Targets", callback_data=f"view_targets_{contract}"),
        ])
    rows.append([InlineKeyboardButton("-- Main Menu --", callback_data="main_menu")])
    return InlineKeyboardMarkup(rows)


def sell_keyboard(contract: str, symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("25%",   callback_data=f"sellpct_25_{contract}"),
            InlineKeyboardButton("50%",   callback_data=f"sellpct_50_{contract}"),
            InlineKeyboardButton("75%",   callback_data=f"sellpct_75_{contract}"),
            InlineKeyboardButton("100%",  callback_data=f"sellpct_100_{contract}"),
        ],
        [InlineKeyboardButton("Custom Amount", callback_data=f"sellcustom_{contract}")],
        [InlineKeyboardButton("Back to Token", callback_data=f"back_token_{contract}")],
        [InlineKeyboardButton("-- Main Menu --", callback_data="main_menu")],
    ])


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Portfolio", callback_data="show_portfolio"),
         InlineKeyboardButton("Balance",   callback_data="show_balance")],
        [InlineKeyboardButton("Stats",     callback_data="show_stats"),
         InlineKeyboardButton("Review",    callback_data="show_review")],
        [InlineKeyboardButton("Leaderboard", callback_data="show_leaderboard")],
        [InlineKeyboardButton("Settings",  callback_data="show_settings")],
        [InlineKeyboardButton("Paste a CA to trade", callback_data="noop")],
    ])


def settings_keyboard(ud: dict) -> InlineKeyboardMarkup:
    pb  = f"${ud['preset_buy']:.0f}"  if ud.get("preset_buy")    else "not set"
    ps  = str(ud["preset_sell"])      if ud.get("preset_sell")    else "not set"
    rsk = f"{ud['risk_pct']}%"        if ud.get("risk_pct")       else "not set"
    mp  = str(ud["max_positions"])    if ud.get("max_positions")  else "not set"
    dl  = str(ud["daily_limit"])      if ud.get("daily_limit")    else "not set"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Default Buy: {pb}",        callback_data="cfg_buy")],
        [InlineKeyboardButton(f"Default Sell: {ps}",       callback_data="cfg_sell")],
        [InlineKeyboardButton(f"Max Risk/Trade: {rsk}",    callback_data="cfg_risk")],
        [InlineKeyboardButton(f"Max Positions: {mp}",      callback_data="cfg_maxpos")],
        [InlineKeyboardButton(f"Daily Trade Limit: {dl}",  callback_data="cfg_daily")],
        [InlineKeyboardButton("Reset Account",             callback_data="reset_prompt")],
        [InlineKeyboardButton("-- Back --",                callback_data="main_menu")],
    ])


def back_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("-- Main Menu --", callback_data="main_menu")]])


def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Cancel", callback_data="main_menu")]])


# ── Auto-sell checker ──────────────────────────────────────────────────────────
async def check_auto_sells(app: Application):
    for uid, ud in list(users.items()):
        for contract, h in list(ud["holdings"].items()):
            info = await get_token(contract)
            if not info:
                continue
            price = info["price"]
            avg = h["avg_price"]
            cx = price / avg if avg > 0 else 0

            # Stop loss
            sl = h.get("stop_loss_pct")
            if sl:
                drop = (price - avg) / avg * 100
                if drop <= -sl:
                    cv = h["amount"] * price
                    result = do_sell_core(ud, uid, contract, cv, price, "stop_loss")
                    ud["followed"] += 1
                    ud["streak"] += 1
                    ud["best_streak"] = max(ud["best_streak"], ud["streak"])
                    try:
                        await app.bot.send_message(
                            chat_id=uid, parse_mode="Markdown",
                            text=(
                                f"STOP LOSS HIT\n\n"
                                f"*${h['symbol']}* dropped {drop:.1f}%\n"
                                f"Sold 100% -> {money(result['received'])}\n"
                                f"Price: {money(price)}\n"
                                f"PnL: {pnl(result['realized'])}\n"
                                f"Cash: {money(ud['balance'])}"
                            ),
                            reply_markup=main_menu_keyboard()
                        )
                    except Exception as e:
                        logger.error(e)
                    continue

            # Auto-sell targets
            for t in sorted([a for a in h.get("auto_sells", []) if not a["triggered"]], key=lambda a: a["x"]):
                if cx < t["x"] or contract not in ud["holdings"]:
                    break
                t["triggered"] = True
                cv = h["amount"] * price
                sv = cv * t["pct"]
                if sv < 0.001:
                    continue
                result = do_sell_core(ud, uid, contract, sv, price, "auto_sell")
                ud["followed"] += 1
                ud["streak"] += 1
                ud["best_streak"] = max(ud["best_streak"], ud["streak"])
                try:
                    await app.bot.send_message(
                        chat_id=uid, parse_mode="Markdown",
                        text=(
                            f"AUTO-SELL TRIGGERED\n\n"
                            f"*${h['symbol']}* hit {t['x']}x\n"
                            f"Sold {t['pct']*100:.0f}% -> {money(result['received'])}\n"
                            f"Price: {money(price)}  |  {cx:.2f}x\n"
                            f"PnL: {pnl(result['realized'])}\n"
                            f"Cash: {money(ud['balance'])}"
                        ),
                        reply_markup=main_menu_keyboard()
                    )
                except Exception as e:
                    logger.error(e)


# ── /start ────────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    get_user(u.id, u.username or u.first_name)
    text = (
        "SOLANA PAPER TRADING BOT\n\n"
        "Paste any Solana CA to get started.\n"
        "Use the menu below for settings and stats."
    )
    kb = main_menu_keyboard()
    if update.message:
        await update.message.reply_text(text, reply_markup=kb)
    else:
        await update.callback_query.edit_message_text(text, reply_markup=kb)


# ── Text handler (CA paste + settings input) ──────────────────────────────────
async def text_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    ud = get_user(u.id, u.username or u.first_name)
    text = update.message.text.strip()
    p = pending.get(u.id)

    # Handle pending inputs
    if p:
        action = p["action"]

        # ── Settings inputs ──
        if action == "cfg_buy":
            try:
                amt = float(text.replace("$", ""))
                assert amt > 0
                ud["preset_buy"] = amt
                pending.pop(u.id, None)
                await update.message.reply_text(f"Default buy set: ${amt:.0f}", reply_markup=settings_keyboard(ud))
            except:
                await update.message.reply_text("Invalid. Enter a number like 100", reply_markup=cancel_kb())
            return

        elif action == "cfg_sell":
            raw = text.replace("$", "")
            try:
                if raw.endswith("%"):
                    pct = float(raw[:-1])
                    assert 0 < pct <= 100
                    ud["preset_sell"] = f"{pct:.0f}%"
                else:
                    amt = float(raw)
                    assert amt > 0
                    ud["preset_sell"] = amt
                pending.pop(u.id, None)
                await update.message.reply_text(f"Default sell set: {text}", reply_markup=settings_keyboard(ud))
            except:
                await update.message.reply_text("Invalid. Enter 50% or 200", reply_markup=cancel_kb())
            return

        elif action == "cfg_risk":
            try:
                pct = float(text.replace("%", ""))
                assert 0 < pct <= 100
                ud["risk_pct"] = pct
                pending.pop(u.id, None)
                await update.message.reply_text(f"Max risk per trade: {pct}%", reply_markup=settings_keyboard(ud))
            except:
                await update.message.reply_text("Invalid. Enter a number like 10", reply_markup=cancel_kb())
            return

        elif action == "cfg_maxpos":
            try:
                n = int(text)
                assert n > 0
                ud["max_positions"] = n
                pending.pop(u.id, None)
                await update.message.reply_text(f"Max positions: {n}", reply_markup=settings_keyboard(ud))
            except:
                await update.message.reply_text("Invalid. Enter a number like 5", reply_markup=cancel_kb())
            return

        elif action == "cfg_daily":
            try:
                n = int(text)
                assert n > 0
                ud["daily_limit"] = n
                pending.pop(u.id, None)
                await update.message.reply_text(f"Daily limit: {n} trades/day", reply_markup=settings_keyboard(ud))
            except:
                await update.message.reply_text("Invalid. Enter a number like 10", reply_markup=cancel_kb())
            return

        # ── Buy custom amount ──
        elif action == "buy_custom":
            contract = p["contract"]
            try:
                amt = float(text.replace("$", ""))
                assert amt > 0
                pending.pop(u.id, None)
                await execute_buy(update, ud, u.id, contract, amt)
            except:
                await update.message.reply_text("Invalid amount. Enter a number like 200", reply_markup=cancel_kb())
            return

        # ── Sell custom ──
        elif action == "sell_custom":
            contract = p["contract"]
            if contract not in ud["holdings"]:
                await update.message.reply_text("Position not found.", reply_markup=back_main())
                pending.pop(u.id, None)
                return
            raw = text.replace("$", "")
            try:
                if raw.endswith("%"):
                    pct = float(raw[:-1]) / 100
                    await execute_sell_msg(update, ud, u.id, contract, pct=pct)
                else:
                    await execute_sell_msg(update, ud, u.id, contract, usd=float(raw))
                pending.pop(u.id, None)
            except:
                await update.message.reply_text("Invalid. Enter 50% or 200", reply_markup=cancel_kb())
            return

        # ── Auto-sell custom targets ──
        elif action == "as_custom":
            contract = p["contract"]
            if contract not in ud["holdings"]:
                pending.pop(u.id, None)
                await update.message.reply_text("Position not found.", reply_markup=back_main())
                return
            parts = text.split()
            if len(parts) % 2 != 0:
                await update.message.reply_text("Format: 50% 2x 100% 5x", reply_markup=cancel_kb())
                return
            targets = []
            try:
                for i in range(0, len(parts), 2):
                    pct = float(parts[i].replace("%", "")) / 100
                    x   = float(parts[i+1].lower().replace("x", ""))
                    assert 0 < pct <= 1 and x > 1
                    targets.append({"pct": pct, "x": x, "triggered": False})
                targets.sort(key=lambda t: t["x"])
                ud["holdings"][contract]["auto_sells"] = targets
                h = ud["holdings"][contract]
                avg = h["avg_price"]
                lines = [f"Auto-sells set for ${h['symbol']}:\n"]
                for t in targets:
                    lines.append(f"  {t['pct']*100:.0f}% at {t['x']}x  (~{money(avg * t['x'])})")
                pending.pop(u.id, None)
                await update.message.reply_text("\n".join(lines), reply_markup=back_main())
            except:
                await update.message.reply_text("Invalid format. Example: 50% 2x 100% 5x", reply_markup=cancel_kb())
            return

        # ── Stop loss custom ──
        elif action == "sl_custom":
            contract = p["contract"]
            try:
                pct = float(text.replace("%", ""))
                assert 0 < pct < 100
                ud["holdings"][contract]["stop_loss_pct"] = pct
                h = ud["holdings"][contract]
                trigger = h["avg_price"] * (1 - pct / 100)
                pending.pop(u.id, None)
                await update.message.reply_text(
                    f"Stop loss set for ${h['symbol']}:\n{pct}% drop -> trigger at {money(trigger)}",
                    reply_markup=back_main()
                )
            except:
                await update.message.reply_text("Invalid. Enter a number like 50", reply_markup=cancel_kb())
            return

        # ── Journal note ──
        elif action == "journal":
            contract = p["contract"]
            if contract in ud["holdings"]:
                ud["holdings"][contract]["journal"] = text
                symbol = ud["holdings"][contract]["symbol"]
                pending.pop(u.id, None)
                await update.message.reply_text(f"Journal saved for ${symbol}:\n\"{text}\"", reply_markup=back_main())
            else:
                pending.pop(u.id, None)
                await update.message.reply_text("Position not found.", reply_markup=back_main())
            return

    # ── No pending action — treat as CA paste ──
    contract = text
    msg = await update.message.reply_text("Fetching token info...")
    info = await get_token(contract)
    if not info:
        await msg.edit_text(
            "Token not found on DexScreener (Solana).\nCheck the contract address and try again.",
            reply_markup=back_main()
        )
        return
    await msg.edit_text(
        token_card(info, contract, ud),
        parse_mode="Markdown",
        reply_markup=buy_keyboard(contract, ud)
    )


# ── Button handler ────────────────────────────────────────────────────────────
async def btn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = update.effective_user
    ud = get_user(u.id, u.username or u.first_name)
    cb = q.data

    if cb == "noop":
        return

    # ── Main menu ──
    elif cb == "main_menu":
        pending.pop(u.id, None)
        await q.edit_message_text(
            "SOLANA PAPER TRADING BOT\n\nPaste any Solana CA to trade.\nUse buttons for settings and stats.",
            reply_markup=main_menu_keyboard()
        )

    # ── Refresh token card ──
    elif cb.startswith("refresh_"):
        contract = cb[len("refresh_"):]
        info = await get_token(contract)
        if not info:
            await q.edit_message_text("Could not refresh. Token unavailable.", reply_markup=back_main())
            return
        await q.edit_message_text(
            token_card(info, contract, ud),
            parse_mode="Markdown",
            reply_markup=buy_keyboard(contract, ud)
        )

    # ── Back to token card ──
    elif cb.startswith("back_token_"):
        contract = cb[len("back_token_"):]
        info = await get_token(contract)
        if not info:
            await q.edit_message_text("Token unavailable.", reply_markup=back_main())
            return
        await q.edit_message_text(
            token_card(info, contract, ud),
            parse_mode="Markdown",
            reply_markup=buy_keyboard(contract, ud)
        )

    # ── Buy preset ──
    elif cb.startswith("buypreset_"):
        contract = cb[len("buypreset_"):]
        pb = ud.get("preset_buy")
        if not pb:
            await q.edit_message_text(
                "No preset buy set.\nGo to Settings to set your default buy amount.",
                reply_markup=back_main()
            )
            return
        await execute_buy_query(q, ud, u.id, contract, pb)

    # ── Buy fixed amount ──
    elif cb.startswith("buyamt_"):
        rest = cb[len("buyamt_"):]
        amt_str, contract = rest.split("_", 1)
        await execute_buy_query(q, ud, u.id, contract, float(amt_str))

    # ── Buy custom ──
    elif cb.startswith("buycustom_"):
        contract = cb[len("buycustom_"):]
        pending[u.id] = {"action": "buy_custom", "contract": contract}
        await q.edit_message_text(
            "Enter amount in USD to buy:\n(e.g. 150 or 1000)",
            reply_markup=cancel_kb()
        )

    # ── Sell token (go to sell screen) ──
    elif cb.startswith("sell_token_"):
        contract = cb[len("sell_token_"):]
        if contract not in ud["holdings"]:
            await q.edit_message_text("No position found.", reply_markup=back_main())
            return
        h = ud["holdings"][contract]
        info = await get_token(contract)
        price = info["price"] if info else h["avg_price"]
        cv = h["amount"] * price
        cx = price / h["avg_price"] if h["avg_price"] > 0 else 0
        ppnl = cv - h["total_invested"]
        await q.edit_message_text(
            f"SELL *${h['symbol']}*\n\n"
            f"Position Value: *{money(cv)}*\n"
            f"Current: *{cx:.2f}x*\n"
            f"PnL: {pnl(ppnl)}\n\n"
            f"How much to sell?",
            parse_mode="Markdown",
            reply_markup=sell_keyboard(contract, h["symbol"])
        )

    # ── Sell percent ──
    elif cb.startswith("sellpct_"):
        rest = cb[len("sellpct_"):]
        pct_str, contract = rest.split("_", 1)
        pct = float(pct_str) / 100
        await execute_sell_query(q, ud, u.id, contract, pct=pct)

    # ── Sell custom ──
    elif cb.startswith("sellcustom_"):
        contract = cb[len("sellcustom_"):]
        pending[u.id] = {"action": "sell_custom", "contract": contract}
        await q.edit_message_text(
            "Enter amount to sell:\n(e.g. 200 or 50%)",
            reply_markup=cancel_kb()
        )

    # ── Auto-sell menu ──
    elif cb.startswith("as_menu_"):
        contract = cb[len("as_menu_"):]
        if contract not in ud["holdings"]:
            await q.edit_message_text("Position not found.", reply_markup=back_main())
            return
        h = ud["holdings"][contract]
        avg = h["avg_price"]
        await q.edit_message_text(
            f"AUTO-SELL  *${h['symbol']}*\n\nBuy price: {money(avg)}\n\nChoose a target preset:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("50% at 2x + 100% at 5x",   callback_data=f"asq_2_5_{contract}")],
                [InlineKeyboardButton("50% at 3x + 100% at 10x",  callback_data=f"asq_3_10_{contract}")],
                [InlineKeyboardButton("25% at 2x + 25% at 5x + 50% at 10x", callback_data=f"asq_2_5_10_{contract}")],
                [InlineKeyboardButton("100% at 2x",               callback_data=f"asq_2_{contract}")],
                [InlineKeyboardButton("Custom Targets",            callback_data=f"ascustom_{contract}")],
                [InlineKeyboardButton("Back", callback_data=f"back_token_{contract}")],
            ])
        )

    elif cb.startswith("asq_2_5_10_"):
        contract = cb[len("asq_2_5_10_"):]
        _set_autosell(ud, contract, [
            {"pct": 0.25, "x": 2.0, "triggered": False},
            {"pct": 0.25, "x": 5.0, "triggered": False},
            {"pct": 0.50, "x": 10.0, "triggered": False},
        ])
        await _confirm_autosell(q, ud, contract)

    elif cb.startswith("asq_2_"):
        contract = cb[len("asq_2_"):]
        _set_autosell(ud, contract, [{"pct": 1.0, "x": 2.0, "triggered": False}])
        await _confirm_autosell(q, ud, contract)

    elif cb.startswith("asq_"):
        rest = cb[len("asq_"):]
        parts = rest.split("_", 2)
        x1, x2, contract = int(parts[0]), int(parts[1]), parts[2]
        _set_autosell(ud, contract, [
            {"pct": 0.5, "x": float(x1), "triggered": False},
            {"pct": 1.0, "x": float(x2), "triggered": False},
        ])
        await _confirm_autosell(q, ud, contract)

    elif cb.startswith("ascustom_"):
        contract = cb[len("ascustom_"):]
        pending[u.id] = {"action": "as_custom", "contract": contract}
        await q.edit_message_text(
            "Enter auto-sell targets:\nFormat: PERCENT X  PERCENT X\nExample: 50% 2x 100% 5x",
            reply_markup=cancel_kb()
        )

    # ── View targets ──
    elif cb.startswith("view_targets_"):
        contract = cb[len("view_targets_"):]
        if contract not in ud["holdings"]:
            await q.edit_message_text("Position not found.", reply_markup=back_main())
            return
        h = ud["holdings"][contract]
        targets = h.get("auto_sells", [])
        sl = h.get("stop_loss_pct")
        avg = h["avg_price"]
        lines = [f"TARGETS  ${h['symbol']}\n"]
        if targets:
            for t in targets:
                status = "[DONE]" if t["triggered"] else "[WAITING]"
                lines.append(f"  {status} Sell {t['pct']*100:.0f}% at {t['x']}x  (~{money(avg * t['x'])})")
        else:
            lines.append("  No auto-sell targets set.")
        if sl:
            trigger = avg * (1 - sl / 100)
            lines.append(f"\n  Stop Loss: {sl}% drop  (~{money(trigger)})")
        else:
            lines.append("\n  No stop loss set.")
        await q.edit_message_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Cancel All", callback_data=f"cancel_targets_{contract}")],
                [InlineKeyboardButton("Back", callback_data=f"back_token_{contract}")],
            ])
        )

    elif cb.startswith("cancel_targets_"):
        contract = cb[len("cancel_targets_"):]
        if contract in ud["holdings"]:
            ud["holdings"][contract]["auto_sells"] = []
            ud["holdings"][contract]["stop_loss_pct"] = None
            symbol = ud["holdings"][contract]["symbol"]
            await q.edit_message_text(
                f"All targets cancelled for ${symbol}.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data=f"back_token_{contract}")]])
            )

    # ── Stop loss menu ──
    elif cb.startswith("sl_menu_"):
        contract = cb[len("sl_menu_"):]
        if contract not in ud["holdings"]:
            await q.edit_message_text("Position not found.", reply_markup=back_main())
            return
        h = ud["holdings"][contract]
        sl = h.get("stop_loss_pct")
        sl_info = f"\nCurrent SL: {sl}%" if sl else ""
        await q.edit_message_text(
            f"STOP LOSS  *${h['symbol']}*{sl_info}\n\nSell ALL if price drops by:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("25%", callback_data=f"slset_25_{contract}"),
                 InlineKeyboardButton("50%", callback_data=f"slset_50_{contract}"),
                 InlineKeyboardButton("75%", callback_data=f"slset_75_{contract}")],
                [InlineKeyboardButton("Custom %", callback_data=f"slcustom_{contract}")],
                [InlineKeyboardButton("Remove SL", callback_data=f"slremove_{contract}")],
                [InlineKeyboardButton("Back", callback_data=f"back_token_{contract}")],
            ])
        )

    elif cb.startswith("slset_"):
        rest = cb[len("slset_"):]
        pct_str, contract = rest.split("_", 1)
        pct = float(pct_str)
        if contract in ud["holdings"]:
            ud["holdings"][contract]["stop_loss_pct"] = pct
            h = ud["holdings"][contract]
            trigger = h["avg_price"] * (1 - pct / 100)
            await q.edit_message_text(
                f"Stop loss set for ${h['symbol']}:\n{pct:.0f}% drop -> trigger at {money(trigger)}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data=f"back_token_{contract}")]])
            )

    elif cb.startswith("slcustom_"):
        contract = cb[len("slcustom_"):]
        pending[u.id] = {"action": "sl_custom", "contract": contract}
        await q.edit_message_text("Enter stop loss % drop (e.g. 60):", reply_markup=cancel_kb())

    elif cb.startswith("slremove_"):
        contract = cb[len("slremove_"):]
        if contract in ud["holdings"]:
            ud["holdings"][contract]["stop_loss_pct"] = None
            symbol = ud["holdings"][contract]["symbol"]
            await q.edit_message_text(
                f"Stop loss removed for ${symbol}.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data=f"back_token_{contract}")]])
            )

    # ── Journal ──
    elif cb.startswith("journal_"):
        contract = cb[len("journal_"):]
        pending[u.id] = {"action": "journal", "contract": contract}
        h = ud["holdings"].get(contract, {})
        existing = h.get("journal", "")
        note = f"\nCurrent: \"{existing}\"" if existing else ""
        await q.edit_message_text(f"Enter trade thesis for ${h.get('symbol','?')}:{note}", reply_markup=cancel_kb())

    # ── Portfolio ──
    elif cb == "show_portfolio":
        if not ud["holdings"]:
            await q.edit_message_text(
                f"No open positions.\nCash: {money(ud['balance'])}\n\nPaste a CA to start trading.",
                reply_markup=back_main()
            )
            return
        lines = ["PORTFOLIO\n"]
        ti = tc = 0.0
        for contract, h in ud["holdings"].items():
            info = await get_token(contract)
            if info:
                cv = h["amount"] * info["price"]
                ppnl = cv - h["total_invested"]
                cx = info["price"] / h["avg_price"] if h["avg_price"] > 0 else 0
                ti += h["total_invested"]
                tc += cv
                direction = "+" if ppnl >= 0 else "-"
                targets = [t for t in h.get("auto_sells", []) if not t["triggered"]]
                if targets:
    parts = []
    for t in targets:
        parts.append(f"{t['pct']*100:.0f}%@{t['x']}x")
    t_info = "  [AS: " + ", ".join(parts) + "]"
else:
    t_info = ""
                sl = h.get("stop_loss_pct")
                sl_info = f"  [SL: {sl}%]" if sl else ""
                lines.append(
                    f"${h['symbol']}  {cx:.2f}x\n"
                    f"  {money(cv)}  {pnl(ppnl)}"
                    f"{t_info}{sl_info}\n"
                )
            else:
                lines.append(f"${h['symbol']} - unavailable\n")
        eq = ud["balance"] + tc
        lines.append(
            f"---\n"
            f"Cash: {money(ud['balance'])}\n"
            f"Holdings: {money(tc)}\n"
            f"Equity: {money(eq)}\n"
            f"Total PnL: {pnl(eq - STARTING_BALANCE)}"
        )
        await q.edit_message_text("\n".join(lines), reply_markup=back_main())

    # ── Balance ──
    elif cb == "show_balance":
        hv, upnl = await portfolio_value(ud)
        eq = ud["balance"] + hv
        await q.edit_message_text(
            f"BALANCE\n\n"
            f"Cash:          {money(ud['balance'])}\n"
            f"Holdings:      {money(hv)}\n"
            f"Total Equity:  {money(eq)}\n\n"
            f"Unrealized:  {pnl(upnl)}\n"
            f"Realized:    {pnl(ud['realized_pnl'])}\n"
            f"Total PnL:   {pnl(eq - STARTING_BALANCE)}",
            reply_markup=back_main()
        )

    # ── Stats ──
    elif cb == "show_stats":
        logs = trade_log.get(u.id, [])
        if not logs:
            await q.edit_message_text(
                "No closed trades yet.\nStats appear after your first closed position.",
                reply_markup=back_main()
            )
            return
        wins   = [t for t in logs if t["realized_pnl"] > 0]
        losses = [t for t in logs if t["realized_pnl"] <= 0]
        total  = len(logs)
        wr     = len(wins) / total * 100
        aw     = sum(t["realized_pnl"] for t in wins) / len(wins) if wins else 0
        al     = sum(t["realized_pnl"] for t in losses) / len(losses) if losses else 0
        ah     = sum(t["hold_h"] for t in logs) / total
        best   = max(logs, key=lambda t: t["realized_pnl"])
        worst  = min(logs, key=lambda t: t["realized_pnl"])
        bestx  = max(logs, key=lambda t: t.get("x", 0))
        tpnl   = sum(t["realized_pnl"] for t in logs)
        rf, rb = ud.get("followed", 0), ud.get("broken", 0)
        dr     = rf / (rf + rb) * 100 if (rf + rb) > 0 else 0
        await q.edit_message_text(
            f"TRADING STATS\n\n"
            f"Trades: {total}  ({len(wins)}W / {len(losses)}L)\n"
            f"Win Rate: {wr:.1f}%\n"
            f"Avg Win:  {money(aw)}\n"
            f"Avg Loss: {money(al)}\n"
            f"Total PnL: {pnl(tpnl)}\n\n"
            f"Best Trade:  {pnl(best['realized_pnl'])}  (${best['symbol']})\n"
            f"Worst Trade: {pnl(worst['realized_pnl'])}  (${worst['symbol']})\n"
            f"Best X:      {bestx.get('x', 0):.2f}x  (${bestx['symbol']})\n\n"
            f"Avg Hold Time: {ah:.1f}h\n"
            f"Rules Followed: {rf}  |  Broken: {rb}\n"
            f"Discipline Rate: {dr:.0f}%\n"
            f"Best Streak: {ud.get('best_streak', 0)}\n"
            f"Current Streak: {ud.get('streak', 0)}",
            reply_markup=back_main()
        )

    # ── Review ──
    elif cb == "show_review":
        logs    = trade_log.get(u.id, [])
        week_ago = datetime.now() - timedelta(days=7)
        weekly  = [t for t in logs if t.get("closed_at", datetime.min) >= week_ago]
        if not weekly:
            await q.edit_message_text("No closed trades in last 7 days.", reply_markup=back_main())
            return
        wins  = [t for t in weekly if t["realized_pnl"] > 0]
        tpnl  = sum(t["realized_pnl"] for t in weekly)
        wr    = len(wins) / len(weekly) * 100
        lines = [f"WEEKLY REVIEW\n\n{len(weekly)} trades  |  WR: {wr:.0f}%  |  {pnl(tpnl)}\n"]
        for t in sorted(weekly, key=lambda x: x["closed_at"], reverse=True):
            j = f"\n  \"{t['journal'][:50]}\"" if t.get("journal") else ""
            fp = " [followed plan]" if t.get("followed_plan") else (" [sold early]" if t.get("followed_plan") is False else "")
            lines.append(f"${t['symbol']}  {pnl(t['realized_pnl'])}  {t.get('x', 0):.2f}x  {t['hold_h']}h{fp}{j}")
        await q.edit_message_text("\n".join(lines), reply_markup=back_main())

    # ── Leaderboard ──
    elif cb == "show_leaderboard":
        if not users:
            await q.edit_message_text("No traders yet.", reply_markup=back_main())
            return
        scores = []
        for uid2, d in users.items():
            hv = 0
            for c, h in d["holdings"].items():
                info = await get_token(c)
                if info: hv += h["amount"] * info["price"]
            eq   = d["balance"] + hv
            logs = trade_log.get(uid2, [])
            wr   = len([t for t in logs if t["realized_pnl"] > 0]) / len(logs) * 100 if logs else 0
            scores.append((d["username"], eq, eq - STARTING_BALANCE, wr))
        scores.sort(key=lambda x: x[1], reverse=True)
        places = ["1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th"]
        lines  = ["LEADERBOARD\n"]
        for i, (name, eq, ppnl_val, wr) in enumerate(scores[:10]):
            lines.append(f"{places[i]}  {name}\n     {money(eq)}  {pnl(ppnl_val)}  WR:{wr:.0f}%")
        await q.edit_message_text("\n".join(lines), reply_markup=back_main())

    # ── Settings ──
    elif cb == "show_settings":
        await q.edit_message_text("SETTINGS\n\nTap any setting to change it:", reply_markup=settings_keyboard(ud))

    elif cb in ("cfg_buy", "cfg_sell", "cfg_risk", "cfg_maxpos", "cfg_daily"):
        prompts = {
            "cfg_buy":    "Enter default buy amount in USD (e.g. 100):",
            "cfg_sell":   "Enter default sell - 50% or fixed amount like 200:",
            "cfg_risk":   "Enter max risk per trade as % (e.g. 10):",
            "cfg_maxpos": "Enter max open positions (e.g. 5):",
            "cfg_daily":  "Enter max trades per day (e.g. 10):",
        }
        pending[u.id] = {"action": cb}
        await q.edit_message_text(prompts[cb], reply_markup=cancel_kb())

    # ── Reset ──
    elif cb == "reset_prompt":
        await q.edit_message_text(
            "RESET ACCOUNT\n\nThis will wipe all holdings, stats and restore $5,000.\n\nAre you sure?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes, Reset", callback_data=f"reset_confirm_{u.id}"),
                 InlineKeyboardButton("Cancel",     callback_data="main_menu")],
            ])
        )

    elif cb == f"reset_confirm_{u.id}":
        ud.update({
            "balance": STARTING_BALANCE, "holdings": {}, "realized_pnl": 0.0,
            "joined_at": datetime.now(), "preset_buy": None, "preset_sell": None,
            "risk_pct": None, "max_positions": None, "daily_limit": None,
            "daily_trades": 0, "last_day": None,
            "planned": 0, "impulse": 0, "followed": 0, "broken": 0, "streak": 0, "best_streak": 0,
        })
        trade_log[u.id] = []
        pending.pop(u.id, None)
        await q.edit_message_text(
            f"Account reset. Starting balance: {money(STARTING_BALANCE)}",
            reply_markup=main_menu_keyboard()
        )


# ── Autosell helpers ───────────────────────────────────────────────────────────
def _set_autosell(ud, contract, targets):
    if contract in ud["holdings"]:
        ud["holdings"][contract]["auto_sells"] = targets


async def _confirm_autosell(q, ud, contract):
    if contract not in ud["holdings"]:
        await q.edit_message_text("Position not found.", reply_markup=back_main())
        return
    h = ud["holdings"][contract]
    avg = h["avg_price"]
    targets = h.get("auto_sells", [])
    lines = [f"Auto-sells set for ${h['symbol']}:\n"]
    for t in targets:
        lines.append(f"  Sell {t['pct']*100:.0f}% at {t['x']}x  (~{money(avg * t['x'])})")
    lines.append(f"\nChecked every {PRICE_CHECK_INTERVAL}s automatically.")
    await q.edit_message_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back to Token", callback_data=f"back_token_{contract}")]])
    )


# ── Core buy logic ────────────────────────────────────────────────────────────
async def _buy_core(ud, uid, contract, usd_amount, planned=True):
    if not check_daily(ud):
        return f"Daily limit of {ud['daily_limit']} trades reached. Resets tomorrow."
    mp = ud.get("max_positions")
    if mp and len(ud["holdings"]) >= mp:
        return f"Max positions ({mp}) reached. Close a position first."
    rsk = ud.get("risk_pct")
    if rsk:
        hv = 0
        for c, h in ud["holdings"].items():
            info = await get_token(c)
            if info: hv += h["amount"] * info["price"]
        max_allowed = (ud["balance"] + hv) * rsk / 100
        if usd_amount > max_allowed:
            ud["broken"] += 1
            ud["streak"] = 0
            return f"Risk limit hit! Max {money(max_allowed)} per trade ({rsk}% rule)."
    if usd_amount > ud["balance"]:
        return f"Insufficient balance. You have {money(ud['balance'])}."
    info = await get_token(contract)
    if not info:
        return "Token not found on DexScreener (Solana)."
    token_amount = usd_amount / info["price"]
    ud["balance"] -= usd_amount
    ud["daily_trades"] += 1
    if contract in ud["holdings"]:
        h = ud["holdings"][contract]
        nt = h["total_invested"] + usd_amount
        na = h["amount"] + token_amount
        h["avg_price"]       = nt / na
        h["amount"]          = na
        h["total_invested"]  = nt
    else:
        ud["holdings"][contract] = {
            "symbol":        info["symbol"],
            "name":          info["name"],
            "amount":        token_amount,
            "avg_price":     info["price"],
            "total_invested": usd_amount,
            "auto_sells":    [],
            "stop_loss_pct": None,
            "bought_at":     datetime.now(),
            "journal":       "",
            "planned":       planned,
            "followed_plan": None,
        }
    if planned:
        ud["planned"] += 1
    else:
        ud["impulse"] += 1
    return info, token_amount


async def execute_buy(update, ud, uid, contract, usd_amount, planned=True):
    msg = await update.message.reply_text("Executing buy...")
    result = await _buy_core(ud, uid, contract, usd_amount, planned)
    if isinstance(result, str):
        await msg.edit_text(result, reply_markup=main_menu_keyboard())
        return
    info, token_amount = result
    liq_warn = "\n!! LOW LIQUIDITY !!" if info["liq"] < 50_000 else ""
    await msg.edit_text(
        f"BUY EXECUTED\n\n"
        f"*{info['name']} (${info['symbol']})*\n"
        f"Spent:    {money(usd_amount)}\n"
        f"Got:      {token_amount:,.4f} {info['symbol']}\n"
        f"Price:    {money(info['price'])}\n"
        f"MC:       {mc_str(info['mc'])}\n"
        f"Liq:      {money(info['liq'])}\n"
        f"Cash left: {money(ud['balance'])}{liq_warn}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Auto-Sell",  callback_data=f"as_menu_{contract}"),
             InlineKeyboardButton("Stop Loss",  callback_data=f"sl_menu_{contract}")],
            [InlineKeyboardButton("Journal",    callback_data=f"journal_{contract}"),
             InlineKeyboardButton("View Token", callback_data=f"back_token_{contract}")],
            [InlineKeyboardButton("-- Main Menu --", callback_data="main_menu")],
        ])
    )


async def execute_buy_query(q, ud, uid, contract, usd_amount, planned=True):
    await q.edit_message_text("Executing buy...")
    result = await _buy_core(ud, uid, contract, usd_amount, planned)
    if isinstance(result, str):
        await q.edit_message_text(result, reply_markup=main_menu_keyboard())
        return
    info, token_amount = result
    liq_warn = "\n!! LOW LIQUIDITY !!" if info["liq"] < 50_000 else ""
    await q.edit_message_text(
        f"BUY EXECUTED\n\n"
        f"*{info['name']} (${info['symbol']})*\n"
        f"Spent:    {money(usd_amount)}\n"
        f"Got:      {token_amount:,.4f} {info['symbol']}\n"
        f"Price:    {money(info['price'])}\n"
        f"MC:       {mc_str(info['mc'])}\n"
        f"Liq:      {money(info['liq'])}\n"
        f"Cash left: {money(ud['balance'])}{liq_warn}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Auto-Sell",  callback_data=f"as_menu_{contract}"),
             InlineKeyboardButton("Stop Loss",  callback_data=f"sl_menu_{contract}")],
            [InlineKeyboardButton("Journal",    callback_data=f"journal_{contract}"),
             InlineKeyboardButton("View Token", callback_data=f"back_token_{contract}")],
            [InlineKeyboardButton("-- Main Menu --", callback_data="main_menu")],
        ])
    )


# ── Core sell logic ───────────────────────────────────────────────────────────
async def execute_sell_query(q, ud, uid, contract, pct=None, usd=None):
    if contract not in ud["holdings"]:
        await q.edit_message_text("Position not found.", reply_markup=back_main())
        return
    info = await get_token(contract)
    if not info:
        await q.edit_message_text("Price unavailable.", reply_markup=back_main())
        return
    h = ud["holdings"][contract]
    cv = h["amount"] * info["price"]
    usd_amount = cv * pct if pct is not None else min(usd, cv)
    usd_amount = min(usd_amount, cv)
    if usd_amount <= 0:
        await q.edit_message_text("Invalid sell amount.", reply_markup=back_main())
        return
    targets_pending = [t for t in h.get("auto_sells", []) if not t["triggered"]]
    if targets_pending:
        ud["broken"] += 1
        ud["streak"] = 0
        h["followed_plan"] = False
    else:
        ud["followed"] += 1
        ud["streak"] += 1
        ud["best_streak"] = max(ud["best_streak"], ud["streak"])
    ud["daily_trades"] += 1
    result = do_sell_core(ud, uid, contract, usd_amount, info["price"])
    cx = info["price"] / h.get("avg_price", info["price"])
    warn = "\n[Sold before auto-sell targets - rule broken]" if targets_pending else ""
    await q.edit_message_text(
        f"SELL EXECUTED\n\n"
        f"${h['symbol']}\n"
        f"Received: {money(result['received'])}\n"
        f"Price:    {money(info['price'])}  |  {cx:.2f}x\n"
        f"Held:     {result['hold_h']}h\n"
        f"PnL:      {pnl(result['realized'])}\n"
        f"Cash:     {money(ud['balance'])}{warn}",
        reply_markup=main_menu_keyboard()
    )


async def execute_sell_msg(update, ud, uid, contract, pct=None, usd=None):
    info = await get_token(contract)
    if not info:
        await update.message.reply_text("Price unavailable.", reply_markup=back_main())
        return
    h = ud["holdings"][contract]
    cv = h["amount"] * info["price"]
    usd_amount = cv * pct if pct is not None else min(usd, cv)
    usd_amount = min(usd_amount, cv)
    targets_pending = [t for t in h.get("auto_sells", []) if not t["triggered"]]
    if targets_pending:
        ud["broken"] += 1; ud["streak"] = 0
    else:
        ud["followed"] += 1; ud["streak"] += 1
        ud["best_streak"] = max(ud["best_streak"], ud["streak"])
    ud["daily_trades"] += 1
    result = do_sell_core(ud, uid, contract, usd_amount, info["price"])
    cx = info["price"] / h.get("avg_price", info["price"])
    await update.message.reply_text(
        f"SELL EXECUTED\n\n"
        f"${h['symbol']}\n"
        f"Received: {money(result['received'])}\n"
        f"Price:    {money(info['price'])}  |  {cx:.2f}x\n"
        f"PnL:      {pnl(result['realized'])}\n"
        f"Cash:     {money(ud['balance'])}",
        reply_markup=main_menu_keyboard()
    )


# ── Job ────────────────────────────────────────────────────────────────────────
async def auto_sell_job(ctx: ContextTypes.DEFAULT_TYPE):
    await check_auto_sells(ctx.application)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_start))
    app.add_handler(CallbackQueryHandler(btn))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.job_queue.run_repeating(auto_sell_job, interval=PRICE_CHECK_INTERVAL, first=10)
    logger.info("Bot running - Maestro style UI")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
