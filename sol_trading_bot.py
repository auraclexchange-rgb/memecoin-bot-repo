#!/usr/bin/env python3
"""AURACLE_XBOT - Advanced Paper Trading Bot"""

import logging
import os
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    CallbackQueryHandler, MessageHandler, filters
)
import httpx

BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens/{}"
PRICE_CHECK_INTERVAL = 30
MAX_BALANCE = 10_000.0
MIN_BALANCE = 1.0

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

users: dict = {}
trade_log: dict = {}
pending: dict = {}

async def get_token(contract: str) -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(DEXSCREENER_API.format(contract))
            data = r.json()
        pairs = data.get("pairs") or []
        if not pairs:
            return None
        best = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        price = float(best.get("priceUsd") or 0)
        if not price:
            return None
        mc = float(best.get("marketCap") or best.get("fdv") or 0)
        liq = float(best.get("liquidity", {}).get("usd", 0) or 0)
        liq_pct = round((liq / mc * 100) if mc > 0 else 0, 2)
        vol_h24 = float(best.get("volume", {}).get("h24", 0) or 0)
        vol_h1  = float(best.get("volume", {}).get("h1", 0) or 0)
        vol_m5  = float(best.get("volume", {}).get("m5", 0) or 0)
        txns = best.get("txns", {})
        buys  = int(txns.get("h24", {}).get("buys",  0) or 0)
        sells = int(txns.get("h24", {}).get("sells", 0) or 0)
        total_tx = buys + sells
        buy_pct = round(buys / total_tx * 100) if total_tx > 0 else 50
        pair_created = best.get("pairCreatedAt")
        age_h = None
        if pair_created:
            age_h = (datetime.now() - datetime.fromtimestamp(pair_created / 1000)).total_seconds() / 3600
        ch = best.get("priceChange", {})
        return {
            "symbol":  best.get("baseToken", {}).get("symbol", "???"),
            "name":    best.get("baseToken", {}).get("name", "Unknown"),
            "chain":   best.get("chainId", "unknown"),
            "dex":     best.get("dexId", "unknown"),
            "price":   price,
            "mc":      mc,
            "liq":     liq,
            "liq_pct": liq_pct,
            "vol_h24": vol_h24,
            "vol_h1":  vol_h1,
            "vol_m5":  vol_m5,
            "ch_m5":   float(ch.get("m5",  0) or 0),
            "ch_h1":   float(ch.get("h1",  0) or 0),
            "ch_h6":   float(ch.get("h6",  0) or 0),
            "ch_h24":  float(ch.get("h24", 0) or 0),
            "buys":    buys,
            "sells":   sells,
            "buy_pct": buy_pct,
            "age_h":   age_h,
        }
    except Exception as e:
        logger.error(f"DexScreener: {e}")
        return None


def score_token(info: dict) -> dict:
    score = 0
    strengths = []
    warnings = []

    liq = info.get("liq", 0)
    if liq >= 100_000:
        score += 15
        strengths.append("Strong liquidity (>$100K)")
    elif liq >= 50_000:
        score += 10
        strengths.append("Good liquidity (>$50K)")
    elif liq >= 20_000:
        score += 5
        warnings.append("Low liquidity (<$50K)")
    else:
        warnings.append("Very low liquidity - HIGH RISK")

    liq_pct = info.get("liq_pct", 0)
    if liq_pct >= 5:
        score += 8
        strengths.append("High liquidity ratio")
    elif liq_pct >= 2:
        score += 4
    else:
        warnings.append("Low liquidity ratio")

    age_h = info.get("age_h")
    if age_h is not None:
        if age_h < 1:
            warnings.append("Less than 1 hour old - EXTREME RISK")
        elif age_h < 24:
            score += 3
            warnings.append("New token (under 24h)")
        elif age_h < 168:
            score += 5
            strengths.append("Token age: " + str(round(age_h/24, 1)) + " days")
        else:
            score += 7
            strengths.append("Established token")

    if info.get("ch_m5", 0) > 0 and info.get("ch_h1", 0) > 0:
        score += 10
        strengths.append("Positive momentum (5m & 1h)")
    elif info.get("ch_m5", 0) > 0 or info.get("ch_h1", 0) > 0:
        score += 5
    else:
        warnings.append("Negative short-term momentum")

    buy_pct = info.get("buy_pct", 50)
    if buy_pct >= 65:
        score += 10
        strengths.append("Strong buy pressure (" + str(buy_pct) + "% buys)")
    elif buy_pct >= 55:
        score += 5
    else:
        warnings.append("Sell pressure (" + str(100 - buy_pct) + "% sells)")

    if info.get("vol_h24", 0) >= 500_000:
        score += 5
        strengths.append("Very high volume")
    elif info.get("vol_h24", 0) >= 100_000:
        score += 3

    mc = info.get("mc", 0)
    if 100_000 <= mc <= 10_000_000:
        score += 15
        strengths.append("Sweet spot MC")
    elif 10_000_000 < mc <= 100_000_000:
        score += 8
    elif mc < 100_000:
        score += 3
        warnings.append("Very low MC - ultra risky")
    else:
        score += 5
        warnings.append("High MC - less upside")

    total_tx = info.get("buys", 0) + info.get("sells", 0)
    if total_tx >= 1000:
        score += 10
        strengths.append("High transaction count")
    elif total_tx >= 500:
        score += 7
    elif total_tx >= 100:
        score += 3
    else:
        warnings.append("Low transaction count")

    if liq < 50_000:
        score = max(0, score - 10)
    if age_h is not None and age_h < 1:
        score = max(0, score - 10)
    if buy_pct < 40:
        score = max(0, score - 5)

    score = min(score, 100)

    if score >= 80:
        verdict = "STRONG BUY"
        icon = "[GREEN]"
    elif score >= 60:
        verdict = "GOOD TRADE"
        icon = "[YELLOW]"
    elif score >= 40:
        verdict = "RISKY - CAUTION"
        icon = "[ORANGE]"
    else:
        verdict = "AVOID"
        icon = "[RED]"

    return {
        "score":     score,
        "verdict":   verdict,
        "icon":      icon,
        "strengths": strengths[:3],
        "warnings":  warnings[:3],
    }


def get_user(uid: int, uname: str) -> dict:
    if uid not in users:
        users[uid] = {
            "username":         uname or "User" + str(uid),
            "balance":          None,
            "starting_balance": None,
            "savings":          0.0,
            "auto_save_pct":    None,
            "holdings":         {},
            "realized_pnl":     0.0,
            "limit_orders":     [],
            "price_alerts":     [],
            "joined_at":        datetime.now(),
            "preset_buy":       None,
            "preset_sell":      None,
            "risk_pct":         None,
            "max_positions":    None,
            "daily_limit":      None,
            "daily_trades":     0,
            "last_day":         None,
            "planned":          0,
            "impulse":          0,
            "followed":         0,
            "broken":           0,
            "streak":           0,
            "best_streak":      0,
            "target_equity":    None,
            "peak_equity":      0.0,
            "max_drawdown":     0.0,
            "consec_losses":    0,
            "trade_hours":      {},
        }
        trade_log[uid] = []
    return users[uid]


def money(n: float) -> str:
    if abs(n) >= 1_000_000_000:
        return "$" + str(round(n/1_000_000_000, 2)) + "B"
    if abs(n) >= 1_000_000:
        return "$" + str(round(n/1_000_000, 2)) + "M"
    if abs(n) >= 1_000:
        return "${:,.2f}".format(n)
    if abs(n) >= 1:
        return "${:.4f}".format(n)
    return "${:.8f}".format(n).rstrip("0").rstrip(".")


def mc_str(n: float) -> str:
    if n >= 1_000_000_000:
        return "$" + str(round(n/1_000_000_000, 2)) + "B"
    if n >= 1_000_000:
        return "$" + str(round(n/1_000_000, 2)) + "M"
    if n >= 1_000:
        return "$" + str(round(n/1_000)) + "K"
    return "$" + str(round(n))


def pstr(n: float) -> str:
    if n >= 0:
        return "+" + money(n)
    return "-" + money(abs(n))


def age_str(h: float) -> str:
    if h < 1:
        return str(round(h*60)) + "m"
    if h < 24:
        return str(round(h, 1)) + "h"
    return str(round(h/24, 1)) + "d"


def chain_icon(c: str) -> str:
    icons = {
        "solana": "SOL", "ethereum": "ETH", "bsc": "BNB",
        "base": "BASE", "arbitrum": "ARB", "polygon": "MATIC",
        "avalanche": "AVAX", "sui": "SUI"
    }
    return icons.get(c.lower(), c.upper())


def check_daily(d: dict) -> bool:
    today = datetime.now().date()
    if d["last_day"] != today:
        d["daily_trades"] = 0
        d["last_day"] = today
    lim = d.get("daily_limit")
    return not (lim and d["daily_trades"] >= lim)


def sell_core(ud: dict, uid: int, contract: str, usd: float, price: float, reason: str = "manual") -> dict:
    h = ud["holdings"][contract]
    tokens = usd / price
    ratio = min(tokens / h["amount"], 1.0) if h["amount"] > 0 else 1.0
    cost = h["total_invested"] * ratio
    realized = usd - cost
    ud["realized_pnl"] += realized
    ud["balance"] += usd
    h["amount"] -= tokens
    h["total_invested"] = max(0, h["total_invested"] - cost)
    hold_h = (datetime.now() - h.get("bought_at", datetime.now())).total_seconds() / 3600
    auto_saved = 0.0

    if realized > 0 and ud.get("auto_save_pct"):
        save_amt = realized * ud["auto_save_pct"] / 100
        if save_amt > 0 and ud["balance"] >= save_amt:
            ud["balance"] -= save_amt
            ud["savings"] += save_amt
            auto_saved = save_amt

    hour = str(datetime.now().hour)
    if hour not in ud["trade_hours"]:
        ud["trade_hours"][hour] = {"wins": 0, "losses": 0, "pnl": 0.0}
    ud["trade_hours"][hour]["pnl"] += realized
    if realized > 0:
        ud["trade_hours"][hour]["wins"] += 1
        ud["consec_losses"] = 0
    else:
        ud["trade_hours"][hour]["losses"] += 1
        ud["consec_losses"] = ud.get("consec_losses", 0) + 1

    closed = False
    if h["amount"] < 0.000001:
        x_val = price / h["avg_price"] if h.get("avg_price", 0) > 0 else 0
        trade_log.setdefault(uid, []).append({
            "symbol":        h["symbol"],
            "contract":      contract,
            "chain":         h.get("chain", "unknown"),
            "invested":      h["total_invested"] + cost,
            "returned":      usd,
            "realized_pnl":  realized,
            "x":             x_val,
            "hold_h":        round(hold_h, 1),
            "reason":        reason,
            "closed_at":     datetime.now(),
            "journal":       h.get("journal", ""),
            "mood":          h.get("mood", ""),
            "planned":       h.get("planned", True),
            "followed_plan": h.get("followed_plan", None),
            "auto_saved":    auto_saved,
        })
        del ud["holdings"][contract]
        closed = True

    return {
        "received":   usd,
        "realized":   realized,
        "closed":     closed,
        "hold_h":     round(hold_h, 1),
        "auto_saved": auto_saved,
    }


async def portfolio_val(ud: dict) -> tuple:
    tv, tc = 0.0, 0.0
    for c, h in ud["holdings"].items():
        info = await get_token(c)
        if info:
            tv += h["amount"] * info["price"]
            tc += h["total_invested"]
    return tv, tv - tc


def token_card(info: dict, contract: str, ud: dict, sc: dict = None) -> str:
    def fc(v):
        v = float(v or 0)
        return ("+" if v >= 0 else "") + str(round(v, 1)) + "%"

    held = contract in ud["holdings"]
    pos_line = ""
    if held:
        h = ud["holdings"][contract]
        cv = h["amount"] * info["price"]
        cx = info["price"] / h["avg_price"] if h.get("avg_price", 0) > 0 else 0
        ppnl = cv - h["total_invested"]
        pos_line = "\n\n-- YOUR POSITION --\nValue: " + money(cv) + "  |  " + str(round(cx, 2)) + "x\nPnL: " + pstr(ppnl)

    age_line = ("Age: " + age_str(info["age_h"]) + "  |  ") if info.get("age_h") is not None else ""
    liq_warn = "\n\nWARNING: LOW LIQUIDITY - HIGH RISK" if info["liq"] < 50_000 else ""

    sc_line = ""
    if sc:
        sc_line = "\n\n-- AURACLE SCORE: " + str(sc["score"]) + "/100 --\n" + sc["icon"] + " " + sc["verdict"]
        if sc["strengths"]:
            sc_line += "\nStrengths: " + " | ".join(sc["strengths"])
        if sc["warnings"]:
            sc_line += "\nWarnings: " + " | ".join(sc["warnings"])

    return (
        "*" + info["name"] + " ($" + info["symbol"] + ")*\n"
        + chain_icon(info["chain"]) + "  |  " + info["dex"].upper() + "\n"
        + "`" + contract + "`\n\n"
        + "Price: *" + money(info["price"]) + "*\n"
        + "MC: *" + mc_str(info["mc"]) + "*\n"
        + "Liq: *" + money(info["liq"]) + "* (" + str(info["liq_pct"]) + "%)\n\n"
        + "5m: " + fc(info["ch_m5"]) + "  1h: " + fc(info["ch_h1"]) + "  6h: " + fc(info["ch_h6"]) + "  24h: " + fc(info["ch_h24"]) + "\n\n"
        + "Vol 24h: *" + money(info["vol_h24"]) + "*\n"
        + age_line
        + "Buys: " + str(info["buys"]) + " (" + str(info["buy_pct"]) + "%)  |  Sells: " + str(info["sells"]) + " (" + str(100 - info["buy_pct"]) + "%)"
        + pos_line + sc_line + liq_warn
    )


def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Positions",      callback_data="v_pos"),
         InlineKeyboardButton("🕐 Active Orders",  callback_data="v_orders")],
        [InlineKeyboardButton("📜 Trade History",  callback_data="v_history"),
         InlineKeyboardButton("💰 Savings",        callback_data="v_savings")],
        [InlineKeyboardButton("📈 Stats",          callback_data="v_stats"),
         InlineKeyboardButton("📅 Weekly Review",  callback_data="v_review")],
        [InlineKeyboardButton("🏆 Leaderboard",    callback_data="v_leader"),
         InlineKeyboardButton("🔔 Price Alerts",   callback_data="v_alerts")],
        [InlineKeyboardButton("⚙️ Settings",       callback_data="v_settings"),
         InlineKeyboardButton("👤 Profile",        callback_data="v_profile")],
        [InlineKeyboardButton("⚡ BUY & SELL NOW!", callback_data="v_trade")],
    ])


def buy_kb(contract: str, ud: dict) -> InlineKeyboardMarkup:
    pb = ud.get("preset_buy")
    preset_lbl = "⚡ Buy $" + str(int(pb)) + " [PRESET]" if pb else "⚡ Set Preset First"
    held = contract in ud["holdings"]
    rows = []
    if held:
        rows.append([
            InlineKeyboardButton("🔄 Refresh",     callback_data="rf_" + contract),
            InlineKeyboardButton("🔴 Go to Sell",  callback_data="gos_" + contract),
        ])
    else:
        rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="rf_" + contract)])
    rows.append([InlineKeyboardButton(preset_lbl, callback_data="bp_" + contract)])
    rows.append([
        InlineKeyboardButton("$50",        callback_data="ba_50_" + contract),
        InlineKeyboardButton("$100",       callback_data="ba_100_" + contract),
        InlineKeyboardButton("$250",       callback_data="ba_250_" + contract),
    ])
    rows.append([
        InlineKeyboardButton("$500",       callback_data="ba_500_" + contract),
        InlineKeyboardButton("$1000",      callback_data="ba_1000_" + contract),
        InlineKeyboardButton("Custom",     callback_data="bc_" + contract),
    ])
    rows.append([
        InlineKeyboardButton("🎯 Limit Buy",    callback_data="lbo_" + contract),
        InlineKeyboardButton("🔔 Price Alert",  callback_data="pal_" + contract),
    ])
    if held:
        h = ud["holdings"][contract]
        as_lbl = "🎯 Auto-Sell [SET]" if h.get("auto_sells") else "🎯 Auto-Sell"
        sl_lbl = "🛑 Stop Loss [SET]" if h.get("stop_loss_pct") else "🛑 Stop Loss"
        rows.append([
            InlineKeyboardButton(as_lbl, callback_data="asm_" + contract),
            InlineKeyboardButton(sl_lbl, callback_data="slm_" + contract),
        ])
        rows.append([
            InlineKeyboardButton("📝 Journal",      callback_data="jnl_" + contract),
            InlineKeyboardButton("👁 View Targets",  callback_data="vtg_" + contract),
        ])
        rows.append([InlineKeyboardButton("🎯 Limit Sell", callback_data="lso_" + contract)])
    rows.append([InlineKeyboardButton("🏠 Main Menu", callback_data="mm")])
    return InlineKeyboardMarkup(rows)


def sell_kb(contract: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("25%",  callback_data="sp_25_" + contract),
         InlineKeyboardButton("50%",  callback_data="sp_50_" + contract),
         InlineKeyboardButton("75%",  callback_data="sp_75_" + contract),
         InlineKeyboardButton("100%", callback_data="sp_100_" + contract)],
        [InlineKeyboardButton("Custom Amount",  callback_data="sca_" + contract)],
        [InlineKeyboardButton("🎯 Limit Sell",  callback_data="lso_" + contract)],
        [InlineKeyboardButton("Back to Token",  callback_data="btt_" + contract)],
        [InlineKeyboardButton("🏠 Main Menu",   callback_data="mm")],
    ])


def settings_kb(ud: dict) -> InlineKeyboardMarkup:
    pb  = "$" + str(int(ud["preset_buy"])) if ud.get("preset_buy") else "not set"
    ps  = str(ud["preset_sell"]) if ud.get("preset_sell") else "not set"
    rsk = str(ud["risk_pct"]) + "%" if ud.get("risk_pct") else "not set"
    mp  = str(ud["max_positions"]) if ud.get("max_positions") else "not set"
    dl  = str(ud["daily_limit"]) if ud.get("daily_limit") else "not set"
    asp = str(ud["auto_save_pct"]) + "%" if ud.get("auto_save_pct") else "not set"
    tgt = money(ud["target_equity"]) if ud.get("target_equity") else "not set"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Default Buy: " + pb,       callback_data="cfg_buy")],
        [InlineKeyboardButton("Default Sell: " + ps,      callback_data="cfg_sell")],
        [InlineKeyboardButton("Max Risk/Trade: " + rsk,   callback_data="cfg_risk")],
        [InlineKeyboardButton("Max Positions: " + mp,     callback_data="cfg_maxpos")],
        [InlineKeyboardButton("Daily Limit: " + dl,       callback_data="cfg_daily")],
        [InlineKeyboardButton("Auto-Save: " + asp,        callback_data="cfg_autosave")],
        [InlineKeyboardButton("Target Equity: " + tgt,    callback_data="cfg_target")],
        [InlineKeyboardButton("Reset Account",            callback_data="rst_prompt")],
        [InlineKeyboardButton("Back",                     callback_data="mm")],
    ])


def back_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu", callback_data="mm")]])


def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Cancel", callback_data="mm")]])


def buy_done_kb(contract: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 Auto-Sell", callback_data="asm_" + contract),
         InlineKeyboardButton("🛑 Stop Loss", callback_data="slm_" + contract)],
        [InlineKeyboardButton("📝 Journal",   callback_data="jnl_" + contract),
         InlineKeyboardButton("View Token",   callback_data="btt_" + contract)],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="mm")],
    ])


async def run_checker(app: Application):
    for uid, ud in list(users.items()):
        if ud.get("balance") is None:
            continue

        # Price alerts
        for alert in list(ud.get("price_alerts", [])):
            if alert.get("triggered"):
                continue
            info = await get_token(alert["contract"])
            if not info:
                continue
            hit = (
                (alert["direction"] == "above" and info["price"] >= alert["target"]) or
                (alert["direction"] == "below" and info["price"] <= alert["target"])
            )
            if hit:
                alert["triggered"] = True
                try:
                    await app.bot.send_message(
                        chat_id=uid, parse_mode="Markdown",
                        text=(
                            "🔔 *PRICE ALERT*\n\n"
                            "*$" + alert["symbol"] + "* hit your target!\n"
                            "Price: *" + money(info["price"]) + "*\n"
                            "Target: *" + money(alert["target"]) + "*"
                        ),
                        reply_markup=main_menu_kb()
                    )
                except Exception as e:
                    logger.error(e)
        ud["price_alerts"] = [a for a in ud.get("price_alerts", []) if not a.get("triggered")]

        # Limit orders
        for order in list(ud.get("limit_orders", [])):
            if order.get("triggered") or order.get("cancelled"):
                continue
            info = await get_token(order["contract"])
            if not info:
                continue
            price = info["price"]

            if order["type"] == "buy" and price <= order["target_price"]:
                order["triggered"] = True
                if ud["balance"] >= order["amount"]:
                    amt = order["amount"]
                    tokens = amt / price
                    c = order["contract"]
                    ud["balance"] -= amt
                    if c in ud["holdings"]:
                        h = ud["holdings"][c]
                        nt = h["total_invested"] + amt
                        na = h["amount"] + tokens
                        h["avg_price"] = nt / na
                        h["amount"] = na
                        h["total_invested"] = nt
                    else:
                        ud["holdings"][c] = {
                            "symbol": info["symbol"], "name": info["name"],
                            "chain": info["chain"], "amount": tokens,
                            "avg_price": price, "total_invested": amt,
                            "auto_sells": [], "stop_loss_pct": None,
                            "bought_at": datetime.now(), "journal": "",
                            "mood": "", "planned": True, "followed_plan": None,
                        }
                    try:
                        await app.bot.send_message(
                            chat_id=uid, parse_mode="Markdown",
                            text=(
                                "✅ *LIMIT BUY EXECUTED*\n\n"
                                "*$" + info["symbol"] + "* hit " + money(order["target_price"]) + "\n"
                                "Bought: " + money(amt) + "\n"
                                "Price: " + money(price) + "\n"
                                "Cash left: " + money(ud["balance"])
                            ),
                            reply_markup=main_menu_kb()
                        )
                    except Exception as e:
                        logger.error(e)

            elif order["type"] == "sell" and order["contract"] in ud["holdings"]:
                if price >= order["target_price"]:
                    order["triggered"] = True
                    h = ud["holdings"][order["contract"]]
                    cv = h["amount"] * price
                    sell_amt = min(order["amount"], cv)
                    result = sell_core(ud, uid, order["contract"], sell_amt, price, "limit_sell")
                    try:
                        await app.bot.send_message(
                            chat_id=uid, parse_mode="Markdown",
                            text=(
                                "✅ *LIMIT SELL EXECUTED*\n\n"
                                "*$" + info["symbol"] + "* hit " + money(order["target_price"]) + "\n"
                                "Sold: " + money(sell_amt) + "\n"
                                "Price: " + money(price) + "\n"
                                "PnL: " + pstr(result["realized"]) + "\n"
                                "Cash: " + money(ud["balance"])
                            ),
                            reply_markup=main_menu_kb()
                        )
                    except Exception as e:
                        logger.error(e)

        ud["limit_orders"] = [
            o for o in ud.get("limit_orders", [])
            if not o.get("triggered") and not o.get("cancelled")
        ]

        # Auto-sells and stop losses
        for contract, h in list(ud["holdings"].items()):
            info = await get_token(contract)
            if not info:
                continue
            price = info["price"]
            avg = h.get("avg_price", price)
            cx = price / avg if avg > 0 else 0

            sl = h.get("stop_loss_pct")
            if sl:
                drop = (price - avg) / avg * 100
                if drop <= -sl:
                    cv = h["amount"] * price
                    result = sell_core(ud, uid, contract, cv, price, "stop_loss")
                    ud["followed"] += 1
                    ud["streak"] += 1
                    ud["best_streak"] = max(ud["best_streak"], ud["streak"])
                    txt = (
                        "🛑 *STOP LOSS HIT*\n\n"
                        "*$" + h["symbol"] + "* dropped " + str(round(drop, 1)) + "%\n"
                        "Sold 100% → " + money(result["received"]) + "\n"
                        "PnL: " + pstr(result["realized"]) + "\n"
                        "Cash: " + money(ud["balance"])
                    )
                    if result["auto_saved"] > 0:
                        txt += "\nAuto-saved: " + money(result["auto_saved"])
                    try:
                        await app.bot.send_message(chat_id=uid, parse_mode="Markdown", text=txt, reply_markup=main_menu_kb())
                    except Exception as e:
                        logger.error(e)
                    continue

            for t in sorted([a for a in h.get("auto_sells", []) if not a.get("triggered")], key=lambda a: a["x"]):
                if cx < t["x"] or contract not in ud["holdings"]:
                    break
                t["triggered"] = True
                cv = h["amount"] * price
                sv = cv * t["pct"]
                if sv < 0.001:
                    continue
                result = sell_core(ud, uid, contract, sv, price, "auto_sell")
                ud["followed"] += 1
                ud["streak"] += 1
                ud["best_streak"] = max(ud["best_streak"], ud["streak"])
                txt = (
                    "🤖 *AUTO-SELL TRIGGERED*\n\n"
                    "*$" + h["symbol"] + "* hit " + str(t["x"]) + "x!\n"
                    "Sold " + str(int(t["pct"]*100)) + "% → " + money(result["received"]) + "\n"
                    "Price: " + money(price) + "  |  " + str(round(cx, 2)) + "x\n"
                    "PnL: " + pstr(result["realized"]) + "\n"
                    "Cash: " + money(ud["balance"])
                )
                if result["auto_saved"] > 0:
                    txt += "\nAuto-saved: " + money(result["auto_saved"])
                try:
                    await app.bot.send_message(chat_id=uid, parse_mode="Markdown", text=txt, reply_markup=main_menu_kb())
                except Exception as e:
                    logger.error(e)

        if ud.get("consec_losses", 0) >= 3:
            ud["consec_losses"] = 0
            try:
                await app.bot.send_message(
                    chat_id=uid,
                    text="⚠️ 3 LOSSES IN A ROW\n\nTake a break. Emotional trading causes more losses.",
                    reply_markup=main_menu_kb()
                )
            except Exception:
                pass


async def daily_summary_job(ctx: ContextTypes.DEFAULT_TYPE):
    for uid, ud in list(users.items()):
        if ud.get("balance") is None:
            continue
        today = datetime.now().date()
        logs = trade_log.get(uid, [])
        today_trades = [t for t in logs if t.get("closed_at", datetime.min).date() == today]
        if not today_trades:
            continue
        wins = [t for t in today_trades if t["realized_pnl"] > 0]
        tpnl = sum(t["realized_pnl"] for t in today_trades)
        wr = round(len(wins)/len(today_trades)*100) if today_trades else 0
        try:
            await ctx.bot.send_message(
                chat_id=uid, parse_mode="Markdown",
                text=(
                    "📅 *DAILY SUMMARY*\n\n"
                    "Trades: " + str(len(today_trades)) + "  |  WR: " + str(wr) + "%\n"
                    "PnL: " + pstr(tpnl) + "\n"
                    "Cash: " + money(ud["balance"]) + "\n"
                    "Savings: " + money(ud["savings"])
                ),
                reply_markup=main_menu_kb()
            )
        except Exception:
            pass


async def checker_job(ctx: ContextTypes.DEFAULT_TYPE):
    await run_checker(ctx.application)


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    ud = get_user(u.id, u.username or u.first_name)
    if ud.get("balance") is None:
        pending[u.id] = {"action": "set_balance"}
        text = (
            "👋 Welcome to *AURACLE_XBOT*!\n\n"
            "Advanced multi-chain paper trading bot.\n\n"
            "Set your starting balance:\n"
            "Min: $1  |  Max: $10,000\n\n"
            "Enter your starting balance:"
        )
        if update.message:
            await update.message.reply_text(text, parse_mode="Markdown", reply_markup=cancel_kb())
        else:
            await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=cancel_kb())
        return
    text = (
        "⚡ *AURACLE_XBOT*\n\n"
        "Welcome back, *" + ud["username"] + "*!\n"
        "💰 Balance: *" + money(ud["balance"]) + "*\n"
        "💎 Savings: *" + money(ud["savings"]) + "*\n\n"
        "Paste any crypto CA to trade 👇"
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_menu_kb())
    else:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=main_menu_kb())


async def do_buy_core(ud: dict, uid: int, contract: str, usd_amount: float, planned: bool = True, mood: str = "") -> str | tuple:
    if not check_daily(ud):
        return "Daily limit of " + str(ud["daily_limit"]) + " trades reached."
    mp = ud.get("max_positions")
    if mp and len(ud["holdings"]) >= mp and contract not in ud["holdings"]:
        return "Max positions (" + str(mp) + ") reached. Close a position first."
    rsk = ud.get("risk_pct")
    if rsk:
        hv = sum(h["total_invested"] for h in ud["holdings"].values())
        max_allowed = (ud["balance"] + hv) * rsk / 100
        if usd_amount > max_allowed:
            ud["broken"] += 1
            ud["streak"] = 0
            return "Risk limit! Max " + money(max_allowed) + " per trade (" + str(rsk) + "% rule)."
    if usd_amount > ud["balance"]:
        return "Insufficient balance. You have " + money(ud["balance"]) + "."
    info = await get_token(contract)
    if not info:
        return "Token not found on DexScreener."
    tokens = usd_amount / info["price"]
    ud["balance"] -= usd_amount
    ud["daily_trades"] += 1
    if contract in ud["holdings"]:
        h = ud["holdings"][contract]
        nt = h["total_invested"] + usd_amount
        na = h["amount"] + tokens
        h["avg_price"] = nt / na
        h["amount"] = na
        h["total_invested"] = nt
    else:
        ud["holdings"][contract] = {
            "symbol":        info["symbol"],
            "name":          info["name"],
            "chain":         info["chain"],
            "amount":        tokens,
            "avg_price":     info["price"],
            "total_invested": usd_amount,
            "auto_sells":    [],
            "stop_loss_pct": None,
            "bought_at":     datetime.now(),
            "journal":       "",
            "mood":          mood,
            "planned":       planned,
            "followed_plan": None,
        }
    if planned:
        ud["planned"] += 1
    else:
        ud["impulse"] += 1
    return info, tokens


async def do_buy_msg(update, ud, uid, contract, amount, mood=""):
    msg = await update.message.reply_text("Executing buy...")
    result = await do_buy_core(ud, uid, contract, amount, mood=mood)
    if isinstance(result, str):
        await msg.edit_text(result, reply_markup=main_menu_kb())
        return
    info, tokens = result
    liq_warn = "\n\nWARNING: LOW LIQUIDITY" if info["liq"] < 50_000 else ""
    await msg.edit_text(
        "✅ *BUY EXECUTED*\n\n"
        "*" + info["name"] + " ($" + info["symbol"] + ")*\n"
        "Spent: *" + money(amount) + "*\n"
        "Got: *" + str(round(tokens, 4)) + " " + info["symbol"] + "*\n"
        "Price: *" + money(info["price"]) + "*\n"
        "MC: *" + mc_str(info["mc"]) + "*\n"
        "Liq: *" + money(info["liq"]) + "*\n"
        "Cash left: *" + money(ud["balance"]) + "*" + liq_warn,
        parse_mode="Markdown",
        reply_markup=buy_done_kb(contract)
    )


async def do_buy_query(q, ud, uid, contract, amount, mood=""):
    await q.edit_message_text("Executing buy...")
    result = await do_buy_core(ud, uid, contract, amount, mood=mood)
    if isinstance(result, str):
        await q.edit_message_text(result, reply_markup=main_menu_kb())
        return
    info, tokens = result
    liq_warn = "\n\nWARNING: LOW LIQUIDITY" if info["liq"] < 50_000 else ""
    await q.edit_message_text(
        "✅ *BUY EXECUTED*\n\n"
        "*" + info["name"] + " ($" + info["symbol"] + ")*\n"
        "Spent: *" + money(amount) + "*\n"
        "Got: *" + str(round(tokens, 4)) + " " + info["symbol"] + "*\n"
        "Price: *" + money(info["price"]) + "*\n"
        "MC: *" + mc_str(info["mc"]) + "*\n"
        "Liq: *" + money(info["liq"]) + "*\n"
        "Cash left: *" + money(ud["balance"]) + "*" + liq_warn,
        parse_mode="Markdown",
        reply_markup=buy_done_kb(contract)
    )


async def do_sell_query(q, ud, uid, contract, pct=None, usd=None):
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
    pending_targets = [t for t in h.get("auto_sells", []) if not t.get("triggered")]
    if pending_targets:
        ud["broken"] += 1
        ud["streak"] = 0
        h["followed_plan"] = False
    else:
        ud["followed"] += 1
        ud["streak"] += 1
        ud["best_streak"] = max(ud["best_streak"], ud["streak"])
    ud["daily_trades"] += 1
    avg = h.get("avg_price", info["price"])
    result = sell_core(ud, uid, contract, usd_amount, info["price"])
    cx = info["price"] / avg if avg > 0 else 0
    warn = "\n\nSold before auto-sell targets - rule broken" if pending_targets else ""
    save_line = "\nAuto-saved: " + money(result["auto_saved"]) if result["auto_saved"] > 0 else ""
    await q.edit_message_text(
        "✅ *SELL EXECUTED*\n\n"
        "Received: *" + money(result["received"]) + "*\n"
        "Price: *" + money(info["price"]) + "*  |  *" + str(round(cx, 2)) + "x*\n"
        "Held: *" + str(result["hold_h"]) + "h*\n"
        "PnL: *" + pstr(result["realized"]) + "*\n"
        "Cash: *" + money(ud["balance"]) + "*" + save_line + warn,
        parse_mode="Markdown",
        reply_markup=main_menu_kb()
    )


async def do_sell_msg(update, ud, uid, contract, pct=None, usd=None):
    info = await get_token(contract)
    if not info:
        await update.message.reply_text("Price unavailable.", reply_markup=back_main())
        return
    h = ud["holdings"][contract]
    cv = h["amount"] * info["price"]
    usd_amount = cv * pct if pct is not None else min(usd, cv)
    usd_amount = min(usd_amount, cv)
    pending_targets = [t for t in h.get("auto_sells", []) if not t.get("triggered")]
    if pending_targets:
        ud["broken"] += 1
        ud["streak"] = 0
    else:
        ud["followed"] += 1
        ud["streak"] += 1
        ud["best_streak"] = max(ud["best_streak"], ud["streak"])
    ud["daily_trades"] += 1
    avg = h.get("avg_price", info["price"])
    result = sell_core(ud, uid, contract, usd_amount, info["price"])
    cx = info["price"] / avg if avg > 0 else 0
    save_line = "\nAuto-saved: " + money(result["auto_saved"]) if result["auto_saved"] > 0 else ""
    await update.message.reply_text(
        "✅ *SELL EXECUTED*\n\n"
        "Received: *" + money(result["received"]) + "*\n"
        "Price: *" + money(info["price"]) + "*  |  *" + str(round(cx, 2)) + "x*\n"
        "PnL: *" + pstr(result["realized"]) + "*\n"
        "Cash: *" + money(ud["balance"]) + "*" + save_line,
        parse_mode="Markdown",
        reply_markup=main_menu_kb()
    )


async def text_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    ud = get_user(u.id, u.username or u.first_name)
    text = update.message.text.strip()
    p = pending.get(u.id)

    if p:
        action = p["action"]

        if action == "set_balance":
            try:
                amt = float(text.replace("$", "").replace(",", ""))
                assert MIN_BALANCE <= amt <= MAX_BALANCE
                ud["balance"] = amt
                ud["starting_balance"] = amt
                ud["peak_equity"] = amt
                pending.pop(u.id, None)
                await update.message.reply_text(
                    "✅ Starting balance: *" + money(amt) + "*\n\nPaste any contract address to start trading!",
                    parse_mode="Markdown", reply_markup=main_menu_kb()
                )
            except Exception:
                await update.message.reply_text(
                    "❌ Enter a number between $1 and $10,000\nExample: 5000",
                    reply_markup=cancel_kb()
                )
            return

        elif action == "cfg_buy":
            try:
                amt = float(text.replace("$", ""))
                assert amt > 0
                ud["preset_buy"] = amt
                pending.pop(u.id, None)
                await update.message.reply_text("✅ Default buy: *" + money(amt) + "*", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await update.message.reply_text("❌ Enter a number like 100", reply_markup=cancel_kb())
            return

        elif action == "cfg_sell":
            raw = text.replace("$", "")
            try:
                if raw.endswith("%"):
                    pct = float(raw[:-1])
                    assert 0 < pct <= 100
                    ud["preset_sell"] = str(int(pct)) + "%"
                else:
                    amt = float(raw)
                    assert amt > 0
                    ud["preset_sell"] = amt
                pending.pop(u.id, None)
                await update.message.reply_text("✅ Default sell: *" + text + "*", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await update.message.reply_text("❌ Enter 50% or 200", reply_markup=cancel_kb())
            return

        elif action == "cfg_risk":
            try:
                pct = float(text.replace("%", ""))
                assert 0 < pct <= 100
                ud["risk_pct"] = pct
                pending.pop(u.id, None)
                await update.message.reply_text("✅ Max risk: *" + str(pct) + "%* per trade", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await update.message.reply_text("❌ Enter a number like 10", reply_markup=cancel_kb())
            return

        elif action == "cfg_maxpos":
            try:
                n = int(text)
                assert n > 0
                ud["max_positions"] = n
                pending.pop(u.id, None)
                await update.message.reply_text("✅ Max positions: *" + str(n) + "*", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await update.message.reply_text("❌ Enter a number like 5", reply_markup=cancel_kb())
            return

        elif action == "cfg_daily":
            try:
                n = int(text)
                assert n > 0
                ud["daily_limit"] = n
                pending.pop(u.id, None)
                await update.message.reply_text("✅ Daily limit: *" + str(n) + "* trades", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await update.message.reply_text("❌ Enter a number like 10", reply_markup=cancel_kb())
            return

        elif action == "cfg_autosave":
            try:
                pct = float(text.replace("%", ""))
                assert 0 < pct <= 100
                ud["auto_save_pct"] = pct
                pending.pop(u.id, None)
                await update.message.reply_text("✅ Auto-save: *" + str(pct) + "%* of profits", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await update.message.reply_text("❌ Enter a percentage like 20", reply_markup=cancel_kb())
            return

        elif action == "cfg_target":
            try:
                amt = float(text.replace("$", "").replace(",", ""))
                assert amt > 0
                ud["target_equity"] = amt
                pending.pop(u.id, None)
                await update.message.reply_text("✅ Target equity: *" + money(amt) + "*", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await update.message.reply_text("❌ Enter a number like 10000", reply_markup=cancel_kb())
            return

        elif action == "buy_custom":
            contract = p["contract"]
            try:
                amt = float(text.replace("$", "").replace(",", ""))
                assert amt > 0
                pending.pop(u.id, None)
                await do_buy_msg(update, ud, u.id, contract, amt)
            except Exception:
                await update.message.reply_text("❌ Enter a number like 200", reply_markup=cancel_kb())
            return

        elif action == "sell_custom":
            contract = p["contract"]
            if contract not in ud["holdings"]:
                pending.pop(u.id, None)
                await update.message.reply_text("❌ Position not found", reply_markup=back_main())
                return
            raw = text.replace("$", "")
            try:
                if raw.endswith("%"):
                    pct = float(raw[:-1]) / 100
                    await do_sell_msg(update, ud, u.id, contract, pct=pct)
                else:
                    await do_sell_msg(update, ud, u.id, contract, usd=float(raw))
                pending.pop(u.id, None)
            except Exception:
                await update.message.reply_text("❌ Enter 50% or 200", reply_markup=cancel_kb())
            return

        elif action == "as_custom":
            contract = p["contract"]
            if contract not in ud["holdings"]:
                pending.pop(u.id, None)
                await update.message.reply_text("❌ Position not found", reply_markup=back_main())
                return
            parts = text.split()
            if len(parts) % 2 != 0:
                await update.message.reply_text("❌ Format: 50% 2x 100% 5x", reply_markup=cancel_kb())
                return
            try:
                targets = []
                for i in range(0, len(parts), 2):
                    pct = float(parts[i].replace("%", "")) / 100
                    x = float(parts[i+1].lower().replace("x", ""))
                    assert 0 < pct <= 1 and x > 1
                    targets.append({"pct": pct, "x": x, "triggered": False})
                targets.sort(key=lambda t: t["x"])
                ud["holdings"][contract]["auto_sells"] = targets
                h = ud["holdings"][contract]
                lines = ["✅ *Auto-sells set for $" + h["symbol"] + "*\n"]
                for t in targets:
                    lines.append("  " + str(int(t["pct"]*100)) + "% at " + str(t["x"]) + "x  (~" + money(h["avg_price"] * t["x"]) + ")")
                pending.pop(u.id, None)
                await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Format: 50% 2x 100% 5x", reply_markup=cancel_kb())
            return

        elif action == "sl_custom":
            contract = p["contract"]
            try:
                pct = float(text.replace("%", ""))
                assert 0 < pct < 100
                if contract in ud["holdings"]:
                    ud["holdings"][contract]["stop_loss_pct"] = pct
                    h = ud["holdings"][contract]
                    trigger = h["avg_price"] * (1 - pct / 100)
                    pending.pop(u.id, None)
                    await update.message.reply_text(
                        "✅ Stop loss: *" + str(pct) + "%* drop → " + money(trigger),
                        parse_mode="Markdown", reply_markup=back_main()
                    )
            except Exception:
                await update.message.reply_text("❌ Enter a number like 50", reply_markup=cancel_kb())
            return

        elif action == "journal":
            contract = p["contract"]
            if contract in ud["holdings"]:
                ud["holdings"][contract]["journal"] = text
                sym = ud["holdings"][contract]["symbol"]
                pending.pop(u.id, None)
                await update.message.reply_text("📝 Journal saved for $" + sym + ":\n\"" + text + "\"", reply_markup=back_main())
            else:
                pending.pop(u.id, None)
                await update.message.reply_text("❌ Position not found", reply_markup=back_main())
            return

        elif action == "limit_buy":
            contract = p["contract"]
            try:
                parts = text.split()
                target_price = float(parts[0].replace("$", ""))
                amount = float(parts[1].replace("$", "")) if len(parts) > 1 else (ud.get("preset_buy") or 0)
                assert target_price > 0 and amount > 0
                ud["limit_orders"].append({
                    "type": "buy", "contract": contract,
                    "symbol": p.get("symbol", "?"),
                    "target_price": target_price, "amount": amount,
                    "created_at": datetime.now(), "triggered": False, "cancelled": False,
                })
                pending.pop(u.id, None)
                await update.message.reply_text(
                    "✅ *Limit Buy Set*\n\nBuy " + money(amount) + " when price hits " + money(target_price),
                    parse_mode="Markdown", reply_markup=back_main()
                )
            except Exception:
                await update.message.reply_text("❌ Format: 0.005 100\n(price amount)", reply_markup=cancel_kb())
            return

        elif action == "limit_sell":
            contract = p["contract"]
            if contract not in ud["holdings"]:
                pending.pop(u.id, None)
                await update.message.reply_text("❌ Position not found", reply_markup=back_main())
                return
            h = ud["holdings"][contract]
            try:
                parts = text.split()
                target_price = float(parts[0].replace("$", ""))
                if len(parts) > 1:
                    raw = parts[1]
                    if raw.endswith("%"):
                        amount = h["total_invested"] * float(raw[:-1]) / 100
                    else:
                        amount = float(raw.replace("$", ""))
                else:
                    amount = h["total_invested"]
                assert target_price > 0 and amount > 0
                ud["limit_orders"].append({
                    "type": "sell", "contract": contract,
                    "symbol": h["symbol"],
                    "target_price": target_price, "amount": amount,
                    "created_at": datetime.now(), "triggered": False, "cancelled": False,
                })
                pending.pop(u.id, None)
                await update.message.reply_text(
                    "✅ *Limit Sell Set*\n\nSell " + money(amount) + " of $" + h["symbol"] + " when price hits " + money(target_price),
                    parse_mode="Markdown", reply_markup=back_main()
                )
            except Exception:
                await update.message.reply_text("❌ Format: 0.012 50%\n(price amount%)", reply_markup=cancel_kb())
            return

        elif action == "price_alert":
            contract = p["contract"]
            try:
                target = float(text.replace("$", ""))
                current = p.get("current_price", 0)
                direction = "above" if target > current else "below"
                ud["price_alerts"].append({
                    "contract": contract, "symbol": p.get("symbol", "?"),
                    "target": target, "direction": direction, "triggered": False,
                })
                pending.pop(u.id, None)
                await update.message.reply_text(
                    "🔔 Alert set: notify when price goes *" + direction + "* " + money(target),
                    parse_mode="Markdown", reply_markup=back_main()
                )
            except Exception:
                await update.message.reply_text("❌ Enter a price like 0.015", reply_markup=cancel_kb())
            return

        elif action == "sav_deposit":
            try:
                amt = float(text.replace("$", "").replace(",", ""))
                assert 0 < amt <= ud["balance"]
                ud["balance"] -= amt
                ud["savings"] += amt
                pending.pop(u.id, None)
                await update.message.reply_text(
                    "✅ *" + money(amt) + "* moved to savings\n\nTrading: " + money(ud["balance"]) + "\nSavings: " + money(ud["savings"]),
                    parse_mode="Markdown", reply_markup=back_main()
                )
            except Exception:
                await update.message.reply_text("❌ Max you can save: " + money(ud["balance"]), reply_markup=cancel_kb())
            return

        elif action == "sav_withdraw":
            try:
                amt = float(text.replace("$", "").replace(",", ""))
                assert 0 < amt <= ud["savings"]
                ud["savings"] -= amt
                ud["balance"] += amt
                pending.pop(u.id, None)
                await update.message.reply_text(
                    "✅ *" + money(amt) + "* moved to trading\n\nTrading: " + money(ud["balance"]) + "\nSavings: " + money(ud["savings"]),
                    parse_mode="Markdown", reply_markup=back_main()
                )
            except Exception:
                await update.message.reply_text("❌ Max you can withdraw: " + money(ud["savings"]), reply_markup=cancel_kb())
            return

    # No pending — treat as CA
    if ud.get("balance") is None:
        await update.message.reply_text("Use /start to set up your account first!")
        return

    contract = text
    msg = await update.message.reply_text("🔍 Scanning token...")
    info = await get_token(contract)
    if not info:
        await msg.edit_text("❌ Token not found. Check the contract address and try again.", reply_markup=back_main())
        return
    sc = score_token(info)
    await msg.edit_text(token_card(info, contract, ud, sc), parse_mode="Markdown", reply_markup=buy_kb(contract, ud))


async def btn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = update.effective_user
    ud = get_user(u.id, u.username or u.first_name)
    cb = q.data

    if cb == "mm":
        pending.pop(u.id, None)
        if ud.get("balance") is None:
            await cmd_start(update, ctx)
            return
        await q.edit_message_text(
            "⚡ *AURACLE_XBOT*\n\nWelcome back, *" + ud["username"] + "*!\n"
            "💰 Balance: *" + money(ud["balance"]) + "*\n"
            "💎 Savings: *" + money(ud["savings"]) + "*\n\n"
            "Paste any CA to trade 👇",
            parse_mode="Markdown", reply_markup=main_menu_kb()
        )

    elif cb == "v_trade":
        await q.edit_message_text(
            "⚡ *BUY and SELL NOW*\n\nPaste any Solana, ETH, BSC or Base contract address in the chat to get started.",
            parse_mode="Markdown", reply_markup=back_main()
        )

    elif cb == "v_pos":
        if not ud["holdings"]:
            await q.edit_message_text(
                "📊 *POSITIONS*\n\nNo open positions.\nPaste a CA to start trading.",
                parse_mode="Markdown", reply_markup=back_main()
            )
            return
        lines = ["📊 *OPEN POSITIONS*\n"]
        for contract, h in ud["holdings"].items():
            info = await get_token(contract)
            if info:
                cv = h["amount"] * info["price"]
                cx = info["price"] / h["avg_price"] if h["avg_price"] > 0 else 0
                ppnl = cv - h["total_invested"]
                sl = h.get("stop_loss_pct")
                targets = [t for t in h.get("auto_sells", []) if not t.get("triggered")]
                sl_txt = "  SL:" + str(sl) + "%" if sl else ""
                as_txt = "  AS:" + str(len(targets)) + " targets" if targets else ""
                lines.append(
                    "*$" + h["symbol"] + "*  " + str(round(cx, 2)) + "x\n"
                    "  " + money(cv) + "  " + pstr(ppnl) + sl_txt + as_txt + "\n"
                )
        buttons = []
        for contract, h in ud["holdings"].items():
            buttons.append([InlineKeyboardButton("Open $" + h["symbol"], callback_data="btt_" + contract)])
        buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="mm")])
        await q.edit_message_text("\n".join(lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons))

    elif cb == "v_orders":
        orders = ud.get("limit_orders", [])
        alerts = ud.get("price_alerts", [])
        if not orders and not alerts:
            await q.edit_message_text("🕐 *ACTIVE ORDERS*\n\nNo active orders.\nOpen a token and set limit orders.", parse_mode="Markdown", reply_markup=back_main())
            return
        lines = ["🕐 *ACTIVE ORDERS*\n"]
        for i, o in enumerate(orders):
            otype = "BUY" if o["type"] == "buy" else "SELL"
            lines.append(otype + " $" + o["symbol"] + " when price hits " + money(o["target_price"]) + "  Amount: " + money(o["amount"]))
        for a in alerts:
            lines.append("ALERT $" + a["symbol"] + " when price goes " + a["direction"] + " " + money(a["target"]))
        cancel_btns = []
        for i, o in enumerate(orders):
            cancel_btns.append([InlineKeyboardButton("Cancel " + o["type"].upper() + " $" + o["symbol"], callback_data="co_" + str(i))])
        cancel_btns.append([InlineKeyboardButton("Cancel All Orders", callback_data="co_all")])
        cancel_btns.append([InlineKeyboardButton("🏠 Main Menu", callback_data="mm")])
        await q.edit_message_text("\n".join(lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(cancel_btns))

    elif cb.startswith("co_"):
        rest = cb[len("co_"):]
        if rest == "all":
            ud["limit_orders"] = []
            ud["price_alerts"] = []
            await q.edit_message_text("All orders cancelled.", reply_markup=back_main())
        else:
            try:
                idx = int(rest)
                if 0 <= idx < len(ud["limit_orders"]):
                    ud["limit_orders"][idx]["cancelled"] = True
                    ud["limit_orders"] = [o for o in ud["limit_orders"] if not o.get("cancelled")]
            except Exception:
                pass
            await q.edit_message_text("Order cancelled.", reply_markup=back_main())

    elif cb == "v_history":
        logs = trade_log.get(u.id, [])
        if not logs:
            await q.edit_message_text("📜 *TRADE HISTORY*\n\nNo closed trades yet.", parse_mode="Markdown", reply_markup=back_main())
            return
        lines = ["📜 *TRADE HISTORY*\n"]
        for t in sorted(logs, key=lambda x: x.get("closed_at", datetime.min), reverse=True)[:15]:
            icon = "+" if t["realized_pnl"] > 0 else "-"
            j = "\n  \"" + t["journal"][:40] + "\"" if t.get("journal") else ""
            lines.append(
                icon + " *$" + t["symbol"] + "*  " + str(round(t.get("x", 0), 2)) + "x  " + pstr(t["realized_pnl"]) + "\n"
                "  Held: " + str(t["hold_h"]) + "h  |  " + t.get("reason", "manual") + j
            )
        await q.edit_message_text("\n".join(lines), parse_mode="Markdown", reply_markup=back_main())

    elif cb == "v_savings":
        asp = str(ud["auto_save_pct"]) + "% of profits" if ud.get("auto_save_pct") else "not set"
        await q.edit_message_text(
            "💰 *SAVINGS WALLET*\n\n"
            "Savings: *" + money(ud["savings"]) + "*\n"
            "Trading: *" + money(ud["balance"]) + "*\n"
            "Auto-save: *" + asp + "*\n\n"
            "Savings are protected from trading.\nTransfer manually when needed.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Deposit to Savings",      callback_data="sav_dep")],
                [InlineKeyboardButton("Withdraw to Trading",     callback_data="sav_wit")],
                [InlineKeyboardButton("Set Auto-Save %",         callback_data="cfg_autosave")],
                [InlineKeyboardButton("🏠 Main Menu",            callback_data="mm")],
            ])
        )

    elif cb == "sav_dep":
        pending[u.id] = {"action": "sav_deposit"}
        await q.edit_message_text(
            "Enter amount to move to savings:\nMax: " + money(ud["balance"]),
            reply_markup=cancel_kb()
        )

    elif cb == "sav_wit":
        if ud["savings"] <= 0:
            await q.edit_message_text("No savings to withdraw.", reply_markup=back_main())
            return
        pending[u.id] = {"action": "sav_withdraw"}
        await q.edit_message_text(
            "Enter amount to move to trading:\nMax: " + money(ud["savings"]),
            reply_markup=cancel_kb()
        )

    elif cb == "v_stats":
        logs = trade_log.get(u.id, [])
        if not logs:
            await q.edit_message_text("📈 *STATS*\n\nNo closed trades yet.", parse_mode="Markdown", reply_markup=back_main())
            return
        wins   = [t for t in logs if t["realized_pnl"] > 0]
        losses = [t for t in logs if t["realized_pnl"] <= 0]
        total  = len(logs)
        wr     = round(len(wins) / total * 100, 1)
        aw     = sum(t["realized_pnl"] for t in wins) / len(wins) if wins else 0
        al     = sum(t["realized_pnl"] for t in losses) / len(losses) if losses else 0
        ah     = sum(t["hold_h"] for t in logs) / total
        tpnl   = sum(t["realized_pnl"] for t in logs)
        best   = max(logs, key=lambda t: t["realized_pnl"])
        worst  = min(logs, key=lambda t: t["realized_pnl"])
        bestx  = max(logs, key=lambda t: t.get("x", 0))
        rf, rb = ud.get("followed", 0), ud.get("broken", 0)
        dr     = round(rf / (rf + rb) * 100) if (rf + rb) > 0 else 0
        sb = ud.get("starting_balance", 5000)
        eq = ud["balance"] + sum(h["total_invested"] for h in ud["holdings"].values()) + ud["savings"]
        growth = round((eq - sb) / sb * 100, 1) if sb > 0 else 0

        best_hour = ""
        if ud.get("trade_hours"):
            bh = max(ud["trade_hours"].items(), key=lambda x: x[1].get("pnl", 0))
            best_hour = "\nBest Hour: " + str(bh[0]) + ":00  (" + pstr(bh[1]["pnl"]) + ")"

        target_line = ""
        if ud.get("target_equity"):
            pct_done = round(min((eq / ud["target_equity"]) * 100, 100), 1)
            target_line = "\nTarget Progress: " + str(pct_done) + "% of " + money(ud["target_equity"])

        await q.edit_message_text(
            "📈 *STATS*\n\n"
            "Trades: " + str(total) + "  (" + str(len(wins)) + "W / " + str(len(losses)) + "L)\n"
            "Win Rate: " + str(wr) + "%\n"
            "Avg Win: " + money(aw) + "\n"
            "Avg Loss: " + money(al) + "\n"
            "Total PnL: " + pstr(tpnl) + "\n\n"
            "Best: " + pstr(best["realized_pnl"]) + " ($" + best["symbol"] + ")\n"
            "Worst: " + pstr(worst["realized_pnl"]) + " ($" + worst["symbol"] + ")\n"
            "Best X: " + str(round(bestx.get("x", 0), 2)) + "x ($" + bestx["symbol"] + ")\n\n"
            "Avg Hold: " + str(round(ah, 1)) + "h\n"
            "Rules Followed: " + str(rf) + "  |  Broken: " + str(rb) + "\n"
            "Discipline: " + str(dr) + "%\n"
            "Best Streak: " + str(ud.get("best_streak", 0)) + "\n"
            "Current Streak: " + str(ud.get("streak", 0)) + "\n"
            "Max Drawdown: " + str(round(ud.get("max_drawdown", 0), 1)) + "%\n"
            "Account Growth: " + str(growth) + "%" + best_hour + target_line,
            parse_mode="Markdown", reply_markup=back_main()
        )

    elif cb == "v_review":
        logs = trade_log.get(u.id, [])
        week_ago = datetime.now() - timedelta(days=7)
        weekly = [t for t in logs if t.get("closed_at", datetime.min) >= week_ago]
        if not weekly:
            await q.edit_message_text("📅 No closed trades in last 7 days.", reply_markup=back_main())
            return
        wins = [t for t in weekly if t["realized_pnl"] > 0]
        tpnl = sum(t["realized_pnl"] for t in weekly)
        wr = round(len(wins) / len(weekly) * 100)
        lines = ["📅 *WEEKLY REVIEW*\n\n" + str(len(weekly)) + " trades  |  WR: " + str(wr) + "%  |  " + pstr(tpnl) + "\n"]
        for t in sorted(weekly, key=lambda x: x.get("closed_at", datetime.min), reverse=True):
            fp = " [followed plan]" if t.get("followed_plan") else (" [sold early]" if t.get("followed_plan") is False else "")
            j = "\n  \"" + t["journal"][:40] + "\"" if t.get("journal") else ""
            mood = "  Mood: " + t["mood"] if t.get("mood") else ""
            lines.append("$" + t["symbol"] + "  " + pstr(t["realized_pnl"]) + "  " + str(round(t.get("x", 0), 2)) + "x  " + str(t["hold_h"]) + "h" + fp + mood + j)
        await q.edit_message_text("\n".join(lines), parse_mode="Markdown", reply_markup=back_main())

    elif cb == "v_leader":
        if not users:
            await q.edit_message_text("No traders yet.", reply_markup=back_main())
            return
        scores = []
        for uid2, d in users.items():
            if d.get("balance") is None:
                continue
            hv = sum(h["total_invested"] for h in d["holdings"].values())
            eq = d["balance"] + hv + d["savings"]
            logs2 = trade_log.get(uid2, [])
            wr2 = round(len([t for t in logs2 if t["realized_pnl"] > 0]) / len(logs2) * 100) if logs2 else 0
            sb = d.get("starting_balance", 5000)
            growth = round((eq - sb) / sb * 100, 1) if sb > 0 else 0
            scores.append((d["username"], eq, eq - sb, wr2, growth))
        scores.sort(key=lambda x: x[1], reverse=True)
        places = ["1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th"]
        lines = ["🏆 *LEADERBOARD*\n"]
        for i, (name, eq, ppnl, wr2, growth) in enumerate(scores[:10]):
            lines.append(places[i] + "  *" + name + "*\n     " + money(eq) + "  " + pstr(ppnl) + "  WR:" + str(wr2) + "%  Growth:" + str(growth) + "%")
        await q.edit_message_text("\n".join(lines), parse_mode="Markdown", reply_markup=back_main())

    elif cb == "v_alerts":
        alerts = ud.get("price_alerts", [])
        if not alerts:
            await q.edit_message_text("🔔 *PRICE ALERTS*\n\nNo active alerts.\nOpen a token and set a price alert.", parse_mode="Markdown", reply_markup=back_main())
            return
        lines = ["🔔 *PRICE ALERTS*\n"]
        for a in alerts:
            lines.append("$" + a["symbol"] + " when price goes " + a["direction"] + " " + money(a["target"]))
        await q.edit_message_text("\n".join(lines), parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Clear All Alerts", callback_data="clear_alerts")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="mm")],
            ])
        )

    elif cb == "clear_alerts":
        ud["price_alerts"] = []
        await q.edit_message_text("All price alerts cleared.", reply_markup=back_main())

    elif cb == "v_settings":
        await q.edit_message_text("⚙️ *SETTINGS*\n\nTap any setting to change:", parse_mode="Markdown", reply_markup=settings_kb(ud))

    elif cb in ("cfg_buy", "cfg_sell", "cfg_risk", "cfg_maxpos", "cfg_daily", "cfg_autosave", "cfg_target"):
        prompts = {
            "cfg_buy":      "Enter default buy amount in USD (e.g. 100):",
            "cfg_sell":     "Enter default sell - 50% or fixed like 200:",
            "cfg_risk":     "Enter max risk per trade as % (e.g. 10):",
            "cfg_maxpos":   "Enter max open positions (e.g. 5):",
            "cfg_daily":    "Enter max trades per day (e.g. 10):",
            "cfg_autosave": "Enter auto-save % of profits (e.g. 20):",
            "cfg_target":   "Enter target equity goal (e.g. 10000):",
        }
        pending[u.id] = {"action": cb}
        await q.edit_message_text(prompts[cb], reply_markup=cancel_kb())

    elif cb == "v_profile":
        sb = ud.get("starting_balance", 0)
        hv = sum(h["total_invested"] for h in ud["holdings"].values())
        eq = ud["balance"] + hv + ud["savings"]
        growth = round((eq - sb) / sb * 100, 1) if sb > 0 else 0
        logs2 = trade_log.get(u.id, [])
        wr2 = round(len([t for t in logs2 if t["realized_pnl"] > 0]) / len(logs2) * 100) if logs2 else 0
        joined = ud.get("joined_at", datetime.now()).strftime("%b %d %Y")
        await q.edit_message_text(
            "👤 *PROFILE*\n\n"
            "Name: *" + ud["username"] + "*\n"
            "Joined: " + joined + "\n\n"
            "Starting Balance: " + money(sb) + "\n"
            "Current Equity: " + money(eq) + "\n"
            "Account Growth: " + str(growth) + "%\n\n"
            "Total Trades: " + str(len(logs2)) + "\n"
            "Win Rate: " + str(wr2) + "%\n"
            "Best Streak: " + str(ud.get("best_streak", 0)) + "\n"
            "Discipline Rate: " + str(round(ud["followed"] / (ud["followed"] + ud["broken"]) * 100) if (ud["followed"] + ud["broken"]) > 0 else 0) + "%",
            parse_mode="Markdown", reply_markup=back_main()
        )

    elif cb == "rst_prompt":
        await q.edit_message_text(
            "RESET ACCOUNT\n\nThis wipes all holdings, history and savings.\n\nAre you sure?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes, Reset", callback_data="rst_confirm_" + str(u.id)),
                 InlineKeyboardButton("Cancel",     callback_data="mm")],
            ])
        )

    elif cb == "rst_confirm_" + str(u.id):
        pending.pop(u.id, None)
        ud.update({
            "balance": None, "starting_balance": None, "savings": 0.0,
            "holdings": {}, "realized_pnl": 0.0, "limit_orders": [], "price_alerts": [],
            "preset_buy": None, "preset_sell": None, "risk_pct": None,
            "max_positions": None, "daily_limit": None, "daily_trades": 0,
            "last_day": None, "planned": 0, "impulse": 0, "followed": 0,
            "broken": 0, "streak": 0, "best_streak": 0, "target_equity": None,
            "peak_equity": 0.0, "max_drawdown": 0.0, "consec_losses": 0,
            "trade_hours": {}, "auto_save_pct": None, "joined_at": datetime.now(),
        })
        trade_log[u.id] = []
        await cmd_start(update, ctx)

    elif cb.startswith("rf_"):
        contract = cb[3:]
        info = await get_token(contract)
        if not info:
            await q.edit_message_text("Token unavailable.", reply_markup=back_main())
            return
        sc = score_token(info)
        await q.edit_message_text(token_card(info, contract, ud, sc), parse_mode="Markdown", reply_markup=buy_kb(contract, ud))

    elif cb.startswith("btt_"):
        contract = cb[4:]
        info = await get_token(contract)
        if not info:
            await q.edit_message_text("Token unavailable.", reply_markup=back_main())
            return
        sc = score_token(info)
        await q.edit_message_text(token_card(info, contract, ud, sc), parse_mode="Markdown", reply_markup=buy_kb(contract, ud))

    elif cb.startswith("gos_"):
        contract = cb[4:]
        if contract not in ud["holdings"]:
            await q.edit_message_text("Position not found.", reply_markup=back_main())
            return
        h = ud["holdings"][contract]
        info = await get_token(contract)
        price = info["price"] if info else h["avg_price"]
        cv = h["amount"] * price
        cx = price / h["avg_price"] if h["avg_price"] > 0 else 0
        ppnl = cv - h["total_invested"]
        await q.edit_message_text(
            "🔴 *SELL $" + h["symbol"] + "*\n\n"
            "Value: *" + money(cv) + "*\n"
            "Current: *" + str(round(cx, 2)) + "x*\n"
            "PnL: " + pstr(ppnl) + "\n\nHow much to sell?",
            parse_mode="Markdown", reply_markup=sell_kb(contract)
        )

    elif cb.startswith("bp_"):
        contract = cb[3:]
        pb = ud.get("preset_buy")
        if not pb:
            await q.edit_message_text("No preset buy set. Go to Settings first.", reply_markup=back_main())
            return
        await do_buy_query(q, ud, u.id, contract, pb)

    elif cb.startswith("ba_"):
        rest = cb[3:]
        amt_str, contract = rest.split("_", 1)
        await do_buy_query(q, ud, u.id, contract, float(amt_str))

    elif cb.startswith("bc_"):
        contract = cb[3:]
        pending[u.id] = {"action": "buy_custom", "contract": contract}
        await q.edit_message_text("Enter buy amount in USD:", reply_markup=cancel_kb())

    elif cb.startswith("sp_"):
        rest = cb[3:]
        pct_str, contract = rest.split("_", 1)
        await do_sell_query(q, ud, u.id, contract, pct=float(pct_str)/100)

    elif cb.startswith("sca_"):
        contract = cb[4:]
        pending[u.id] = {"action": "sell_custom", "contract": contract}
        await q.edit_message_text("Enter amount to sell (e.g. 200 or 50%):", reply_markup=cancel_kb())

    elif cb.startswith("tks_"):
        contract = cb[4:]
        info = await get_token(contract)
        if not info:
            await q.edit_message_text("Token unavailable.", reply_markup=back_main())
            return
        sc = score_token(info)
        strengths_txt = "\nStrengths:\n" + "\n".join(["  + " + s for s in sc["strengths"]]) if sc["strengths"] else ""
        warnings_txt  = "\nWarnings:\n"  + "\n".join(["  ! " + w for w in sc["warnings"]])  if sc["warnings"] else ""
        await q.edit_message_text(
            "📊 *AURACLE SCORE*\n\n"
            "*$" + info["symbol"] + "*\n\n"
            "Score: *" + str(sc["score"]) + "/100*\n"
            "Verdict: *" + sc["verdict"] + "*\n\n"
            "MC: " + mc_str(info["mc"]) + "\n"
            "Liq: " + money(info["liq"]) + " (" + str(info["liq_pct"]) + "%)\n"
            "Buys: " + str(info["buy_pct"]) + "%  |  Sells: " + str(100 - info["buy_pct"]) + "%"
            + strengths_txt + warnings_txt,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back to Token", callback_data="btt_" + contract)]])
        )

    elif cb.startswith("lbo_"):
        contract = cb[4:]
        info = await get_token(contract)
        sym = info["symbol"] if info else "?"
        price = info["price"] if info else 0
        pending[u.id] = {"action": "limit_buy", "contract": contract, "symbol": sym, "current_price": price}
        await q.edit_message_text(
            "🎯 *LIMIT BUY*\n\nCurrent price: " + money(price) + "\n\n"
            "Enter target price and amount:\nFormat: price amount\nExample: 0.005 100",
            parse_mode="Markdown", reply_markup=cancel_kb()
        )

    elif cb.startswith("lso_"):
        contract = cb[4:]
        if contract not in ud["holdings"]:
            await q.edit_message_text("Position not found.", reply_markup=back_main())
            return
        h = ud["holdings"][contract]
        info = await get_token(contract)
        price = info["price"] if info else h["avg_price"]
        pending[u.id] = {"action": "limit_sell", "contract": contract, "symbol": h["symbol"], "current_price": price}
        await q.edit_message_text(
            "🎯 *LIMIT SELL*\n\nCurrent price: " + money(price) + "\n\n"
            "Enter target price and amount:\nFormat: price amount%\nExample: 0.012 50%",
            parse_mode="Markdown", reply_markup=cancel_kb()
        )

    elif cb.startswith("pal_"):
        contract = cb[4:]
        info = await get_token(contract)
        sym = info["symbol"] if info else "?"
        price = info["price"] if info else 0
        pending[u.id] = {"action": "price_alert", "contract": contract, "symbol": sym, "current_price": price}
        await q.edit_message_text(
            "🔔 *PRICE ALERT*\n\nCurrent price: " + money(price) + "\n\nEnter target price:",
            parse_mode="Markdown", reply_markup=cancel_kb()
        )

    elif cb.startswith("asm_"):
        contract = cb[4:]
        if contract not in ud["holdings"]:
            await q.edit_message_text("Position not found.", reply_markup=back_main())
            return
        h = ud["holdings"][contract]
        avg = h["avg_price"]
        await q.edit_message_text(
            "🎯 *AUTO-SELL  $" + h["symbol"] + "*\n\nBuy price: " + money(avg) + "\n\nChoose a preset:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("50% at 2x + 100% at 5x",          callback_data="asq_2_5_"   + contract)],
                [InlineKeyboardButton("50% at 3x + 100% at 10x",         callback_data="asq_3_10_"  + contract)],
                [InlineKeyboardButton("25% at 2x + 25% at 5x + 50% at 10x", callback_data="asq_2_5_10_" + contract)],
                [InlineKeyboardButton("100% at 2x",                      callback_data="asq_2_"     + contract)],
                [InlineKeyboardButton("Custom Targets",                  callback_data="ascus_"     + contract)],
                [InlineKeyboardButton("Back",                            callback_data="btt_"       + contract)],
            ])
        )

    elif cb.startswith("asq_2_5_10_"):
        contract = cb[len("asq_2_5_10_"):]
        if contract in ud["holdings"]:
            ud["holdings"][contract]["auto_sells"] = [
                {"pct": 0.25, "x": 2.0, "triggered": False},
                {"pct": 0.25, "x": 5.0, "triggered": False},
                {"pct": 0.50, "x": 10.0, "triggered": False},
            ]
            avg = ud["holdings"][contract]["avg_price"]
            sym = ud["holdings"][contract]["symbol"]
            await q.edit_message_text(
                "✅ Auto-sells set for $" + sym + ":\n"
                "  25% at 2x (~" + money(avg*2) + ")\n"
                "  25% at 5x (~" + money(avg*5) + ")\n"
                "  50% at 10x (~" + money(avg*10) + ")",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="btt_" + contract)]])
            )

    elif cb.startswith("asq_2_"):
        contract = cb[len("asq_2_"):]
        if contract in ud["holdings"]:
            ud["holdings"][contract]["auto_sells"] = [{"pct": 1.0, "x": 2.0, "triggered": False}]
            avg = ud["holdings"][contract]["avg_price"]
            sym = ud["holdings"][contract]["symbol"]
            await q.edit_message_text(
                "✅ Auto-sell set for $" + sym + ":\n  100% at 2x (~" + money(avg*2) + ")",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="btt_" + contract)]])
            )

    elif cb.startswith("asq_"):
        rest = cb[4:]
        parts = rest.split("_", 2)
        x1, x2, contract = int(parts[0]), int(parts[1]), parts[2]
        if contract in ud["holdings"]:
            ud["holdings"][contract]["auto_sells"] = [
                {"pct": 0.5, "x": float(x1), "triggered": False},
                {"pct": 1.0, "x": float(x2), "triggered": False},
            ]
            avg = ud["holdings"][contract]["avg_price"]
            sym = ud["holdings"][contract]["symbol"]
            await q.edit_message_text(
                "✅ Auto-sells set for $" + sym + ":\n"
                "  50% at " + str(x1) + "x (~" + money(avg*x1) + ")\n"
                "  100% at " + str(x2) + "x (~" + money(avg*x2) + ")",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="btt_" + contract)]])
            )

    elif cb.startswith("ascus_"):
        contract = cb[6:]
        pending[u.id] = {"action": "as_custom", "contract": contract}
        await q.edit_message_text(
            "Enter targets:\nFormat: 50% 2x 100% 5x",
            reply_markup=cancel_kb()
        )

    elif cb.startswith("vtg_"):
        contract = cb[4:]
        if contract not in ud["holdings"]:
            await q.edit_message_text("Position not found.", reply_markup=back_main())
            return
        h = ud["holdings"][contract]
        targets = h.get("auto_sells", [])
        sl = h.get("stop_loss_pct")
        avg = h["avg_price"]
        lines = ["👁 *TARGETS  $" + h["symbol"] + "*\n"]
        if targets:
            for t in targets:
                status = "[DONE]" if t.get("triggered") else "[WAITING]"
                lines.append(status + " Sell " + str(int(t["pct"]*100)) + "% at " + str(t["x"]) + "x  (~" + money(avg * t["x"]) + ")")
        else:
            lines.append("No auto-sell targets set.")
        if sl:
            lines.append("\nStop Loss: " + str(sl) + "% drop (~" + money(avg * (1 - sl/100)) + ")")
        else:
            lines.append("\nNo stop loss set.")
        await q.edit_message_text(
            "\n".join(lines), parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Cancel All Targets", callback_data="cat_" + contract)],
                [InlineKeyboardButton("Back", callback_data="btt_" + contract)],
            ])
        )

    elif cb.startswith("cat_"):
        contract = cb[4:]
        if contract in ud["holdings"]:
            ud["holdings"][contract]["auto_sells"] = []
            ud["holdings"][contract]["stop_loss_pct"] = None
            sym = ud["holdings"][contract]["symbol"]
            await q.edit_message_text(
                "All targets cancelled for $" + sym,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="btt_" + contract)]])
            )

    elif cb.startswith("slm_"):
        contract = cb[4:]
        if contract not in ud["holdings"]:
            await q.edit_message_text("Position not found.", reply_markup=back_main())
            return
        h = ud["holdings"][contract]
        sl = h.get("stop_loss_pct")
        sl_info = "  Current: " + str(sl) + "%" if sl else ""
        await q.edit_message_text(
            "🛑 *STOP LOSS  $" + h["symbol"] + "*" + sl_info + "\n\nSell ALL if price drops by:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("25%", callback_data="sls_25_" + contract),
                 InlineKeyboardButton("50%", callback_data="sls_50_" + contract),
                 InlineKeyboardButton("75%", callback_data="sls_75_" + contract)],
                [InlineKeyboardButton("Custom %",  callback_data="slc_" + contract)],
                [InlineKeyboardButton("Remove SL", callback_data="slr_" + contract)],
                [InlineKeyboardButton("Back",      callback_data="btt_" + contract)],
            ])
        )

    elif cb.startswith("sls_"):
        rest = cb[4:]
        pct_str, contract = rest.split("_", 1)
        pct = float(pct_str)
        if contract in ud["holdings"]:
            ud["holdings"][contract]["stop_loss_pct"] = pct
            h = ud["holdings"][contract]
            trigger = h["avg_price"] * (1 - pct / 100)
            await q.edit_message_text(
                "✅ Stop loss set: " + str(int(pct)) + "% drop → " + money(trigger),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="btt_" + contract)]])
            )

    elif cb.startswith("slc_"):
        contract = cb[4:]
        pending[u.id] = {"action": "sl_custom", "contract": contract}
        await q.edit_message_text("Enter stop loss % drop (e.g. 60):", reply_markup=cancel_kb())

    elif cb.startswith("slr_"):
        contract = cb[4:]
        if contract in ud["holdings"]:
            ud["holdings"][contract]["stop_loss_pct"] = None
            sym = ud["holdings"][contract]["symbol"]
            await q.edit_message_text(
                "Stop loss removed for $" + sym,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="btt_" + contract)]])
            )

    elif cb.startswith("jnl_"):
        contract = cb[4:]
        pending[u.id] = {"action": "journal", "contract": contract}
        h = ud["holdings"].get(contract, {})
        existing = "  Current: \"" + h.get("journal", "") + "\"" if h.get("journal") else ""
        await q.edit_message_text(
            "📝 Journal for $" + h.get("symbol", "?") + existing + "\n\nEnter your trade thesis:",
            reply_markup=cancel_kb()
        )


def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_start))
    app.add_handler(CallbackQueryHandler(btn))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.job_queue.run_repeating(checker_job, interval=PRICE_CHECK_INTERVAL, first=10)
    app.job_queue.run_daily(daily_summary_job, time=__import__("datetime").time(23, 59))
    logger.info("AURACLE_XBOT running...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
            
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
                t_info = (
    f" [AS: {', '.join(f'{t['pct'] * 100:.2f}%' for t in targets)}]"
    if targets
    else ""
                )
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
