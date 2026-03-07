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
import io
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    MATPLOTLIB_OK = True
except ImportError:
    MATPLOTLIB_OK = False
try:
    from PIL import Image, ImageDraw, ImageFont
    PILLOW_OK = True
except ImportError:
    PILLOW_OK = False

import asyncio as _asyncio
import time as _time
import json as _json
import urllib.parse as _urlparse
import re as _re
from persistence import load_all, save_user, save_trade_log, autosave_job

BOT_TOKEN = os.getenv("BOT_TOKEN", "")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env var not set. Add it in Railway → Variables.")

DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens/{}"
PRICE_CHECK_INTERVAL = 20
MAX_BALANCE = 10_000.0
MIN_BALANCE = 1.0
SNIPER_SEEN_EXPIRY_H = 10     # forget seen tokens after 10 HOURS (sniper_job runs every 5 min)

# ── RugCheck rate limiter — max 3 concurrent calls to avoid 429s ─────────────
_rugcheck_semaphore = None

async def _get_rugcheck_semaphore():
    global _rugcheck_semaphore
    if _rugcheck_semaphore is None:
        _rugcheck_semaphore = _asyncio.Semaphore(3)
    return _rugcheck_semaphore
SNIPER_LOG_MAX = 200          # max sniper log entries per user

_http: httpx.AsyncClient | None = None

async def get_http() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(timeout=10, limits=httpx.Limits(max_connections=20))
    return _http

# ── Token price cache (8s TTL) ───────────────────────────────────────────────
_token_cache: dict = {}
CACHE_TTL = 12.0
_sol_price_cache: dict = {"price": 150.0, "ts": 0.0}  # cached SOL/USD, refreshed hourly




logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

async def get_sol_price() -> float:
    """Get current SOL/USD price. Cached for 60s. Falls back to 150 if unavailable."""
    import time as _time
    now = _time.time()
    if now - _sol_price_cache["ts"] < 60:
        return _sol_price_cache["price"]
    try:
        client = await get_http()
        r = await client.get(
            "https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112",
            timeout=5
        )
        if r.status_code == 200:
            pairs = r.json().get("pairs", [])
            sol_pairs = [p for p in pairs if p.get("quoteToken", {}).get("symbol") == "USDC"]
            if sol_pairs:
                price = float(sol_pairs[0].get("priceUsd", 150))
                _sol_price_cache["price"] = price
                _sol_price_cache["ts"]    = now
                return price
    except Exception as e:
        logger.debug(f"SOL price fetch failed: {e}")
    return _sol_price_cache["price"]  # return last known / default



async def get_helius_maker_pct(contract: str, api_key: str) -> dict:
    """
    Optional Helius enrichment — fetches last 100 txns and calculates:
      - maker_pct:    % of unique wallets that are net buyers
      - maker_count:  number of unique buyer wallets
      - top3_vol_pct: % of volume from top 3 wallets (wash trade signal)
    Returns empty dict gracefully if key missing / rate limited.
    """
    if not api_key:
        return {}
    try:
        client = await get_http()
        url = f"https://api.helius.xyz/v1/addresses/{contract}/transactions"
        params = {"api-key": api_key, "limit": "100", "type": "SWAP"}
        r = await client.get(url, params=params, timeout=8)
        if r.status_code == 429:
            logger.info("Helius rate limit hit — skipping maker enrichment")
            return {}
        if r.status_code != 200:
            return {}
        txns = r.json()
        if not isinstance(txns, list) or not txns:
            return {}

        buyer_vols:  dict = {}   # wallet → volume bought
        seller_vols: dict = {}   # wallet → volume sold

        for tx in txns:
            try:
                fee_payer = tx.get("feePayer", "")
                native_transfers = tx.get("nativeTransfers", []) or []
                token_transfers  = tx.get("tokenTransfers",  []) or []
                # Determine if this is a buy (feePayer received token) or sell
                is_buy = any(
                    t.get("toUserAccount") == fee_payer
                    for t in token_transfers
                    if t.get("mint") == contract
                )
                # Approximate volume from native SOL moved
                sol_moved = sum(
                    abs(t.get("amount", 0))
                    for t in native_transfers
                    if t.get("fromUserAccount") == fee_payer or t.get("toUserAccount") == fee_payer
                ) / 1e9  # lamports → SOL
                if is_buy:
                    buyer_vols[fee_payer]  = buyer_vols.get(fee_payer,  0) + sol_moved
                else:
                    seller_vols[fee_payer] = seller_vols.get(fee_payer, 0) + sol_moved
            except Exception:
                continue

        total_wallets = len(set(buyer_vols.keys()) | set(seller_vols.keys()))
        if total_wallets == 0:
            return {}

        maker_pct   = round(len(buyer_vols) / total_wallets * 100) if total_wallets > 0 else 50
        maker_count = len(buyer_vols)

        # Top 3 wallet concentration
        all_vols = {**buyer_vols, **seller_vols}
        total_vol = sum(all_vols.values()) or 1
        top3_vol  = sum(sorted(all_vols.values(), reverse=True)[:3])
        top3_vol_pct = round(top3_vol / total_vol * 100)

        return {
            "maker_pct":    maker_pct,
            "maker_count":  maker_count,
            "top3_vol_pct": top3_vol_pct,
        }
    except Exception as e:
        logger.debug(f"Helius maker enrichment failed: {e}")
        return {}


# ── LANGUAGE SYSTEM ───────────────────────────────────────────────────────────
TRANSLATIONS: dict = {
    "en": {
        "welcome":        "👋 Welcome to *AURACLE_XBOT*!\n\nAdvanced multi-chain paper trading bot.\n\nSet your starting balance:\nMin: $1  |  Max: $10,000\n\nEnter your starting balance:",
        "welcome_back":   "⚡ *AURACLE_XBOT*\n\nWelcome back, *{username}*!\n💰 Balance: *{balance}*\n💎 Savings: *{savings}*\n\nPaste any crypto CA to trade 👇",
        "buy_exec":       "✅ *BUY EXECUTED*\n\n*{name} (${symbol})*\nSpent: *{spent}*\nGot: *{tokens} {symbol}*\nPrice: *{price}*\nMC: *{mc}*\nLiq: *{liq}*\nCash left: *{cash}*",
        "sell_exec":      "✅ *SELL EXECUTED*\n\nReceived: *{received}*\nPrice: *{price}*  |  *{cx}x*\nHeld: *{held}h*\nPnL: *{pnl}*\nCash: *{cash}*",
        "risk_card":      "🧮 *RISK CALCULATOR*\n\n*${symbol}*  |  MC: {mc}\n\nYou are risking *{amount}* ({pct}% of balance)\n\n📈 *If it goes up:*\n  2x → *+{gain2x}* (have {bal2x})\n  5x → *+{gain5x}* (have {bal5x})\n  10x → *+{gain10x}* (have {bal10x})\n\n📉 *If it goes down:*\n  -50% → *-{loss50}* (have {balL50})\n  -80% → *-{loss80}* (have {balL80})\n  -100% → *-{amount}* (have {balL100})\n\nProceed with this trade?",
        "sniper_on":      "🎯 Sniper mode *ON* — watching for new tokens matching your filters.",
        "sniper_off":     "🎯 Sniper mode *OFF*.",
        "sniper_fired":   "🎯 *SNIPER FIRED!*\n\n*${symbol}* matched your filters!\nScore: *{score}/100* — {verdict}\nMC: *{mc}*\nLiq: *{liq}*\nBought: *{amount}*\nPrice: *{price}*\nCash left: *{cash}*",
        "dca_set":        "✅ *DCA Orders Set for ${symbol}*\n\n{lines}\n\nThe bot will auto-buy when each MC target is reached.",
        "dca_fired":      "📉 *DCA BUY TRIGGERED*\n\n*${symbol}* reached {mc} MC!\nBought: *{amount}*\nPrice: *{price}*\nCash left: *{cash}*",
        "lang_set":       "✅ Language set to *English*.",
        "confirm_buy":    "✅ Confirm Buy",
        "cancel":         "❌ Cancel",
        "back":           "Back",
        "main_menu":      "🏠 Main Menu",
    },
    "es": {
        "welcome":        "👋 ¡Bienvenido a *AURACLE_XBOT*!\n\nBot avanzado de trading simulado multi-cadena.\n\nConfigura tu saldo inicial:\nMín: $1  |  Máx: $10,000\n\nIngresa tu saldo inicial:",
        "welcome_back":   "⚡ *AURACLE_XBOT*\n\n¡Bienvenido de nuevo, *{username}*!\n💰 Saldo: *{balance}*\n💎 Ahorros: *{savings}*\n\nPega cualquier CA para operar 👇",
        "buy_exec":       "✅ *COMPRA EJECUTADA*\n\n*{name} (${symbol})*\nGastado: *{spent}*\nRecibido: *{tokens} {symbol}*\nPrecio: *{price}*\nMC: *{mc}*\nLiq: *{liq}*\nSaldo restante: *{cash}*",
        "sell_exec":      "✅ *VENTA EJECUTADA*\n\nRecibido: *{received}*\nPrecio: *{price}*  |  *{cx}x*\nMantenido: *{held}h*\nGanancia: *{pnl}*\nSaldo: *{cash}*",
        "risk_card":      "🧮 *CALCULADORA DE RIESGO*\n\n*${symbol}*  |  MC: {mc}\n\nEstás arriesgando *{amount}* ({pct}% del saldo)\n\n📈 *Si sube:*\n  2x → *+{gain2x}* (tendrás {bal2x})\n  5x → *+{gain5x}* (tendrás {bal5x})\n  10x → *+{gain10x}* (tendrás {bal10x})\n\n📉 *Si baja:*\n  -50% → *-{loss50}* (tendrás {balL50})\n  -80% → *-{loss80}* (tendrás {balL80})\n  -100% → *-{amount}* (tendrás {balL100})\n\n¿Proceder con esta operación?",
        "sniper_on":      "🎯 Modo sniper *ACTIVADO* — buscando nuevos tokens según tus filtros.",
        "sniper_off":     "🎯 Modo sniper *DESACTIVADO*.",
        "sniper_fired":   "🎯 *¡SNIPER DISPARADO!*\n\n*${symbol}* coincide con tus filtros!\nPuntaje: *{score}/100* — {verdict}\nMC: *{mc}*\nLiq: *{liq}*\nComprado: *{amount}*\nPrecio: *{price}*\nSaldo restante: *{cash}*",
        "dca_set":        "✅ *Órdenes DCA configuradas para ${symbol}*\n\n{lines}\n\nEl bot comprará automáticamente cuando se alcance cada MC objetivo.",
        "dca_fired":      "📉 *COMPRA DCA ACTIVADA*\n\n*${symbol}* alcanzó {mc} de MC!\nComprado: *{amount}*\nPrecio: *{price}*\nSaldo restante: *{cash}*",
        "lang_set":       "✅ Idioma establecido a *Español*.",
        "confirm_buy":    "✅ Confirmar Compra",
        "cancel":         "❌ Cancelar",
        "back":           "Volver",
        "main_menu":      "🏠 Menú Principal",
    },
    "pt": {
        "welcome":        "👋 Bem-vindo ao *AURACLE_XBOT*!\n\nBot avançado de trading simulado multi-chain.\n\nDefina seu saldo inicial:\nMín: $1  |  Máx: $10,000\n\nDigite seu saldo inicial:",
        "welcome_back":   "⚡ *AURACLE_XBOT*\n\nBem-vindo de volta, *{username}*!\n💰 Saldo: *{balance}*\n💎 Poupança: *{savings}*\n\nCole qualquer CA para negociar 👇",
        "buy_exec":       "✅ *COMPRA EXECUTADA*\n\n*{name} (${symbol})*\nGasto: *{spent}*\nRecebido: *{tokens} {symbol}*\nPreço: *{price}*\nMC: *{mc}*\nLiq: *{liq}*\nSaldo restante: *{cash}*",
        "sell_exec":      "✅ *VENDA EXECUTADA*\n\nRecebido: *{received}*\nPreço: *{price}*  |  *{cx}x*\nMantido: *{held}h*\nLucro: *{pnl}*\nSaldo: *{cash}*",
        "risk_card":      "🧮 *CALCULADORA DE RISCO*\n\n*${symbol}*  |  MC: {mc}\n\nVocê está arriscando *{amount}* ({pct}% do saldo)\n\n📈 *Se subir:*\n  2x → *+{gain2x}* (terá {bal2x})\n  5x → *+{gain5x}* (terá {bal5x})\n  10x → *+{gain10x}* (terá {bal10x})\n\n📉 *Se cair:*\n  -50% → *-{loss50}* (terá {balL50})\n  -80% → *-{loss80}* (terá {balL80})\n  -100% → *-{amount}* (terá {balL100})\n\nProsseguir com esta operação?",
        "sniper_on":      "🎯 Modo sniper *ATIVADO* — procurando novos tokens com seus filtros.",
        "sniper_off":     "🎯 Modo sniper *DESATIVADO*.",
        "sniper_fired":   "🎯 *SNIPER DISPARADO!*\n\n*${symbol}* corresponde aos seus filtros!\nPontuação: *{score}/100* — {verdict}\nMC: *{mc}*\nLiq: *{liq}*\nComprado: *{amount}*\nPreço: *{price}*\nSaldo restante: *{cash}*",
        "dca_set":        "✅ *Ordens DCA configuradas para ${symbol}*\n\n{lines}\n\nO bot comprará automaticamente quando cada MC alvo for atingido.",
        "dca_fired":      "📉 *COMPRA DCA ATIVADA*\n\n*${symbol}* atingiu {mc} de MC!\nComprado: *{amount}*\nPreço: *{price}*\nSaldo restante: *{cash}*",
        "lang_set":       "✅ Idioma definido para *Português*.",
        "confirm_buy":    "✅ Confirmar Compra",
        "cancel":         "❌ Cancelar",
        "back":           "Voltar",
        "main_menu":      "🏠 Menu Principal",
    },
    "fr": {
        "welcome":        "👋 Bienvenue sur *AURACLE_XBOT*!\n\nBot de trading papier multi-chaîne avancé.\n\nDéfinissez votre solde de départ:\nMin: $1  |  Max: $10 000\n\nEntrez votre solde de départ:",
        "welcome_back":   "⚡ *AURACLE_XBOT*\n\nBienvenue, *{username}*!\n💰 Solde: *{balance}*\n💎 Épargne: *{savings}*\n\nCollez n'importe quelle CA pour trader 👇",
        "buy_exec":       "✅ *ACHAT EXÉCUTÉ*\n\n*{name} (${symbol})*\nDépensé: *{spent}*\nReçu: *{tokens} {symbol}*\nPrix: *{price}*\nMC: *{mc}*\nLiq: *{liq}*\nSolde restant: *{cash}*",
        "sell_exec":      "✅ *VENTE EXÉCUTÉE*\n\nReçu: *{received}*\nPrix: *{price}*  |  *{cx}x*\nDétenu: *{held}h*\nGain: *{pnl}*\nSolde: *{cash}*",
        "risk_card":      "🧮 *CALCULATEUR DE RISQUE*\n\n*${symbol}*  |  MC: {mc}\n\nVous risquez *{amount}* ({pct}% du solde)\n\n📈 *Si ça monte:*\n  2x → *+{gain2x}* (aurez {bal2x})\n  5x → *+{gain5x}* (aurez {bal5x})\n  10x → *+{gain10x}* (aurez {bal10x})\n\n📉 *Si ça baisse:*\n  -50% → *-{loss50}* (aurez {balL50})\n  -80% → *-{loss80}* (aurez {balL80})\n  -100% → *-{amount}* (aurez {balL100})\n\nProcéder avec ce trade?",
        "sniper_on":      "🎯 Mode sniper *ACTIVÉ* — surveillance de nouveaux tokens selon vos filtres.",
        "sniper_off":     "🎯 Mode sniper *DÉSACTIVÉ*.",
        "sniper_fired":   "🎯 *SNIPER DÉCLENCHÉ!*\n\n*${symbol}* correspond à vos filtres!\nScore: *{score}/100* — {verdict}\nMC: *{mc}*\nLiq: *{liq}*\nAcheté: *{amount}*\nPrix: *{price}*\nSolde restant: *{cash}*",
        "dca_set":        "✅ *Ordres DCA configurés pour ${symbol}*\n\n{lines}\n\nLe bot achètera automatiquement à chaque MC cible atteint.",
        "dca_fired":      "📉 *ACHAT DCA DÉCLENCHÉ*\n\n*${symbol}* a atteint {mc} de MC!\nAcheté: *{amount}*\nPrix: *{price}*\nSolde restant: *{cash}*",
        "lang_set":       "✅ Langue définie sur *Français*.",
        "confirm_buy":    "✅ Confirmer l'achat",
        "cancel":         "❌ Annuler",
        "back":           "Retour",
        "main_menu":      "🏠 Menu Principal",
    },
    "zh": {
        "welcome":        "👋 欢迎使用 *AURACLE_XBOT*!\n\n高级多链模拟交易机器人。\n\n设置起始余额:\n最低: $1  |  最高: $10,000\n\n请输入起始余额:",
        "welcome_back":   "⚡ *AURACLE_XBOT*\n\n欢迎回来，*{username}*！\n💰 余额: *{balance}*\n💎 储蓄: *{savings}*\n\n粘贴任意合约地址开始交易 👇",
        "buy_exec":       "✅ *买入成功*\n\n*{name} (${symbol})*\n花费: *{spent}*\n获得: *{tokens} {symbol}*\n价格: *{price}*\n市值: *{mc}*\n流动性: *{liq}*\n剩余余额: *{cash}*",
        "sell_exec":      "✅ *卖出成功*\n\n收到: *{received}*\n价格: *{price}*  |  *{cx}x*\n持有时间: *{held}h*\n盈亏: *{pnl}*\n余额: *{cash}*",
        "risk_card":      "🧮 *风险计算器*\n\n*${symbol}*  |  市值: {mc}\n\n您正在冒险 *{amount}*（余额的 {pct}%）\n\n📈 *如果上涨:*\n  2x → *+{gain2x}*（将有 {bal2x}）\n  5x → *+{gain5x}*（将有 {bal5x}）\n  10x → *+{gain10x}*（将有 {bal10x}）\n\n📉 *如果下跌:*\n  -50% → *-{loss50}*（将有 {balL50}）\n  -80% → *-{loss80}*（将有 {balL80}）\n  -100% → *-{amount}*（将有 {balL100}）\n\n确认进行此交易？",
        "sniper_on":      "🎯 狙击手模式已 *开启* — 正在监控符合您筛选条件的新代币。",
        "sniper_off":     "🎯 狙击手模式已 *关闭*。",
        "sniper_fired":   "🎯 *狙击触发！*\n\n*${symbol}* 符合您的筛选条件！\n评分: *{score}/100* — {verdict}\n市值: *{mc}*\n流动性: *{liq}*\n已买入: *{amount}*\n价格: *{price}*\n剩余余额: *{cash}*",
        "dca_set":        "✅ *已为 ${symbol} 设置 DCA 订单*\n\n{lines}\n\n机器人将在每个市值目标达到时自动买入。",
        "dca_fired":      "📉 *DCA 买入触发*\n\n*${symbol}* 市值达到 {mc}！\n已买入: *{amount}*\n价格: *{price}*\n剩余余额: *{cash}*",
        "lang_set":       "✅ 语言已设置为 *中文*。",
        "confirm_buy":    "✅ 确认买入",
        "cancel":         "❌ 取消",
        "back":           "返回",
        "main_menu":      "🏠 主菜单",
    },
}

def t(ud: dict, key: str, **kwargs) -> str:
    """Return translated string for the user's language, falling back to English."""
    lang = ud.get("language", "en") if ud else "en"
    lang_dict = TRANSLATIONS.get(lang, TRANSLATIONS["en"])
    text = lang_dict.get(key, TRANSLATIONS["en"].get(key, key))
    return text.format(**kwargs) if kwargs else text


def risk_card_text(ud: dict, symbol: str, mc: float, amount: float) -> str:
    """Build the risk calculator card text."""
    bal    = ud.get("balance", 0)
    pct    = round(amount / bal * 100, 1) if bal > 0 else 0
    gain2x = money(amount * 1)
    gain5x = money(amount * 4)
    gain10x= money(amount * 9)
    loss50 = money(amount * 0.5)
    loss80 = money(amount * 0.8)
    return t(ud, "risk_card",
        symbol=symbol, mc=mc_str(mc), amount=money(amount), pct=pct,
        gain2x=gain2x,  bal2x=money(bal + amount),
        gain5x=gain5x,  bal5x=money(bal + amount * 4),
        gain10x=gain10x,bal10x=money(bal + amount * 9),
        loss50=loss50,  balL50=money(bal - amount * 0.5),
        loss80=loss80,  balL80=money(bal - amount * 0.8),
        balL100=money(bal - amount),
    )

users: dict = {}
trade_log: dict = {}
pending: dict = {}
chart_msg_ids: dict = {}   # uid -> message_id of last chart sent, for deletion on refresh
ca_msg_ids:    dict = {}   # uid -> message_id of the user's CA paste, deleted with the card


async def _cleanup_token_view(bot, uid: int, chat_id: int):
    """
    Delete the chart photo + the user's original CA message for this user.
    Called whenever the user navigates away from a token card (Back, Main Menu,
    buy/sell complete, new CA pasted, etc.)
    Safe to call even when nothing exists — all errors are swallowed.
    """
    # Delete chart photo
    chart_id = chart_msg_ids.pop(uid, None)
    if chart_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=chart_id)
        except Exception:
            pass
    # Delete the original CA message the user typed
    ca_id = ca_msg_ids.pop(uid, None)
    if ca_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=ca_id)
        except Exception:
            pass

# ── [CRIT-2] Contract address validation ─────────────────────────────────────
_SOLANA_ADDR_RE = _re.compile(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$')
_EVM_ADDR_RE    = _re.compile(r'^0x[0-9a-fA-F]{40}$')

def is_valid_contract(addr: str) -> bool:
    """Return True if addr looks like a valid Solana or EVM contract address."""
    if not addr or not isinstance(addr, str):
        return False
    addr = addr.strip()
    return bool(_SOLANA_ADDR_RE.match(addr) or _EVM_ADDR_RE.match(addr))

# ── [HIGH-3] Per-user async locks for trade operations ───────────────────────
_user_locks: dict = {}

# Cache of AI signal data for channel card toggle (contract → data, TTL 24h)
# Lets the AI Analysis toggle re-render without re-fetching
_channel_ai_cache: dict = {}   # contract -> {info, sc, ai, channel_id, ts}

def _prune_ai_cache():
    """Remove entries older than 24 h."""
    cutoff = _time.time() - 86400
    stale  = [k for k, v in _channel_ai_cache.items() if v.get("ts", 0) < cutoff]
    for k in stale:
        del _channel_ai_cache[k]

def get_user_lock(uid: int) -> _asyncio.Lock:
    """Return (creating if needed) a per-user asyncio.Lock."""
    if uid not in _user_locks:
        _user_locks[uid] = _asyncio.Lock()
    return _user_locks[uid]

# ── [HIGH-4] Per-user rate limiting ──────────────────────────────────────────
_last_action: dict = {}
_RATE_LIMIT_GAP = 1.5  # seconds

def is_rate_limited(uid: int) -> bool:
    """Return True if user is acting too fast. Updates timestamp on allow."""
    now = _time.time()
    if now - _last_action.get(uid, 0) < _RATE_LIMIT_GAP:
        return True
    _last_action[uid] = now
    return False

# ── [MED-6] Collection size caps ─────────────────────────────────────────────
MAX_LIMIT_ORDERS = 50
MAX_PRICE_ALERTS = 20
MAX_DCA_TARGETS  = 10
MAX_KOL_WALLETS  = 20
MAX_WATCHLIST    = 50
MAX_TRADE_LOG    = 500

# ── [MED-7] Username sanitisation ────────────────────────────────────────────
_MD_SPECIAL_RE = _re.compile(r'[_*\[\]()~`>#+\-=|{}.!\\]')

def sanitise_username(name: str) -> str:
    """Strip Markdown special chars and truncate to 32 chars."""
    return _MD_SPECIAL_RE.sub('', str(name or "")).strip()[:32] or "User"
_sniper_analysis_cache: dict = {}  # uid -> {contract -> {info, sc, ai}} for View Analysis button

# ── Channel Milestone Tracker ─────────────────────────────────────────────────
# Tracks every token broadcast to a channel so we can post milestone updates.
# Structure: {uid: {contract: {
#     "symbol":      str,
#     "entry_mc":    float,    # MC at time of call
#     "entry_price": float,    # price at time of call
#     "called_at":   str,      # ISO timestamp
#     "channel_id":  int,      # channel to post updates to
#     "milestones_hit": set,   # e.g. {2, 5, 10} to avoid re-posting
# }}}
_channel_calls: dict = {}
_rug_liq_prev: dict = {}            # uid -> {contract -> last_liq} for rug pull detection
_competitions: dict = {}            # code -> competition dict (in-memory; not persisted across restart)
_bot_username: str = ""                 # fetched once on first sniper_job run, used for deep links

_FONT_CACHE: dict = {}   # {"bold": path, "regular": path} — resolved once

def _resolve_fonts() -> tuple:
    """Scan font paths once and cache the result."""
    if _FONT_CACHE:
        return _FONT_CACHE.get("bold"), _FONT_CACHE.get("regular")
    _dir = os.path.dirname(os.path.abspath(__file__))
    bold = None
    for _p in [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Black.ttc",
        os.path.join(_dir, "DejaVuSans-Bold.ttf"),
        os.path.join("/app", "DejaVuSans-Bold.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    ]:
        if os.path.exists(_p): bold = _p; break
    regular = None
    for _p in [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Medium.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-DemiLight.ttc",
        os.path.join(_dir, "DejaVuSans.ttf"),
        os.path.join("/app", "DejaVuSans.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    ]:
        if os.path.exists(_p): regular = _p; break
    _FONT_CACHE["bold"] = bold
    _FONT_CACHE["regular"] = regular
    return bold, regular


def generate_trade_card(symbol: str, chain: str, pnl_str: str, x_val: str, held_h: str, bought_str: str, position_str: str, username: str, pnl_pct: str, pnl_positive: bool, closed_at: datetime | None = None) -> "io.BytesIO | None":
    if not PILLOW_OK:
        return None
    try:
        W, H = 1100, 580
        img = Image.new("RGB", (W, H), color=(8, 10, 18))
        draw = ImageDraw.Draw(img)

        # ── Fonts: use cached path resolution ────────────────────────────────
        _bold, _regular = _resolve_fonts()

        try:
            if not _bold or not _regular:
                raise Exception("fonts missing")
            font_pill        = ImageFont.truetype(_bold,    68)
            font_label       = ImageFont.truetype(_regular, 30)
            font_value       = ImageFont.truetype(_bold,    30)
            font_brand       = ImageFont.truetype(_bold,    24)
            font_user        = ImageFont.truetype(_bold,    28)
            font_tiny        = ImageFont.truetype(_regular, 19)
            font_badge       = ImageFont.truetype(_bold,    22)
            font_stamp       = ImageFont.truetype(_regular, 24)
            font_stamp_bold  = ImageFont.truetype(_bold,    24)
        except Exception:
            font_pill = font_label = font_value = font_brand = font_user = \
            font_tiny = font_badge = font_stamp = font_stamp_bold = ImageFont.load_default()

        # ── Chain label & colours ─────────────────────────────────────────────
        chain_short  = {"solana":"SOL","sol":"SOL","ethereum":"ETH","eth":"ETH",
                        "base":"BASE","bsc":"BNB","bnb":"BNB","arbitrum":"ARB",
                        "arb":"ARB","polygon":"MATIC","matic":"MATIC",
                        "avalanche":"AVAX","avax":"AVAX","sui":"SUI"}
        chain_label  = chain_short.get(chain.lower(), chain.upper()[:4])
        chain_colors = {"SOL":(153,69,255),"ETH":(98,126,234),"BASE":(0,82,255),
                        "BNB":(243,186,47),"ARB":(40,160,240),"MATIC":(130,71,229),
                        "AVAX":(232,65,66),"SUI":(78,122,255)}
        badge_col = chain_colors.get(chain_label, (80,100,160))

        # ── Background gradient ───────────────────────────────────────────────
        for y in range(H):
            t = y / H
            draw.line([(0,y),(W,y)], fill=(int(8+t*4), int(10+t*6), int(18+t*14)))

        # ── Side glow (green win / red loss) ─────────────────────────────────
        glow_col = (0,45,20) if pnl_positive else (45,6,6)
        for i in range(100, 0, -1):
            draw.rectangle([0, 0, i*2, H], fill=glow_col)

        # ── Character art right side ──────────────────────────────────────────
        base_dir  = os.path.dirname(os.path.abspath(__file__))
        char_file = "win_char.jpg" if pnl_positive else "loss_char.jpg"
        char_path = os.path.join(base_dir, char_file)
        if not os.path.exists(char_path):
            char_path = os.path.join("/app", char_file)
        if os.path.exists(char_path):
            char   = Image.open(char_path).convert("RGBA")
            char_w = int(char.width * H / char.height)
            char   = char.resize((char_w, H), Image.LANCZOS)
            char_x = W - char_w - 5
            img.paste(char, (char_x, 0), char)
            # Fade edge so text on left stays readable
            overlay  = Image.new("RGBA", (W, H), (0,0,0,0))
            ov_draw  = ImageDraw.Draw(overlay)
            fade_end = char_x + 180
            for x in range(max(0, char_x), min(fade_end, W)):
                t2    = (x - char_x) / 180
                alpha = int((1 - t2) * 200)
                ov_draw.line([(x,0),(x,H)], fill=(8,10,18,alpha))
            img  = Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")
            draw = ImageDraw.Draw(img)

        # ── Chain badge (top left) ────────────────────────────────────────────
        badge_w = len(chain_label) * 15 + 30
        draw.rounded_rectangle([38, 22, 38+badge_w, 58], radius=12, fill=badge_col)
        draw.text((38+badge_w//2, 40), chain_label, font=font_badge, fill=(255,255,255), anchor="mm")

        # ── Brand (top right) ────────────────────────────────────────────────
        draw.text((W-40, 40), "AURACLE_XBOT", font=font_brand, fill=(185,200,230), anchor="rm")

        # ── Token symbol — dynamic size so any ticker fits ───────────────────
        sym_display = "$" + symbol
        sym_font    = ImageFont.truetype(_bold, 86) if _bold else font_pill
        for size in [86, 72, 60, 48, 38]:
            _f   = ImageFont.truetype(_bold, size) if _bold else font_pill
            bbox = _f.getbbox(sym_display)
            if bbox[2] - bbox[0] < 620:
                sym_font = _f
                break
        draw.text((38, 75), sym_display, font=sym_font, fill=(240, 245, 255))

        # ── PnL pill ─────────────────────────────────────────────────────────
        clean_pnl      = pnl_str.lstrip("$")
        clean_bought   = bought_str.lstrip("$")
        clean_position = position_str.lstrip("$")
        pill_col = (0,200,105) if pnl_positive else (205,38,38)
        txt_col  = (5,15,8)   if pnl_positive else (255,235,235)
        prefix   = "+"        if pnl_positive else "-"
        draw.rounded_rectangle([38, 195, 590, 298], radius=20, fill=pill_col)
        draw.text((68, 246), chain_label + "  " + prefix + "$" + clean_pnl,
                  font=font_pill, fill=txt_col, anchor="lm")

        # ── Stats rows ───────────────────────────────────────────────────────
        pnl_col = (0,220,120) if pnl_positive else (220,75,75)
        stats = [
            ("PNL",      prefix + pnl_pct,                    pnl_col),
            ("Bought",   chain_label + " - $" + clean_bought, (195,210,235)),
            ("Position", chain_label + " - $" + clean_position,(195,210,235)),
            ("Held",     held_h,                               (195,210,235)),
        ]
        for i, (lbl, val, vcol) in enumerate(stats):
            y = 322 + i * 50
            draw.text((38,  y), lbl, font=font_label, fill=(125,140,170))
            draw.text((370, y), val, font=font_value,  fill=vcol)

        # ── Divider — full width ──────────────────────────────────────────────
        draw.line([(38, H-72), (W-40, H-72)], fill=(35,45,68), width=1)

        # ── Bottom left: avatar circle + @username ───────────────────────────
        ax, ay = 55, H - 36
        draw.ellipse([ax-22, ay-22, ax+22, ay+22], fill=(50,70,140))
        draw.text((ax, ay), (username[0].upper() if username else "A"),
                  font=font_tiny, fill=(200,220,255), anchor="mm")
        draw.text((ax+32, ay), "@" + username, font=font_user,
                  fill=(200,215,240), anchor="lm")

        # ── Bottom right: time  •  date ──────────────────────────────────────
        ts = closed_at if closed_at else datetime.now()
        time_str = ts.strftime("%-I:%M %p")          # e.g. 3:40 PM
        date_str = ts.strftime("%b %d, %Y")           # e.g. Mar 05, 2026
        dot      = "  •  "

        # Right-align the three pieces: [time][dot][date]
        t_w  = font_stamp_bold.getbbox(time_str)[2] - font_stamp_bold.getbbox(time_str)[0]
        dt_w = font_stamp.getbbox(dot)[2]            - font_stamp.getbbox(dot)[0]
        d_w  = font_stamp.getbbox(date_str)[2]       - font_stamp.getbbox(date_str)[0]

        rx = W - 40
        draw.text((rx, ay), date_str,  font=font_stamp,      fill=(130,145,175), anchor="rm")
        rx -= d_w
        draw.text((rx, ay), dot,       font=font_stamp,      fill=(60,75,100),   anchor="rm")
        rx -= dt_w
        draw.text((rx, ay), time_str,  font=font_stamp_bold, fill=(185,200,230), anchor="rm")

        # ── Save ─────────────────────────────────────────────────────────────
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        return buf

    except Exception as e:
        logger.error("Card generation error: " + str(e))
        return None


async def get_token(contract: str, force: bool = False) -> dict | None:
    # ── Cache check ──────────────────────────────────────────────────────────
    if not force:
        cached = _token_cache.get(contract)
        if cached and (_time.time() - cached["ts"]) < CACHE_TTL:
            return cached["data"]
    try:
        client = await get_http()
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
        # Multi-timeframe txn data for M/T/V intelligence card
        def _bpct(b, s): return round(b / (b + s) * 100) if (b + s) > 0 else 50
        buys_m5  = int(txns.get("m5",  {}).get("buys",  0) or 0)
        sells_m5 = int(txns.get("m5",  {}).get("sells", 0) or 0)
        buys_h1  = int(txns.get("h1",  {}).get("buys",  0) or 0)
        sells_h1 = int(txns.get("h1",  {}).get("sells", 0) or 0)
        buys_h6  = int(txns.get("h6",  {}).get("buys",  0) or 0)
        sells_h6 = int(txns.get("h6",  {}).get("sells", 0) or 0)
        vol_h6   = float(best.get("volume", {}).get("h6", 0) or 0)
        pair_created = best.get("pairCreatedAt")
        age_h = None
        if pair_created:
            age_h = (datetime.now() - datetime.fromtimestamp(pair_created / 1000)).total_seconds() / 3600
        ch = best.get("priceChange", {})

        # Extract social links
        socials_raw = best.get("info", {}).get("socials", [])
        websites_raw = best.get("info", {}).get("websites", [])
        twitter = ""
        telegram = ""
        website  = ""
        for s in socials_raw:
            stype = s.get("type","").lower()
            url   = s.get("url","").strip()
            if not url:
                continue
            # Ensure fully-qualified URL (Telegram inline buttons require https://)
            if not url.startswith("http"):
                url = "https://" + url
            if stype in ("twitter","x") and not twitter:
                twitter = url
            elif stype == "telegram" and not telegram:
                telegram = url
        for w in websites_raw:
            url = w.get("url","").strip()
            if url:
                if not url.startswith("http"):
                    url = "https://" + url
                if not website:
                    website = url

        # ATH — use highest point from priceChange data
        # DexScreener doesn't have a dedicated ATH endpoint
        # Best proxy: reconstruct from price + % changes
        ath_price = 0.0
        ath_mc    = 0.0
        try:
            cur_price = price
            ch_h24_v  = float(ch.get("h24", 0) or 0)
            ch_h6_v   = float(ch.get("h6",  0) or 0)
            ch_h1_v   = float(ch.get("h1",  0) or 0)
            # Price 24h ago
            p_24h = cur_price / (1 + ch_h24_v/100) if ch_h24_v != -100 else cur_price
            # Price 6h ago
            p_6h  = cur_price / (1 + ch_h6_v/100)  if ch_h6_v  != -100 else cur_price
            # Price 1h ago
            p_1h  = cur_price / (1 + ch_h1_v/100)  if ch_h1_v  != -100 else cur_price
            # ATH = highest of current vs all reconstructed prices
            ath_price = max(cur_price, p_24h, p_6h, p_1h)
            if mc > 0 and cur_price > 0:
                ath_mc = mc * (ath_price / cur_price)
        except Exception:
            pass

        # ── RugCheck security audit (Solana only, best-effort) ───────────────
        no_mint  = None
        freeze   = None
        lp_burn  = None
        top10    = None
        insider  = None
        rug_risks= []
        # New holder-intelligence fields
        top20          = None
        holder_count_rc = None
        sniper_count_rc = 0
        fresh_pct_rc    = None
        smart_wallet_rc = None
        dev_sold_rc     = False
        chain_id_raw = best.get("chainId", "").lower()
        if chain_id_raw in ("solana", "sol"):
            try:
                sem = await _get_rugcheck_semaphore()
                async with sem:
                    await _asyncio.sleep(0.5)   # rate-limit respect
                    rc = await get_http()
                    rc_r = await rc.get(
                        f"https://api.rugcheck.xyz/v1/tokens/{contract}/report",
                        headers={"Accept": "application/json"},
                    )
                if rc_r.status_code == 429:
                    logger.debug("RugCheck rate limited — skipping security audit")
                    rc_r = None
                if rc_r and rc_r.status_code == 200:
                    rc_data = rc_r.json()
                    # Mint / freeze authority
                    no_mint = rc_data.get("mintAuthority") is None
                    freeze  = rc_data.get("freezeAuthority") is None
                    # LP burn %
                    markets = rc_data.get("markets") or []
                    if markets:
                        lp_burn = markets[0].get("lp", {}).get("lpLockedPct", None)
                        if lp_burn is not None:
                            lp_burn = round(float(lp_burn), 1)
                    # Top holders — top10, top20, sniper count
                    rc_holders = rc_data.get("topHolders") or []
                    if rc_holders:
                        top10 = round(sum(float(h.get("pct", 0)) for h in rc_holders[:10]), 1)
                        top20 = round(sum(float(h.get("pct", 0)) for h in rc_holders[:20]), 1)
                        # Count insider-flagged holders as snipers
                        sniper_count_rc = sum(1 for h in rc_holders if h.get("insider") or h.get("sniper"))
                    # Total holder count (RugCheck may expose via tokenMeta or a top-level field)
                    holder_count_rc = (
                        rc_data.get("holders")
                        or rc_data.get("tokenMeta", {}).get("holders")
                        or rc_data.get("holderCount")
                        or None
                    )
                    # Fresh wallet % and SmartWallet count if RugCheck provides them
                    fresh_pct_rc = rc_data.get("freshWalletPct") or rc_data.get("newWalletPct") or None
                    smart_wallet_rc = rc_data.get("smartWallets") or rc_data.get("smartMoneyCount") or None
                    # Insider / dev %
                    insider_pct_v = rc_data.get("insiderNetworkStats", {}).get("insiderPct", None)
                    if insider_pct_v is not None:
                        insider = round(float(insider_pct_v), 1)
                    # Dev sold: if any risk flag mentions dev sold
                    dev_sold_rc = any(
                        "dev" in r.get("name", "").lower() and "sold" in r.get("name", "").lower()
                        for r in rc_data.get("risks", [])
                    )
                    # Risk flags
                    for risk in rc_data.get("risks", []):
                        lvl    = risk.get("level", "").lower()
                        name_r = risk.get("name", "")
                        if lvl in ("danger", "warn") and name_r:
                            rug_risks.append(name_r)
            except Exception as rc_err:
                logger.debug(f"RugCheck fetch failed: {rc_err}")

        result = {
            "symbol":   best.get("baseToken", {}).get("symbol", "???"),
            "name":     best.get("baseToken", {}).get("name", "Unknown"),
            "chain":    best.get("chainId", "unknown"),
            "dex":      best.get("dexId", "unknown"),
            "pair_addr":best.get("pairAddress",""),
            "price":    price,
            "mc":       mc,
            "liq":      liq,
            "liq_pct":  liq_pct,
            "vol_h24":  vol_h24,
            "vol_h1":   vol_h1,
            "vol_m5":   vol_m5,
            "ch_m5":    float(ch.get("m5",  0) or 0),
            "ch_h1":    float(ch.get("h1",  0) or 0),
            "ch_h6":    float(ch.get("h6",  0) or 0),
            "ch_h24":   float(ch.get("h24", 0) or 0),
            "buys":     buys,
            "sells":    sells,
            "buy_pct":  buy_pct,
            "age_h":    age_h,
            "twitter":   twitter,
            "telegram":  telegram,
            "website":   website,
            "ath_price": round(ath_price, 12) if ath_price else 0,
            "ath_mc":    round(ath_mc, 2)    if ath_mc    else 0,
            # Multi-timeframe trade data (for M/T/V intelligence display)
            "buys_m5":    buys_m5,   "sells_m5":  sells_m5,
            "buys_h1":    buys_h1,   "sells_h1":  sells_h1,
            "buys_h6":    buys_h6,   "sells_h6":  sells_h6,
            "buy_pct_m5": _bpct(buys_m5, sells_m5),
            "buy_pct_h1": _bpct(buys_h1, sells_h1),
            "buy_pct_h6": _bpct(buys_h6, sells_h6),
            "vol_h6":     vol_h6,
            # RugCheck security fields (None = not available)
            "no_mint":   no_mint,
            "no_freeze": freeze,
            "lp_burn":   lp_burn,
            "top10_pct": top10,
            "top20_pct": top20,
            "holder_count": holder_count_rc,
            "sniper_count": sniper_count_rc,
            "fresh_pct":    fresh_pct_rc,
            "smart_wallets": smart_wallet_rc,
            "dev_sold":    dev_sold_rc,
            "insider_pct": insider,
            "rug_risks": rug_risks,
            # Pump.fun enrichment (filled by sniper_scan when available)
            "pf_curve":     None,   # bonding curve % 0-100
            "pf_replies":   0,      # community reply count
            "pf_graduated": False,  # True = graduated to Raydium
            "pf_dev_pct":   None,   # dev holding %
            # Helius wallet intelligence (filled when API key set)
            "maker_pct":    None,   # unique buyer wallet % (requires Helius)
            "maker_count":  None,   # unique buyer wallets
            "top3_vol_pct": None,   # top 3 wallet % of volume (wash trade signal)
            # Boost spend (social attention proxy)
            "boost_amount": 0,      # total SOL spent boosting on DexScreener
        }
        _token_cache[contract] = {"data": result, "ts": _time.time()}
        return result
    except Exception as e:
        logger.error(f"DexScreener: {e}")
        return None


def sniper_score(info: dict) -> dict:
    """
    7-category sniper scoring — rebuilt with RugCheck bundle detection,
    pump.fun bonding curve, multi-timeframe volume divergence, and
    Helius maker% when available.
    Max 100 pts.
    """
    score     = 0
    strengths = []
    warnings  = []
    flags     = []

    # ── Raw data ─────────────────────────────────────────────────────────────
    age_h       = info.get("age_h") or 0
    liq         = info.get("liq", 0)
    mc          = info.get("mc", 0)
    liq_pct     = info.get("liq_pct", 0)
    buy_pct     = info.get("buy_pct", 50)
    buy_pct_h1  = info.get("buy_pct_h1", buy_pct)
    buy_pct_m5  = info.get("buy_pct_m5", buy_pct)
    buys        = info.get("buys", 0)
    sells       = info.get("sells", 0)
    buys_h1     = info.get("buys_h1", 0)
    sells_h1    = info.get("sells_h1", 0)
    vol_h1      = info.get("vol_h1", 0)
    vol_m5      = info.get("vol_m5", 0)
    ch_m5       = info.get("ch_m5", 0)
    ch_h1       = info.get("ch_h1", 0)
    twitter     = info.get("twitter", "")
    telegram    = info.get("telegram", "")
    website     = info.get("website", "")
    # RugCheck fields
    no_mint     = info.get("no_mint")
    no_freeze   = info.get("no_freeze")
    lp_burn     = info.get("lp_burn")
    top10_pct   = info.get("top10_pct")
    insider_pct = info.get("insider_pct")
    rug_risks   = info.get("rug_risks", []) or []
    # Pump.fun fields
    pf_curve     = info.get("pf_curve")
    pf_replies   = info.get("pf_replies", 0) or 0
    pf_graduated = info.get("pf_graduated", False)
    # Helius fields
    maker_pct    = info.get("maker_pct")
    top3_vol_pct = info.get("top3_vol_pct")
    # Boost spend
    boost_amount = info.get("boost_amount", 0) or 0

    # ── CATEGORY 1: SAFETY / RUG CHECK (0–30 pts) ────────────────────────────
    # Parse RugCheck risks for critical danger flags
    danger_risks = [r for r in rug_risks if isinstance(r, str)]
    bundle_flag  = any("bundle" in r.lower() for r in danger_risks)
    dev_sold     = any("deployer sold" in r.lower() or "creator sold" in r.lower() for r in danger_risks)
    copycat      = any("copycat" in r.lower() for r in danger_risks)

    if bundle_flag:
        flags.append("🚨 Bundle activity detected — insiders sniped launch")
    if dev_sold:
        flags.append("🚨 Deployer already sold — red flag")
    if copycat:
        flags.append("🚨 Copycat token detected")

    # LP burn scoring
    is_solana = info.get("chain", "").lower() in ("solana", "sol")
    if is_solana:
        if lp_burn is not None:
            if lp_burn >= 90:
                score += 12
                strengths.append(f"🔒 LP burned {lp_burn}%")
            elif lp_burn >= 50:
                score += 8
                strengths.append(f"LP {lp_burn}% burned")
            elif lp_burn >= 20:
                score += 4
                warnings.append(f"LP only {lp_burn}% burned")
            else:
                score += 1
                warnings.append(f"LP barely burned ({lp_burn}%)")
        else:
            score += 4  # No RugCheck data — neutral
    else:
        score += 6  # Non-Solana — LP burn less standard

    # Mint authority
    if no_mint is True:
        score += 8
        strengths.append("✅ No mint authority")
    elif no_mint is False:
        flags.append("🚨 Mint authority active — supply can be inflated")
    # else None = not available (non-Solana) — no score change

    # Freeze authority
    if no_freeze is True:
        score += 5
        strengths.append("✅ No freeze authority")
    elif no_freeze is False:
        warnings.append("Freeze authority enabled")

    # Hard flags from on-chain data
    if liq < 10_000:
        flags.append(f"🚨 Liq ${liq:,.0f} — near-certain rug")
    elif liq < 20_000:
        warnings.append(f"Low liquidity (${liq:,.0f})")

    if liq_pct < 5 and mc > 0:
        flags.append(f"🚨 Liq only {liq_pct}% of MC — easy drain")

    if top10_pct is not None and top10_pct > 65:
        flags.append(f"🚨 Top 10 wallets hold {top10_pct}% — whale trap")
    elif top10_pct is not None and top10_pct > 45:
        warnings.append(f"Top 10 hold {top10_pct}% of supply")

    if insider_pct is not None and insider_pct > 25:
        flags.append(f"🚨 Insider/dev holds {insider_pct}% — dump risk")
    elif insider_pct is not None and insider_pct > 12:
        warnings.append(f"Insider holding elevated ({insider_pct}%)")

    # Helius wash trade signal
    if top3_vol_pct is not None and top3_vol_pct > 70:
        flags.append(f"🚨 Top 3 wallets = {top3_vol_pct}% of volume — wash trading")
    elif top3_vol_pct is not None and top3_vol_pct > 55:
        warnings.append(f"Volume concentrated in top 3 wallets ({top3_vol_pct}%)")

    # ── CATEGORY 2: LAUNCH TIMING (0–20 pts) ─────────────────────────────────
    if pf_curve is not None and is_solana:
        # Pump.fun bonding curve — more precise than age
        if pf_graduated:
            score += 12
            strengths.append("🎓 Graduated to Raydium — proven demand")
        elif 35 <= pf_curve <= 65:
            score += 20
            strengths.append(f"⚡ Sweet spot curve ({pf_curve}%) — gaining traction")
        elif 20 <= pf_curve < 35:
            score += 14
            strengths.append(f"Early curve ({pf_curve}%) — still very fresh")
        elif 65 < pf_curve < 85:
            score += 10
            warnings.append(f"Curve {pf_curve}% — approaching graduation")
        elif pf_curve >= 85 and not pf_graduated:
            score += 6
            warnings.append(f"Curve {pf_curve}% — graduation imminent, may dump")
        else:
            score += 4
    else:
        # Age-based timing for non-Pumpfun or when curve unavailable
        if age_h < 0.25:
            score += 6
            warnings.append("Under 15min — dev bots still active")
        elif age_h < 0.5:
            score += 16
            strengths.append("🔥 Very fresh (15–30min)")
        elif age_h < 1.5:
            score += 20
            strengths.append("⚡ Optimal window (30min–1.5h)")
        elif age_h < 3:
            score += 14
            strengths.append("Early entry (1.5–3h)")
        elif age_h < 5:
            score += 7
            warnings.append(f"Getting late ({round(age_h,1)}h in)")
        elif age_h < 6:
            score += 2
            warnings.append(f"Nearly too old ({round(age_h,1)}h)")
        else:
            score += 0

    # ── CATEGORY 3: SOCIAL ATTENTION (0–15 pts) ──────────────────────────────
    social_pts = 0
    social_count = sum([bool(twitter), bool(telegram), bool(website)])
    if social_count >= 3:
        social_pts += 8
        strengths.append("Full social profile (TW + TG + Web)")
    elif social_count == 2:
        social_pts += 6
        strengths.append("Twitter + Telegram present")
    elif social_count == 1:
        social_pts += 2
        warnings.append("Only 1 social link — incomplete profile")
    else:
        flags.append("🚨 Zero socials — throwaway token")

    # Boost spend = team paid real money to promote (social attention proxy)
    if boost_amount >= 50:
        social_pts += 5
        strengths.append(f"High boost spend ({boost_amount:.0f} SOL) — serious team")
    elif boost_amount >= 20:
        social_pts += 3
        strengths.append(f"Active boost ({boost_amount:.0f} SOL)")
    elif boost_amount >= 5:
        social_pts += 1

    # Pump.fun community engagement
    if pf_replies >= 100:
        social_pts += 4
        strengths.append(f"Active community ({pf_replies} replies)")
    elif pf_replies >= 30:
        social_pts += 2

    score += min(social_pts, 15)

    # ── CATEGORY 4: ENTRY MC (0–15 pts) ──────────────────────────────────────
    if 20_000 <= mc <= 100_000:
        score += 15
        strengths.append(f"🎯 Micro cap entry ({mc/1000:.0f}K)")
    elif 100_000 < mc <= 400_000:
        score += 12
        strengths.append(f"Good entry MC ({mc/1000:.0f}K)")
    elif 400_000 < mc <= 1_000_000:
        score += 7
        warnings.append(f"Mid MC ({mc/1000:.0f}K — less upside)")
    elif 1_000_000 < mc <= 2_000_000:
        score += 3
        warnings.append(f"High MC (${mc/1e6:.1f}M)")
    elif mc < 20_000:
        score += 4
        warnings.append(f"Very low MC (${mc:,.0f}) — ultra risky")
    else:
        score += 0
        warnings.append(f"Already pumped (${mc/1e6:.1f}M MC)")

    # ── CATEGORY 5: ORGANIC SPREAD (0–10 pts) ────────────────────────────────
    # Use h1 buys if available, else h24 as fallback
    h1_tx    = buys_h1 + sells_h1
    h1_buys  = buys_h1 if h1_tx > 0 else buys
    eff_age  = max(age_h, 0.25)
    buyers_per_hour = h1_buys / min(eff_age, 1)  # normalise to 1h window

    if buyers_per_hour >= 80:
        score += 10
        strengths.append(f"🚀 Viral spread ({int(buyers_per_hour)}/hr buyers)")
    elif buyers_per_hour >= 40:
        score += 8
        strengths.append(f"Strong buyer spread ({int(buyers_per_hour)}/hr)")
    elif buyers_per_hour >= 15:
        score += 5
    elif buyers_per_hour >= 5:
        score += 2
        warnings.append(f"Low buyer rate ({int(buyers_per_hour)}/hr)")
    else:
        score += 0
        warnings.append(f"Very few buyers — thin interest")

    # Average buy size check (organic = small buys)
    avg_buy_size = (vol_h1 / h1_buys) if h1_buys > 0 else 0
    if 0 < avg_buy_size < 150:
        score += 3  # bonus: small retail buys
        strengths.append(f"Retail organic (avg buy ${avg_buy_size:.0f})")
    elif avg_buy_size > 3_000:
        flags.append(f"🚨 Avg buy ${avg_buy_size:,.0f} — whale dominated")
    elif avg_buy_size > 1_500:
        warnings.append(f"Large avg buy size (${avg_buy_size:,.0f})")

    # Volume/MC ratio — wash trading check
    vol_mc_ratio = (vol_h1 / mc) if mc > 0 else 0
    if vol_mc_ratio > 12:
        flags.append(f"🚨 Vol/MC={vol_mc_ratio:.1f}x — probable wash trading")
    elif vol_mc_ratio > 7:
        warnings.append(f"High vol/MC ({vol_mc_ratio:.1f}x) — verify organic")

    # ── CATEGORY 6: BUY PRESSURE + MOMENTUM (0–10 pts) ───────────────────────
    # Use h1 buy_pct for momentum, m5 for immediate pressure
    momentum_pts = 0
    if buy_pct_h1 >= 65:
        momentum_pts += 6
        strengths.append(f"Dominant buy pressure H1 ({buy_pct_h1}%)")
    elif buy_pct_h1 >= 55:
        momentum_pts += 4
    elif buy_pct_h1 >= 50:
        momentum_pts += 2
    else:
        warnings.append(f"Weak buy pressure H1 ({buy_pct_h1}%)")

    # 5m momentum
    if buy_pct_m5 >= 60 and ch_m5 > 0:
        momentum_pts += 4
        strengths.append(f"Accelerating right now (5m:{ch_m5:+.1f}%)")
    elif ch_m5 > 0 and ch_h1 > 0 and ch_h1 < 200:
        momentum_pts += 3
        strengths.append(f"Building momentum (5m:{ch_m5:+.1f}% 1h:{ch_h1:+.1f}%)")
    elif ch_h1 >= 200:
        momentum_pts += 1
        warnings.append(f"Parabolic (+{ch_h1:.0f}%) — late entry risk")
    elif ch_m5 < -10 and ch_h1 < -10:
        warnings.append(f"Dumping hard ({ch_m5:.1f}% / {ch_h1:.1f}%)")

    # Volume acceleration (m5 vs h1 average)
    vol_h1_per_5m = (vol_h1 / 12) if vol_h1 > 0 else 0
    if vol_m5 > vol_h1_per_5m * 2 and vol_m5 > 500:
        momentum_pts += 2
        strengths.append("Volume spike now 🔥")

    # Helius maker% bonus
    if maker_pct is not None:
        if maker_pct >= 60:
            momentum_pts += 3
            strengths.append(f"Healthy maker distribution ({maker_pct}% buyers)")
        elif maker_pct >= 50:
            momentum_pts += 1
        elif maker_pct < 35:
            warnings.append(f"Few unique buyers ({maker_pct}%) — possible shill")

    score += min(momentum_pts, 10)

    # ── CATEGORY 7: PUMPFUN GRADUATION BONUS (0–8 pts) ───────────────────────
    if is_solana and pf_curve is not None:
        if pf_graduated:
            score += 8  # Already counted in timing, but graduation = validated demand
        elif pf_curve is not None and pf_curve > 50 and age_h < 2:
            score += 5
            strengths.append(f"Fast curve fill ({pf_curve}% in {round(age_h,1)}h)")

    # ── HARD FLAG PENALTIES ───────────────────────────────────────────────────
    flag_count = len(flags)
    if flag_count >= 3:
        score = max(0, score - 35)
    elif flag_count == 2:
        score = max(0, score - 20)
    elif flag_count == 1:
        score = max(0, score - 10)

    score = min(score, 100)

    # ── Verdict ──────────────────────────────────────────────────────────────
    if flag_count >= 2:
        verdict = "🚨 HIGH RISK — CHECK FLAGS"
    elif score >= 75:
        verdict = "🟢 STRONG SNIPE CANDIDATE"
    elif score >= 55:
        verdict = "🟡 POSSIBLE ENTRY — VERIFY"
    elif score >= 35:
        verdict = "🟠 WEAK SETUP — CAUTION"
    else:
        verdict = "🔴 SKIP"

    return {
        "score":     score,
        "verdict":   verdict,
        "strengths": strengths[:5],
        "warnings":  warnings[:4],
        "flags":     flags,
        "icon":      "🟢" if score >= 75 else "🟡" if score >= 55 else "🟠" if score >= 35 else "🔴",
    }



def score_token(info: dict) -> dict:
    """AURACLE token scoring for manual CA scans (distinct from sniper_score)."""
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
            "username":         sanitise_username(uname) or "User" + str(uid),
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
            "mood_tracking":    False,
            "mood_stats":       {},
            "daily_trade_counts": [],
            "avg_daily_trades": 0,
            "balance_limit":    10_000.0,
            "unlocked_rewards": [],
            "competitions":     {},
            "watchlist":        {},
            "price_alerts_mc":  {},
            "limit_orders_mc":  {},
            "challenge":        None,
            "referrer":         None,
            "referrals":        [],
            "channel_id":       None,
            "accounts":         {},
            "active_account":   "main",
            "whale_alerts":     True,
            "copy_trading":     None,
            "copy_paused":      False,
            # Risk Calculator
            "risk_calc":        False,
            # Token Sniper v2
            "sniper_auto":       False,   # Mode 1: fully automatic
            "sniper_advisory":   False,   # Mode 2: AI report, user confirms
            "sniper_auto_notify":    True,
            "sniper_adv_notify":     True,
            "sniper_auto_sl":        True,   # auto stop loss after snipe
            "sniper_auto_sl_pct":    40.0,
            "sniper_auto_tp":        True,   # auto take profit after snipe
            "sniper_auto_tp_x":      [2.0, 5.0],  # sell 50% at 2x, 50% at 5x
            "sniper_daily_budget":   500.0,
            "sniper_daily_spent":    0.0,
            "sniper_daily_date":     None,
            "sniper_chains": {
                "solana": True, "ethereum": True, "base": True,
                "bsc": True, "arbitrum": True,
            },
            "sniper_filters": {
                "min_score":        62,      # raised from 45 — above average only
                "min_liq":          20_000,  # raised from $15K
                "min_mc":           30_000,
                "max_mc":           500_000, # lowered from $5M — cut high MC rugs
                "max_age_h":        2,       # lowered from 6h — first 2h window only
                "buy_amount":       100,
                "min_buys_h1":      30,
                "min_buy_pct":      55,      # raised from 52
                "max_vol_mc_ratio": 6.0,     # lowered from 8.0
                "min_liq_pct":      8,       # NEW: liq must be ≥8% of MC
                "max_top10_pct":    55,      # NEW: hard block if top10 > 55%
                "min_lp_burn":      80,      # NEW: LP must be ≥80% burned (Solana)
            },
            "sniper_bought":    [],
            "sniper_seen":      {},   # {contract: timestamp} — persistent dedup memory
            "sniper_log":       [],   # history of every sniper decision
            "kol_wallets":      [],   # list of {address, label, chain} to track
            "kol_alerts_on":    True, # KOL buy alert notifications
            "sniper_broadcast_channel": None,   # channel/group ID for signal broadcasts
            # Quick Buy
            "quick_buy_amount":  100.0,        # Feature: one-tap quick buy amount
            # Milestone notifications
            "milestone_notif":      True,       # Feature: holdings x milestone alerts
            "milestone_notif_dump": True,       # Feature: -50% dump alert
            # Rug Pull Warning
            "rug_warn_enabled":  False,         # Feature: liq drop early warning (OFF by default)
            "rug_warn_threshold": 30,           # Feature: % liq drop in one cycle to trigger
            # DCA by Market Cap
            "dca_orders":       [],
            # Language
            "language":         "en",
        }
        trade_log[uid] = []
        save_user(uid, users[uid])   # persist immediately
    return users[uid]


async def fetch_ohlcv(pair_addr: str, chain_id: str) -> list:
    try:
        url = (
            f"https://api.geckoterminal.com/api/v2/networks/{chain_id}"
            f"/pools/{pair_addr}/ohlcv/minute?aggregate=5&limit=60"
        )
        client = await get_http()
        r = await client.get(url, headers={"Accept": "application/json"})
        if r.status_code == 200:
            return r.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
    except Exception:
        pass
    return []


def generate_price_chart(info: dict, ohlcv: list):
    try:
        import matplotlib.patches as patches
        from datetime import datetime as dt

        bg_col    = "#0a0d18"
        green_col = "#00c86a"
        red_col   = "#e02626"
        grid_col  = "#1a2035"
        text_col  = "#8090b0"
        symbol = info.get("symbol", "TOKEN")
        price  = float(info.get("price", 0))
        ch_24  = float(info.get("ch_h24", 0))
        ch_col = green_col if ch_24 >= 0 else red_col
        ch_str = ("+" if ch_24 >= 0 else "") + str(round(ch_24, 1)) + "%"
        mc     = info.get("mc", 0)

        fig, ax = plt.subplots(figsize=(9, 4))
        fig.patch.set_facecolor(bg_col)
        ax.set_facecolor(bg_col)
        ax.spines[:].set_visible(False)
        ax.grid(axis="y", color=grid_col, linewidth=0.7, zorder=0)
        ax.tick_params(colors=text_col, labelsize=8)

        def _mc_str(v):
            if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
            if v >= 1_000:     return f"${v/1_000:.1f}K"
            return f"${v:.0f}"

        if ohlcv and len(ohlcv) >= 3:
            candles = list(reversed(ohlcv))[-48:]  # oldest left, newest right
            w = 0.6
            for i, c in enumerate(candles):
                o, h, l, cl = c[1], c[2], c[3], c[4]
                col = green_col if cl >= o else red_col
                ax.plot([i, i], [l, h], color=col, linewidth=0.9, zorder=2)
                rect = patches.Rectangle(
                    (i - w/2, min(o, cl)), w,
                    max(abs(cl - o), (h - l) * 0.015),
                    facecolor=col, edgecolor=col, linewidth=0, zorder=3
                )
                ax.add_patch(rect)
            tick_pos = list(range(0, len(candles), 12))
            tick_lbl = [dt.utcfromtimestamp(candles[i][0]).strftime("%H:%M") for i in tick_pos]
            ax.set_xticks(tick_pos)
            ax.set_xticklabels(tick_lbl, color=text_col, fontsize=8)
            ax.set_xlim(-1, len(candles))
            highs = [c[2] for c in candles]
            ath_i = int(np.argmax(highs))
            ax.annotate(f"ATH ${highs[ath_i]:.6g}", xy=(ath_i, highs[ath_i]),
                        xytext=(0, 8), textcoords="offset points",
                        color="#ffd700", fontsize=7, ha="center", fontweight="bold")
        else:
            ch_m5  = float(info.get("ch_m5",  0)) / 100
            ch_h1  = float(info.get("ch_h1",  0)) / 100
            ch_h6  = float(info.get("ch_h6",  0)) / 100
            ch_h24v = float(info.get("ch_h24", 0)) / 100
            p_24h  = price / (1 + ch_h24v) if ch_h24v != -1 else price
            p_6h   = price / (1 + ch_h6)  if ch_h6  != -1 else price
            p_1h   = price / (1 + ch_h1)  if ch_h1  != -1 else price
            p_5m   = price / (1 + ch_m5)  if ch_m5  != -1 else price
            times  = ["-24h", "-6h", "-1h", "-5m", "Now"]
            prices = [p_24h, p_6h, p_1h, p_5m, price]
            lc     = green_col if price >= p_24h else red_col
            xs     = np.arange(len(times))
            ax.plot(xs, prices, color=lc, linewidth=2.5, zorder=3)
            ax.fill_between(xs, prices, min(prices)*0.995, color=lc+"22", zorder=2)
            ax.scatter(xs, prices, color=lc, s=55, zorder=4)
            ax.set_xticks(xs)
            ax.set_xticklabels(times, color=text_col, fontsize=10)

        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"${v:.6g}"))
        fig.text(0.03, 0.96, f"${symbol}  |  MC: {_mc_str(mc)}  |  5M Chart",
                 color="white", fontsize=12, fontweight="bold", va="top")
        fig.text(0.97, 0.96, f"24h: {ch_str}  Now: ${price:.6g}",
                 color=ch_col, fontsize=10, fontweight="bold", va="top", ha="right")
        plt.tight_layout(rect=[0, 0, 1, 0.92])
        buf = io.BytesIO()
        plt.savefig(buf, format="PNG", dpi=140, bbox_inches="tight",
                    facecolor=bg_col, edgecolor="none")
        plt.close(fig)
        buf.seek(0)
        return buf
    except Exception as e:
        logger.warning(f"Chart error: {e}")
        return None


def money(n: float) -> str:
    if abs(n) >= 1_000_000_000:
        v = round(n/1_000_000_000, 2)
        return "$" + (str(int(v)) if v == int(v) else str(v)) + "B"
    if abs(n) >= 1_000_000:
        v = round(n/1_000_000, 2)
        return "$" + (str(int(v)) if v == int(v) else str(v)) + "M"
    if abs(n) >= 1_000:
        v = round(n, 2)
        return "$" + ("{:,.0f}".format(v) if v == int(v) else "{:,.2f}".format(v))
    if abs(n) >= 1:
        v = round(n, 2)
        return "$" + ("{:.0f}".format(v) if v == int(v) else "{:.2f}".format(v))
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
    h["total_sold"]     = h.get("total_sold", 0.0) + usd
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
    is_win = realized > 0
    if is_win:
        ud["trade_hours"][hour]["wins"] += 1
        ud["consec_losses"] = 0
    else:
        ud["trade_hours"][hour]["losses"] += 1
        ud["consec_losses"] = ud.get("consec_losses", 0) + 1
    # Track mood performance (shared logic for win & loss)
    mood = h.get("mood", "")
    if mood:
        ms = ud.setdefault("mood_stats", {})
        if mood not in ms:
            ms[mood] = {"trades": 0, "wins": 0, "pnl": 0.0}
        ms[mood]["trades"] += 1
        if is_win:
            ms[mood]["wins"] += 1
        ms[mood]["pnl"] += realized

    closed = False
    if h["amount"] < 0.000001:
        total_invested_full = h["total_invested"] + cost  # includes cost of this last sell
        total_returned_full = h.get("total_sold", 0.0)    # all proceeds including this sell
        # Blended x = total money out / total money in
        x_val = round(total_returned_full / total_invested_full, 4) if total_invested_full > 0 else 0
        trade_log.setdefault(uid, []).append({
            "symbol":        h["symbol"],
            "contract":      contract,
            "chain":         h.get("chain", "unknown"),
            "invested":      total_invested_full,
            "returned":      total_returned_full,
            "realized_pnl":  realized,
            "x":             x_val,
            "hold_h":        round(hold_h, 1),
            "reason":        reason,
            "closed_at":     datetime.now(),
            "bought_at":     h.get("bought_at", datetime.now()),
            "avg_price":     h.get("avg_price", 0),
            "exit_price":    price,
            "peak_price":    h.get("peak_price", price),
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


def _persist_after_sell(uid: int, ud: dict):
    """Save user data + trade log after a sell. Trim log if oversized."""
    tl = trade_log.get(uid, [])
    if len(tl) > MAX_TRADE_LOG:
        trade_log[uid] = tl[-MAX_TRADE_LOG:]
    save_user(uid, ud)
    save_trade_log(uid, trade_log.get(uid, []))


async def portfolio_val(ud: dict) -> tuple:
    holdings = ud["holdings"]
    if not holdings:
        return 0.0, 0.0
    contracts = list(holdings.keys())
    infos = await _asyncio.gather(*[get_token(c) for c in contracts])
    tv, tc = 0.0, 0.0
    for c, info in zip(contracts, infos):
        if info:
            tv += holdings[c]["amount"] * info["price"]
            tc += holdings[c]["total_invested"]
    return tv, tv - tc


_CHAIN_ICONS: dict = {
    "SOLANA": "◎", "ETHEREUM": "Ξ", "BSC": "⬡",
    "BASE": "🔵", "ARBITRUM": "🔷", "POLYGON": "⬟",
    "SUI": "💧", "AVALANCHE": "🔺",
}
_DEX_CHAIN_MAP: dict = {
    "solana":"solana","sol":"solana","ethereum":"ethereum","eth":"ethereum",
    "bsc":"bsc","bnb":"bsc","base":"base","arbitrum":"arbitrum",
}
_GMGN_CHAIN_MAP: dict = {
    "solana":"sol","sol":"sol","ethereum":"eth","eth":"eth",
    "bsc":"bsc","bnb":"bsc","base":"base",
}

def token_card(info: dict, contract: str, ud: dict, sc: dict = None) -> str:
    def fc(v):
        v = float(v or 0)
        if   v >= 100: e = "🚀"
        elif v >= 20:  e = "📈"
        elif v >= 0:   e = "🟢"
        elif v >= -20: e = "🔴"
        else:          e = "💀"
        return e + " " + ("+" if v >= 0 else "") + str(round(v, 1)) + "%"

    def _safe(s):
        return _re.sub(r'[_*\[\]()~`>#+\-=|{}.!]', '', str(s))

    try:
        name     = str(info.get("name", "Unknown"))
        symbol   = str(info.get("symbol", "???"))
        chain    = str(info.get("chain", "SOL")).upper()
        dex      = str(info.get("dex", "")).upper()
        price    = info.get("price", 0)
        mc       = info.get("mc", 0)
        liq      = info.get("liq", 0)
        liq_pct  = info.get("liq_pct", 0)
        age_h    = info.get("age_h") or 0
        buy_pct  = info.get("buy_pct", 50)
        sell_pct = 100 - buy_pct
        buys     = info.get("buys", 0)
        sells    = info.get("sells", 0)
        vol_h24  = info.get("vol_h24", 0)
        ath_price= info.get("ath_price", 0)
        ath_mc   = info.get("ath_mc", 0)
        chain_raw= str(info.get("chain", "solana")).lower()

        chain_sym = _CHAIN_ICONS.get(chain, "⛓")
        dex_clean = dex.replace("_V2","").replace("_V3","").replace("_","")

        # ── age ───────────────────────────────────────────────────────────────
        if age_h < 1:
            age_display, age_e = str(round(age_h * 60)) + "m", "🆕"
        elif age_h < 24:
            age_display, age_e = str(round(age_h, 1)) + "h", "⏰"
        else:
            age_display, age_e = str(round(age_h / 24, 1)) + "d", "📅"

        # ── ATH ───────────────────────────────────────────────────────────────
        ath_line = ""
        if ath_price and ath_price > price and price > 0:
            ath_down = round((1 - price / ath_price) * 100, 1)
            ath_line = "🏆 ATH  " + mc_str(ath_mc) + "  (-" + str(ath_down) + "%)\n"

        # ── pressure bar ──────────────────────────────────────────────────────
        bar = "█" * round(buy_pct / 10) + "░" * (10 - round(buy_pct / 10))
        if buy_pct >= 60:
            pressure_label, pressure_e = "● BUYING",  "🟢"
        elif sell_pct >= 60:
            pressure_label, pressure_e = "● SELLING", "🔴"
        else:
            pressure_label, pressure_e = "● NEUTRAL", "⚖️"

        # ── liq warning ───────────────────────────────────────────────────────
        liq_warn = "\n🚨 LOW LIQUIDITY — HIGH RISK" if liq < 50_000 else ""

        # ── security audit (RugCheck, Solana only) ────────────────────────────
        sec_block = ""
        no_mint   = info.get("no_mint")
        no_freeze = info.get("no_freeze")
        lp_burn   = info.get("lp_burn")
        top10     = info.get("top10_pct")
        insider   = info.get("insider_pct")
        rug_risks = info.get("rug_risks", [])
        if any(x is not None for x in [no_mint, no_freeze, lp_burn, top10, insider]):
            audit_parts = []
            if no_mint   is not None: audit_parts.append("✅ No Mint"    if no_mint   else "🚨 Mint")
            if no_freeze is not None: audit_parts.append("✅ No Freeze"  if no_freeze else "🚨 Freeze")
            if lp_burn   is not None: audit_parts.append("🔥 LP " + str(lp_burn) + "%")
            holder_line = ""
            if top10   is not None: holder_line += "Top10: " + str(top10) + "%"
            if insider is not None: holder_line += ("  |  " if holder_line else "") + "Insiders: " + str(insider) + "%"
            risk_line = ""
            if rug_risks:
                risk_line = "\n⚠️ " + "  |  ".join(rug_risks[:3])
            sec_block = (
                "─────────────────────────\n"
                "🔒 " + "  ".join(audit_parts) + "\n"
                + (holder_line + "\n" if holder_line else "")
                + risk_line + ("\n" if risk_line else "")
            )

        # ── YOUR POSITION (Telegram blockquote highlight style) ───────────────
        pos_block = ""
        if contract in ud.get("holdings", {}):
            h      = ud["holdings"][contract]
            cv     = h["amount"] * price
            cx     = price / h["avg_price"] if h.get("avg_price", 0) > 0 else 0
            ppnl   = cv - h["total_invested"]
            pnl_e  = "💚" if ppnl >= 0 else "🔴"
            cx_e   = "🚀" if cx >= 3 else "📈" if cx >= 1.5 else "📉" if cx < 1 else "➡️"
            avg_mc = h.get("avg_cost_mc", 0)
            sold   = h.get("total_sold", 0)
            # Telegram blockquote (>) creates highlighted left-border box
            pos_block = (
                "─────────────────────────\n"
                "> 💰 *YOUR POSITION*\n"
                ">\n"
                "> 💵 Value      " + money(cv) + "   " + cx_e + " *" + str(round(cx, 2)) + "x*\n"
                "> " + pnl_e + " PnL        " + pstr(ppnl) + "\n"
                "> 🧾 Invested   " + money(h.get("total_invested", 0)) + "\n"
                + ("> 💸 Sold      " + money(sold) + "\n" if sold > 0 else "")
                + "> 🪙 Holding   " + str(round(h["amount"], 4)) + "\n"
                + ("> 📍 Avg MC    " + mc_str(avg_mc) + "\n" if avg_mc else "")
                + ">\n"
            )

        # ── socials ───────────────────────────────────────────────────────────
        twitter      = info.get("twitter", "")
        telegram_url = info.get("telegram", "")
        website      = info.get("website", "")
        soc_parts = []
        if twitter:      soc_parts.append("🐦 [Twitter / X]("  + twitter      + ")")
        if telegram_url: soc_parts.append("💬 [Telegram]("     + telegram_url + ")")
        if website:      soc_parts.append("🌐 [Website]("      + website      + ")")
        social_line = ("─────────────────────────\n" + "\n".join(soc_parts) + "\n") if soc_parts else ""

        # ── X search ─────────────────────────────────────────────────────────
        base_x   = "https://x.com/search?f=live&q={}&src=typed_query"
        safe_sym = _safe(symbol).strip() or "SYM"
        safe_nm  = _safe(name)[:10].strip() or "Token"
        combined = _urlparse.quote(f"({name} OR ${symbol} OR {contract})")
        x_line = (
            "🔍 Search 𝕏:  "
            + "[All]("       + base_x.format(combined)                              + ")  "
            + "[CA]("        + base_x.format(_urlparse.quote(contract))             + ")  "
            + "[" + safe_nm  + "](" + base_x.format(_urlparse.quote(name))         + ")  "
            + "[$" + safe_sym + "](" + base_x.format(_urlparse.quote("$"+symbol))  + ")\n"
        )

        # ── footer links: GT · DT · DS · DV · BE · PF ────────────────────────
        dex_chain  = _DEX_CHAIN_MAP.get(chain_raw,"solana")
        gmgn_chain = _GMGN_CHAIN_MAP.get(chain_raw,"sol")
        gt_url  = "https://www.geckoterminal.com/" + dex_chain + "/pools/" + contract
        dt_url  = "https://www.dextools.io/app/en/" + dex_chain + "/pair-explorer/" + contract
        ds_url  = "https://dexscreener.com/"        + dex_chain + "/" + contract
        dv_url  = "https://www.dexview.com/"        + dex_chain + "/" + contract
        be_url  = "https://birdeye.so/token/"       + contract  + "?chain=" + dex_chain
        pf_url  = "https://pump.fun/"               + contract

        if chain_raw in ("solana", "sol"):
            ext_line = (
                "─────────────────────────\n"
                "[GT](" + gt_url + ")  [DT](" + dt_url + ")  [DS](" + ds_url + ")  [DV](" + dv_url + ")  [BE](" + be_url + ")  [PF](" + pf_url + ")\n"
            )
        else:
            ext_line = (
                "─────────────────────────\n"
                "[GT](" + gt_url + ")  [DT](" + dt_url + ")  [DS](" + ds_url + ")  [DV](" + dv_url + ")  [BE](" + be_url + ")\n"
            )
        card = (
            "🪙 *" + name + "* ($" + symbol + ")\n"
            + chain_sym + " " + chain + "  🏦 " + dex_clean + "\n"
            + "`" + contract + "`\n"
            + "─────────────────────────\n"
            + "💲 Price  *$" + str(price) + "*\n"
            + "📊 MC     *" + mc_str(mc) + "*\n"
            + "💧 Liq    *" + money(liq) + "*  (" + str(liq_pct) + "%)\n"
            + age_e + " Age    *" + age_display + "*\n"
            + ath_line
            + "─────────────────────────\n"
            + "5m " + fc(info.get("ch_m5", 0))
            + "   1h " + fc(info.get("ch_h1", 0)) + "\n"
            + "6h " + fc(info.get("ch_h6", 0))
            + "  24h " + fc(info.get("ch_h24", 0)) + "\n"
            + "─────────────────────────\n"
            + "📈 Vol 24h  *" + money(vol_h24) + "*\n"
            + pressure_e + " " + pressure_label + "\n"
            + "`" + bar + "`\n"
            + "🛒 Buys " + str(buys) + " (" + str(buy_pct) + "%)"
            + "  🏃 Sells " + str(sells) + " (" + str(sell_pct) + "%)"
            + liq_warn + "\n"
            + sec_block
            + pos_block
            + ("─────────────────────────\n" if not pos_block and not social_line else "")
            + social_line
            + ("\n" if social_line else "")
            + x_line
            + "\n"
            + ext_line
        )

        if len(card) > 4096:
            card = card[:4092] + "…"
        return card

    except Exception as e:
        logger.warning(f"token_card render error: {e}")
        return (
            "🪙 *" + str(info.get("name", "Token")) + "* ($" + str(info.get("symbol", "???")) + ")\n"
            + "`" + contract + "`\n\n"
            + "Price: $" + str(info.get("price", 0)) + "\n"
            + "MC: " + mc_str(info.get("mc", 0))
        )


async def send_token_card(
    target,           # update.message  OR  callback query (q)
    info: dict,
    contract: str,
    ud: dict,
    sc: dict,
    ctx,              # ContextTypes.DEFAULT_TYPE
    is_query: bool = False,
):
    """
    1. Delete previous chart for this user (prevents pile-up on refresh)
    2. Send fresh chart as plain photo (no buttons)
    3. Delete old token card message (if refresh)
    4. Send token card as text message with all buttons
    """
    card_txt = token_card(info, contract, ud, sc)
    kb       = buy_kb(contract, ud)

    # Determine chat_id and user_id
    if is_query:
        chat_id = target.message.chat_id
        uid     = target.from_user.id
    else:
        chat_id = target.chat_id
        uid     = target.from_user.id if target.from_user else 0

    # ── Step 1: Clean up previous chart + CA message for this user ───────────
    await _cleanup_token_view(ctx.bot, uid, chat_id)

    # ── Step 2: Generate and send new chart (OHLCV fetched in parallel) ─────────
    try:
        ohlcv     = []
        pair_addr = info.get("pair_addr", "")
        chain_raw = info.get("chain", "solana").lower()
        chain_map = {"solana": "solana", "sol": "solana", "ethereum": "eth",
                     "eth": "eth", "bsc": "bsc", "bnb": "bsc",
                     "base": "base", "arbitrum": "arbitrum"}
        chain_id  = chain_map.get(chain_raw, chain_raw)
        if pair_addr:
            ohlcv = await _asyncio.wait_for(fetch_ohlcv(pair_addr, chain_id), timeout=6)
        chart_buf = generate_price_chart(info, ohlcv)
        if chart_buf:
            chart_msg = await ctx.bot.send_photo(
                chat_id=chat_id,
                photo=chart_buf,
                caption=(
                    "*$" + info["symbol"] + "*  |  "
                    + info.get("chain", "SOL").upper() + "  |  MC: "
                    + mc_str(info["mc"])
                ),
                parse_mode="Markdown",
            )
            # Store message_id so next refresh can delete it
            chart_msg_ids[uid] = chart_msg.message_id
    except Exception as e:
        logger.warning(f"Chart send failed: {e}")

    # ── Step 3: Delete old token card message (refresh case) ──────────────────
    if is_query:
        try:
            await target.message.delete()
        except Exception:
            pass

    # ── Step 4: Send fresh token card with buttons ────────────────────────────
    await ctx.bot.send_message(
        chat_id=chat_id,
        text=card_txt,
        parse_mode="Markdown",
        reply_markup=kb,
    )


def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📈 Positions",      callback_data="v_pos"),
         InlineKeyboardButton("⏰ Orders",         callback_data="v_orders")],
        [InlineKeyboardButton("💰 Savings",        callback_data="v_savings"),
         InlineKeyboardButton("👁 Watchlist",      callback_data="v_watchlist")],
        [InlineKeyboardButton("👥 Accounts",       callback_data="v_accounts"),
         InlineKeyboardButton("⚙️ Settings",       callback_data="v_settings")],
        [InlineKeyboardButton("📋 More ▸",         callback_data="v_more"),
         InlineKeyboardButton("🎯 Sniper",         callback_data="v_sniper")],
        [InlineKeyboardButton("⚡ BUY & SELL NOW!", callback_data="v_trade")],
    ])


def more_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Stats",          callback_data="v_stats"),
         InlineKeyboardButton("📜 History",        callback_data="v_history")],
        [InlineKeyboardButton("🏆 Leaderboard",    callback_data="v_leader"),
         InlineKeyboardButton("🏁 Compete",        callback_data="v_compete")],
        [InlineKeyboardButton("🎯 Challenge",      callback_data="v_challenge"),
         InlineKeyboardButton("🔁 Copy Trading",   callback_data="v_copy")],
        [InlineKeyboardButton("🔔 Alerts",         callback_data="v_alerts"),
         InlineKeyboardButton("🐋 Whales",         callback_data="v_whale")],
        [InlineKeyboardButton("🚀 Milestones",     callback_data="v_milestone_notif"),
         InlineKeyboardButton("🔥 Rug Warning",    callback_data="v_rug_warn")],
        [InlineKeyboardButton("🔗 Referrals",      callback_data="v_referrals"),
         InlineKeyboardButton("👤 Profile",        callback_data="v_profile")],
        [InlineKeyboardButton("📁 Export CSV",     callback_data="v_export"),
         InlineKeyboardButton("📖 Help & Docs",    callback_data="v_help")],
        [InlineKeyboardButton("🏠 Main Menu",      callback_data="mm")],
    ])


def buy_kb(contract: str, ud: dict) -> InlineKeyboardMarkup:
    """Main token card keyboard — matches the approved sketch layout."""
    # Guard: migrate watchlist from list to dict if user has old session
    if isinstance(ud.get("watchlist"), list):
        ud["watchlist"] = {}
    held      = contract in ud["holdings"]
    h         = ud["holdings"].get(contract, {})
    has_as    = bool(h.get("auto_sells"))
    has_sl    = bool(h.get("stop_loss_pct"))
    as_lbl    = "🎯 Auto Sell ✅" if has_as else "🎯 Auto Sell"
    sl_lbl    = "🛑 Stop Loss ✅" if has_sl else "🛑 Stop Loss"
    track_lbl = "👁 Track ✅"     if contract in ud.get("watchlist", {}) else "👁 Track"

    # Alert — show Cancel if active for this token
    has_alert = any(a.get("contract") == contract for a in ud.get("price_alerts", []))
    alert_lbl = "🔔 Alert ✅"  if has_alert else "🔔 Set Alert"
    alert_cb  = "al_cancel_ca_" + contract if has_alert else "pal_" + contract

    # External link row — chain-aware
    chain_raw  = ud.get("last_chain", "solana").lower()
    dex_chain  = {"solana":"solana","sol":"solana","ethereum":"ethereum","eth":"ethereum",
                  "bsc":"bsc","bnb":"bsc","base":"base","arbitrum":"arbitrum","arb":"arbitrum"}.get(chain_raw,"solana")
    gmgn_chain = {"solana":"sol","sol":"sol","ethereum":"eth","eth":"eth",
                  "bsc":"bsc","bnb":"bsc","base":"base"}.get(chain_raw,"sol")
    dex_url  = f"https://dexscreener.com/{dex_chain}/{contract}"
    gmgn_url = f"https://gmgn.ai/{gmgn_chain}/token/{contract}"
    pump_url = f"https://pump.fun/{contract}"
    axiom_url= f"https://axiom.trade/t/{contract}"

    ext_row = [
        InlineKeyboardButton("📊 Dex",   url=dex_url),
        InlineKeyboardButton("🔍 GmGn",  url=gmgn_url),
    ]
    if chain_raw in ("solana", "sol"):
        ext_row.append(InlineKeyboardButton("🎯 Pump",  url=pump_url))
        ext_row.append(InlineKeyboardButton("⚡ Axiom", url=axiom_url))

    # Auto-sell button: if targets set → go to targets view (where you can cancel)
    as_cb = "vtg_" + contract if has_as else "asm_" + contract
    # Stop loss button: if SL set → go to targets view (where you can cancel SL)
    sl_cb = "vtg_" + contract if has_sl else "slm_" + contract

    # Quick Buy label shows configured amount
    qb_amt  = ud.get("quick_buy_amount", 100.0)
    qb_lbl  = "⚡ Quick Buy $" + str(int(qb_amt))
    rows = [
        [InlineKeyboardButton("🔄 Refresh",       callback_data="rf_"  + contract)],
        [InlineKeyboardButton("⚡ Buy",            callback_data="bts_" + contract),
         InlineKeyboardButton("🔴 Sell",           callback_data="sts_" + contract)],
        [InlineKeyboardButton(qb_lbl,              callback_data="qb_"  + contract),
         InlineKeyboardButton("📊 Limit Buy",      callback_data="lbo_" + contract)],
        [InlineKeyboardButton(as_lbl,              callback_data=as_cb),
         InlineKeyboardButton(sl_lbl,              callback_data=sl_cb)],
        [InlineKeyboardButton("📉 DCA",            callback_data="dca_" + contract),
         InlineKeyboardButton(alert_lbl,           callback_data=alert_cb)],
        [InlineKeyboardButton(track_lbl,           callback_data="wl_"  + contract),
         InlineKeyboardButton("🧠 Score",          callback_data="tks_" + contract)],
        [InlineKeyboardButton("◀ Back",            callback_data="mm")],
    ]
    return InlineKeyboardMarkup(rows)


def buy_sub_kb(contract: str, ud: dict) -> InlineKeyboardMarkup:
    """Buy amount submenu — shown when user taps ⚡ Buy."""
    pb         = ud.get("preset_buy")
    preset_lbl = "⚡ $" + str(int(pb)) + " [Preset]" if pb else "⚙️ Set Preset"
    preset_cb  = "bp_" + contract if pb else "set_preset"
    qb_amt     = ud.get("quick_buy_amount", 100.0)
    qb_set_lbl = "⚡ Quick Buy: $" + str(int(qb_amt)) + " ⚙️"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("$25",         callback_data="ba_25_"   + contract),
         InlineKeyboardButton("$50",         callback_data="ba_50_"   + contract),
         InlineKeyboardButton("$100",        callback_data="ba_100_"  + contract),
         InlineKeyboardButton("$250",        callback_data="ba_250_"  + contract)],
        [InlineKeyboardButton("$500",        callback_data="ba_500_"  + contract),
         InlineKeyboardButton("$1000",       callback_data="ba_1000_" + contract),
         InlineKeyboardButton("✏️ Custom",   callback_data="bc_"      + contract),
         InlineKeyboardButton(preset_lbl,    callback_data=preset_cb)],
        [InlineKeyboardButton(qb_set_lbl,    callback_data="qb_set_"  + contract)],
        [InlineKeyboardButton("◀ Back",      callback_data="btt_" + contract)],
    ])


def sell_sub_kb(contract: str) -> InlineKeyboardMarkup:
    """Sell amount submenu — shown when user taps 🔴 Sell."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("25%",          callback_data="sp_25_"  + contract),
         InlineKeyboardButton("50%",          callback_data="sp_50_"  + contract),
         InlineKeyboardButton("75%",          callback_data="sp_75_"  + contract),
         InlineKeyboardButton("100%",         callback_data="sp_100_" + contract)],
        [InlineKeyboardButton("✏️ Custom %",  callback_data="sca_"    + contract),
         InlineKeyboardButton("🎯 Limit Sell",callback_data="lso_"    + contract)],
        [InlineKeyboardButton("◀ Back",       callback_data="btt_"    + contract)],
    ])


def sell_kb(contract: str) -> InlineKeyboardMarkup:
    """Kept for legacy references — delegates to sell_sub_kb."""
    return sell_sub_kb(contract)


def settings_kb(ud: dict) -> InlineKeyboardMarkup:
    pb  = "$" + str(int(ud["preset_buy"])) if ud.get("preset_buy") else "not set"
    ps  = str(ud["preset_sell"]) if ud.get("preset_sell") else "not set"
    rsk = str(ud["risk_pct"]) + "%" if ud.get("risk_pct") else "not set"
    mp  = str(ud["max_positions"]) if ud.get("max_positions") else "not set"
    dl  = str(ud["daily_limit"]) if ud.get("daily_limit") else "not set"
    asp = str(ud["auto_save_pct"]) + "%" if ud.get("auto_save_pct") else "not set"
    tgt = money(ud["target_equity"]) if ud.get("target_equity") else "not set"
    mdt = "ON" if ud.get("mood_tracking", True) else "OFF"
    rct = "ON ✅" if ud.get("risk_calc", True) else "OFF ❌"
    lang_labels = {"en": "🇬🇧 English", "es": "🇪🇸 Español", "pt": "🇧🇷 Português", "fr": "🇫🇷 Français", "zh": "🇨🇳 中文"}
    lang_lbl = lang_labels.get(ud.get("language", "en"), "🌐 Language")
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Default Buy: " + pb,        callback_data="cfg_buy")],
        [InlineKeyboardButton("Default Sell: " + ps,       callback_data="cfg_sell")],
        [InlineKeyboardButton("Max Risk/Trade: " + rsk,    callback_data="cfg_risk")],
        [InlineKeyboardButton("Max Positions: " + mp,      callback_data="cfg_maxpos")],
        [InlineKeyboardButton("Daily Limit: " + dl,        callback_data="cfg_daily")],
        [InlineKeyboardButton("Auto-Save: " + asp,         callback_data="cfg_autosave")],
        [InlineKeyboardButton("Target Equity: " + tgt,     callback_data="cfg_target")],
        [InlineKeyboardButton("Mood Tracking: " + mdt,     callback_data="cfg_mood")],
        [InlineKeyboardButton("Risk Calc: " + rct,         callback_data="cfg_riskcalc")],
        [InlineKeyboardButton("🌐 Language: " + lang_lbl,  callback_data="cfg_lang")],
        [InlineKeyboardButton("Reset Account",             callback_data="rst_prompt")],
        [InlineKeyboardButton("◀ Back",                    callback_data="mm")],
    ])


def back_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu", callback_data="mm")]])


def back_more() -> InlineKeyboardMarkup:
    """Back button for screens accessed from the More menu."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ Back to More", callback_data="v_more")],
        [InlineKeyboardButton("🏠 Main Menu",   callback_data="mm")],
    ])


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
    import time as _time_local

    await bundle_sell_detector(app)

    # ── Pre-warm cache: collect all unique contracts across all users ──────────
    # then fetch them ALL in parallel. Subsequent get_token() calls in the loop
    # hit the cache instantly instead of making separate HTTP requests.
    all_contracts: set = set()
    for ud in users.values():
        if ud.get("balance") is None:
            continue
        for a in ud.get("price_alerts", []):
            if not a.get("triggered"):
                all_contracts.add(a["contract"])
        for o in ud.get("limit_orders", []):
            if not o.get("triggered") and not o.get("cancelled"):
                all_contracts.add(o["contract"])
        all_contracts.update(ud.get("holdings", {}).keys())
        all_contracts.update(ud.get("watchlist", {}).keys())
        for dca in ud.get("dca_orders", []):
            if not dca.get("cancelled"):
                all_contracts.add(dca["contract"])
    if all_contracts:
        await _asyncio.gather(*[get_token(c, force=True) for c in all_contracts])
    # ─────────────────────────────────────────────────────────────────────────

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
                    async with get_user_lock(uid):
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
                    h = ud["holdings"].get(order["contract"])
                    if not h:
                        continue  # position closed by user between check and execution
                    cv = h["amount"] * price
                    sell_amt = min(order["amount"], cv)
                    async with get_user_lock(uid):
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
                    async with get_user_lock(uid):
                        if contract not in ud["holdings"]:
                            continue   # already sold by user — skip
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
                async with get_user_lock(uid):
                    if contract not in ud["holdings"]:
                        break   # position closed by user between check and execution
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

        # Notify copy followers about sells too
        # (handled separately - followers see position updates via portfolio)

        # Whale alerts — detect large volume spikes on held/watched tokens
        if ud.get("whale_alerts", True):
            whale_candidates = set(ud["holdings"].keys()) | set(ud.get("watchlist", {}).keys())
            for wca in whale_candidates:
                try:
                    wi = await get_token(wca)
                    if not wi:
                        continue
                    vol_m5 = wi.get("vol_m5", 0)
                    vol_h1  = wi.get("vol_h1", 0)
                    # Average 5-min slice over the last hour
                    avg_5m = vol_h1 / 12 if vol_h1 > 0 else 0
                    # Whale threshold: last 5-min volume is 4x the hourly average
                    # AND the absolute spike is at least $20K to filter micro-caps noise
                    if avg_5m > 0 and vol_m5 >= avg_5m * 4 and vol_m5 >= 20_000:
                        sym = wi.get("symbol", "?")
                        spike_x = round(vol_m5 / avg_5m, 1)
                        # Deduplicate: only alert once per token per hour
                        last_whale = ud.setdefault("_whale_last", {})
                        now_h = datetime.now().strftime("%Y%m%d%H")
                        alert_key = wca + "_" + now_h
                        if alert_key not in last_whale:
                            last_whale[alert_key] = True
                            # Clean up old keys
                            ud["_whale_last"] = {k: v for k, v in last_whale.items()
                                                 if k.split("_")[-1] >= (datetime.now() - timedelta(hours=2)).strftime("%Y%m%d%H")}
                            holding_line = ""
                            if wca in ud["holdings"]:
                                h = ud["holdings"][wca]
                                cv = h["amount"] * wi["price"]
                                cx = wi["price"] / h["avg_price"] if h.get("avg_price", 0) > 0 else 0
                                holding_line = "\n📌 Your position: " + money(cv) + "  (" + str(round(cx, 2)) + "x)"
                            try:
                                await app.bot.send_message(
                                    chat_id=uid,
                                    parse_mode="Markdown",
                                    text=(
                                        "🐋 *WHALE ALERT*\n\n"
                                        "*$" + sym + "* is seeing a massive volume spike!\n"
                                        "5m Volume: *" + money(vol_m5) + "* (" + str(spike_x) + "x avg)\n"
                                        "Price: *" + money(wi["price"]) + "*\n"
                                        "MC: *" + mc_str(wi["mc"]) + "*" + holding_line
                                    ),
                                    reply_markup=main_menu_kb()
                                )
                            except Exception as _we:
                                logger.warning(f"Whale alert send failed: {_we}")
                except Exception:
                    continue

        # Watchlist alerts
        for wca, wt in list(ud.get("watchlist", {}).items()):
            try:
                wi = await get_token(wca)
                if not wi:
                    continue
                tp = wt.get("target_price")
                tm = wt.get("target_mc")
                if tp and wi["price"] >= tp:
                    await app.bot.send_message(uid,
                        f"👁 *WATCHLIST ALERT*\n\n"
                        f"${wt['symbol']} hit your target price!\n"
                        f"Price: ${wi['price']:.8g} (target: ${tp:.8g})\n"
                        f"MC: {mc_str(wi['mc'])}",
                        parse_mode="Markdown")
                    ud["watchlist"][wca]["target_price"] = None
                if tm and wi["mc"] >= tm:
                    await app.bot.send_message(uid,
                        f"👁 *WATCHLIST ALERT*\n\n"
                        f"${wt['symbol']} hit your target MC!\n"
                        f"MC: {mc_str(wi['mc'])} (target: {mc_str(tm)})\n"
                        f"Price: ${wi['price']:.8g}",
                        parse_mode="Markdown")
                    ud["watchlist"][wca]["target_mc"] = None
            except Exception:
                continue

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

        # ── HOLDINGS: peak price update + milestone alerts + rug pull warning ──
        for hca, h in list(ud.get("holdings", {}).items()):
            try:
                hi = await get_token(hca)
                if not hi:
                    continue
                cur_price = hi["price"]

                # Update peak price for replay
                if cur_price > h.get("peak_price", 0):
                    h["peak_price"] = cur_price

                avg_price = h.get("avg_price", 0)
                if avg_price <= 0:
                    continue
                current_x = cur_price / avg_price

                # ── MILESTONE ALERTS ──────────────────────────────────────────
                if ud.get("milestone_notif", True):
                    milestones_hit = h.setdefault("milestones_hit", [])
                    invested = h.get("total_invested", 0)
                    cur_value = h["amount"] * cur_price

                    for level, icon, color_word in [
                        (2,  "🚀", "2×"), (3, "🚀", "3×"), (5, "🎯", "5×"),
                        (10, "🏆", "10×"), (20, "💎", "20×"), (50, "👑", "50×"),
                    ]:
                        if current_x >= level and level not in milestones_hit:
                            milestones_hit.append(level)
                            profit = cur_value - invested
                            try:
                                await app.bot.send_message(
                                    chat_id=uid, parse_mode="Markdown",
                                    text=(
                                        icon + " *" + color_word + " MILESTONE HIT*\n\n"
                                        "*$" + h["symbol"] + "*  ·  " + h.get("chain","?").upper() + "\n"
                                        "━━━━━━━━━━━━━━━━\n"
                                        "Invested: *" + money(invested) + "*\n"
                                        "Value now: *" + money(cur_value) + "*\n"
                                        "Profit: *+" + money(profit) + "*"
                                    ),
                                    reply_markup=InlineKeyboardMarkup([
                                        [InlineKeyboardButton("🔍 View Token", callback_data="btt_" + hca),
                                         InlineKeyboardButton("💰 Sell Now",   callback_data="sts_" + hca)],
                                    ])
                                )
                            except Exception as _me:
                                logger.warning(f"Milestone alert error: {_me}")

                # ── DUMP ALERT ────────────────────────────────────────────────
                if ud.get("milestone_notif_dump", True):
                    dump_hit = h.setdefault("dump_alerted", False)
                    if not dump_hit and current_x <= 0.5:
                        h["dump_alerted"] = True
                        invested = h.get("total_invested", 0)
                        cur_value = h["amount"] * cur_price
                        loss = cur_value - invested
                        try:
                            await app.bot.send_message(
                                chat_id=uid, parse_mode="Markdown",
                                text=(
                                    "🚨 *DUMP ALERT  –50%*\n\n"
                                    "*$" + h["symbol"] + "*  ·  " + h.get("chain","?").upper() + "\n"
                                    "━━━━━━━━━━━━━━━━\n"
                                    "Invested: *" + money(invested) + "*\n"
                                    "Value now: *" + money(cur_value) + "*\n"
                                    "Loss: *" + money(loss) + "*"
                                ),
                                reply_markup=InlineKeyboardMarkup([
                                    [InlineKeyboardButton("🔍 View Token", callback_data="btt_" + hca),
                                     InlineKeyboardButton("🛑 Cut Loss",   callback_data="sts_" + hca)],
                                ])
                            )
                        except Exception as _de:
                            logger.warning(f"Dump alert error: {_de}")

                # ── RUG PULL WARNING ──────────────────────────────────────────
                if ud.get("rug_warn_enabled", False):
                    cur_liq  = hi.get("liq", 0)
                    threshold = ud.get("rug_warn_threshold", 30) / 100.0
                    prev_liq  = _rug_liq_prev.setdefault(uid, {}).get(hca, cur_liq)
                    if prev_liq > 0 and cur_liq < prev_liq * (1 - threshold):
                        drop_pct = round((1 - cur_liq / prev_liq) * 100, 1)
                        invested = h.get("total_invested", 0)
                        cur_value = h["amount"] * cur_price
                        rug_key = hca + "_rug_" + str(int(prev_liq))
                        if rug_key not in h.get("rug_warned", []):
                            h.setdefault("rug_warned", []).append(rug_key)
                            try:
                                await app.bot.send_message(
                                    chat_id=uid, parse_mode="Markdown",
                                    text=(
                                        "🔥 *RUG PULL WARNING*\n\n"
                                        "*$" + h["symbol"] + "*  ·  " + h.get("chain","?").upper() + "\n"
                                        "━━━━━━━━━━━━━━━━\n"
                                        "💧 Liquidity dropped *–" + str(drop_pct) + "%* this cycle\n"
                                        "Was: *" + money(prev_liq) + "*  →  Now: *" + money(cur_liq) + "*\n\n"
                                        "Your bag: *" + money(cur_value) + "*\n"
                                        "⚠️ LP may be being pulled — consider exiting"
                                    ),
                                    reply_markup=InlineKeyboardMarkup([
                                        [InlineKeyboardButton("😤 Ignore",          callback_data="btt_" + hca),
                                         InlineKeyboardButton("🚨 Sell Everything", callback_data="sts_" + hca)],
                                    ])
                                )
                            except Exception as _re:
                                logger.warning(f"Rug warn error: {_re}")
                    _rug_liq_prev.setdefault(uid, {})[hca] = cur_liq

            except Exception as _hce:
                logger.warning(f"Holdings checker error for {hca}: {_hce}")

        # DCA by Market Cap — trigger buys when token hits set MC milestones
        for dca in list(ud.get("dca_orders", [])):
            if dca.get("cancelled"):
                continue
            try:
                di = await get_token(dca["contract"])
                if not di:
                    continue
                for tgt in dca.get("mc_targets", []):
                    if tgt.get("triggered"):
                        continue
                    if di["mc"] >= tgt["mc"]:
                        tgt["triggered"] = True
                        buy_amt = tgt["amount"]
                        if ud["balance"] < buy_amt:
                            await app.bot.send_message(
                                chat_id=uid, parse_mode="Markdown",
                                text="📉 *DCA SKIPPED*\n\n$" + dca["symbol"] + " hit " + mc_str(tgt["mc"]) + " MC but you don't have enough balance.",
                                reply_markup=main_menu_kb()
                            )
                            continue
                        result = await do_buy_core(ud, uid, dca["contract"], buy_amt, planned=True, mood="DCA")
                        if isinstance(result, tuple):
                            info2, _ = result
                            await app.bot.send_message(
                                chat_id=uid, parse_mode="Markdown",
                                text=t(ud, "dca_fired",
                                    symbol=dca["symbol"], mc=mc_str(tgt["mc"]),
                                    amount=money(buy_amt), price=money(info2["price"]),
                                    cash=money(ud["balance"])
                                ),
                                reply_markup=main_menu_kb()
                            )
                # Clean fully triggered DCA orders
                all_done = all(t2.get("triggered") for t2 in dca.get("mc_targets", []))
                if all_done:
                    dca["cancelled"] = True
            except Exception as _dce:
                logger.warning(f"DCA checker error: {_dce}")
        ud["dca_orders"] = [d for d in ud.get("dca_orders", []) if not d.get("cancelled")]


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


async def monthly_report_job(ctx: ContextTypes.DEFAULT_TYPE):
    now = datetime.now()
    if now.day != 1:
        return
    month_ago = now - timedelta(days=30)
    for uid, ud in list(users.items()):
        if ud.get("balance") is None:
            continue
        logs = trade_log.get(uid, [])
        monthly = [t for t in logs if t.get("closed_at", datetime.min) >= month_ago]
        if not monthly:
            continue
        wins = [t for t in monthly if t["realized_pnl"] > 0]
        losses = [t for t in monthly if t["realized_pnl"] <= 0]
        tpnl = sum(t["realized_pnl"] for t in monthly)
        wr = round(len(wins) / len(monthly) * 100) if monthly else 0
        aw = sum(t["realized_pnl"] for t in wins) / len(wins) if wins else 0
        al = sum(t["realized_pnl"] for t in losses) / len(losses) if losses else 0
        best = max(monthly, key=lambda t: t["realized_pnl"])
        worst = min(monthly, key=lambda t: t["realized_pnl"])
        sb = ud.get("starting_balance", 0)
        eq = ud["balance"] + sum(h["total_invested"] for h in ud["holdings"].values()) + ud["savings"]
        growth = round((eq - sb) / sb * 100, 1) if sb > 0 else 0

        mood_txt = ""
        if ud.get("mood_stats"):
            best_mood = max(ud["mood_stats"].items(), key=lambda x: x[1]["pnl"])
            worst_mood = min(ud["mood_stats"].items(), key=lambda x: x[1]["pnl"])
            mood_txt = (
                "\n\nBest entry reason: " + best_mood[0] + " (" + pstr(best_mood[1]["pnl"]) + ")\n"
                "Worst entry reason: " + worst_mood[0] + " (" + pstr(worst_mood[1]["pnl"]) + ")"
            )

        try:
            await ctx.bot.send_message(
                chat_id=uid, parse_mode="Markdown",
                text=(
                    "📊 *MONTHLY REPORT*\n\n"
                    "Trades: " + str(len(monthly)) + "  (" + str(len(wins)) + "W / " + str(len(losses)) + "L)\n"
                    "Win Rate: " + str(wr) + "%\n"
                    "Avg Win: " + money(aw) + "\n"
                    "Avg Loss: " + money(al) + "\n"
                    "Total PnL: " + pstr(tpnl) + "\n\n"
                    "Best Trade: " + pstr(best["realized_pnl"]) + " ($" + best["symbol"] + ")\n"
                    "Worst Trade: " + pstr(worst["realized_pnl"]) + " ($" + worst["symbol"] + ")\n\n"
                    "Account Equity: " + money(eq) + "\n"
                    "Savings: " + money(ud["savings"]) + "\n"
                    "Growth: " + str(growth) + "%" + mood_txt
                ),
                reply_markup=main_menu_kb()
            )
        except Exception:
            pass


async def checker_job(ctx: ContextTypes.DEFAULT_TYPE):
    await run_checker(ctx.application)


async def _dca_show_plan(q, contract: str, p: dict):
    """Show the current DCA plan being built with Add More / Confirm buttons."""
    targets = p.get("targets", [])
    sym     = p.get("symbol", "?")
    lines   = "\n".join(
        "  " + str(i+1) + ". Buy *" + money(tgt["amount"]) + "* at *" + mc_str(tgt["mc"]) + "* MC"
        for i, tgt in enumerate(sorted(targets, key=lambda x: x["mc"]))
    )
    await q.edit_message_text(
        "📉 *DCA PLAN — $" + sym + "*\n\n" + lines + "\n\n"
        "Add another target or confirm to save:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Another Target", callback_data="dca_addmore_" + contract)],
            [InlineKeyboardButton("✅ Confirm & Save",     callback_data="dca_confirm_" + contract)],
            [InlineKeyboardButton("🗑 Start Over",         callback_data="dca_" + contract)],
        ])
    )


async def sniper_scan() -> list:
    """
    3-feed scan strategy:
      1. pump.fun/coins — brand new Solana tokens at birth (minutes 1-30)
      2. dexscreener token-boosts — tokens teams paid to promote (social signal)
      3. dexscreener token-profiles — tokens with complete social profiles

    All 3 feeds are fetched in parallel for speed.
    Returns merged, deduplicated list with extra metadata attached.
    """
    results: dict = {}   # tokenAddress → item

    async def _fetch_pumpfun():
        try:
            client = await get_http()
            # frontend-api.pump.fun is deprecated — try multiple endpoints
            pf_resp = None
            for pf_url in [
                "https://frontend-api-v3.pump.fun/coins",
                "https://frontend-api.pump.fun/coins",
                "https://client-api-2-74b1891ee9f9.herokuapp.com/coins",
            ]:
                try:
                    r = await client.get(
                        pf_url,
                        params={"offset": 0, "limit": 50, "sort": "creation_time", "order": "DESC"},
                        headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
                        timeout=6,
                    )
                    if r.status_code == 200:
                        pf_resp = r
                        break
                except Exception:
                    continue
            if pf_resp and pf_resp.status_code == 200:
                pf_data = pf_resp.json()
                if isinstance(pf_data, list):
                    _pf_initial = 30.0
                    _pf_grad    = 55.0
                    out = {}
                    for coin in pf_data:
                        mint = coin.get("mint", "")
                        if not mint:
                            continue
                        v_sol = float(coin.get("virtual_sol_reserves", 0) or 0)
                        curve_pct = round(min(max((v_sol - _pf_initial) / (_pf_grad - _pf_initial) * 100, 0), 100), 1) if v_sol > _pf_initial else 0.0
                        graduated = bool(coin.get("raydium_pool")) or bool(coin.get("complete"))
                        if graduated:
                            curve_pct = 100.0
                        out[mint] = {
                            "tokenAddress": mint,
                            "chainId":      "solana",
                            "source":       "pumpfun",
                            "links":        [],
                            "_pf_curve":    curve_pct,
                            "_pf_replies":  int(coin.get("reply_count", 0) or 0),
                            "_pf_graduated":graduated,
                            # pump.fun returns raw token amount, not %. 
                            # Total supply is always 1,000,000,000 tokens on pump.fun
                            "_pf_dev_pct":  round(float(coin.get("creator_token_holdings", 0) or 0) / 1_000_000_000 * 100, 2),
                            "_pf_name":     coin.get("name", ""),
                            "_pf_symbol":   coin.get("symbol", ""),
                            "_pf_twitter":  coin.get("twitter", ""),
                            "_pf_telegram": coin.get("telegram", ""),
                            "_pf_website":  coin.get("website", ""),
                        }
                    return out
        except Exception as e:
            logger.warning(f"Pump.fun feed error: {e}")
        return {}

    async def _fetch_ds(url: str):
        try:
            client = await get_http()
            r = await client.get(url, headers={"Accept": "application/json"}, timeout=8)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    return data
        except Exception as e:
            logger.warning(f"DexScreener feed error {url}: {e}")
        return []

    # ── Fetch all 3 feeds in parallel ─────────────────────────────────────────
    pf_results, ds_boosts, ds_profiles = await _asyncio.gather(
        _fetch_pumpfun(),
        _fetch_ds("https://api.dexscreener.com/token-boosts/latest/v1"),
        _fetch_ds("https://api.dexscreener.com/token-profiles/latest/v1"),
    )

    results.update(pf_results)
    logger.info(f"Pump.fun feed: {len(pf_results)} tokens")

    # ── Merge DexScreener feeds ────────────────────────────────────────────────
    for ds_list in (ds_boosts, ds_profiles):
        for item in ds_list:
            addr = item.get("tokenAddress", "")
            if not addr:
                continue
            if addr not in results:
                item["source"] = "dexscreener"
                item["_pf_curve"]     = None
                item["_pf_replies"]   = 0
                item["_pf_graduated"] = False
                item["_pf_dev_pct"]   = None
                results[addr] = item
            elif "totalAmount" in item:
                results[addr]["_boost_amount"] = float(item.get("totalAmount", 0) or 0)

    # ── Pre-filter: must have at least 1 social signal ────────────────────────
    filtered = []
    for item in results.values():
        links = item.get("links", []) or []
        has_any_social = (
            any(l.get("type","").lower() in ("twitter","x","telegram","website","web") for l in links)
            or bool(item.get("_pf_twitter"))
            or bool(item.get("_pf_telegram"))
            or bool(item.get("_pf_website"))
        )
        if has_any_social:
            filtered.append(item)

    logger.info(f"Sniper scan: {len(results)} raw → {len(filtered)} with social signals")
    return filtered



_SNIPER_CHAIN_MAP: dict = {
    "solana": "solana", "sol": "solana",
    "ethereum": "ethereum", "eth": "ethereum",
    "base": "base",
    "bsc": "bsc", "bnb": "bsc",
    "arbitrum": "arbitrum", "arb": "arbitrum",
}

def _sniper_chain_id(chain: str) -> str:
    """Normalise DexScreener chainId to sniper_chains key."""
    return _SNIPER_CHAIN_MAP.get(chain.lower(), "")


def _sniper_daily_reset(ud: dict):
    """Reset daily sniper budget if it's a new day."""
    today = datetime.now().date()
    if ud.get("sniper_daily_date") != today:
        ud["sniper_daily_spent"] = 0.0
        ud["sniper_daily_date"] = today


def _build_history_context(ud: dict, uid: int) -> str:
    """Summarise user's past sniper trades for AI context."""
    sniper_trades = [
        t for t in trade_log.get(uid, [])
        if t.get("mood") in ("Sniper", "AI-Sniper")
    ]
    if not sniper_trades:
        return "No past sniper trades yet."
    wins   = [t for t in sniper_trades if t["realized_pnl"] > 0]
    losses = [t for t in sniper_trades if t["realized_pnl"] <= 0]
    wr     = round(len(wins) / len(sniper_trades) * 100) if sniper_trades else 0
    avg_w  = round(sum(t["realized_pnl"] for t in wins) / len(wins), 2) if wins else 0
    avg_l  = round(sum(t["realized_pnl"] for t in losses) / len(losses), 2) if losses else 0
    best_x = max((t.get("x", 0) for t in sniper_trades), default=0)
    lines  = [
        f"Past sniper trades: {len(sniper_trades)} total, {wr}% win rate",
        f"Avg win: ${avg_w}  |  Avg loss: ${avg_l}  |  Best X: {round(best_x,2)}x",
    ]
    for t in sorted(sniper_trades, key=lambda x: x.get("closed_at", datetime.min), reverse=True)[:5]:
        outcome = "WIN" if t["realized_pnl"] > 0 else "LOSS"
        lines.append(
            f"  {outcome} ${t['symbol']} {round(t.get('x',0),2)}x  "
            f"PnL:{round(t['realized_pnl'],2)}  Held:{t.get('hold_h',0)}h  "
            f"Mood:{t.get('mood','?')}"
        )
    return "\n".join(lines)


async def ai_analyze_token(info: dict, sc: dict, ud: dict, uid: int = 0) -> dict:
    """
    Call Claude to analyze a token for sniper quality.
    Returns dict with: verdict, confidence, suggested_amount, thesis, red_flags, green_flags
    """
    history_ctx = _build_history_context(ud, uid)
    bal          = ud.get("balance", 1000)
    sf           = ud.get("sniper_filters", {})
    max_buy      = float(sf.get("buy_amount", 100))

    prompt = f"""You are an expert memecoin sniper trader. Analyze this brand new token launch.

TOKEN DATA:
Symbol: ${info['symbol']} ({info.get('name','?')})
Chain: {info.get('chain','?')}
Price: ${info.get('price',0):.10g}
Market Cap: ${info.get('mc',0):,.0f}
Liquidity: ${info.get('liq',0):,.0f} ({info.get('liq_pct',0)}% of MC)
Age: {round(info.get('age_h',0),2)} hours old
24h Volume: ${info.get('vol_h24',0):,.0f}
1h Volume: ${info.get('vol_h1',0):,.0f}
5m Volume: ${info.get('vol_m5',0):,.0f}
Buys (24h): {info.get('buys',0)} ({info.get('buy_pct',50)}% of txns)
Sells (24h): {info.get('sells',0)} ({100-info.get('buy_pct',50)}% of txns)
Avg buy size: ${(info.get('vol_h1',0)/info.get('buys',1)) if info.get('buys',0)>0 else 0:,.0f}
Price Change 5m: {info.get('ch_m5',0)}%
Price Change 1h: {info.get('ch_h1',0)}%
Price Change 6h: {info.get('ch_h6',0)}%
Twitter: {'✅ ' + info.get('twitter','') if info.get('twitter') else '❌ None'}
Telegram: {'✅ ' + info.get('telegram','') if info.get('telegram') else '❌ None'}
Website: {'✅ ' + info.get('website','') if info.get('website') else '❌ None'}

PUMP.FUN DATA (Solana only):
Bonding Curve: {str(info.get('pf_curve')) + '%' if info.get('pf_curve') is not None else 'N/A (not a Pumpfun token)'}
Graduated to Raydium: {'✅ YES — proven demand' if info.get('pf_graduated') else '❌ Not yet'}
Community Replies: {info.get('pf_replies', 0)}
Boost Spend: {str(info.get('boost_amount', 0)) + ' SOL' if info.get('boost_amount', 0) > 0 else 'None'}

RUGCHECK SECURITY (Solana):
LP Burned: {str(info.get('lp_burn')) + '%' if info.get('lp_burn') is not None else 'Unknown'}
Mint Authority: {'❌ ACTIVE — supply can be inflated' if info.get('no_mint') is False else ('✅ Disabled' if info.get('no_mint') is True else 'Unknown')}
Top 10 Holders: {str(info.get('top10_pct')) + '%' if info.get('top10_pct') is not None else 'Unknown'}
Insider %: {str(info.get('insider_pct')) + '%' if info.get('insider_pct') is not None else 'Unknown'}
RugCheck Risks: {', '.join(info.get('rug_risks', [])) or 'None flagged'}

WALLET INTELLIGENCE (Helius):
Maker %: {str(info.get('maker_pct')) + '% unique buyer wallets' if info.get('maker_pct') is not None else 'Not available (add HELIUS_API_KEY)'}
Top 3 Wallet Vol %: {str(info.get('top3_vol_pct')) + '% of volume (HIGH = wash trading)' if info.get('top3_vol_pct') is not None else 'Not available'}

MULTI-TIMEFRAME PRESSURE:
M5:  {info.get('buys_m5',0)} buys / {info.get('sells_m5',0)} sells ({info.get('buy_pct_m5', info.get('buy_pct',50))}% buy pressure)
H1:  {info.get('buys_h1',0)} buys / {info.get('sells_h1',0)} sells ({info.get('buy_pct_h1', info.get('buy_pct',50))}% buy pressure)
H24: {info.get('buys',0)} buys / {info.get('sells',0)} sells ({info.get('buy_pct',50)}% buy pressure)

AURACLE SNIPER SCORE: {sc['score']}/100 — {sc['verdict']}
Strengths: {', '.join(sc.get('strengths',[])) or 'None'}
Warnings: {', '.join(sc.get('warnings',[])) or 'None'}
Hard Flags: {', '.join(sc.get('flags',[])) or 'None'}

USER HISTORY:
{history_ctx}

Balance: ${bal:,.2f} | Max buy: ${max_buy}

ANALYZE — focus on these 6 things:
1. ORGANIC VOLUME: Is this real retail? Check avg buy size, maker%, top3 wallet concentration.
2. RUG SAFETY: LP burn%, mint authority, insider%, rug_risks from RugCheck.
3. ENTRY TIMING: Bonding curve % (Pumpfun sweet spot: 35-70%). Age vs momentum.
4. SOCIAL PROOF: Twitter/TG + community replies + boost spend = serious team.
5. MOMENTUM QUALITY: Steady H1 climb vs parabolic spike. M5 vs H1 divergence.
6. BUNDLE RISK: Any bundle/insider flags from RugCheck = coordinated dump setup.

Be DECISIVE. Score ≥ 60 + no hard flags → lean SNIPE.
Hard flags (bundle, mint active, dev sold, zero socials) → SKIP regardless of score.

Respond ONLY in this exact JSON (no markdown):
{{
  "verdict": "SNIPE" | "SKIP" | "WAIT",
  "confidence": <1-10 integer>,
  "suggested_amount": <float, max {max_buy}>,
  "thesis": "<2-3 sentence explanation>",
  "red_flags": ["<flag1>", "<flag2>"],
  "green_flags": ["<flag1>", "<flag2>"],
  "rug_risk": "LOW" | "MEDIUM" | "HIGH",
  "momentum": "STRONG" | "MODERATE" | "WEAK" | "NEGATIVE",
  "social_score": "GOOD" | "PARTIAL" | "NONE"
}}"""

    # ── Rate limiter: max 4 AI calls/min to stay within free tier limits ──────
    # Uses a simple module-level token bucket shared across all calls.
    now = datetime.now().timestamp()
    if not hasattr(ai_analyze_token, "_call_times"):
        ai_analyze_token._call_times = []
    # Purge calls older than 60 seconds
    ai_analyze_token._call_times = [t for t in ai_analyze_token._call_times if now - t < 60]
    if len(ai_analyze_token._call_times) >= 4:
        # Wait until oldest call falls out of the 60s window
        wait_s = 60 - (now - ai_analyze_token._call_times[0]) + 0.5
        if wait_s > 0:
            logger.info(f"AI rate limiter: waiting {wait_s:.1f}s to avoid free-tier cap")
            await _asyncio.sleep(wait_s)
    ai_analyze_token._call_times.append(datetime.now().timestamp())
    # ─────────────────────────────────────────────────────────────────────────

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("AI token analysis: ANTHROPIC_API_KEY not set")
        return {
            "verdict": "WAIT",
            "confidence": 0,
            "suggested_amount": max_buy,
            "thesis": "⚠️ ANTHROPIC_API_KEY not set on Railway. Go to Railway → Variables → add ANTHROPIC_API_KEY=sk-ant-...",
            "red_flags": ["API key missing — set ANTHROPIC_API_KEY in Railway Variables"],
            "green_flags": [],
            "rug_risk": "UNKNOWN",
            "momentum": "UNKNOWN",
            "social_score": "UNKNOWN",
        }
    for attempt in range(2):   # retry once on transient failure
        try:
            client = await get_http()
            if True:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "Content-Type": "application/json",
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                    },
                    json={
                        # claude-haiku-4-5-20251001 — fastest & cheapest (~25x cheaper than Sonnet).
                        # To upgrade to Sonnet later once you have paid credits, change this to:
                        #   "claude-sonnet-4-5"
                        # To upgrade to Opus (highest quality), change to:
                        #   "claude-opus-4-5"
                        "model": "claude-haiku-4-5-20251001",
                        "max_tokens": 600,
                        "messages": [{"role": "user", "content": prompt}],
                    }
                )
            if resp.status_code == 200:
                raw = resp.json()
                text = raw["content"][0]["text"].strip()
                # Strip any accidental markdown fences
                text = text.replace("```json", "").replace("```", "").strip()
                result = _json.loads(text)
                # Validate required fields are present
                assert "verdict" in result and result["verdict"] in ("SNIPE", "SKIP", "WAIT")
                assert "confidence" in result
                # Clamp suggested_amount
                result["suggested_amount"] = min(
                    float(result.get("suggested_amount", max_buy)),
                    max_buy,
                    ud.get("balance", 0)
                )
                return result
            else:
                err_body = resp.text[:300]
                logger.warning(f"AI API error (attempt {attempt+1}): HTTP {resp.status_code} — {err_body}")
                if resp.status_code in (400, 401, 403):
                    break   # don't retry auth/bad-request errors
        except _json.JSONDecodeError as e:
            logger.warning(f"AI response JSON parse failed (attempt {attempt+1}): {e} | raw: {text[:200]}")
        except Exception as e:
            logger.warning(f"AI token analysis failed (attempt {attempt+1}): {e}")

    # Fallback if AI call fails
    return {
        "verdict": "WAIT",
        "confidence": 0,
        "suggested_amount": max_buy,
        "thesis": "AI analysis unavailable. Check Railway logs for error details.",
        "red_flags": ["AI call failed — check Railway logs"],
        "green_flags": [],
        "rug_risk": "UNKNOWN",
        "momentum": "UNKNOWN",
        "social_score": "UNKNOWN",
    }


def _mtv_block(info: dict) -> str:
    """
    Build the M · T · V intelligence block — shows Makers/Trades/Volume
    buy pressure across M5, H1, H6, H24 timeframes, matching the style
    from the image reference.
    M = maker% (Helius, optional)
    T = trade buy% (buy txns / total txns)
    V = volume per window
    """
    def _pct_dot(pct: float) -> str:
        """Colour dot based on buy pressure %."""
        if pct >= 60:   return "🟢"
        elif pct >= 52: return "🟡"
        else:           return "🔴"

    def _fmt_vol(v: float) -> str:
        if v >= 1_000_000: return f"{v/1_000_000:.2f}M"
        if v >= 1_000:     return f"{v/1_000:.2f}K"
        return f"{v:.0f}"

    maker_pct   = info.get("maker_pct")   # Helius — may be None
    maker_count = info.get("maker_count")

    # Timeframe rows: (label, trades_buys, trades_sells, trade_buy_pct, volume)
    rows = [
        ("M5",  info.get("buys_m5",0),  info.get("sells_m5",0),  info.get("buy_pct_m5", info.get("buy_pct",50)), info.get("vol_m5",0)),
        ("H1",  info.get("buys_h1",0),  info.get("sells_h1",0),  info.get("buy_pct_h1", info.get("buy_pct",50)), info.get("vol_h1",0)),
        ("H6",  info.get("buys_h6",0),  info.get("sells_h6",0),  info.get("buy_pct_h6", info.get("buy_pct",50)), info.get("vol_h6",0)),
        ("H24", info.get("buys",0),      info.get("sells",0),     info.get("buy_pct",50),                         info.get("vol_h24",0)),
    ]

    lines = ["", "📊 *M · T · V Intelligence*"]
    for label, b, s, t_pct, vol in rows:
        total_t = b + s
        t_dot   = _pct_dot(t_pct)
        v_dot   = _pct_dot(t_pct)   # use trade pct as vol proxy (DS doesn't split vol by side)
        t_line  = f"  ├ T:  {total_t:>4}  {t_dot} {t_pct:.1f}%"
        v_line  = f"  └ V:  {_fmt_vol(vol):>7}  {v_dot} {t_pct:.1f}%"
        lines.append(f"*{label}*")
        # Maker row — only if Helius data available and on H24 row
        if maker_pct is not None and label == "H24":
            m_dot  = _pct_dot(maker_pct)
            m_line = f"  ├ M:  {maker_count or '?':>4}  {m_dot} {maker_pct:.1f}%"
            lines.append(m_line)
        lines.append(t_line)
        lines.append(v_line)

    # Legend
    m_note = "_(M)akers · " if maker_pct is not None else "_"
    lines.append(f"{m_note}(T)rades · (V)olume_")
    lines.append("")
    return "\n".join(lines)


def _ai_report_text(info: dict, sc: dict, ai: dict, contract: str = "") -> str:
    """Build the full AI advisory report message with M·T·V intelligence."""
    verdict        = ai.get("verdict", "WAIT")
    confidence     = ai.get("confidence", 0)
    verdict_emoji  = {"SNIPE": "🟢", "SKIP": "🔴", "WAIT": "🟡"}.get(verdict, "⚪")
    rug_emoji      = {"LOW": "✅", "MEDIUM": "⚠️", "HIGH": "🚨", "UNKNOWN": "❓"}.get(ai.get("rug_risk","UNKNOWN"), "❓")
    mom_emoji      = {"STRONG": "🚀", "MODERATE": "📈", "WEAK": "📉", "NEGATIVE": "💀", "UNKNOWN": "❓"}.get(ai.get("momentum","UNKNOWN"), "❓")
    soc_emoji      = {"GOOD": "✅", "PARTIAL": "⚠️", "NONE": "🚨", "UNKNOWN": "❓"}.get(ai.get("social_score","UNKNOWN"), "❓")

    filled    = "█" * confidence
    empty     = "░" * (10 - confidence)
    conf_bar  = filled + empty

    score     = sc.get("score", 0)
    score_tag = "🔴 WEAK" if score < 40 else "🟡 MODERATE" if score < 65 else "🟢 STRONG"

    red_flags   = ai.get("red_flags", [])
    green_flags = ai.get("green_flags", [])
    hard_flags  = sc.get("flags", [])
    strengths   = sc.get("strengths", [])
    warnings    = sc.get("warnings", [])

    red_lines   = "\n".join("  🚨 " + f for f in red_flags)   or "  None detected"
    green_lines = "\n".join("  ✅ " + f for f in green_flags) or "  None detected"

    hard_block = ""
    if hard_flags:
        hard_block = "\n⛔ *Hard Flags:*\n" + "\n".join("  🚫 " + f for f in hard_flags) + "\n"

    strength_block = ""
    if strengths:
        strength_block = "\n💪 *Sniper Strengths:*\n" + "\n".join("  ✅ " + s for s in strengths[:4]) + "\n"

    warning_block = ""
    if warnings:
        warning_block = "\n⚠️ *Warnings:*\n" + "\n".join("  ⚠️ " + w for w in warnings[:3]) + "\n"

    age_h   = round(info.get("age_h", 0), 1)
    liq     = info.get("liq", 0)
    liq_pct = info.get("liq_pct", 0)
    mc      = info.get("mc", 0)
    ch_h1   = info.get("ch_h1", 0)
    ch_h24  = info.get("ch_h24", 0)

    # Pump.fun extras
    pf_curve     = info.get("pf_curve")
    pf_graduated = info.get("pf_graduated", False)
    pf_replies   = info.get("pf_replies", 0) or 0
    pf_line = ""
    if pf_curve is not None:
        grad_str = " 🎓 *GRADUATED*" if pf_graduated else f" Curve: *{pf_curve}%*"
        replies_str = f"  💬 Replies: *{pf_replies}*" if pf_replies > 0 else ""
        pf_line = "🟣 Pump.fun:" + grad_str + replies_str + "\n"

    # RugCheck safety line
    lp_burn    = info.get("lp_burn")
    no_mint    = info.get("no_mint")
    no_freeze  = info.get("no_freeze")
    top10_pct  = info.get("top10_pct")
    insider_pct= info.get("insider_pct")
    rug_parts  = []
    if lp_burn is not None:   rug_parts.append(f"LP: *{lp_burn}%* burned")
    if no_mint  is True:      rug_parts.append("✅ No mint")
    elif no_mint is False:    rug_parts.append("🚨 Mint active")
    if top10_pct is not None: rug_parts.append(f"Top10: *{top10_pct}%*")
    if insider_pct is not None and insider_pct > 0: rug_parts.append(f"Insider: *{insider_pct}%*")
    rc_line = ("🔐 " + "  ·  ".join(rug_parts) + "\n") if rug_parts else ""

    # Boost spend line
    boost = info.get("boost_amount", 0) or 0
    boost_line = (f"🚀 Boost spend: *{boost:.0f} SOL* — team promoting\n") if boost >= 5 else ""

    # MTV intelligence block
    mtv = _mtv_block(info)

    return (
        "🤖 *AI SNIPER ADVISORY*\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🪙 *$" + info["symbol"] + "*  ·  " + info.get("chain","?").upper() + "\n"
        + ("`" + contract + "`\n" if contract else "")
        + "📊 MC: *" + mc_str(mc) + "*  ·  ⏱ Age: *" + str(age_h) + "h*\n"
        "💧 Liq: *" + money(liq) + "* (" + str(liq_pct) + "%)\n"
        "📈 1h: *" + (("+" if ch_h1 >= 0 else "") + str(round(ch_h1,1)) + "%") + "*"
        "  24h: *" + (("+" if ch_h24 >= 0 else "") + str(round(ch_h24,1)) + "%") + "*\n"
        + pf_line
        + rc_line
        + boost_line
        + mtv
        + "━━━━━━━━━━━━━━━━━━\n"
        + verdict_emoji + " *VERDICT: " + verdict + "*\n"
        "🎯 Confidence: *" + str(confidence) + "/10*  `" + conf_bar + "`\n\n"
        "📝 *Analysis:*\n"
        "> " + ai.get("thesis", "No analysis available.") + "\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🧠 *Sniper Score: " + str(score) + "/100*  " + score_tag + "\n"
        + hard_block
        + strength_block
        + warning_block
        + "\n"
        + rug_emoji + " Rug Risk: *" + ai.get("rug_risk","UNKNOWN") + "*\n"
        + mom_emoji + " Momentum: *" + ai.get("momentum","UNKNOWN") + "*\n"
        + soc_emoji + " Socials:  *" + ai.get("social_score","UNKNOWN") + "*\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🚩 *Red Flags:*\n" + red_lines + "\n\n"
        "💚 *Green Flags:*\n" + green_lines + "\n\n"
        "💵 *Suggested Entry: " + money(ai.get("suggested_amount", 0)) + "*"
    )


def _sniper_auto_kb(contract: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 Set Auto-Sell", callback_data="asm_" + contract),
         InlineKeyboardButton("🛑 Stop Loss",     callback_data="slm_" + contract)],
        [InlineKeyboardButton("📝 Journal",        callback_data="jnl_" + contract),
         InlineKeyboardButton("🏠 Main Menu",      callback_data="mm")],
    ])


# ════════════════════════════════════════════════════════════════════════════
# CHANNEL CARD — V3 (clean card + AI Analysis toggle)
# ════════════════════════════════════════════════════════════════════════════

def _fmt_opt(val, fmt="{}", suffix="", unknown="?") -> str:
    """Format an optional value, returning unknown string if None."""
    if val is None:
        return unknown
    try:
        return fmt.format(val) + suffix
    except Exception:
        return str(val) + suffix


def _channel_card_text(info: dict, sc: dict, ai: dict, contract: str) -> str:
    """
    Build the CLEAN channel card (no AI verdict section).
    Sections: hero · CA · core stats · holder/dev block · score · MTV · socials · search footer
    The AI analysis section is separate, revealed via toggle button.
    """
    symbol  = info.get("symbol", "???")
    chain   = info.get("chain",  "SOL").upper()
    mc      = info.get("mc",  0)
    liq     = info.get("liq", 0)
    liq_pct = info.get("liq_pct", 0)
    age_h   = round(info.get("age_h", 0), 1)
    ch_h1   = info.get("ch_h1",  0)
    ch_h24  = info.get("ch_h24", 0)

    verdict       = ai.get("verdict", "WAIT")
    verdict_emoji = {"SNIPE": "🟢", "SKIP": "🔴", "WAIT": "🟡"}.get(verdict, "⚪")
    score         = sc.get("score", 0)
    score_tag     = "🔴 WEAK" if score < 40 else "🟡 MODERATE" if score < 65 else "🟢 STRONG"

    # ── LP / Mint line ────────────────────────────────────────────────────
    lp_burn  = info.get("lp_burn")
    no_mint  = info.get("no_mint")
    no_freeze= info.get("no_freeze")
    lp_str   = f"LP: *{lp_burn}%* burned" if lp_burn is not None else ""
    mint_str = "✅ No mint" if no_mint is True else ("🚨 Mint active" if no_mint is False else "")
    sec_parts = [p for p in [lp_str, mint_str] if p]
    sec_line  = ("🔐 " + "  ·  ".join(sec_parts) + "\n") if sec_parts else ""

    # ── Pump.fun line ─────────────────────────────────────────────────────
    pf_curve     = info.get("pf_curve")
    pf_graduated = info.get("pf_graduated", False)
    pf_replies   = info.get("pf_replies", 0) or 0
    pf_line = ""
    if pf_curve is not None:
        grad_str   = " 🎓 *GRADUATED*" if pf_graduated else f" Curve: *{pf_curve}%*"
        rep_str    = f"  💬 Replies: *{pf_replies}*" if pf_replies > 0 else ""
        pf_line    = "🟣 Pump.fun:" + grad_str + rep_str + "\n"

    # ── Holder / Dev block ────────────────────────────────────────────────
    h_count    = info.get("holder_count")
    top10_pct  = info.get("top10_pct")
    top20_pct  = info.get("top20_pct")
    insider_pct= info.get("insider_pct")
    sniper_cnt = info.get("sniper_count", 0)
    fresh_pct  = info.get("fresh_pct")
    smart_w    = info.get("smart_wallets")
    dev_pct    = info.get("pf_dev_pct")   # % held by dev wallet
    dev_sold   = info.get("dev_sold", False)
    boost      = info.get("boost_amount", 0) or 0
    dex_paid   = boost > 0

    # Build holder line
    holder_parts = []
    if h_count is not None:
        holder_parts.append(f"Holders: *{h_count}*")
    if top10_pct is not None:
        holder_parts.append(f"Top10: *{top10_pct}%*")
    if top20_pct is not None:
        holder_parts.append(f"Top20: *{top20_pct}%*")
    holder_line = "👨‍👩‍👧 " + "  |  ".join(holder_parts) + "\n" if holder_parts else ""

    # Insider / sniper line
    ins_str    = f"🐀 Insiders: *{insider_pct}%*" if insider_pct is not None else ""
    snip_str   = f"🎯 Snipers: *{sniper_cnt}*"
    ins_line   = "  ·  ".join(p for p in [ins_str, snip_str] if p) + "\n"

    # Fresh / SmartWallet line
    fresh_str  = f"🫧 Fresh: *{fresh_pct}%*" if fresh_pct is not None else ""
    smart_str  = f"💰 SmartWallets: *{smart_w}*" if smart_w is not None else ""
    fw_line    = "  ·  ".join(p for p in [fresh_str, smart_str] if p)
    fw_line    = (fw_line + "\n") if fw_line else ""

    # Dev / DEX line
    dev_pct_str = f"({round(dev_pct, 1)}%)" if dev_pct is not None else "(-%)"
    sold_dot    = "🟢" if dev_sold else ("🔴" if dev_pct is not None else "❓")
    dex_dot     = "🟢" if dex_paid else "🔴"
    dev_line    = f"👤 DEV {dev_pct_str} Sold: {sold_dot}  ·  ⚡ DEX PAID: {dex_dot}\n"

    holder_block = holder_line + ins_line + fw_line + dev_line

    # ── MTV intelligence ──────────────────────────────────────────────────
    mtv = _mtv_block(info)

    # ── Socials — real hyperlinks in card body ────────────────────────────
    soc_parts = []
    if info.get("twitter"):
        soc_parts.append(f"[🐦 Twitter]({info['twitter']})")
    if info.get("telegram"):
        soc_parts.append(f"[✈️ Telegram]({info['telegram']})")
    if info.get("website"):
        soc_parts.append(f"[🌐 Website]({info['website']})")
    socials_line = ("\n🔗  " + "   ·   ".join(soc_parts) + "\n") if soc_parts else ""

    # ── Explorer footer — GT · DT · DS · DV · BE · PF ────────────────────
    chain_rc  = info.get("chain", "solana").lower()
    _dex_c    = {"solana":"solana","ethereum":"ethereum","base":"base",
                 "bsc":"bsc","arbitrum":"arbitrum"}.get(chain_rc, chain_rc)
    _gt = f"https://www.geckoterminal.com/{_dex_c}/pools/{contract}"
    _dt = f"https://www.dextools.io/app/en/{_dex_c}/pair-explorer/{contract}"
    _ds = f"https://dexscreener.com/{_dex_c}/{contract}"
    _dv = f"https://www.dexview.com/{_dex_c}/{contract}"
    _be = f"https://birdeye.so/token/{contract}?chain={_dex_c}"
    _pf = f"https://pump.fun/{contract}"
    explorer_line = (
        f"[GT]({_gt})   [DT]({_dt})   [DS]({_ds})"
        f"   [DV]({_dv})   [BE]({_be})   [PF]({_pf})\n"
    )

    # ── Search footer — all pills search on X/Twitter ─────────────────────
    _bx = "https://x.com/search?f=live&q={}&src=typed_query"
    search_line = (
        f"\n🔍  [𝕏 ${symbol}]({_bx.format(_urlparse.quote('$' + symbol))})"
        f"   ·   [CA]({_bx.format(_urlparse.quote(contract))})"
        f"   ·   [Name]({_bx.format(_urlparse.quote(name))})"
    )

    # ── Assemble ──────────────────────────────────────────────────────────
    ch1   = ("+" if ch_h1  >= 0 else "") + str(round(ch_h1, 1))  + "%"
    ch24  = ("+" if ch_h24 >= 0 else "") + str(round(ch_h24, 1)) + "%"

    return (
        f"{verdict_emoji} *${symbol}*  ·  {chain}  ·  *{verdict}*\n"
        + ("\n`" + contract + "`\n" if contract else "")
        + f"\n📊 MC: *{mc_str(mc)}*  ·  ⏱ Age: *{age_h}h*\n"
        f"💧 Liq: *{money(liq)}* ({liq_pct}%)\n"
        f"📈 1h: *{ch1}*  ·  24h: *{ch24}*\n"
        + pf_line
        + sec_line
        + "━━━━━━━━━━━━━━━━━━\n"
        + holder_block
        + f"🧠 *Sniper Score: {score}/100*  {score_tag}\n"
        + "━━━━━━━━━━━━━━━━━━\n"
        + mtv
        + socials_line
        + "━━━━━━━━━━━━━━━━━━\n"
        + explorer_line
        + search_line
    )


def _ai_section_text(info: dict, sc: dict, ai: dict) -> str:
    """
    The collapsible AI analysis section appended when user taps 'AI Analysis'.
    Contains: Verdict · Confidence · Thesis · Strengths · Warnings · Risk · Entry
    """
    verdict       = ai.get("verdict", "WAIT")
    confidence    = ai.get("confidence", 0)
    verdict_emoji = {"SNIPE": "🟢", "SKIP": "🔴", "WAIT": "🟡"}.get(verdict, "⚪")
    rug_emoji     = {"LOW": "✅", "MEDIUM": "⚠️", "HIGH": "🚨", "UNKNOWN": "❓"}.get(ai.get("rug_risk","UNKNOWN"), "❓")
    mom_emoji     = {"STRONG": "🚀", "MODERATE": "📈", "WEAK": "📉", "NEGATIVE": "💀", "UNKNOWN": "❓"}.get(ai.get("momentum","UNKNOWN"), "❓")

    filled   = "█" * confidence
    empty    = "░" * (10 - confidence)
    conf_bar = filled + empty

    strengths  = sc.get("strengths", [])
    warnings   = sc.get("warnings", [])
    hard_flags = sc.get("flags", [])
    red_flags  = ai.get("red_flags", [])
    green_flags= ai.get("green_flags", [])

    str_block  = "\n".join("  ✅ " + s for s in strengths[:4]) or "  None"
    warn_block = "\n".join("  ⚠️ " + w for w in warnings[:3])  or "  None"
    hf_block   = "\n".join("  🚫 " + f for f in hard_flags)    or ""
    rf_block   = "\n".join("  🚨 " + f for f in red_flags)     or "  None detected"
    gf_block   = "\n".join("  ✅ " + f for f in green_flags)   or "  None detected"

    hard_section = (f"\n⛔ *Hard Flags:*\n{hf_block}\n") if hard_flags else ""

    return (
        "\n\n🧠 *AI ANALYSIS*\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"{verdict_emoji} *VERDICT: {verdict}*\n"
        f"🎯 Confidence: *{confidence}/10*  `{conf_bar}`\n\n"
        "📝 *Thesis:*\n"
        "> " + ai.get("thesis", "No analysis available.") + "\n\n"
        + hard_section
        + "💪 *Strengths:*\n" + str_block + "\n\n"
        + "⚠️ *Warnings:*\n"  + warn_block + "\n\n"
        + rug_emoji + " Rug Risk: *" + ai.get("rug_risk","UNKNOWN") + "*\n"
        + mom_emoji + " Momentum: *" + ai.get("momentum","UNKNOWN") + "*\n\n"
        + "🚩 *Red Flags:*\n"  + rf_block + "\n\n"
        + "💚 *Green Flags:*\n" + gf_block + "\n\n"
        + "💵 *Suggested Entry: " + money(ai.get("suggested_amount", 0)) + "*"
    )


def _channel_card_kb(
    contract: str,
    bot_url: str | None,
    info: dict,
    explorer_urls: dict,
    expanded: bool = False,
) -> InlineKeyboardMarkup:
    """Build the keyboard for the channel broadcast card."""
    toggle_label = "🧠 AI Analysis ▲" if expanded else "🧠 AI Analysis ▼"
    toggle_cb    = "aic_" + contract if expanded else "aiex_" + contract

    # Social URL buttons (only if links exist)
    soc_row = []
    if info.get("twitter"):
        soc_row.append(InlineKeyboardButton("🐦 Twitter",  url=info["twitter"]))
    if info.get("telegram"):
        soc_row.append(InlineKeyboardButton("✈️ Telegram", url=info["telegram"]))
    if info.get("website"):
        soc_row.append(InlineKeyboardButton("🌐 Website",  url=info["website"]))

    # Explorer footer row — GT · DT · DS · DV · BE · PF
    exp_row = []
    labels = [("GT", "gt"), ("DT", "dt"), ("DS", "ds"),
              ("DV", "dv"), ("BE", "be"), ("PF", "pf")]
    for label, key in labels:
        url = explorer_urls.get(key)
        if url:
            exp_row.append(InlineKeyboardButton(label, url=url))

    rows = []
    if soc_row:
        rows.append(soc_row)
    rows.append([InlineKeyboardButton(toggle_label, callback_data=toggle_cb)])
    if bot_url:
        rows.append([InlineKeyboardButton("⚡ Buy on Auracle", url=bot_url)])
    if exp_row:
        rows.append(exp_row)
    return InlineKeyboardMarkup(rows)




def _compact_pill_text(info: dict, sc: dict, ai: dict) -> str:
    """Build the small 'New Token Spotted' DM notification pill."""
    verdict_emoji = {"SNIPE": "🟢", "SKIP": "🔴", "WAIT": "🟡"}.get(ai.get("verdict","WAIT"), "⚪")
    score = sc.get("score", 0)
    score_tag = "🔴 WEAK" if score < 40 else "🟡 MODERATE" if score < 65 else "🟢 STRONG"
    age_h = round(info.get("age_h", 0), 1)
    return (
        "🔔 *New Token Spotted*\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🪙 *$" + info["symbol"] + "*  ·  " + info.get("chain","?").upper() + "\n"
        "📊 MC: *" + mc_str(info.get("mc",0)) + "*  ·  ⏱ Age: *" + str(age_h) + "h*  ·  Liq: *" + mc_str(info.get("liq",0)) + "*\n"
        "🧠 Score: *" + str(score) + "/100*  " + score_tag + "\n"
        + verdict_emoji + " Verdict: *" + ai.get("verdict","WAIT") + "*  ·  Confidence: *" + str(ai.get("confidence",0)) + "/10*"
    )


async def _broadcast_to_channel(bot, channel_id: int, info: dict, sc: dict, ai: dict, contract: str, uid: int = 0) -> bool:
    """Post the clean V3 AI signal card to the broadcast channel. Returns True on success."""
    try:
        _prune_ai_cache()
        chain     = info.get("chain","solana").lower().replace(" ","")
        dex_chain = {"solana":"solana","ethereum":"ethereum","base":"base","bsc":"bsc","arbitrum":"arbitrum"}.get(chain, chain)
        gt_url    = "https://www.geckoterminal.com/" + dex_chain + "/pools/" + contract
        dt_url    = "https://www.dextools.io/app/en/" + dex_chain + "/pair-explorer/" + contract
        ds_url    = "https://dexscreener.com/"        + dex_chain + "/" + contract
        dv_url    = "https://www.dexview.com/"        + dex_chain + "/" + contract
        be_url    = "https://birdeye.so/token/"       + contract  + "?chain=" + dex_chain
        pf_url    = "https://pump.fun/"               + contract
        bot_url   = ("https://t.me/" + _bot_username + "?start=" + contract) if _bot_username else None

        explorer_urls = {"gt": gt_url, "dt": dt_url, "ds": ds_url,
                         "dv": dv_url, "be": be_url, "pf": pf_url}

        card_text = _channel_card_text(info, sc, ai, contract)
        kb        = _channel_card_kb(contract, bot_url, info, explorer_urls, expanded=False)

        msg_sent = await bot.send_message(
            chat_id=channel_id,
            text=card_text,
            parse_mode="Markdown",
            reply_markup=kb,
            disable_web_page_preview=True,
        )
        # Cache for toggle re-render (24h TTL)
        _channel_ai_cache[contract] = {
            "info":          info,
            "sc":            sc,
            "ai":            ai,
            "channel_id":    channel_id,
            "explorer_urls": explorer_urls,
            "bot_url":       bot_url,
            "ts":            _time.time(),
        }
        if uid:
            _register_channel_call(uid, contract, info, channel_id,
                                   signal_msg_id=msg_sent.message_id)
        return True
    except Exception as e:
        logger.warning(f"Broadcast to channel {channel_id} failed: {e}")
        return False


# ════════════════════════════════════════════════════════════════════════════
# KOL / SMART WALLET TRACKER
# Monitors a user's saved wallet addresses via Helius RPC.
# Every 5 minutes, checks for new buys and alerts the user.
# Requires HELIUS_API_KEY in Railway environment variables.
# ════════════════════════════════════════════════════════════════════════════

# Module-level: last-seen transaction signature per wallet per user
# {uid: {wallet_address: last_tx_signature}}
_kol_last_sig: dict = {}


async def get_kol_recent_buys(wallet: str, helius_key: str, last_sig: str | None = None) -> list:
    """
    Fetch recent SWAP transactions for a wallet via Helius.
    Returns list of NEW buy events since last_sig (newest-first from API).
    Each item: {mint, sol_spent, signature, timestamp}
    
    FIX: Never pass 'before' param — that fetches OLDER txns.
    Always fetch latest 20, then stop when we hit last_sig.
    """
    if not helius_key:
        return []
    try:
        client = await get_http()
        # Always fetch latest — DO NOT pass 'before' (that returns older txns)
        params: dict = {
            "api-key": helius_key,
            "limit":   "20",
            "type":    "SWAP",
        }
        url = f"https://api.helius.xyz/v1/addresses/{wallet}/transactions"
        r = await client.get(url, params=params, timeout=8)
        if r.status_code == 429:
            logger.info("Helius KOL rate limit — skipping this cycle")
            return []
        if r.status_code != 200:
            return []
        txns = r.json()
        if not isinstance(txns, list):
            return []

        buys = []
        for tx in txns:
            sig = tx.get("signature", "")
            if sig == last_sig:
                break   # reached last known tx
            try:
                ts        = tx.get("timestamp", 0)
                tt        = tx.get("tokenTransfers", []) or []
                nt        = tx.get("nativeTransfers", []) or []
                fee_payer = tx.get("feePayer", "")

                # Find tokens received by this wallet (buys)
                for transfer in tt:
                    if transfer.get("toUserAccount") == fee_payer:
                        mint = transfer.get("mint", "")
                        if not mint or mint in ("So11111111111111111111111111111111111111112",):
                            continue   # skip wrapped SOL
                        # SOL spent = outgoing native transfers from this wallet
                        sol_spent = sum(
                            abs(n.get("amount", 0))
                            for n in nt
                            if n.get("fromUserAccount") == fee_payer
                        ) / 1e9
                        buys.append({
                            "mint":       mint,
                            "sol_spent":  round(sol_spent, 4),
                            "signature":  sig,
                            "timestamp":  ts,
                        })
            except Exception:
                continue

        return buys

    except Exception as e:
        logger.debug(f"KOL fetch error for {wallet}: {e}")
        return []


async def kol_tracker_job(ctx: ContextTypes.DEFAULT_TYPE):
    """
    Runs every 5 minutes alongside sniper_job.
    For each user with KOL wallets set up:
      - Checks each wallet for new SWAP buys via Helius
      - If new buy found, fetches token data and sends alert
    Requires HELIUS_API_KEY.
    """
    helius_key = os.environ.get("HELIUS_API_KEY", "")
    if not helius_key:
        return   # silently skip — no key, no tracker

    active_users = [
        (uid, ud) for uid, ud in users.items()
        if ud.get("kol_wallets") and ud.get("kol_alerts_on", True) and ud.get("balance", 0) > 0
    ]
    if not active_users:
        return

    for uid, ud in active_users:
        wallets = ud.get("kol_wallets", [])
        user_sigs = _kol_last_sig.setdefault(uid, {})

        for wallet_entry in wallets:
            try:
                wallet_addr  = wallet_entry.get("address", "")
                wallet_label = wallet_entry.get("label", wallet_addr[:8] + "...")
                wallet_chain = wallet_entry.get("chain", "solana")
                if not wallet_addr or wallet_chain != "solana":
                    continue   # Helius = Solana only

                last_sig = user_sigs.get(wallet_addr)
                new_buys = await get_kol_recent_buys(wallet_addr, helius_key, last_sig)

                if not new_buys:
                    continue

                # Update last seen signature
                user_sigs[wallet_addr] = new_buys[0]["signature"]

                # For each new buy, fetch token data and alert
                for buy in new_buys[:3]:   # max 3 alerts per wallet per cycle
                    mint = buy["mint"]
                    try:
                        info = await get_token(mint)
                        if not info:
                            # Token too new for DexScreener — alert with basic data
                            await ctx.bot.send_message(
                                chat_id=uid,
                                parse_mode="Markdown",
                                text=(
                                    "👀 *KOL WALLET APE DETECTED*\n\n"
                                    "🏷 Wallet: *" + wallet_label + "*\n"
                                    "`" + wallet_addr + "`\n\n"
                                    "🪙 Token: `" + mint + "`\n"
                                    "💰 SOL spent: *" + str(buy['sol_spent']) + " SOL*\n\n"
                                    "_Token too new for full data. Check manually._\n"
                                    "[View on Solscan](https://solscan.io/tx/" + buy['signature'] + ")"
                                ),
                                disable_web_page_preview=True
                            )
                            continue

                        sc  = sniper_score(info)
                        sol = buy["sol_spent"]
                        sol_usd = await get_sol_price()
                        usd_est = sol * sol_usd

                        # Build alert message
                        score_tag  = "🔴 WEAK" if sc["score"] < 40 else "🟡 MODERATE" if sc["score"] < 65 else "🟢 STRONG"
                        pf_line    = ""
                        if info.get("pf_curve") is not None:
                            pf_line = "🟣 Curve: *" + str(info["pf_curve"]) + "%*" + (" 🎓 GRADUATED" if info.get("pf_graduated") else "") + "\n"

                        chain = info.get("chain","solana").lower()
                        dex_chain = {"solana":"solana","ethereum":"ethereum","base":"base","bsc":"bsc","arbitrum":"arbitrum"}.get(chain, chain)
                        gt_url = "https://www.geckoterminal.com/" + dex_chain + "/pools/" + mint
                        ds_url = "https://dexscreener.com/" + dex_chain + "/" + mint

                        kb = InlineKeyboardMarkup([
                            [InlineKeyboardButton("📈 DexScreener", url=ds_url),
                             InlineKeyboardButton("🔍 GeckoTerminal", url=gt_url)],
                            [InlineKeyboardButton("⚡ Trade on Auracle", callback_data="tc_" + mint)],
                        ])

                        await ctx.bot.send_message(
                            chat_id=uid,
                            parse_mode="Markdown",
                            text=(
                                "👀 *KOL APE ALERT*\n"
                                "━━━━━━━━━━━━━━━━━━\n"
                                "🏷 *" + wallet_label + "*\n"
                                "`" + wallet_addr[:20] + "...`\n\n"
                                "🪙 *$" + info["symbol"] + "*  ·  " + info.get("chain","?").upper() + "\n"
                                "`" + mint + "`\n\n"
                                "💰 Bought: *" + str(sol) + " SOL* (~$" + f"{usd_est:,.0f}" + ")\n"
                                "📊 MC: *" + mc_str(info["mc"]) + "*  ·  Age: *" + str(round(info.get("age_h",0),1)) + "h*\n"
                                "💧 Liq: *" + money(info["liq"]) + "*\n"
                                + pf_line
                                + "🧠 Score: *" + str(sc["score"]) + "/100*  " + score_tag + "\n\n"
                                + ("\n".join("  ✅ " + s for s in sc.get("strengths",[])[:3]) + "\n" if sc.get("strengths") else "")
                                + ("\n".join("  🚨 " + f for f in sc.get("flags",[])) + "\n" if sc.get("flags") else "")
                            ),
                            reply_markup=kb,
                            disable_web_page_preview=True
                        )

                    except Exception as _te:
                        logger.warning(f"KOL token alert error {mint}: {_te}")

            except Exception as _we:
                logger.warning(f"KOL wallet error {wallet_entry}: {_we}")


# ════════════════════════════════════════════════════════════════════════════
# CHANNEL MILESTONE TRACKER
# After a token is broadcast to a channel, this job tracks its price and
# posts milestone updates (2x, 5x, 10x, 20x, 50x) back to the channel.
# ════════════════════════════════════════════════════════════════════════════

_MILESTONE_XS = [2, 5, 10, 20, 50]   # multiples to announce

async def channel_milestone_job(ctx: ContextTypes.DEFAULT_TYPE):
    """
    Runs every 5 minutes. For each tracked channel call, checks current MC.
    If it crossed a new milestone (2x, 5x, 10x, 20x, 50x), posts an update
    to the channel — just like the GemTools bot.
    Auto-removes calls older than 7 days.
    """
    if not _channel_calls:
        return

    now = datetime.now()
    cutoff = (now - timedelta(days=7)).isoformat()

    for uid, calls in list(_channel_calls.items()):
        for contract, call_data in list(calls.items()):
            try:
                # Auto-expire old calls
                if call_data.get("called_at", "9999") < cutoff:
                    del calls[contract]
                    continue

                ch_id       = call_data.get("channel_id")
                entry_mc    = call_data.get("entry_mc", 0)
                entry_price = call_data.get("entry_price", 0)
                symbol      = call_data.get("symbol", "?")
                called_at   = call_data.get("called_at", now.isoformat())
                milestones  = call_data.setdefault("milestones_hit", set())

                if not ch_id or entry_mc <= 0 or entry_price <= 0:
                    continue

                # Fetch current price
                info = await get_token(contract)
                if not info:
                    continue

                cur_mc    = info.get("mc", 0)
                cur_price = info.get("price", 0)
                if cur_mc <= 0 or cur_price <= 0:
                    continue

                # Calculate current multiple
                current_x = cur_mc / entry_mc

                # Check each milestone
                for milestone_x in _MILESTONE_XS:
                    if milestone_x in milestones:
                        continue   # already announced
                    if current_x >= milestone_x:
                        milestones.add(milestone_x)

                        # Calculate elapsed time
                        try:
                            called_dt  = datetime.fromisoformat(called_at)
                            elapsed    = now - called_dt
                            hrs  = int(elapsed.total_seconds() // 3600)
                            mins = int((elapsed.total_seconds() % 3600) // 60)
                            elapsed_str = f"{hrs}h {mins}m" if hrs > 0 else f"{mins}m"
                        except Exception:
                            elapsed_str = "?"

                        # Build milestone message
                        rocket = "🚀" * min(milestone_x, 5)
                        msg = (
                            f"{rocket} *${symbol}* — *{milestone_x}x*\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"📊 MC: *{mc_str(entry_mc)}* → *{mc_str(cur_mc)}*\n"
                            f"⏱ Time to {milestone_x}x: *{elapsed_str}*"
                        )

                        # Buy button — deep links to bot to buy the token
                        bot_url = ("https://t.me/" + _bot_username + "?start=" + contract) if _bot_username else None
                        signal_msg_id = call_data.get("signal_msg_id")
                        kb_rows = []
                        if bot_url:
                            kb_rows.append([InlineKeyboardButton(f"⚡ Buy ${symbol}", url=bot_url)])
                        try:
                            await ctx.bot.send_message(
                                chat_id=ch_id,
                                text=msg,
                                parse_mode="Markdown",
                                reply_markup=InlineKeyboardMarkup(kb_rows) if kb_rows else None,
                                reply_to_message_id=signal_msg_id,
                                disable_web_page_preview=True
                            )
                            logger.info(f"Milestone {milestone_x}x posted for ${symbol} to channel {ch_id}")
                        except Exception as _me:
                            logger.warning(f"Milestone post failed for {contract}: {_me}")

            except Exception as _ce:
                logger.warning(f"Milestone tracker error for {contract}: {_ce}")


def _register_channel_call(uid: int, contract: str, info: dict, channel_id: int,
                           signal_msg_id: int = None):
    """Record a token call to the channel so milestone_job can track it."""
    user_calls = _channel_calls.setdefault(uid, {})
    if contract not in user_calls:   # don't reset if already tracking
        user_calls[contract] = {
            "symbol":         info.get("symbol", "?"),
            "entry_mc":       info.get("mc", 0),
            "entry_price":    info.get("price", 0),
            "called_at":      datetime.now().isoformat(),
            "channel_id":     channel_id,
            "signal_msg_id":  signal_msg_id,   # original card message to reply to
            "milestones_hit": set(),
        }
        # Cap at 500 tracked calls per user
        if len(user_calls) > 500:
            oldest_key = min(user_calls, key=lambda k: user_calls[k].get("called_at",""))
            del user_calls[oldest_key]

def _prune_sniper_log(ud: dict):
    """
    Remove SKIPPED tokens from sniper_log that are older than 10 minutes.
    Bought/SNIPE/WAIT entries are kept permanently.
    Called at the start of every sniper_job cycle.
    """
    log = ud.get("sniper_log")
    if not log:
        return
    cutoff = (datetime.now() - timedelta(minutes=10)).isoformat()
    ud["sniper_log"] = [
        entry for entry in log
        if entry.get("bought") or                          # always keep bought
           entry.get("verdict") not in ("SKIP",) or       # keep non-skip verdicts
           entry.get("timestamp", "9999") >= cutoff        # keep recent skips
    ]


async def sniper_job(ctx: ContextTypes.DEFAULT_TYPE):
    """Main AI sniper job — runs every 5 minutes."""
    global _bot_username
    try:
        if not _bot_username:
            try:
                me = await ctx.bot.get_me()
                _bot_username = me.username or ""
            except Exception:
                pass

        active_users = [
            (uid, ud) for uid, ud in users.items()
            if (ud.get("sniper_auto") or ud.get("sniper_advisory")) and ud.get("balance", 0) > 0
        ]
        if not active_users:
            return

        raw_tokens = await sniper_scan()
        if not raw_tokens:
            return

        # ── Deduplicate raw feed ──────────────────────────────────────────────
        seen_this_run: set = set()
        unique_items: list = []
        for item in raw_tokens:
            c = item.get("tokenAddress", "")
            if c and c not in seen_this_run:
                seen_this_run.add(c)
                unique_items.append(item)

        # ── Pre-fetch all token data in parallel (hits DexScreener + RugCheck) ─
        await _asyncio.gather(*[get_token(c["tokenAddress"], force=True) for c in unique_items], return_exceptions=True)

        # ── Helius API key (optional) ─────────────────────────────────────────
        helius_key = os.environ.get("HELIUS_API_KEY", "")

        # ── Expire stale sniper_seen entries + prune old skips from log ─────
        now_ts = datetime.now().timestamp()
        expiry_secs = SNIPER_SEEN_EXPIRY_H * 3600
        for uid, ud in active_users:
            # Expire seen memory
            seen_map = ud.get("sniper_seen", {})
            stale = [k for k, ts in seen_map.items() if now_ts - ts > expiry_secs]
            for k in stale:
                del seen_map[k]
            # Auto-delete skipped tokens from log after 10 min
            _prune_sniper_log(ud)

        # ════════════════════════════════════════════════════════════════════
        # MAIN LOOP — for each token × each user (properly nested)
        # ════════════════════════════════════════════════════════════════════
        for item in unique_items:
            contract = item.get("tokenAddress", "")
            if not contract:
                continue

            try:
                info = await get_token(contract)
                if not info:
                    continue

                # ── Enrich info with pump.fun fields from scan metadata ────
                info["pf_curve"]     = item.get("_pf_curve")
                info["pf_replies"]   = item.get("_pf_replies", 0)
                info["pf_graduated"] = item.get("_pf_graduated", False)
                info["pf_dev_pct"]   = item.get("_pf_dev_pct")
                info["boost_amount"] = item.get("_boost_amount", 0)
                # Override socials from pump.fun if DexScreener has none
                if not info.get("twitter")  and item.get("_pf_twitter"):
                    info["twitter"]  = item["_pf_twitter"]
                if not info.get("telegram") and item.get("_pf_telegram"):
                    info["telegram"] = item["_pf_telegram"]
                if not info.get("website")  and item.get("_pf_website"):
                    info["website"]  = item["_pf_website"]

                token_chain = _sniper_chain_id(info.get("chain", ""))
                sc = sniper_score(info)

            except Exception as e:
                logger.warning(f"sniper_job token fetch failed {contract}: {e}")
                continue

            # ── Per-user evaluation ───────────────────────────────────────
            for uid, ud in active_users:
                try:
                    sf     = ud.get("sniper_filters", {})
                    chains = ud.get("sniper_chains", {})

                    # Chain filter
                    if token_chain and not chains.get(token_chain, False):
                        continue

                    # ── Dedup: sniper_seen is the source of truth ─────────
                    seen_map      = ud.setdefault("sniper_seen", {})
                    sniper_bought = ud.setdefault("sniper_bought", [])
                    if contract in seen_map or contract in sniper_bought:
                        continue
                    # Mark seen immediately so parallel users don't double-process
                    seen_map[contract] = now_ts

                    # ── Trim seen map if oversized ────────────────────────
                    if len(seen_map) > 2000:
                        oldest = sorted(seen_map.items(), key=lambda x: x[1])[:200]
                        for k, _ in oldest:
                            del seen_map[k]

                    # ── Pre-filters ───────────────────────────────────────
                    skip_reason = None
                    age_h       = info.get("age_h") or 0
                    buys_h1     = info.get("buys_h1", 0)
                    sells_h1    = info.get("sells_h1", 0)
                    buy_pct_h1  = info.get("buy_pct_h1", info.get("buy_pct", 50))
                    vol_h1      = info.get("vol_h1", 0)
                    mc          = info.get("mc", 1)
                    vol_mc_ratio = (vol_h1 / mc) if mc > 0 else 0

                    # Hard flags always skip
                    is_pumpfun = info.get("pf_curve") is not None
                    if sc.get("flags"):
                        skip_reason = "Hard flag: " + sc["flags"][0]
                    elif sc["score"] < int(sf.get("min_score", 45)):
                        skip_reason = f"Score too low ({sc['score']}/100)"
                    elif not is_pumpfun and info["liq"] < float(sf.get("min_liq", 15_000)):
                        # Skip liq check for pump.fun bonding curve tokens (liq shows $0 until graduation)
                        skip_reason = f"Liq too low (${info['liq']:,.0f})"
                    elif not (float(sf.get("min_mc", 30_000)) <= info["mc"] <= float(sf.get("max_mc", 5_000_000))):
                        skip_reason = f"MC out of range (${info['mc']:,.0f})"
                    elif age_h > float(sf.get("max_age_h", 6)):
                        skip_reason = f"Too old ({round(age_h,1)}h)"
                    elif buys_h1 < int(sf.get("min_buys_h1", 30)) and not info.get("pf_curve"):
                        # Skip low-activity tokens (pump.fun tokens exempt — bonding curve proves activity)
                        skip_reason = f"Low activity H1 ({buys_h1} buys < {sf.get('min_buys_h1', 30)} min)"
                    elif buy_pct_h1 < int(sf.get("min_buy_pct", 52)):
                        skip_reason = f"Sell pressure H1 ({buy_pct_h1}% buys)"
                    elif vol_mc_ratio > float(sf.get("max_vol_mc_ratio", 6.0)):
                        skip_reason = f"Wash trade signal (vol/MC={round(vol_mc_ratio,1)}x)"
                    elif not info.get("twitter") and not info.get("telegram"):
                        skip_reason = "No socials"
                    # ── New hard filters ──────────────────────────────────
                    else:
                        liq_pct_val  = info.get("liq_pct", 100)
                        top10_val    = info.get("top10_pct")
                        lp_burn_val  = info.get("lp_burn")
                        is_sol       = info.get("chain","").lower() in ("solana","sol")
                        min_liq_pct  = float(sf.get("min_liq_pct", 8))
                        max_top10    = float(sf.get("max_top10_pct", 55))
                        min_lp_burn  = float(sf.get("min_lp_burn", 80))
                        if not is_pumpfun and liq_pct_val < min_liq_pct and mc > 0:
                            # pump.fun bonding-curve tokens show liq=$0 until graduation — exempt them
                            skip_reason = f"Liq% too low ({round(liq_pct_val,1)}% of MC < {min_liq_pct}%)"
                        elif top10_val is not None and top10_val > max_top10:
                            skip_reason = f"Whale trap — top10 hold {top10_val}% (max {max_top10}%)"
                        elif is_sol and lp_burn_val is not None and lp_burn_val < min_lp_burn:
                            skip_reason = f"LP only {lp_burn_val}% burned (min {min_lp_burn}%)"

                    # Log skips briefly (just symbol + reason, no AI call)
                    if skip_reason:
                        log = ud.setdefault("sniper_log", [])
                        log.append({
                            "contract":  contract,
                            "symbol":    info["symbol"],
                            "chain":     info.get("chain", "?"),
                            "mc":        info["mc"],
                            "liq":       info.get("liq", 0),
                            "score":     sc["score"],
                            "verdict":   "SKIP",
                            "confidence":0,
                            "rug_risk":  "UNKNOWN",
                            "momentum":  "UNKNOWN",
                            "social":    "UNKNOWN",
                            "thesis":    skip_reason,
                            "red_flags": [skip_reason],
                            "green_flags":[],
                            "hard_flags": sc.get("flags", []),
                            "timestamp": datetime.now().isoformat(),
                            "bought":    False,
                            "skip_stage":"pre-filter",
                        })
                        # Trim log
                        if len(log) > SNIPER_LOG_MAX:
                            ud["sniper_log"] = log[-SNIPER_LOG_MAX:]
                        continue

                    # ── Daily budget check ────────────────────────────────
                    _sniper_daily_reset(ud)
                    budget  = ud.get("sniper_daily_budget", 500.0)
                    spent   = ud.get("sniper_daily_spent", 0.0)
                    max_buy = float(sf.get("buy_amount", 100))
                    if spent + max_buy > budget:
                        logger.info(f"Sniper daily budget hit for {uid}")
                        continue

                    # ── Helius enrichment (optional, Solana only) ─────────
                    if helius_key and info.get("chain","").lower() in ("solana","sol"):
                        try:
                            helius_data = await get_helius_maker_pct(contract, helius_key)
                            if helius_data:
                                info["maker_pct"]    = helius_data.get("maker_pct")
                                info["maker_count"]  = helius_data.get("maker_count")
                                info["top3_vol_pct"] = helius_data.get("top3_vol_pct")
                                # Re-score with maker data
                                sc = sniper_score(info)
                        except Exception as _he:
                            logger.debug(f"Helius enrichment skip: {_he}")

                    # ── AI Analysis ───────────────────────────────────────
                    ai = await ai_analyze_token(info, sc, ud, uid)

                    # Build full log entry
                    log_entry = {
                        "contract":    contract,
                        "symbol":      info["symbol"],
                        "chain":       info.get("chain", "?"),
                        "mc":          info["mc"],
                        "liq":         info.get("liq", 0),
                        "age_h":       round(age_h, 2),
                        "score":       sc["score"],
                        "verdict":     ai["verdict"],
                        "confidence":  ai["confidence"],
                        "rug_risk":    ai["rug_risk"],
                        "momentum":    ai["momentum"],
                        "social":      ai["social_score"],
                        "thesis":      ai.get("thesis", ""),
                        "red_flags":   ai.get("red_flags", []),
                        "green_flags": ai.get("green_flags", []),
                        "hard_flags":  sc.get("flags", []),
                        "sniper_strengths": sc.get("strengths", []),
                        "sniper_warnings":  sc.get("warnings", []),
                        "pf_curve":    info.get("pf_curve"),
                        "pf_graduated":info.get("pf_graduated", False),
                        "maker_pct":   info.get("maker_pct"),
                        "top3_vol_pct":info.get("top3_vol_pct"),
                        "timestamp":   datetime.now().isoformat(),
                        "bought":      False,
                    }
                    log = ud.setdefault("sniper_log", [])
                    log.append(log_entry)
                    if len(log) > SNIPER_LOG_MAX:
                        ud["sniper_log"] = log[-SNIPER_LOG_MAX:]

                    # ════════════════════════════════════════════════════
                    # MODE 1 — FULL AUTO
                    # ════════════════════════════════════════════════════
                    if ud.get("sniper_auto") and ai["verdict"] == "SNIPE":
                        buy_amt = min(ai["suggested_amount"], ud["balance"], budget - spent)
                        if buy_amt < 1:
                            continue
                        sniper_bought.append(contract)
                        if len(sniper_bought) > 500:
                            ud["sniper_bought"] = sniper_bought[-500:]
                        ud["sniper_daily_spent"] = spent + buy_amt
                        ud["sniper_log"][-1]["bought"] = True
                        ud["sniper_log"][-1]["amount"] = buy_amt

                        result = await do_buy_core(ud, uid, contract, buy_amt, planned=True, mood="AI-Sniper")
                        if isinstance(result, tuple):
                            info2, _ = result
                            h = ud["holdings"].get(contract, {})
                            if ud.get("sniper_auto_sl") and h:
                                sl_pct = ud.get("sniper_auto_sl_pct", 40.0)
                                if ai["rug_risk"] == "HIGH":
                                    sl_pct = min(sl_pct, 20.0)
                                h["stop_loss_pct"] = sl_pct
                            if ud.get("sniper_auto_tp") and h:
                                tp_xs    = ud.get("sniper_auto_tp_x", [2.0, 5.0])
                                pct_each = round(1.0 / len(tp_xs), 2)
                                h["auto_sells"] = [{"pct": pct_each, "x": x, "triggered": False} for x in tp_xs]
                                if h["auto_sells"]:
                                    h["auto_sells"][-1]["pct"] = round(1.0 - pct_each * (len(tp_xs) - 1), 2)

                            if ud.get("sniper_auto_notify", True):
                                sl_line = f"\n🛑 Stop Loss: {ud.get('sniper_auto_sl_pct',40)}%" if ud.get("sniper_auto_sl") else ""
                                tp_line = ""
                                if ud.get("sniper_auto_tp"):
                                    tp_xs2   = ud.get("sniper_auto_tp_x", [2.0, 5.0])
                                    pct_e2   = round(1.0 / len(tp_xs2), 2)
                                    tp_parts = [f"{int(pct_e2*100)}% at {x}x" for x in tp_xs2]
                                    tp_line  = "\n🎯 TP: " + "  |  ".join(tp_parts)
                                try:
                                    await ctx.bot.send_message(
                                        chat_id=uid, parse_mode="Markdown",
                                        text=(
                                            "🤖 *AI AUTO-SNIPE EXECUTED*\n\n"
                                            "*$" + info2["symbol"] + "*  " + info2.get("chain","").upper() + "\n"
                                            "Confidence: *" + str(ai["confidence"]) + "/10*\n"
                                            "Rug Risk: *" + ai["rug_risk"] + "*\n\n"
                                            "📝 " + ai["thesis"] + "\n\n"
                                            "💵 Bought: *" + money(buy_amt) + "*\n"
                                            "Price: *" + money(info2["price"]) + "*\n"
                                            "MC: *" + mc_str(info2["mc"]) + "*\n"
                                            "Cash left: *" + money(ud["balance"]) + "*"
                                            + sl_line + tp_line
                                        ),
                                        reply_markup=_sniper_auto_kb(contract)
                                    )
                                except Exception as _ne:
                                    logger.error(f"Auto snipe notify error: {_ne}")

                    # ════════════════════════════════════════════════════
                    # MODE 2 — ADVISORY
                    # Exclusive toggle:
                    #   sniper_adv_notify = True  → DM pill only, channel silent
                    #   sniper_adv_notify = False → Channel only, no DM
                    # ════════════════════════════════════════════════════
                    if ud.get("sniper_advisory"):
                        _sniper_analysis_cache.setdefault(uid, {})[contract] = {
                            "info": info, "sc": sc, "ai": ai
                        }
                        cache = _sniper_analysis_cache[uid]
                        if len(cache) > 20:
                            for k in list(cache.keys())[:-20]:
                                del cache[k]

                        dm_notify = ud.get("sniper_adv_notify", True)
                        ch_id     = ud.get("sniper_broadcast_channel")

                        if dm_notify:
                            # ── DM MODE: send pill to user, channel is silent ──
                            if ai["verdict"] == "SNIPE" or ai["confidence"] >= 5 or ai["confidence"] == 0:
                                pill = _compact_pill_text(info, sc, ai)
                                view_row = [
                                    InlineKeyboardButton("👁 View Analysis", callback_data="snp_view_" + contract),
                                    InlineKeyboardButton("❌ Dismiss",        callback_data="snp_skip_" + contract),
                                ]
                                try:
                                    await ctx.bot.send_message(
                                        chat_id=uid, parse_mode="Markdown",
                                        text=pill,
                                        reply_markup=InlineKeyboardMarkup([view_row])
                                    )
                                except Exception as _ae:
                                    logger.error(f"Advisory pill error {uid}: {_ae}")
                        else:
                            # ── CHANNEL MODE: broadcast to channel, no DM ──────
                            if ch_id:
                                await _broadcast_to_channel(ctx.bot, int(ch_id), info, sc, ai, contract, uid=uid)
                            # No DM notification at all when channel mode is active

                except Exception as _ue:
                    logger.warning(f"Sniper job user error {uid}: {_ue}")

    except Exception as e:
        logger.error(f"sniper_job crashed: {e}", exc_info=True)


async def bundle_sell_detector(app):
    """
    Runs inside run_checker. Watches AI-sniped positions for dump patterns
    and exits immediately if detected.
    """
    for uid, ud in list(users.items()):
        if not ud.get("sniper_auto"):
            continue
        for contract, h in list(ud.get("holdings", {}).items()):
            if h.get("mood") not in ("AI-Sniper", "Sniper"):
                continue
            try:
                info = await get_token(contract)
                if not info:
                    continue

                price  = info["price"]
                avg    = h.get("avg_price", price)
                drop   = (price - avg) / avg * 100 if avg > 0 else 0
                vol_m5 = info.get("vol_m5", 0)
                vol_h1 = info.get("vol_h1", 0)
                buy_pct = info.get("buy_pct", 50)
                liq     = info.get("liq", 0)

                # Bundle sell signals:
                # 1. Price dropped >25% from entry AND sell pressure >70%
                # 2. 5m volume spike 5x hourly avg AND buy_pct < 35% (dump in progress)
                # 3. Liquidity dropped >40% — LP being pulled
                avg_5m = vol_h1 / 12 if vol_h1 > 0 else 0
                liq_at_buy = h.get("liq_at_buy", liq)

                bundle_detected = False
                reason = ""

                if drop <= -25 and buy_pct < 30:
                    bundle_detected = True
                    reason = f"Price dropped {round(drop,1)}% + heavy sell pressure ({100-buy_pct}% sells)"
                elif avg_5m > 0 and vol_m5 >= avg_5m * 5 and buy_pct < 35:
                    bundle_detected = True
                    reason = f"Massive volume spike ({round(vol_m5/avg_5m,1)}x avg) with {100-buy_pct}% sells"
                elif liq_at_buy > 0 and liq < liq_at_buy * 0.6:
                    bundle_detected = True
                    reason = f"Liquidity pulled: {money(liq_at_buy)} → {money(liq)}"

                if bundle_detected and contract in ud["holdings"]:
                    async with get_user_lock(uid):
                        if contract not in ud["holdings"]:
                            continue   # already sold
                        cv = h["amount"] * price
                        result = sell_core(ud, uid, contract, cv, price, "bundle_sell_exit")
                    if ud.get("sniper_auto_notify", True):
                        try:
                            await app.bot.send_message(
                                chat_id=uid, parse_mode="Markdown",
                                text=(
                                    "🚨 *BUNDLE SELL DETECTED — EMERGENCY EXIT*\n\n"
                                    "*$" + h["symbol"] + "* — AI sniper position closed!\n\n"
                                    "⚠️ *Signal:* " + reason + "\n\n"
                                    "Sold: *" + money(cv) + "*\n"
                                    "PnL: *" + pstr(result["realized"]) + "*\n"
                                    "Cash: *" + money(ud["balance"]) + "*"
                                ),
                                reply_markup=main_menu_kb()
                            )
                        except Exception as _be:
                            logger.error(f"Bundle sell notify error: {_be}")
            except Exception:
                continue


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    ud = get_user(u.id, u.username or u.first_name)

    # Handle deep link: /start CA — from channel "Buy on Auracle" button
    payload = (ctx.args[0] if ctx.args else "").strip()
    if payload and is_valid_contract(payload) and ud.get("balance") is not None:
        # Looks like a CA — auto-load the token card
        info_dl = await get_token(payload)
        if info_dl:
            from telegram import Message as _Msg
            card_text = token_card(info_dl, payload, ud)
            if update.message:
                await update.message.reply_text(card_text, parse_mode="Markdown", reply_markup=buy_kb(payload, ud))
            return

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
        h["avg_price"]     = nt / na
        h["amount"]        = na
        h["total_invested"] = nt
        # Keep avg_cost_mc as weighted average MC at buy time
        cur_mc  = info.get("mc", 0)
        old_mc  = h.get("avg_cost_mc", cur_mc)
        old_inv = nt - usd_amount
        h["avg_cost_mc"] = ((old_mc * old_inv) + (cur_mc * usd_amount)) / nt if nt > 0 else cur_mc
    else:
        ud["holdings"][contract] = {
            "symbol":         info["symbol"],
            "name":           info["name"],
            "chain":          info["chain"],
            "amount":         tokens,
            "avg_price":      info["price"],
            "total_invested": usd_amount,
            "total_sold":     0.0,
            "avg_cost_mc":    info.get("mc", 0),
            "auto_sells":     [],
            "stop_loss_pct":  None,
            "bought_at":      datetime.now(),
            "liq_at_buy":     info.get("liq", 0),
            "journal":        "",
            "mood":           mood,
            "planned":        planned,
            "followed_plan":  None,
            "peak_price":     info["price"],   # tracks highest price seen for replay
        }
    if planned:
        ud["planned"] += 1
    else:
        ud["impulse"] += 1

    save_user(uid, ud)   # persist balance + new holding

    # Notify copy traders
    for follower_id, follower in list(users.items()):
        if follower.get("copy_trading") == uid and not follower.get("copy_paused"):
            if follower.get("balance", 0) >= usd_amount * 0.5:
                copy_amt = min(usd_amount, follower["balance"] * 0.1)
                copy_tokens = copy_amt / info["price"]
                follower["balance"] -= copy_amt
                if contract in follower["holdings"]:
                    fh = follower["holdings"][contract]
                    nt = fh["total_invested"] + copy_amt
                    na = fh["amount"] + copy_tokens
                    fh["avg_price"] = nt / na
                    fh["amount"] = na
                    fh["total_invested"] = nt
                else:
                    follower["holdings"][contract] = {
                        "symbol": info["symbol"], "name": info["name"],
                        "chain": info["chain"], "amount": copy_tokens,
                        "avg_price": info["price"], "total_invested": copy_amt,
                        "auto_sells": [], "stop_loss_pct": None,
                        "bought_at": datetime.now(), "journal": "Copy trade from " + ud["username"],
                        "mood": "Copy Trade", "planned": True, "followed_plan": None,
                    }
                try:
                    from telegram.ext import Application as _App
                    _bot = _App.get_current().bot
                    await _bot.send_message(
                        chat_id=follower_id,
                        parse_mode="Markdown",
                        text=(
                            "🔁 *COPY TRADE EXECUTED*\n\n"
                            "Copied @" + ud["username"] + "'s buy\n"
                            "*$" + info["symbol"] + "*  " + mc_str(info["mc"]) + "\n"
                            "Invested: *" + money(copy_amt) + "*\n"
                            "Price: *" + money(info["price"]) + "*\n"
                            "Cash left: *" + money(follower["balance"]) + "*"
                        ),
                        reply_markup=main_menu_kb()
                    )
                except Exception as _ce:
                    logger.warning(f"Copy trade notify failed for {follower_id}: {_ce}")

    # Overtrading alert
    today = datetime.now().date()
    counts = ud.get("daily_trade_counts", [])
    counts = [c for c in counts if c["date"] >= (today - timedelta(days=30))]
    today_entry = next((c for c in counts if c["date"] == today), None)
    if today_entry:
        today_entry["count"] = ud["daily_trades"]
    else:
        counts.append({"date": today, "count": ud["daily_trades"]})
    ud["daily_trade_counts"] = counts
    if len(counts) >= 3:
        avg = sum(c["count"] for c in counts[:-1]) / len(counts[:-1])
        ud["avg_daily_trades"] = round(avg, 1)

    return info, tokens


async def do_buy_msg(update, ud, uid, contract, amount, mood=""):
    # Risk calculator intercept
    if ud.get("risk_calc", True):
        info_pre = await get_token(contract)
        if info_pre:
            pending[uid] = {"action": "risk_confirm", "contract": contract, "amount": amount, "mood": mood}
            await update.message.reply_text(
                risk_card_text(ud, info_pre["symbol"], info_pre["mc"], amount),
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(t(ud, "confirm_buy"), callback_data="rc_yes"),
                     InlineKeyboardButton(t(ud, "cancel"),      callback_data="rc_no")],
                ])
            )
            return
    msg = await update.message.reply_text("Executing buy...")
    async with get_user_lock(uid):
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
    # Risk calculator intercept
    if ud.get("risk_calc", True):
        info_pre = await get_token(contract)
        if info_pre:
            pending[uid] = {"action": "risk_confirm", "contract": contract, "amount": amount, "mood": mood}
            await q.edit_message_text(
                risk_card_text(ud, info_pre["symbol"], info_pre["mc"], amount),
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(t(ud, "confirm_buy"), callback_data="rc_yes"),
                     InlineKeyboardButton(t(ud, "cancel"),      callback_data="rc_no")],
                ])
            )
            return
    await q.edit_message_text("Executing buy...")
    async with get_user_lock(uid):
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
    async with get_user_lock(uid):
        result = sell_core(ud, uid, contract, usd_amount, info["price"])
    _persist_after_sell(uid, ud)
    cx = info["price"] / avg if avg > 0 else 0
    warn = "\n\nSold before auto-sell targets - rule broken" if pending_targets else ""
    save_line = "\nAuto-saved: " + money(result["auto_saved"]) if result["auto_saved"] > 0 else ""
    share_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Share This Trade", callback_data="share_" + contract)],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="mm")],
    ])
    await q.edit_message_text(
        "✅ *SELL EXECUTED*\n\n"
        "Received: *" + money(result["received"]) + "*\n"
        "Price: *" + money(info["price"]) + "*  |  *" + str(round(cx, 2)) + "x*\n"
        "Held: *" + str(result["hold_h"]) + "h*\n"
        "PnL: *" + pstr(result["realized"]) + "*\n"
        "Cash: *" + money(ud["balance"]) + "*" + save_line + warn,
        parse_mode="Markdown",
        reply_markup=share_kb
    )
    # ── Auto PnL card on full close ──────────────────────────────────────────
    if result["closed"]:
        try:
            logs = trade_log.get(uid, [])
            tr = next((t for t in reversed(logs) if t["contract"] == contract), None)
            if tr:
                invested = tr.get("invested", 0)
                pnl_pct_val = round((tr["realized_pnl"] / invested * 100), 2) if invested > 0 else 0
                card = generate_trade_card(
                    symbol=tr["symbol"], chain=tr.get("chain", "SOL"),
                    pnl_str=money(abs(tr["realized_pnl"])),
                    x_val=str(round(tr.get("x", 0), 2)),
                    held_h=str(tr["hold_h"]) + "h",
                    bought_str=money(invested),
                    position_str=money(tr.get("returned", 0)),
                    username=ud.get("username", "trader"),
                    pnl_pct=str(abs(pnl_pct_val)) + "%",
                    pnl_positive=tr["realized_pnl"] > 0,
                    closed_at=tr.get("closed_at"),
                )
                if card:
                    caption = (("✅ " if tr["realized_pnl"] > 0 else "❌ ") +
                        "$" + tr["symbol"] + "  " + str(round(tr.get("x",0),2)) + "x  " +
                        ("+" if tr["realized_pnl"] > 0 else "") + money(tr["realized_pnl"]))
                    await q.message.reply_photo(photo=card, caption=caption, reply_markup=share_kb)
        except Exception as _ce:
            logger.warning(f"PnL card: {_ce}")


async def do_sell_msg(update, ud, uid, contract, pct=None, usd=None):
    if contract not in ud["holdings"]:
        await update.message.reply_text("Position not found.", reply_markup=back_main())
        return
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
    async with get_user_lock(uid):
        result = sell_core(ud, uid, contract, usd_amount, info["price"])
    _persist_after_sell(uid, ud)
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
    # ── Auto PnL card on full close ──────────────────────────────────────────
    if result["closed"]:
        try:
            logs = trade_log.get(uid, [])
            tr = next((t for t in reversed(logs) if t["contract"] == contract), None)
            if tr:
                invested = tr.get("invested", 0)
                pnl_pct_val = round((tr["realized_pnl"] / invested * 100), 2) if invested > 0 else 0
                card = generate_trade_card(
                    symbol=tr["symbol"], chain=tr.get("chain", "SOL"),
                    pnl_str=money(abs(tr["realized_pnl"])),
                    x_val=str(round(tr.get("x", 0), 2)),
                    held_h=str(tr["hold_h"]) + "h",
                    bought_str=money(invested),
                    position_str=money(tr.get("returned", 0)),
                    username=ud.get("username", "trader"),
                    pnl_pct=str(abs(pnl_pct_val)) + "%",
                    pnl_positive=tr["realized_pnl"] > 0,
                    closed_at=tr.get("closed_at"),
                )
                if card:
                    caption = (("✅ " if tr["realized_pnl"] > 0 else "❌ ") +
                        "$" + tr["symbol"] + "  " + str(round(tr.get("x",0),2)) + "x  " +
                        ("+" if tr["realized_pnl"] > 0 else "") + money(tr["realized_pnl"]))
                    await update.message.reply_photo(photo=card, caption=caption, reply_markup=main_menu_kb())
        except Exception as _ce:
            logger.warning(f"PnL card: {_ce}")


async def text_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if is_rate_limited(u.id):
        return  # silently drop — no reply to spammers
    ud = get_user(u.id, u.username or u.first_name)
    message = update.message          # shorthand used throughout handler
    text = message.text.strip()
    p = pending.get(u.id)

    async def _clean(keep_prompt_id: int | None = None):
        """Delete user's input + bot's prompt to keep chat clean."""
        try:
            await message.delete()
        except Exception:
            pass
        try:
            # Check keep_prompt_id first, then pending dict, then ud fallback
            _p = pending.get(u.id, {})
            prompt_id = keep_prompt_id or _p.get("_prompt_msg_id") or ud.get("_last_prompt_msg_id")
            if prompt_id:
                await ctx.bot.delete_message(chat_id=u.id, message_id=prompt_id)
                ud.pop("_last_prompt_msg_id", None)
        except Exception:
            pass

    if p:
        action = p.get("action", "")
        if not action:
            # Pending exists but no action — clear it and treat as CA
            pending.pop(u.id, None)
            p = None
        elif len(text) > 30 and action not in ("set_balance", "comp_bet", "comp_join", "acc_new", "sniper_channel_input", "qb_custom_input"):
            # Looks like a CA pasted while in a non-CA pending state — clear and scan
            pending.pop(u.id, None)
            p = None
            # Fall through to CA scanner below (skip the pending action block)

    if p and p.get("action", ""):
        action = p.get("action", "")
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
                assert 0 < amt <= MAX_BALANCE
                ud["preset_buy"] = amt
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Default buy: *" + money(amt) + "*", parse_mode="Markdown", reply_markup=settings_kb(ud))
                else:
                    await message.reply_text("✅ Default buy: *" + money(amt) + "*", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await message.reply_text("❌ Enter a number like 100", reply_markup=cancel_kb())
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
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Default sell: *" + text + "*", parse_mode="Markdown", reply_markup=settings_kb(ud))
                else:
                    await message.reply_text("✅ Default sell: *" + text + "*", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await message.reply_text("❌ Enter 50% or 200", reply_markup=cancel_kb())
            return

        elif action == "cfg_risk":
            try:
                pct = float(text.replace("%", ""))
                assert 0 < pct <= 100
                ud["risk_pct"] = pct
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Max risk: *" + str(pct) + "%* per trade", parse_mode="Markdown", reply_markup=settings_kb(ud))
                else:
                    await message.reply_text("✅ Max risk: *" + str(pct) + "%* per trade", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await message.reply_text("❌ Enter a number like 10", reply_markup=cancel_kb())
            return

        elif action == "cfg_maxpos":
            try:
                n = int(text)
                assert n > 0
                ud["max_positions"] = n
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Max positions: *" + str(n) + "*", parse_mode="Markdown", reply_markup=settings_kb(ud))
                else:
                    await message.reply_text("✅ Max positions: *" + str(n) + "*", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await message.reply_text("❌ Enter a number like 5", reply_markup=cancel_kb())
            return

        elif action == "cfg_daily":
            try:
                n = int(text)
                assert n > 0
                ud["daily_limit"] = n
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Daily limit: *" + str(n) + "* trades", parse_mode="Markdown", reply_markup=settings_kb(ud))
                else:
                    await message.reply_text("✅ Daily limit: *" + str(n) + "* trades", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await message.reply_text("❌ Enter a number like 10", reply_markup=cancel_kb())
            return

        elif action == "cfg_autosave":
            try:
                pct = float(text.replace("%", ""))
                assert 0 < pct <= 100
                ud["auto_save_pct"] = pct
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Auto-save: *" + str(pct) + "%* of profits", parse_mode="Markdown", reply_markup=settings_kb(ud))
                else:
                    await message.reply_text("✅ Auto-save: *" + str(pct) + "%* of profits", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await message.reply_text("❌ Enter a percentage like 20", reply_markup=cancel_kb())
            return

        elif action == "cfg_target":
            try:
                amt = float(text.replace("$", "").replace(",", ""))
                assert amt > 0
                ud["target_equity"] = amt
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Target equity: *" + money(amt) + "*", parse_mode="Markdown", reply_markup=settings_kb(ud))
                else:
                    await message.reply_text("✅ Target equity: *" + money(amt) + "*", parse_mode="Markdown", reply_markup=settings_kb(ud))
            except Exception:
                await message.reply_text("❌ Enter a number like 10000", reply_markup=cancel_kb())
            return

        elif action == "buy_custom":
            contract = p["contract"]
            try:
                amt = float(text.replace("$", "").replace(",", ""))
                assert amt > 0
                if ud.get("mood_tracking", True):
                    pending[u.id] = {"action": "buy_mood", "contract": contract, "amount": amt}
                    await update.message.reply_text(
                        "🧠 *MOOD CHECK*\n\nWhy are you buying this?\n\n"
                        "1 - Research\n2 - Chart looks good\n3 - Community tip\n4 - FOMO\n5 - Gut feeling\n\nReply with a number:",
                        parse_mode="Markdown", reply_markup=cancel_kb()
                    )
                else:
                    pending.pop(u.id, None)
                    await do_buy_msg(update, ud, u.id, contract, amt)
            except Exception:
                await update.message.reply_text("❌ Enter a number like 200", reply_markup=cancel_kb())
            return

        elif action == "buy_mood":
            contract = p["contract"]
            amount = p["amount"]
            mood_map = {
                "1": "Research",
                "2": "Chart looks good",
                "3": "Community tip",
                "4": "FOMO",
                "5": "Gut feeling",
            }
            mood = mood_map.get(text.strip(), text.strip())
            # Sanitise mood for callback_data: strip underscores (used as delimiter), cap at 20 chars
            mood_safe = mood.replace("_", "-")[:20]
            pending.pop(u.id, None)
            # Overtrading check
            avg = ud.get("avg_daily_trades", 0)
            today_count = ud.get("daily_trades", 0)
            if avg > 0 and today_count >= avg * 1.5:
                await update.message.reply_text(
                    "⚠️ *OVERTRADING ALERT*\n\n"
                    "You have made " + str(today_count) + " trades today.\n"
                    "Your daily average is " + str(ud['avg_daily_trades']) + " trades.\n\n"
                    "Are you sure you want to continue?",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("Yes, Continue", callback_data="ot_yes_" + contract + "_" + str(amount) + "_" + mood_safe)],
                        [InlineKeyboardButton("No, Stop", callback_data="mm")],
                    ])
                )
                return
            await do_buy_msg(update, ud, u.id, contract, amount, mood=mood)
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
                if len(ud["limit_orders"]) >= MAX_LIMIT_ORDERS:
                    await update.message.reply_text(f"❌ Max {MAX_LIMIT_ORDERS} limit orders reached. Cancel one first.", reply_markup=back_main())
                    return
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
                symbol = p.get("symbol", "?")
                if len(ud["price_alerts"]) >= MAX_PRICE_ALERTS:
                    await message.reply_text(f"❌ Max {MAX_PRICE_ALERTS} price alerts reached. Clear some first.", reply_markup=back_main())
                    return
                ud["price_alerts"].append({
                    "contract": contract, "symbol": symbol,
                    "target": target, "direction": direction, "triggered": False,
                })
                pending.pop(u.id, None)
                # Delete user input + old prompt to keep chat clean
                try:
                    await message.delete()
                except Exception:
                    pass
                # Always send a fresh confirmation — don't try to edit deleted prompt
                arrow = "⬆️" if direction == "above" else "⬇️"
                msg = (
                    "🔔 *Price Alert Set!*\n\n"
                    "$" + symbol + "\n"
                    + arrow + " Notify when price goes *" + direction + "* " + money(target)
                )
                await ctx.bot.send_message(
                    chat_id=u.id,
                    text=msg,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("◀ Back to Token", callback_data="btt_" + contract)
                    ]])
                )
            except (ValueError, KeyError):
                await message.reply_text("❌ Enter a valid price. Example: 0.002648", reply_markup=cancel_kb())
            return

        elif action == "ch_custom_target":
            try:
                target = float(text)
                pending[u.id] = {"action": "ch_custom_days", "target": target}
                await message.reply_text(f"✅ Target: {money(target)}\n\nNow enter number of days for the challenge:\nExample: 30")
            except (ValueError, KeyError):
                await message.reply_text("❌ Enter a valid number. Example: 10000")

        elif action == "ch_custom_days":
            try:
                days = int(text)
                target = pending[u.id].get("target", 10000)
                start_eq = ud["balance"] + sum(h["total_invested"] for h in ud["holdings"].values())
                ud["challenge"] = {"start_eq": start_eq, "target_eq": target, "days": days, "started": datetime.now().isoformat()}
                del pending[u.id]
                await message.reply_text(f"🎯 *Challenge Started!*\n\n{money(start_eq)} → {money(target)} in {days} days\n\nGood luck!", parse_mode="Markdown", reply_markup=main_menu_kb())
            except (ValueError, KeyError):
                await message.reply_text("❌ Enter a valid number of days. Example: 30")

        elif action == "acc_new":
            name = text.lower().strip().replace(" ", "_")
            if not name:
                await message.reply_text("❌ Please enter a valid name.")
                return
            if not ud.get("accounts"):
                ud["accounts"] = {}
            if name in ud["accounts"]:
                await message.reply_text(f"❌ Account *{name}* already exists!", parse_mode="Markdown", reply_markup=cancel_kb())
                return
            ud["accounts"][name] = {"balance": 5000.0, "holdings": {}, "savings": 0.0}
            del pending[u.id]
            await message.reply_text(
                f"✅ Account *{name}* created!\n\n💰 Balance: $5,000\n\nSwitch to it from Accounts menu.",
                parse_mode="Markdown", reply_markup=main_menu_kb()
            )



        elif action == "qb_custom_input":
            contract = p.get("contract", "")
            try:
                amt = float(text.replace("$","").replace(",","").strip())
                assert 0 < amt <= MAX_BALANCE
                ud["quick_buy_amount"] = amt
                del pending[u.id]
                try:
                    await message.delete()
                except Exception:
                    pass
                await ctx.bot.send_message(
                    chat_id=u.id,
                    text="✅ Quick Buy set to *$" + str(int(amt)) + "*\n\nTap ⚡ Quick Buy on any token card.",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("◀ Back to Token", callback_data="btt_" + contract)
                    ]]) if contract else back_main()
                )
            except (ValueError, AssertionError):
                await message.reply_text("❌ Enter a valid amount. Example: 75", reply_markup=cancel_kb())
            return

        elif action == "sniper_channel_input":
            # User pasted a channel/group ID
            raw = text.strip().replace(" ", "")
            try:
                ch_id = int(raw)
            except ValueError:
                await message.reply_text(
                    "❌ Invalid ID. It should be a number like `-1001234567890`.\n\nTry again or tap Cancel.",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Cancel", callback_data="sniper_adv_menu")]])
                )
                return
            # Test that bot can actually post to the channel
            await message.reply_text("⏳ Testing connection to channel...")
            try:
                test_msg = await ctx.bot.send_message(
                    chat_id=ch_id,
                    text="✅ *AURACLE_XBOT connected!*\n\nAI Sniper signals will be posted here.",
                    parse_mode="Markdown"
                )
                # Try to get chat name
                try:
                    chat_info = await ctx.bot.get_chat(ch_id)
                    ch_name = chat_info.title or str(ch_id)
                except Exception:
                    ch_name = str(ch_id)
                # Verify the requesting user is an admin in this channel
                try:
                    member = await ctx.bot.get_chat_member(ch_id, u.id)
                    if member.status not in ("administrator", "creator"):
                        await message.reply_text(
                            "❌ You must be an admin of that channel to link it here.",
                            parse_mode="Markdown",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Cancel", callback_data="sniper_adv_menu")]])
                        )
                        return
                except Exception as _admin_err:
                    logger.warning(f"Could not verify channel admin for {ch_id}: {_admin_err}")
                ud["sniper_broadcast_channel"] = ch_id
                ud["sniper_broadcast_name"] = ch_name
                del pending[u.id]
                adv_on = ud.get("sniper_advisory", False)
                notify = ud.get("sniper_adv_notify", True)
                await message.reply_text(
                    "✅ *Channel connected!*\n\n"
                    "📡 *" + ch_name + "* will now receive full AI signal cards.\n"
                    "Your DM will get a compact notification only.",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(("🔴 Disable Advisory" if adv_on else "🟢 Enable Advisory"), callback_data="sniper_adv_toggle")],
                        [InlineKeyboardButton("📡 Change Channel", callback_data="sniper_channel_setup")],
                        [InlineKeyboardButton("🗑 Remove Channel", callback_data="sniper_channel_remove")],
                        [InlineKeyboardButton("◀ Back", callback_data="v_sniper")],
                    ])
                )
            except Exception as e:
                err = str(e)
                await message.reply_text(
                    "❌ *Could not post to that channel.*\n\n"
                    "Make sure *Auracle-xbot* is an admin in the channel/group, then try again.\n\n"
                    "_Error: " + err[:100] + "_",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Cancel", callback_data="sniper_adv_menu")]])
                )
            return

        elif action == "wl_waiting":
            # User typed something while waiting for watchlist choice — ignore, remind them
            contract = pending[u.id].get("contract", "")
            await message.reply_text(
                "👇 Please tap a button above to set your alert, or tap Cancel.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Alert by Price",        callback_data="wl_add_price")],
                    [InlineKeyboardButton("Alert by Market Cap",   callback_data="wl_add_mc")],
                    [InlineKeyboardButton("No Alert — Just Watch", callback_data="mm")],
                    [InlineKeyboardButton("◀ Back to Token",       callback_data="btt_" + contract)],
                ])
            )
            return

        elif action == "wl_target_price":
            try:
                target = float(text)
                contract = pending[u.id].get("contract","")
                if contract and ud.get("watchlist", {}).get(contract):
                    ud["watchlist"][contract]["target_price"] = target
                del pending[u.id]
                await message.reply_text(f"✅ Price alert set at ${target:.8g}", reply_markup=main_menu_kb())
            except (ValueError, KeyError):
                await message.reply_text("❌ Enter a valid price. Example: 0.00005")

        elif action == "wl_target_mc":
            try:
                target = float(text)
                contract = pending[u.id].get("contract","")
                if contract and ud.get("watchlist", {}).get(contract):
                    ud["watchlist"][contract]["target_mc"] = target
                del pending[u.id]
                await message.reply_text(f"✅ MC alert set at {mc_str(target)}", reply_markup=main_menu_kb())
            except (ValueError, KeyError):
                await message.reply_text("❌ Enter a valid market cap. Example: 100000")

        elif action == "comp_bet":
            # Step 1: user entered bet amount
            try:
                bet = float(text.replace("$","").replace(",",""))
                if bet < 0:
                    await message.reply_text("❌ Enter 0 for free or a positive amount.", reply_markup=cancel_kb())
                    return
                if bet > 0 and bet > ud["balance"]:
                    await message.reply_text(
                        f"❌ Not enough balance.\nYou have {money(ud['balance'])}\n\nEnter a lower amount or 0 for free:",
                        reply_markup=cancel_kb()
                    )
                    return
                # Move to step 2: ask days — show buttons for common durations
                pending[u.id] = {"action": "comp_days", "bet": bet}
                bet_label = "🆓 Free" if bet == 0 else money(bet) + " per player"
                await message.reply_text(
                    f"✅ Bet set: *{bet_label}*\n\n"
                    f"⏳ *Step 2/2 — How many days?*\n\n"
                    f"Tap a duration or type a number (1–90):",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("3 days",  callback_data=f"comp_days_3_{bet}"),
                         InlineKeyboardButton("7 days",  callback_data=f"comp_days_7_{bet}"),
                         InlineKeyboardButton("14 days", callback_data=f"comp_days_14_{bet}")],
                        [InlineKeyboardButton("30 days", callback_data=f"comp_days_30_{bet}"),
                         InlineKeyboardButton("60 days", callback_data=f"comp_days_60_{bet}"),
                         InlineKeyboardButton("90 days", callback_data=f"comp_days_90_{bet}")],
                        [InlineKeyboardButton("❌ Cancel", callback_data="mm")],
                    ])
                )
            except ValueError:
                await message.reply_text("❌ Enter a number. Example: 500 or 0 for free.", reply_markup=cancel_kb())

        elif action == "comp_days":
            # Step 2: user entered days → create competition
            try:
                days = int(text.strip())
                if days < 1 or days > 90:
                    await message.reply_text("❌ Enter between 1 and 90 days.", reply_markup=cancel_kb())
                    return
                bet = pending[u.id].get("bet", 0)
                import random, string
                code = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
                start_eq = ud["balance"] + sum(h["total_invested"] for h in ud["holdings"].values())
                comp = {
                    "code":      code,
                    "creator_id": str(u.id),
                    "bet":       bet,
                    "pot":       bet,
                    "days":      days,
                    "end_time":  (datetime.now() + timedelta(days=days)).isoformat(),
                    "members":   {
                        str(u.id): {
                            "username":    ud["username"],
                            "start_eq":    start_eq,
                            "start_bal":   ud["balance"],
                        }
                    }
                }
                if bet > 0:
                    ud["balance"] -= bet
                _competitions[code] = comp
                if not ud.get("competitions"):
                    ud["competitions"] = {}
                ud["competitions"][code] = True
                del pending[u.id]
                end_str = (datetime.now() + timedelta(days=days)).strftime("%b %d, %Y")
                pot_line = f"💰 Bet: {money(bet)} per player | Pot: {money(bet)}" if bet > 0 else "🆓 Free to join"
                await message.reply_text(
                    f"🏁 *COMPETITION CREATED!*\n\n"
                    f"📋 Code: `{code}`\n"
                    f"⏳ Duration: {days} days\n"
                    f"🏁 Ends: {end_str}\n"
                    f"{pot_line}\n\n"
                    f"Share code *{code}* with friends!\n"
                    f"Winner takes the entire pot 🏆",
                    parse_mode="Markdown", reply_markup=main_menu_kb()
                )
            except (ValueError, TypeError):
                # If they pasted a CA or something else, clear pending and scan it
                if len(text) > 20:
                    pending.pop(u.id, None)
                    await message.reply_text("⚠️ Competition cancelled. Scanning token...")
                    # Fall through to CA scanner below
                    p = None
                else:
                    await message.reply_text("❌ Enter a whole number of days. Example: 7", reply_markup=cancel_kb())
                    return

        elif action == "comp_join":
            # User entered competition code
            code = text.strip().upper()
            _comps = _competitions
            if code not in _comps:
                await message.reply_text(
                    "❌ Competition not found.\nCheck the code and try again.",
                    reply_markup=cancel_kb()
                )
                return
            comp = _comps[code]
            # Check already joined
            if str(u.id) in comp.get("members", {}):
                await message.reply_text("❌ You already joined this competition!", reply_markup=main_menu_kb())
                del pending[u.id]
                return
            # Check ended
            end_dt = datetime.fromisoformat(comp["end_time"])
            if datetime.now() > end_dt:
                await message.reply_text("❌ This competition has already ended.", reply_markup=main_menu_kb())
                del pending[u.id]
                return
            bet = comp.get("bet", 0)
            if bet > 0 and bet > ud["balance"]:
                await message.reply_text(
                    f"❌ Need {money(bet)} to join. Your balance: {money(ud['balance'])}",
                    reply_markup=cancel_kb()
                )
                return
            if bet > 0:
                ud["balance"] -= bet
                comp["pot"] = comp.get("pot", 0) + bet
            start_eq = ud["balance"] + sum(h["total_invested"] for h in ud["holdings"].values())
            comp["members"][str(u.id)] = {
                "username":  ud["username"],
                "start_eq":  start_eq,
                "start_bal": ud["balance"],
            }
            ud.setdefault("competitions", {})[code] = True
            del pending[u.id]
            days_left = max(0, (end_dt - datetime.now()).days)
            pot_line = f"💰 Pot: {money(comp.get('pot', 0))}" if bet > 0 else "🆓 Free competition"
            await message.reply_text(
                f"✅ *Joined Competition!*\n\n"
                f"📋 Code: `{code}`\n"
                f"{pot_line}\n"
                f"👥 Players: {len(comp['members'])}\n"
                f"⏳ Days left: {days_left}\n\n"
                f"Trade hard! Winner takes all 🏆",
                parse_mode="Markdown", reply_markup=main_menu_kb()
            )

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

        # ── SNIPER CONFIG INPUTS ───────────────────────────────────────────────
        elif action == "sniper_score":
            try:
                val = int(text)
                assert 0 <= val <= 100
                ud.setdefault("sniper_filters", {})["min_score"] = val
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Min score set to *" + str(val) + "/100*", parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]]))
                else:
                    await message.reply_text("✅ Min score set to *" + str(val) + "/100*", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await message.reply_text("❌ Enter a number between 0 and 100", reply_markup=cancel_kb())
            return

        elif action == "sniper_liq":
            try:
                val = float(text.replace("$", "").replace(",", ""))
                assert val >= 0
                ud.setdefault("sniper_filters", {})["min_liq"] = val
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Min liquidity set to *" + money(val) + "*", parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]]))
                else:
                    await message.reply_text("✅ Min liquidity set to *" + money(val) + "*", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await message.reply_text("❌ Enter a number like 15000", reply_markup=cancel_kb())
            return

        elif action == "sniper_mc":
            try:
                parts = text.replace("$", "").replace(",", "").split()
                assert len(parts) == 2
                min_mc, max_mc = float(parts[0]), float(parts[1])
                assert 0 < min_mc < max_mc
                ud.setdefault("sniper_filters", {})["min_mc"] = min_mc
                ud["sniper_filters"]["max_mc"] = max_mc
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ MC range: *" + mc_str(min_mc) + "* → *" + mc_str(max_mc) + "*", parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]]))
                else:
                    await message.reply_text("✅ MC range: *" + mc_str(min_mc) + "* → *" + mc_str(max_mc) + "*", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await message.reply_text("❌ Format: min max\nExample: 20000 1000000", reply_markup=cancel_kb())
            return

        elif action == "sniper_age":
            try:
                val = float(text)
                assert val > 0
                ud.setdefault("sniper_filters", {})["max_age_h"] = val
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Max age: *" + str(val) + "h*", parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]]))
                else:
                    await message.reply_text("✅ Max age: *" + str(val) + "h*", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await message.reply_text("❌ Enter hours like 6", reply_markup=cancel_kb())
            return

        elif action == "sniper_amt":
            try:
                val = float(text.replace("$", "").replace(",", ""))
                assert val > 0
                ud.setdefault("sniper_filters", {})["buy_amount"] = val
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Buy amount: *" + money(val) + "* per snipe", parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]]))
                else:
                    await message.reply_text("✅ Buy amount: *" + money(val) + "* per snipe", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await message.reply_text("❌ Enter a number like 100", reply_markup=cancel_kb())
            return

        elif action == "sniper_sl_pct":
            try:
                val = float(text.replace("%", ""))
                assert 5 <= val <= 95
                ud["sniper_auto_sl_pct"] = val
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Auto SL set to *" + str(val) + "%* drop", parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_auto_menu")]]))
                else:
                    await message.reply_text("✅ Auto SL set to *" + str(val) + "%* drop", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await message.reply_text("❌ Enter a % between 5 and 95", reply_markup=cancel_kb())
            return

        elif action == "sniper_tp_x":
            try:
                parts = text.replace("x","").split()
                xs = [float(p2) for p2 in parts]
                assert all(x > 1 for x in xs) and len(xs) >= 1
                xs.sort()
                ud["sniper_auto_tp_x"] = xs
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                tp_str = "  |  ".join(str(x) + "x" for x in xs)
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Take profit targets: *" + tp_str + "*", parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_auto_menu")]]))
                else:
                    await message.reply_text("✅ Take profit targets: *" + tp_str + "*", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await message.reply_text("❌ Format: 2 5 10", reply_markup=cancel_kb())
            return

        elif action == "kol_add_wallet":
            _clean(u.id)
            parts   = text.strip().split(None, 1)
            address = parts[0]
            label   = parts[1] if len(parts) > 1 else address[:8] + "..."
            if not _re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', address):
                await update.message.reply_text(
                    "⚠️ Invalid Solana address. Must be 32-44 base58 characters.\nTry again or tap Cancel.",
                    parse_mode="Markdown"
                )
                pending[u.id] = {"action": "kol_add_wallet"}
                return
            wallets = ud.setdefault("kol_wallets", [])
            if any(w.get("address") == address for w in wallets):
                await update.message.reply_text(
                    "⚠️ That wallet is already being tracked.",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ KOL Menu", callback_data="kol_menu")]])
                )
                return
            if len(wallets) >= MAX_KOL_WALLETS:
                await update.message.reply_text(
                    f"❌ Max {MAX_KOL_WALLETS} KOL wallets reached. Remove one first.",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ KOL Menu", callback_data="kol_menu")]])
                )
                return
            wallets.append({"address": address, "label": label, "chain": "solana"})
            await update.message.reply_text(
                "✅ *" + label + "* added to KOL tracker!\n\n"
                "`" + address + "`\n\n"
                "You'll get alerted next time this wallet buys a new token.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👀 KOL Menu", callback_data="kol_menu")]])
            )
            return

        elif action == "sniper_buys_h1":
            try:
                val = int(text.strip())
                assert 0 <= val <= 500
                ud.setdefault("sniper_filters", {})["min_buys_h1"] = val
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                reply_text = "✅ Min Buys/1h set to *" + str(val) + "*"
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text=reply_text, parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Filters", callback_data="sniper_filters_menu")]]))
                else:
                    await message.reply_text(reply_text, parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Enter a whole number (e.g. 20)", reply_markup=cancel_kb())
            return

        elif action == "sniper_buy_pct":
            try:
                val = int(text.strip().replace("%",""))
                assert 0 <= val <= 100
                ud.setdefault("sniper_filters", {})["min_buy_pct"] = val
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                reply_text = "✅ Min Buy% set to *" + str(val) + "%*"
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text=reply_text, parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Filters", callback_data="sniper_filters_menu")]]))
                else:
                    await message.reply_text(reply_text, parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Enter 40–80 (e.g. 55)", reply_markup=cancel_kb())
            return

        elif action == "sniper_vol_mc":
            try:
                val = float(text.strip().replace("x",""))
                assert 0.5 <= val <= 50
                ud.setdefault("sniper_filters", {})["max_vol_mc_ratio"] = val
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                reply_text = "✅ Vol/MC cap set to *" + str(val) + "x*"
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text=reply_text, parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Filters", callback_data="sniper_filters_menu")]]))
                else:
                    await message.reply_text(reply_text, parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Enter a decimal (e.g. 6.0)", reply_markup=cancel_kb())
            return

        elif action == "sniper_liq_pct":
            try:
                val = float(text.strip().replace("%",""))
                assert 0 <= val <= 100
                ud.setdefault("sniper_filters", {})["min_liq_pct"] = val
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                reply_text = "✅ Min Liq% set to *" + str(val) + "% of MC*"
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text=reply_text, parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Filters", callback_data="sniper_filters_menu")]]))
                else:
                    await message.reply_text(reply_text, parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Enter 3–30 (e.g. 8)", reply_markup=cancel_kb())
            return

        elif action == "sniper_top10":
            try:
                val = float(text.strip().replace("%",""))
                assert 10 <= val <= 100
                ud.setdefault("sniper_filters", {})["max_top10_pct"] = val
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                reply_text = "✅ Max Top10% set to *" + str(val) + "%*"
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text=reply_text, parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Filters", callback_data="sniper_filters_menu")]]))
                else:
                    await message.reply_text(reply_text, parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Enter 30–80 (e.g. 55)", reply_markup=cancel_kb())
            return

        elif action == "sniper_lp_burn":
            try:
                val = float(text.strip().replace("%",""))
                assert 0 <= val <= 100
                ud.setdefault("sniper_filters", {})["min_lp_burn"] = val
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                reply_text = "✅ Min LP Burn set to *" + str(val) + "%*"
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text=reply_text, parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Filters", callback_data="sniper_filters_menu")]]))
                else:
                    await message.reply_text(reply_text, parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Enter 0–100 (e.g. 80)", reply_markup=cancel_kb())
            return

        elif action == "sniper_budget":
            try:
                val = float(text.replace("$", "").replace(",", ""))
                assert 0 < val <= MAX_BALANCE
                ud["sniper_daily_budget"] = val
                prompt_id = p.get("_prompt_msg_id")
                pending.pop(u.id, None)
                await _clean()
                if prompt_id:
                    await ctx.bot.edit_message_text(chat_id=u.id, message_id=prompt_id,
                        text="✅ Daily sniper budget: *" + money(val) + "*", parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="v_sniper")]]))
                else:
                    await message.reply_text("✅ Daily sniper budget: *" + money(val) + "*", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Enter a number like 300", reply_markup=cancel_kb())
            return

        # ── DCA STEP-BY-STEP INPUT ─────────────────────────────────────────────
        elif action == "dca_mc_input":
            contract = p["contract"]
            try:
                mc_val = float(text.replace("$","").replace(",","").replace("k","e3").replace("K","e3").replace("m","e6").replace("M","e6"))
                assert mc_val > 0
                pending[u.id]["pending_mc"] = mc_val
                pending[u.id]["action"]     = "dca_amt_input"
                await update.message.reply_text(
                    "💵 *SET BUY AMOUNT*\n\n"
                    "MC trigger: *" + mc_str(mc_val) + "*\n\n"
                    "How much USD to buy?",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("$50",   callback_data="dca_amt_quick_50_"  + contract),
                         InlineKeyboardButton("$100",  callback_data="dca_amt_quick_100_" + contract),
                         InlineKeyboardButton("$250",  callback_data="dca_amt_quick_250_" + contract)],
                        [InlineKeyboardButton("$500",  callback_data="dca_amt_quick_500_" + contract),
                         InlineKeyboardButton("Custom ↩", callback_data="dca_setamt_" + contract)],
                    ])
                )
            except Exception:
                await update.message.reply_text(
                    "❌ Enter a valid MC number\nExamples:  `500000`  `1000000`  `5000000`",
                    parse_mode="Markdown", reply_markup=cancel_kb()
                )
            return

        elif action == "dca_amt_input":
            contract = p["contract"]
            mc_val   = p.get("pending_mc", 0)
            try:
                amt = float(text.replace("$","").replace(",",""))
                assert amt > 0
                targets = p.get("targets", [])
                targets.append({"mc": mc_val, "amount": amt, "triggered": False})
                pending[u.id]["targets"] = targets
                pending[u.id].pop("pending_mc", None)
                pending[u.id]["action"] = "dca_build"

                # Build a fake query-like object to call helper
                class _FakeQ:
                    async def edit_message_text(self, *a, **kw):
                        await update.message.reply_text(*a, **kw)
                await _dca_show_plan(_FakeQ(), contract, pending[u.id])
            except Exception:
                await update.message.reply_text(
                    "❌ Enter a valid amount\nExamples:  `50`  `100`  `250`",
                    parse_mode="Markdown", reply_markup=cancel_kb()
                )
            return

    # No pending (or pending was cleared for CA) — treat as CA
    if ud.get("balance") is None:
        await update.message.reply_text("Use /start to set up your account first!")
        return

    contract = text
    if not is_valid_contract(contract):
        await update.message.reply_text(
            "❌ That doesn't look like a valid contract address.\n\nPaste a Solana or EVM (0x...) address.",
            reply_markup=main_menu_kb()
        )
        return
    # Track the user's raw CA message so we can delete it with the card later
    ca_msg_ids[u.id] = update.message.message_id
    msg = await update.message.reply_text("🔍 Scanning token...")
    info = await get_token(contract)
    if not info:
        await msg.edit_text("❌ Token not found. Check the contract address and try again.", reply_markup=back_main())
        return
    sc = score_token(info)
    ud["last_chain"] = info.get("chain", "solana")
    save_user(u.id, ud)   # persist last_chain + any pending state changes
    # Delete the "Scanning..." stub then send chart+card together
    try:
        await msg.delete()
    except Exception:
        pass
    try:
        await send_token_card(update.message, info, contract, ud, sc, ctx, is_query=False)
    except Exception as card_err:
        logger.error(f"Token card error: {card_err}")
        await update.message.reply_text(f"❌ Error loading token: {card_err}", reply_markup=back_main())


async def export_csv(bot, uid: int, ud: dict):
    """Generate and send trade history as a CSV file."""
    import csv, io as _io
    logs = trade_log.get(uid, [])
    if not logs:
        await bot.send_message(chat_id=uid, text="No trade history to export yet.", reply_markup=back_main())
        return
    buf = _io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Date", "Symbol", "Contract", "Chain", "Invested", "Returned",
                     "PnL", "X", "Hold(h)", "Reason", "Mood", "Journal", "Planned"])
    for tr in sorted(logs, key=lambda x: x.get("closed_at", datetime.min)):
        writer.writerow([
            tr.get("closed_at", "").strftime("%Y-%m-%d %H:%M") if hasattr(tr.get("closed_at"), "strftime") else "",
            tr.get("symbol", ""),
            tr.get("contract", ""),
            tr.get("chain", ""),
            round(tr.get("invested", 0), 4),
            round(tr.get("returned", 0), 4),
            round(tr.get("realized_pnl", 0), 4),
            round(tr.get("x", 0), 4),
            tr.get("hold_h", ""),
            tr.get("reason", ""),
            tr.get("mood", ""),
            tr.get("journal", ""),
            tr.get("planned", ""),
        ])
    buf.seek(0)
    filename = "auracle_trades_" + datetime.now().strftime("%Y%m%d") + ".csv"
    await bot.send_document(
        chat_id=uid,
        document=_io.BytesIO(buf.getvalue().encode("utf-8")),
        filename=filename,
        caption="📁 *Your full trade history*\n" + str(len(logs)) + " trades exported.",
        parse_mode="Markdown",
    )


async def btn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = update.effective_user
    if is_rate_limited(u.id):
        return  # silently drop rapid taps
    ud = get_user(u.id, u.username or u.first_name)
    cb = q.data

    if cb == "mm":
        pending.pop(u.id, None)
        if ud.get("balance") is None:
            await cmd_start(update, ctx)
            return
        # Clean up orphaned chart photo + CA message when returning to main menu
        await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
        await q.edit_message_text(
            "⚡ *AURACLE_XBOT*\n\nWelcome back, *" + ud["username"] + "*!\n"
            "💰 Balance: *" + money(ud["balance"]) + "*\n"
            "💎 Savings: *" + money(ud["savings"]) + "*\n\n"
            "Paste any CA to trade 👇",
            parse_mode="Markdown", reply_markup=main_menu_kb()
        )

    elif cb == "v_trade":
        await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
        await q.edit_message_text(
            "⚡ *BUY and SELL NOW*\n\nPaste any Solana, ETH, BSC or Base contract address in the chat to get started.",
            parse_mode="Markdown", reply_markup=back_main()
        )

    elif cb == "v_pos":
        await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
        if not ud["holdings"]:
            await q.edit_message_text(
                "📊 *POSITIONS*\n\nNo open positions.\nPaste a CA to start trading.",
                parse_mode="Markdown", reply_markup=back_main()
            )
            return
        lines = ["📊 *OPEN POSITIONS*\n"]
        _pos_contracts = list(ud["holdings"].keys())
        _pos_infos = await _asyncio.gather(*[get_token(c) for c in _pos_contracts])
        for contract, info in zip(_pos_contracts, _pos_infos):
            h = ud["holdings"][contract]
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
        await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
        orders = ud.get("limit_orders", [])
        alerts = ud.get("price_alerts", [])
        if not orders and not alerts:
            await q.edit_message_text(
                "🕐 *ACTIVE ORDERS & ALERTS*\n\nNone active.\nOpen a token to set limit orders or price alerts.",
                parse_mode="Markdown", reply_markup=back_main())
            return

        lines = ["🕐 *ACTIVE ORDERS & ALERTS*\n"]
        buttons = []

        if orders:
            lines.append("📋 *Limit Orders:*")
            for i, o in enumerate(orders):
                otype = "BUY" if o["type"] == "buy" else "SELL"
                lines.append("  " + otype + " $" + o["symbol"] + " @ " + money(o["target_price"]) + "  (" + money(o["amount"]) + ")")
                buttons.append([InlineKeyboardButton(
                    "🗑 Cancel " + otype + " $" + o["symbol"],
                    callback_data="co_" + str(i)
                )])

        if alerts:
            lines.append("\n🔔 *Price Alerts:*")
            for i, a in enumerate(alerts):
                lines.append("  $" + a["symbol"] + " → " + a["direction"] + " " + money(a["target"]))
                buttons.append([InlineKeyboardButton(
                    "🗑 Cancel Alert $" + a["symbol"],
                    callback_data="al_del_" + str(i)
                )])

        buttons.append([InlineKeyboardButton("🗑 Cancel ALL", callback_data="co_all")])
        buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="mm")])
        await q.edit_message_text("\n".join(lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons))

    elif cb.startswith("al_del_"):
        # Cancel individual price alert
        try:
            idx = int(cb[7:])
            alerts = ud.get("price_alerts", [])
            if 0 <= idx < len(alerts):
                sym = alerts[idx].get("symbol", "?")
                alerts.pop(idx)
                ud["price_alerts"] = alerts
                # Refresh orders view
                orders = ud.get("limit_orders", [])
                if not orders and not alerts:
                    await q.edit_message_text("🗑 Alert for *$" + sym + "* cancelled.\n\nNo more active orders.", parse_mode="Markdown", reply_markup=back_main())
                else:
                    await q.edit_message_text("🗑 Alert for *$" + sym + "* cancelled.", parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back to Orders", callback_data="v_orders")]]))
            else:
                await q.edit_message_text("Alert not found.", reply_markup=back_main())
        except Exception:
            await q.edit_message_text("Could not cancel alert.", reply_markup=back_main())

    elif cb.startswith("co_"):
        rest = cb[3:]
        if rest == "all":
            ud["limit_orders"] = []
            ud["price_alerts"] = []
            await q.edit_message_text("🗑 All orders and alerts cancelled.", reply_markup=back_main())
        else:
            try:
                idx = int(rest)
                if 0 <= idx < len(ud["limit_orders"]):
                    cancelled_sym = ud["limit_orders"][idx].get("symbol", "?")
                    ud["limit_orders"].pop(idx)
                    orders = ud.get("limit_orders", [])
                    alerts = ud.get("price_alerts", [])
                    if not orders and not alerts:
                        await q.edit_message_text("🗑 Order for *$" + cancelled_sym + "* cancelled.\n\nNo more active orders.", parse_mode="Markdown", reply_markup=back_main())
                    else:
                        await q.edit_message_text("🗑 Order for *$" + cancelled_sym + "* cancelled.", parse_mode="Markdown",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back to Orders", callback_data="v_orders")]]))
                else:
                    await q.edit_message_text("Order not found.", reply_markup=back_main())
            except Exception:
                await q.edit_message_text("Could not cancel order.", reply_markup=back_main())

    elif cb.startswith("ot_yes_"):
        rest = cb[7:]
        parts = rest.split("_", 2)
        contract = parts[0]
        amount = float(parts[1])
        mood = parts[2] if len(parts) > 2 else ""
        await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
        await do_buy_query(q, ud, u.id, contract, amount, mood=mood)

    elif cb == "cfg_mood":
        ud["mood_tracking"] = not ud.get("mood_tracking", True)
        status = "ON" if ud["mood_tracking"] else "OFF"
        await q.edit_message_text(
            "🧠 Mood tracking turned *" + status + "*",
            parse_mode="Markdown", reply_markup=settings_kb(ud)
        )

    elif cb == "v_history":
        logs = trade_log.get(u.id, [])
        if not logs:
            await q.edit_message_text("📜 *TRADE HISTORY*\n\nNo closed trades yet.", parse_mode="Markdown", reply_markup=back_more())
            return
        lines = ["📜 *TRADE HISTORY*\n"]
        for t in sorted(logs, key=lambda x: x.get("closed_at", datetime.min), reverse=True)[:15]:
            icon = "+" if t["realized_pnl"] > 0 else "-"
            j = "\n  \"" + t["journal"][:40] + "\"" if t.get("journal") else ""
            lines.append(
                icon + " *$" + t["symbol"] + "*  " + str(round(t.get("x", 0), 2)) + "x  " + pstr(t["realized_pnl"]) + "\n"
                "  Held: " + str(t["hold_h"]) + "h  |  " + t.get("reason", "manual") + j
            )
        await q.edit_message_text("\n".join(lines), parse_mode="Markdown", reply_markup=back_more())

    elif cb == "v_savings":
        await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
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
            await q.edit_message_text("📈 *STATS*\n\nNo closed trades yet.", parse_mode="Markdown", reply_markup=back_more())
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

        mood_txt = ""
        if ud.get("mood_stats"):
            mood_txt = "\n\n🧠 *MOOD BREAKDOWN*\n"
            for mood, ms in sorted(ud["mood_stats"].items(), key=lambda x: x[1]["pnl"], reverse=True):
                wr_m = round(ms["wins"] / ms["trades"] * 100) if ms["trades"] > 0 else 0
                mood_txt += mood + ": " + str(ms["trades"]) + " trades  WR:" + str(wr_m) + "%  " + pstr(ms["pnl"]) + "\n"

        ot_txt = ""
        if ud.get("avg_daily_trades", 0) > 0:
            ot_txt = "\nAvg Daily Trades: " + str(ud["avg_daily_trades"])

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
            "Account Growth: " + str(growth) + "%" + best_hour + target_line + ot_txt + mood_txt,
            parse_mode="Markdown", reply_markup=back_more()
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
            await q.edit_message_text("No traders yet.", reply_markup=back_more())
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
        await q.edit_message_text("\n".join(lines), parse_mode="Markdown", reply_markup=back_more())

    elif cb == "v_alerts":
        alerts = ud.get("price_alerts", [])
        if not alerts:
            await q.edit_message_text("🔔 *PRICE ALERTS*\n\nNo active alerts.\nOpen a token and set a price alert.", parse_mode="Markdown", reply_markup=back_more())
            return
        lines = ["🔔 *PRICE ALERTS*\n"]
        for a in alerts:
            lines.append("$" + a["symbol"] + " when price goes " + a["direction"] + " " + money(a["target"]))
        await q.edit_message_text("\n".join(lines), parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Clear All Alerts", callback_data="clear_alerts")],
                [InlineKeyboardButton("◀ Back to More",  callback_data="v_more")],
                [InlineKeyboardButton("🏠 Main Menu",    callback_data="mm")],
            ])
        )

    elif cb == "clear_alerts":
        ud["price_alerts"] = []
        await q.edit_message_text("All price alerts cleared.", reply_markup=back_more())

    elif cb == "v_settings":
        await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
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
        pending[u.id] = {"action": cb, "_prompt_msg_id": q.message.message_id}
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
            parse_mode="Markdown", reply_markup=back_more()
        )


    elif cb.startswith("share_"):
        contract = cb[6:]
        logs = trade_log.get(u.id, [])
        trade = next((t for t in reversed(logs) if t["contract"] == contract), None)
        if not trade:
            await q.edit_message_text("Trade not found.", reply_markup=back_main())
            return
        pnl_positive = trade["realized_pnl"] > 0
        invested   = trade.get("invested", 0)
        returned   = trade.get("returned", 0)
        pnl_pct_val = round((trade["realized_pnl"] / invested * 100), 2) if invested > 0 else 0
        card = generate_trade_card(
            symbol       = trade["symbol"],
            chain        = trade.get("chain", "SOL"),
            pnl_str      = money(abs(trade["realized_pnl"])),
            x_val        = str(round(trade.get("x", 0), 2)),
            held_h       = str(trade["hold_h"]) + "h",
            bought_str   = money(invested),
            position_str = money(returned),
            username     = ud.get("username", "trader"),
            pnl_pct      = str(abs(pnl_pct_val)) + "%",
            pnl_positive = pnl_positive,
            closed_at    = trade.get("closed_at"),
        )
        caption = (
            "AURACLE_XBOT TRADE\n"
            "$" + trade["symbol"] + "  " + str(round(trade.get("x", 0), 2)) + "x\n"
            + ("+" if pnl_positive else "") + money(trade["realized_pnl"]) + "\n"
            "Held: " + str(trade["hold_h"]) + "h\n"
            "Paper Trading | AURACLE_XBOT"
        )
        if card:
            await q.message.reply_photo(
                photo=card,
                caption=caption,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="mm")]])
            )
            # Auto post to channel if connected
            ch_id = ud.get("channel_id")
            if ch_id:
                try:
                    card.seek(0)
                    await ctx.bot.send_photo(chat_id=ch_id, photo=card, caption=caption)
                except Exception:
                    pass
            await q.answer()
        else:
            await q.edit_message_text(
                "📤 *SHARE THIS TRADE*\n\n" + caption,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="mm")]])
            )

    elif cb == "v_copy":
        ct = ud.get("copy_trading")
        paused = ud.get("copy_paused", False)
        # Build leaderboard for choosing who to copy
        scores = []
        for uid2, d in users.items():
            if uid2 == u.id or d.get("balance") is None:
                continue
            logs2 = trade_log.get(uid2, [])
            if len(logs2) < 3:
                continue
            wins2 = [t for t in logs2 if t["realized_pnl"] > 0]
            wr2 = round(len(wins2) / len(logs2) * 100) if logs2 else 0
            tpnl2 = sum(t["realized_pnl"] for t in logs2)
            hv2 = sum(h["total_invested"] for h in d["holdings"].values())
            eq2 = d["balance"] + hv2
            scores.append((uid2, d["username"], wr2, tpnl2, eq2, len(logs2)))
        scores.sort(key=lambda x: x[4], reverse=True)

        status_txt = ""
        if ct:
            trader_name = users[ct]["username"] if ct in users else "Unknown"
            status_txt = "\nCurrently copying: *" + trader_name + "*"
            if paused:
                status_txt += " [PAUSED]"

        buttons = []
        for uid2, uname2, wr2, tpnl2, eq2, ntrades in scores[:5]:
            lbl = uname2 + "  WR:" + str(wr2) + "%  " + str(ntrades) + " trades"
            buttons.append([InlineKeyboardButton(lbl, callback_data="copy_sel_" + str(uid2))])

        if ct:
            pause_lbl = "Resume Copy Trading" if paused else "Pause Copy Trading"
            buttons.append([InlineKeyboardButton(pause_lbl, callback_data="copy_pause")])
            buttons.append([InlineKeyboardButton("Stop Copy Trading", callback_data="copy_stop")])
        buttons.append([InlineKeyboardButton("📽 Trade Replay",   callback_data="copy_replay"),
                         InlineKeyboardButton("⚙️ Settings",      callback_data="copy_settings")])
        buttons.append([InlineKeyboardButton("◀ Back to More", callback_data="v_more")])
        buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="mm")])

        await q.edit_message_text(
            "🔁 *COPY TRADING*\n\n"
            "Mirror trades from top performing traders.\n"
            "You copy up to 10% of your balance per trade." + status_txt + "\n\n"
            "Top Traders to Copy:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif cb.startswith("copy_sel_"):
        target_id = int(cb[9:])
        if target_id not in users:
            await q.edit_message_text("Trader not found.", reply_markup=back_main())
            return
        target = users[target_id]
        logs2 = trade_log.get(target_id, [])
        wins2 = [t for t in logs2 if t["realized_pnl"] > 0]
        wr2 = round(len(wins2) / len(logs2) * 100) if logs2 else 0
        tpnl2 = sum(t["realized_pnl"] for t in logs2)
        hv2 = sum(h["total_invested"] for h in target["holdings"].values())
        eq2 = target["balance"] + hv2
        sb2 = target.get("starting_balance", 5000)
        growth2 = round((eq2 - sb2) / sb2 * 100, 1) if sb2 > 0 else 0
        await q.edit_message_text(
            "🔁 *COPY TRADER PROFILE*\n\n"
            "Trader: *" + target["username"] + "*\n"
            "Equity: *" + money(eq2) + "*\n"
            "Growth: *" + str(growth2) + "%*\n"
            "Win Rate: *" + str(wr2) + "%*\n"
            "Total Trades: *" + str(len(logs2)) + "*\n"
            "Total PnL: *" + pstr(tpnl2) + "*\n\n"
            "Copy this trader? Their future buys will be mirrored to your account at 10% of your balance per trade.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes, Copy This Trader", callback_data="copy_confirm_" + str(target_id))],
                [InlineKeyboardButton("Back", callback_data="v_copy")],
            ])
        )

    elif cb.startswith("copy_confirm_"):
        target_id = int(cb[13:])
        if target_id == u.id:
            await q.edit_message_text("❌ You cannot copy yourself.", reply_markup=back_main())
            return
        if target_id not in users:
            await q.edit_message_text("Trader not found.", reply_markup=back_main())
            return
        ud["copy_trading"] = target_id
        ud["copy_paused"] = False
        trader_name = users[target_id]["username"]
        await q.edit_message_text(
            "✅ *Copy Trading Active!*\n\n"
            "Now copying: *" + trader_name + "*\n\n"
            "Every time they buy a token, you will automatically buy up to 10% of your balance.\n"
            "You can pause or stop anytime from the Copy Trading menu.",
            parse_mode="Markdown",
            reply_markup=back_main()
        )

    elif cb == "copy_pause":
        ud["copy_paused"] = not ud.get("copy_paused", False)
        status = "PAUSED" if ud["copy_paused"] else "RESUMED"
        await q.edit_message_text(
            "Copy trading *" + status + "*.",
            parse_mode="Markdown",
            reply_markup=back_main()
        )

    elif cb == "copy_stop":
        ud["copy_trading"] = None
        ud["copy_paused"] = False
        await q.edit_message_text(
            "Copy trading stopped.",
            reply_markup=back_main()
        )

    elif cb == "v_public":
        logs2 = trade_log.get(u.id, [])
        wins2 = [t for t in logs2 if t["realized_pnl"] > 0]
        wr2 = round(len(wins2) / len(logs2) * 100) if logs2 else 0
        tpnl2 = sum(t["realized_pnl"] for t in logs2)
        hv2 = sum(h["total_invested"] for h in ud["holdings"].values())
        eq2 = ud["balance"] + hv2 + ud["savings"]
        sb2 = ud.get("starting_balance", 5000)
        growth2 = round((eq2 - sb2) / sb2 * 100, 1) if sb2 > 0 else 0
        rf = ud.get("followed", 0)
        rb = ud.get("broken", 0)
        dr = round(rf / (rf + rb) * 100) if (rf + rb) > 0 else 0
        best_trade = max(logs2, key=lambda t: t["realized_pnl"]) if logs2 else None
        best_line = ""
        if best_trade:
            best_line = "\nBest Trade: " + pstr(best_trade["realized_pnl"]) + " ($" + best_trade["symbol"] + "  " + str(round(best_trade.get("x", 0), 2)) + "x)"
        unlocked = ud.get("unlocked_rewards", [])
        badge = ""
        if "50 Streak" in unlocked:
            badge = "  [LEGEND]"
        elif "30 Streak" in unlocked:
            badge = "  [ELITE]"
        elif "20 Streak" in unlocked:
            badge = "  [PRO]"
        elif "10 Streak" in unlocked:
            badge = "  [SKILLED]"
        elif "5 Streak" in unlocked:
            badge = "  [DISCIPLINED]"

        profile_card = (
            "AURACLE_XBOT - TRADER PROFILE\n"
            "================================\n"
            "Trader: " + ud["username"] + badge + "\n"
            "Equity: " + money(eq2) + "\n"
            "Growth: " + str(growth2) + "%\n\n"
            "Trades: " + str(len(logs2)) + "\n"
            "Win Rate: " + str(wr2) + "%\n"
            "Total PnL: " + pstr(tpnl2) + "\n"
            "Discipline: " + str(dr) + "%\n"
            "Best Streak: " + str(ud.get("best_streak", 0)) + "\n"
            "Savings: " + money(ud["savings"]) + best_line + "\n"
            "================================\n"
            "Trade on AURACLE_XBOT"
        )
        await q.edit_message_text(
            "🌐 *PUBLIC PROFILE*\n\nCopy and share this card:\n\n" + profile_card,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="mm")]])
        )


    # ── WATCHLIST ──────────────────────────────────────────────────────────
    elif cb == "v_watchlist":
        await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
        wl = ud.get("watchlist", {})
        if not wl:
            await q.edit_message_text(
                "👁 *WATCHLIST*\n\nNo tokens being watched.\nPaste a CA then use the Watchlist button to add.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("◀ Back", callback_data="mm")],
                ]))
        else:
            txt = "👁 *WATCHLIST* — " + str(len(wl)) + " token" + ("s" if len(wl) != 1 else "") + "\n\nTap to view  |  🗑 to remove:\n"
            buttons = []
            for ca, w in list(wl.items()):
                sym      = w.get("symbol", "?")
                added_mc = mc_str(w.get("added_mc", 0))
                tp       = w.get("target_price")
                tm       = w.get("target_mc")
                alert_tag = " 🔔" if (tp or tm) else ""
                buttons.append([
                    InlineKeyboardButton("🪙 $" + sym + "  |  " + added_mc + alert_tag, callback_data="btt_" + ca),
                    InlineKeyboardButton("🗑", callback_data="wl_del_" + ca),
                ])
            buttons.append([InlineKeyboardButton("🗑 Clear All", callback_data="wl_del_all")])
            buttons.append([InlineKeyboardButton("◀ Back", callback_data="mm")])
            await q.edit_message_text(txt, parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(buttons))

    elif cb.startswith("wl_del_"):
        rest = cb[7:]
        if rest == "all":
            ud["watchlist"] = {}
            await q.edit_message_text("🗑 Watchlist cleared.", reply_markup=back_main())
        else:
            contract = rest
            wl = ud.get("watchlist", {})
            sym = wl.get(contract, {}).get("symbol", contract[:8])
            wl.pop(contract, None)
            ud["watchlist"] = wl
            # Refresh watchlist view
            if not wl:
                await q.edit_message_text("🗑 *$" + sym + "* removed.\n\nWatchlist is now empty.", parse_mode="Markdown", reply_markup=back_main())
            else:
                txt = "🗑 *$" + sym + "* removed.\n\n👁 *WATCHLIST* — " + str(len(wl)) + " token" + ("s" if len(wl) != 1 else "") + "\n\nTap to view  |  🗑 to remove:\n"
                buttons = []
                for ca, w in list(wl.items()):
                    s2       = w.get("symbol", "?")
                    added_mc = mc_str(w.get("added_mc", 0))
                    alert_tag = " 🔔" if (w.get("target_price") or w.get("target_mc")) else ""
                    buttons.append([
                        InlineKeyboardButton("🪙 $" + s2 + "  |  " + added_mc + alert_tag, callback_data="btt_" + ca),
                        InlineKeyboardButton("🗑", callback_data="wl_del_" + ca),
                    ])
                buttons.append([InlineKeyboardButton("🗑 Clear All", callback_data="wl_del_all")])
                buttons.append([InlineKeyboardButton("◀ Back", callback_data="mm")])
                await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons))

    # ── WHALE ALERTS ───────────────────────────────────────────────────────
    elif cb == "v_whale":
        status = "ON 🟢" if ud.get("whale_alerts", True) else "OFF 🔴"
        await q.edit_message_text(
            f"🐋 *WHALE ALERTS*\n\n"
            f"Status: *{status}*\n\n"
            f"When whale wallets (>$50K) buy a token you're holding or watching, you'll get an instant alert.\n\n"
            f"Toggle below:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Toggle Whale Alerts", callback_data="whale_toggle")],
                [InlineKeyboardButton("◀ Back to More",      callback_data="v_more")],
                [InlineKeyboardButton("🏠 Main Menu",        callback_data="mm")],
            ]))

    elif cb == "whale_toggle":
        ud["whale_alerts"] = not ud.get("whale_alerts", True)
        status = "ON 🟢" if ud["whale_alerts"] else "OFF 🔴"
        await q.edit_message_text(
            f"🐋 Whale alerts turned *{status}*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ Back to More", callback_data="v_more")],
                [InlineKeyboardButton("🏠 Main Menu",   callback_data="mm")],
            ])
        )

    # ── PORTFOLIO CHART ────────────────────────────────────────────────────
    elif cb == "v_chart":
        logs = trade_log.get(u.id, [])
        if len(logs) < 2:
            await q.edit_message_text("📊 *PORTFOLIO CHART*\n\nNot enough trades yet to generate a chart.\nMake at least 2 trades first!",
                parse_mode="Markdown", reply_markup=back_main())
            return
        # Build equity curve
        eq = ud.get("starting_balance", 5000)
        points = [eq]
        for t in sorted(logs, key=lambda x: x.get("closed_at", "")):
            eq += t["realized_pnl"]
            points.append(round(eq, 2))
        # Text chart
        mn, mx = min(points), max(points)
        rows = 8
        chart_lines = []
        for row in range(rows, -1, -1):
            threshold = mn + (mx - mn) * row / rows
            line = f"{mc_str(threshold):>8} │"
            for p in points[-20:]:
                line += "█" if p >= threshold else " "
            chart_lines.append(line)
        chart_lines.append("         └" + "─" * min(len(points), 20))
        chart_txt = "\n".join(chart_lines)
        start_eq = points[0]
        end_eq   = points[-1]
        growth   = round((end_eq - start_eq) / start_eq * 100, 1) if start_eq > 0 else 0
        await q.edit_message_text(
            f"📊 *PORTFOLIO CHART*\n\n"
            f"```\n{chart_txt}\n```\n\n"
            f"Start: *{money(start_eq)}*  →  Now: *{money(end_eq)}*\n"
            f"Growth: *{'+' if growth >= 0 else ''}{growth}%*\n"
            f"Trades: *{len(logs)}*",
            parse_mode="Markdown", reply_markup=back_main())

    # ── CHALLENGE MODE ─────────────────────────────────────────────────────
    elif cb == "v_challenge":
        ch = ud.get("challenge")
        if ch:
            elapsed = (datetime.now() - datetime.fromisoformat(ch["started"])).days
            remaining = ch["days"] - elapsed
            current_eq = ud["balance"] + sum(h["total_invested"] for h in ud["holdings"].values())
            progress = round((current_eq - ch["start_eq"]) / (ch["target_eq"] - ch["start_eq"]) * 100, 1) if ch["target_eq"] > ch["start_eq"] else 0
            await q.edit_message_text(
                f"🎯 *ACTIVE CHALLENGE*\n\n"
                f"📌 {money(ch['start_eq'])} → {money(ch['target_eq'])}\n"
                f"⏰ {remaining} days remaining\n"
                f"📊 Progress: {progress}%\n"
                f"💰 Current: {money(current_eq)}\n\n"
                f"Share your progress with friends!",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Abandon Challenge", callback_data="ch_abandon")],
                    [InlineKeyboardButton("◀ Back to More",      callback_data="v_more")],
                    [InlineKeyboardButton("🏠 Main Menu",        callback_data="mm")],
                ]))
        else:
            await q.edit_message_text(
                "🎯 *CHALLENGE MODE*\n\n"
                "Set a public goal and track your progress!\n\n"
                "Examples:\n"
                "• $1,000 → $10,000 in 30 days\n"
                "• $500 → $5,000 in 60 days\n\n"
                "Choose a challenge:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("$1K → $10K  (30 days)", callback_data="ch_1")],
                    [InlineKeyboardButton("$1K → $5K   (60 days)", callback_data="ch_2")],
                    [InlineKeyboardButton("Custom Challenge",        callback_data="ch_custom")],
                    [InlineKeyboardButton("◀ Back to More",         callback_data="v_more")],
                    [InlineKeyboardButton("🏠 Main Menu",           callback_data="mm")],
                ]))

    elif cb.startswith("ch_") and cb != "ch_abandon" and cb != "ch_custom":
        presets = {
            "ch_1": (1000, 10000, 30),
            "ch_2": (1000, 5000,  60),
        }
        if cb in presets:
            s, t, d = presets[cb]
            current_eq = ud["balance"] + sum(h["total_invested"] for h in ud["holdings"].values())
            ud["challenge"] = {"start_eq": current_eq, "target_eq": t, "days": d, "started": datetime.now().isoformat()}
            await q.edit_message_text(
                f"🎯 *CHALLENGE STARTED!*\n\n"
                f"Starting: {money(current_eq)}\n"
                f"Goal: {money(t)}\n"
                f"Duration: {d} days\n\n"
                f"Good luck! Your progress is being tracked. 💪",
                parse_mode="Markdown", reply_markup=main_menu_kb())

    elif cb == "ch_abandon":
        ud["challenge"] = None
        await q.edit_message_text("Challenge abandoned.", reply_markup=back_main())

    elif cb == "ch_custom":
        pending[u.id] = {"action": "ch_custom_target"}
        await q.edit_message_text("🎯 Enter your target amount\n\nExample: 10000", reply_markup=cancel_kb())

    # ── MULTI ACCOUNT ──────────────────────────────────────────────────────
    elif cb == "v_accounts":
        await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
        accounts = ud.get("accounts", {})
        active   = ud.get("active_account", "main")
        txt = f"👥 *MULTI ACCOUNT*\n\nActive: *{active}*\n\nYour accounts:\n"
        txt += f"  • main — {money(ud['balance'])}\n"
        for name, acc in accounts.items():
            txt += f"  • {name} — {money(acc.get('balance', 5000))}\n"
        await q.edit_message_text(txt, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ New Account",    callback_data="acc_new")],
                [InlineKeyboardButton("🔄 Switch Account", callback_data="acc_switch")],
                [InlineKeyboardButton("🏠 Main Menu",      callback_data="mm")],
            ]))

    elif cb == "acc_new":
        pending[u.id] = {"action": "acc_new"}
        await q.edit_message_text("👥 Enter a name for your new account:\n\nExample: scalping, degen, safe", reply_markup=cancel_kb())

    elif cb == "acc_switch":
        accounts = ud.get("accounts", {})
        if not accounts:
            await q.edit_message_text("No extra accounts yet. Create one first.", reply_markup=back_main())
            return
        buttons = [[InlineKeyboardButton(f"main", callback_data="acc_use_main")]]
        for name in accounts:
            buttons.append([InlineKeyboardButton(name, callback_data=f"acc_use_{name}")])
        buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="mm")])
        await q.edit_message_text("👥 Switch to which account?", reply_markup=InlineKeyboardMarkup(buttons))

    elif cb.startswith("acc_use_"):
        name = cb[8:]
        ud["active_account"] = name
        await q.edit_message_text(f"✅ Switched to account: *{name}*", parse_mode="Markdown", reply_markup=back_main())

    # ── REFERRALS ──────────────────────────────────────────────────────────
    elif cb == "v_referrals":
        refs = ud.get("referrals", [])
        ref_link = f"https://t.me/auracle_xbot?start=ref_{u.id}"
        await q.edit_message_text(
            f"🔗 *REFERRAL SYSTEM*\n\n"
            f"Invite friends and earn rewards!\n\n"
            f"Your referral link:\n`{ref_link}`\n\n"
            f"Friends referred: *{len(refs)}*\n"
            f"Bonus earned: *{money(len(refs) * 100)}*\n\n"
            f"Every friend who joins gives you *$100* added to your balance!",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="mm")]]))

    # ── CHANNEL SETUP ──────────────────────────────────────────────────────


    elif cb == "ch_disconnect":
        ud["channel_id"] = None
        await q.edit_message_text("Channel disconnected.", reply_markup=back_main())

    # ── HELP & DOCS ────────────────────────────────────────────────────────
    elif cb == "v_more":
        await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
        await q.edit_message_text(
            "📋 *MORE FEATURES*\n\nSelect a feature:",
            parse_mode="Markdown",
            reply_markup=more_menu_kb()
        )

    elif cb == "v_help":
        await q.edit_message_text(
            "📖 *AURACLE_XBOT HELP*\n\n"
            "Welcome to AURACLE_XBOT — your paper trading terminal!\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "🚀 *GETTING STARTED*\n"
            "1. Paste any Solana/ETH/BSC/Base contract address\n"
            "2. Review the token score and info\n"
            "3. Tap Buy to paper trade\n"
            "4. Monitor in Portfolio\n"
            "5. Sell when ready\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "📊 *KEY FEATURES*\n"
            "• Auto stop loss & take profit\n"
            "• Mood & psychology tracking\n"
            "• Streak rewards system\n"
            "• Copy trading from top traders\n"
            "• Group competitions with bets\n"
            "• Trade sharing cards\n"
            "• Savings wallet\n"
            "• Watchlist & price alerts\n"
            "• Portfolio chart\n"
            "• Multi account\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "📖 Tap below for the full documentation!\n"
            "💬 *Support:* @auracle_support",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📖 Full Docs", url="https://auracle-xbot.gitbook.io/auracle_xbot-docs/")],
                [InlineKeyboardButton("◀ Back to More", callback_data="v_more")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="mm")]
            ]))

    # ── COMPETITION BETS UPDATE ────────────────────────────────────────────
    elif cb == "v_compete":
        await q.edit_message_text(
            "🏁 *GROUP COMPETITIONS*\n\n"
            "Challenge friends to see who grows their balance the most!\n\n"
            "• Create a competition & share the code\n"
            "• Friends join with the code\n"
            "• Winner takes the entire pot 🏆",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Create Competition", callback_data="comp_create")],
                [InlineKeyboardButton("🔗 Join Competition",  callback_data="comp_join")],
                [InlineKeyboardButton("📊 My Competitions",   callback_data="comp_track")],
                [InlineKeyboardButton("◀ Back to More",       callback_data="v_more")],
                [InlineKeyboardButton("🏠 Main Menu",         callback_data="mm")],
            ])
        )

    elif cb == "comp_create":
        # Check if user already has an active competition
        _comps = _competitions
        my_active = [
            c for c in ud.get("competitions", {})
            if c in _comps and datetime.now() < datetime.fromisoformat(_comps[c]["end_time"])
        ]
        if my_active:
            code = my_active[0]
            comp = _comps[code]
            end_dt = datetime.fromisoformat(comp["end_time"])
            days_left = max(0, (end_dt - datetime.now()).days)
            await q.edit_message_text(
                "❌ *You already have an active competition!*\n\n"
                f"📋 Code: `{code}`\n"
                f"⏳ {days_left} days left\n\n"
                "You can only create or join *one competition at a time*.\n"
                "Wait for it to end before creating a new one.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📊 View My Competition", callback_data="comp_track")],
                    [InlineKeyboardButton("🏠 Main Menu", callback_data="mm")],
                ])
            )
            return
        pending[u.id] = {"action": "comp_bet"}
        await q.edit_message_text(
            "🏁 *CREATE COMPETITION*\n\n"
            "Step 1/2 — Enter the bet amount per player:\n\n"
            "• Enter *0* for a free competition\n"
            "• Enter an amount (e.g. *500*) to deduct from each player's balance\n\n"
            "The winner takes the entire pot!",
            parse_mode="Markdown",
            reply_markup=cancel_kb()
        )

    elif cb == "comp_join":
        # Check if user already in an active competition
        _comps = _competitions
        my_active = [
            c for c in ud.get("competitions", {})
            if c in _comps and datetime.now() < datetime.fromisoformat(_comps[c]["end_time"])
        ]
        if my_active:
            code = my_active[0]
            comp = _comps[code]
            end_dt = datetime.fromisoformat(comp["end_time"])
            days_left = max(0, (end_dt - datetime.now()).days)
            await q.edit_message_text(
                "❌ *You already have an active competition!*\n\n"
                f"📋 Code: `{code}`\n"
                f"⏳ {days_left} days left\n\n"
                "You can only be in *one competition at a time*.\n"
                "Wait for it to end before joining another.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📊 View My Competition", callback_data="comp_track")],
                    [InlineKeyboardButton("🏠 Main Menu", callback_data="mm")],
                ])
            )
            return
        pending[u.id] = {"action": "comp_join"}
        await q.edit_message_text(
            "🔗 *JOIN COMPETITION*\n\n"
            "Enter the 6-character competition code:\n\n"
            "Example: *AB1C2D*",
            parse_mode="Markdown",
            reply_markup=cancel_kb()
        )

    elif cb.startswith("comp_days_"):
        # comp_days_7_500 → days=7, bet=500
        parts = cb.split("_")
        try:
            days = int(parts[2])
            bet  = float(parts[3])
        except Exception:
            await q.edit_message_text("❌ Error. Please try again.", reply_markup=back_main())
            return
        import random, string
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        start_eq = ud["balance"] + sum(h["total_invested"] for h in ud["holdings"].values())
        comp = {
            "code":       code,
            "creator_id": str(u.id),
            "bet":        bet,
            "pot":        bet,
            "days":       days,
            "end_time":   (datetime.now() + timedelta(days=days)).isoformat(),
            "members":    {
                str(u.id): {
                    "username":  ud["username"],
                    "start_eq":  start_eq,
                    "start_bal": ud["balance"],
                }
            }
        }
        if bet > 0:
            if bet > ud.get("balance", 0):
                await q.edit_message_text(
                    f"❌ Insufficient balance.\nBet: {money(bet)}  |  Your balance: {money(ud.get('balance',0))}",
                    reply_markup=back_main()
                )
                return
            ud["balance"] -= bet
        _competitions[code] = comp
        ud.setdefault("competitions", {})[code] = True
        pending.pop(u.id, None)
        end_str = (datetime.now() + timedelta(days=days)).strftime("%b %d, %Y")
        pot_line = f"💰 Bet: {money(bet)} per player  |  Pot: {money(bet)}" if bet > 0 else "🆓 Free to join"
        await q.edit_message_text(
            f"🏁 *COMPETITION CREATED!*\n\n"
            f"📋 Code: `{code}`\n"
            f"⏳ Duration: {days} days\n"
            f"📅 Ends: {end_str}\n"
            f"{pot_line}\n\n"
            f"Share code *{code}* with friends to join!\n"
            f"Winner takes the entire pot 🏆",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Track Competition", callback_data="comp_track")],
                [InlineKeyboardButton("🏠 Main Menu",         callback_data="mm")],
            ])
        )

    elif cb == "comp_track":
        _comps = _competitions
        my_codes = [c for c in ud.get("competitions", {}) if c in _comps]

        async def send_comp_msg(txt, kb):
            try:
                await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb)
            except Exception:
                await q.message.reply_text(txt, parse_mode="Markdown", reply_markup=kb)

        if not my_codes:
            await send_comp_msg(
                "📊 *MY COMPETITIONS*\n\nYou have no active competitions.\n\nCreate or join one!",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Create", callback_data="comp_create"),
                     InlineKeyboardButton("🔗 Join",   callback_data="comp_join")],
                    [InlineKeyboardButton("🏠 Main Menu", callback_data="mm")],
                ])
            )
            return
        txt = "📊 *MY COMPETITIONS*\n\n"
        for code in my_codes:
            comp = _comps[code]
            end_dt = datetime.fromisoformat(comp["end_time"])
            days_left = max(0, (end_dt - datetime.now()).days)
            pot = comp.get("pot", 0)
            members = comp.get("members", {})
            # Build leaderboard — ranked by WIN RATE (anti-cheat)
            rankings = []
            for uid_str, m in members.items():
                try:
                    m_ud = users.get(int(uid_str))
                    uname = m.get("username", "?")
                    if m_ud:
                        logs = trade_log.get(int(uid_str), [])
                        # Only count trades made AFTER joining competition
                        join_time = m.get("joined", comp.get("end_time",""))
                        try:
                            join_dt = datetime.fromisoformat(m.get("joined", "2000-01-01"))
                        except (ValueError, TypeError):
                            join_dt = datetime(2000, 1, 1)
                        comp_trades = [t for t in logs if t.get("closed_at", datetime.min) >= join_dt]
                        total  = len(comp_trades)
                        wins   = len([t for t in comp_trades if t["realized_pnl"] > 0])
                        total_pnl = sum(t["realized_pnl"] for t in comp_trades)
                        win_rate = round(wins / total * 100, 1) if total > 0 else 0.0
                        rankings.append((uname, win_rate, wins, total, total_pnl))
                    else:
                        rankings.append((uname, 0.0, 0, 0, 0.0))
                except Exception:
                    pass
            rankings.sort(key=lambda x: (x[1], x[4]), reverse=True)
            txt += f"📋 Code: `{code}`\n"
            txt += f"⏳ {days_left} days left  |  "
            txt += f"{'💰 Pot: ' + money(pot) if pot > 0 else '🆓 Free'}\n"
            txt += f"👥 {len(members)} players\n\n"
            txt += "🏆 *Leaderboard (Win Rate):*\n"
            medals = ["🥇","🥈","🥉"]
            for i, (uname, wr, wins, total, pnl) in enumerate(rankings[:5]):
                medal = medals[i] if i < 3 else f"{i+1}."
                pnl_str = ("+" if pnl >= 0 else "") + money(pnl)
                txt += f"  {medal} @{uname}  {wr}% WR ({wins}/{total})  {pnl_str}\n"
            txt += "\n"
        await send_comp_msg(
            txt,
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh", callback_data="comp_track")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="mm")],
            ])
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
        await send_token_card(q, info, contract, ud, sc, ctx, is_query=True)

    elif cb.startswith("btt_"):
        contract = cb[4:]
        info = await get_token(contract)
        if not info:
            await q.edit_message_text("Token unavailable.", reply_markup=back_main())
            return
        sc = score_token(info)
        await send_token_card(q, info, contract, ud, sc, ctx, is_query=True)

    elif cb.startswith("bts_"):
        # Buy submenu — show amount picker
        contract = cb[4:]
        info = await get_token(contract)
        sym  = info["symbol"] if info else contract[:8]
        price_line = ("Price: *$" + str(info["price"]) + "*  |  MC: *" + mc_str(info["mc"]) + "*") if info else ""
        await q.edit_message_text(
            "⚡ *BUY $" + sym + "*\n\n" + price_line + "\n\nSelect amount:",
            parse_mode="Markdown",
            reply_markup=buy_sub_kb(contract, ud)
        )

    elif cb.startswith("sts_"):
        # Sell submenu — show % picker
        contract = cb[4:]
        if contract not in ud["holdings"]:
            await q.edit_message_text("You don't hold this token.", reply_markup=back_main())
            return
        h    = ud["holdings"][contract]
        info = await get_token(contract)
        price = info["price"] if info else h["avg_price"]
        cv    = h["amount"] * price
        cx    = price / h["avg_price"] if h.get("avg_price", 0) > 0 else 0
        ppnl  = cv - h["total_invested"]
        await q.edit_message_text(
            "🔴 *SELL $" + h["symbol"] + "*\n\n"
            "Value: *" + money(cv) + "*  |  *" + str(round(cx, 2)) + "x*\n"
            "PnL: " + pstr(ppnl) + "\n\nHow much to sell?",
            parse_mode="Markdown",
            reply_markup=sell_sub_kb(contract)
        )

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
        if ud.get("mood_tracking", True):
            pending[u.id] = {"action": "buy_mood", "contract": contract, "amount": pb}
            await q.edit_message_text(
                "🧠 *MOOD CHECK*\n\nWhy are you buying this?\n\n"
                "1 - Research\n2 - Chart looks good\n3 - Community tip\n4 - FOMO\n5 - Gut feeling\n\nReply with a number:",
                parse_mode="Markdown", reply_markup=cancel_kb()
            )
        else:
            await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
            await do_buy_query(q, ud, u.id, contract, pb)

    elif cb.startswith("ba_"):
        rest = cb[3:]
        amt_str, contract = rest.split("_", 1)
        amount = float(amt_str)
        if ud.get("mood_tracking", True):
            pending[u.id] = {"action": "buy_mood", "contract": contract, "amount": amount}
            await q.edit_message_text(
                "🧠 *MOOD CHECK*\n\nWhy are you buying this?\n\n"
                "1 - Research\n2 - Chart looks good\n3 - Community tip\n4 - FOMO\n5 - Gut feeling\n\nReply with a number:",
                parse_mode="Markdown", reply_markup=cancel_kb()
            )
        else:
            await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
            await do_buy_query(q, ud, u.id, contract, amount)

    elif cb.startswith("bc_"):
        contract = cb[3:]
        pending[u.id] = {"action": "buy_custom", "contract": contract, "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text("Enter buy amount in USD:", reply_markup=cancel_kb())

    elif cb.startswith("sp_"):
        rest = cb[3:]
        pct_str, contract = rest.split("_", 1)
        await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
        await do_sell_query(q, ud, u.id, contract, pct=float(pct_str)/100)

    elif cb.startswith("sca_"):
        contract = cb[4:]
        pending[u.id] = {"action": "sell_custom", "contract": contract, "_prompt_msg_id": q.message.message_id}
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
        pending[u.id] = {"action": "limit_buy", "contract": contract, "symbol": sym, "current_price": price, "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "🎯 *LIMIT BUY*\n\nCurrent price: " + money(price) + "\n\n"
            "Enter target price and amount:\nFormat: price amount\nExample: 0.005 100",
            parse_mode="Markdown", reply_markup=cancel_kb()
        )

    elif cb == "wl_add_price":
        contract = pending.get(u.id, {}).get("contract", "")
        pending[u.id] = {"action": "wl_target_price", "contract": contract, "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "👁 Enter target *PRICE* to alert:\nExample: `0.00005`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ Back", callback_data="wl_" + contract)],
                [InlineKeyboardButton("Cancel",  callback_data="mm")],
            ])
        )

    elif cb == "wl_add_mc":
        contract = pending.get(u.id, {}).get("contract", "")
        pending[u.id] = {"action": "wl_target_mc", "contract": contract, "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "👁 Enter target *MARKET CAP* to alert:\nExample: `100000` (=$100K)",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ Back", callback_data="wl_" + contract)],
                [InlineKeyboardButton("Cancel",  callback_data="mm")],
            ])
        )

    elif cb.startswith("lso_"):
        contract = cb[4:]
        if contract not in ud["holdings"]:
            await q.edit_message_text("Position not found.", reply_markup=back_main())
            return
        h = ud["holdings"][contract]
        info = await get_token(contract)
        price = info["price"] if info else h["avg_price"]
        pending[u.id] = {"action": "limit_sell", "contract": contract, "symbol": h["symbol"], "current_price": price, "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "🎯 *LIMIT SELL*\n\nCurrent price: " + money(price) + "\n\n"
            "Enter target price and amount:\nFormat: price amount%\nExample: 0.012 50%",
            parse_mode="Markdown", reply_markup=cancel_kb()
        )

    elif cb.startswith("wl_") and len(cb) > 10:
        contract = cb[3:]
        info = await get_token(contract)
        if not info:
            await q.edit_message_text("Token not found.", reply_markup=back_main())
            return
        if not ud.get("watchlist"):
            ud["watchlist"] = {}
        ud["watchlist"][contract] = {
            "symbol": info["symbol"], "name": info["name"],
            "added_price": info["price"], "added_mc": info["mc"],
            "target_price": None, "target_mc": None,
        }
        pending[u.id] = {"action": "wl_waiting", "contract": contract}
        await q.edit_message_text(
            f"👁 *${info['symbol']}* added to watchlist!\n\nSet an alert target (optional):",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Alert by Price",        callback_data="wl_add_price")],
                [InlineKeyboardButton("Alert by Market Cap",   callback_data="wl_add_mc")],
                [InlineKeyboardButton("No Alert — Just Watch", callback_data="mm")],
                [InlineKeyboardButton("◀ Back to Token",       callback_data="btt_" + contract)],
            ]))

    elif cb.startswith("pal_"):
        contract = cb[4:]
        info = await get_token(contract)
        sym = info["symbol"] if info else "?"
        price = info["price"] if info else 0
        pending[u.id] = {"action": "price_alert", "contract": contract, "symbol": sym, "current_price": price, "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "🔔 *PRICE ALERT*\n\nCurrent price: " + money(price) + "\n\nEnter target price:",
            parse_mode="Markdown", reply_markup=cancel_kb()
        )

    elif cb.startswith("al_cancel_ca_"):
        # Cancel alert for this specific token directly from the token card
        contract = cb[13:]
        alerts = ud.get("price_alerts", [])
        removed = [a for a in alerts if a.get("contract") == contract]
        ud["price_alerts"] = [a for a in alerts if a.get("contract") != contract]
        if removed:
            sym = removed[0].get("symbol", "?")
            await q.edit_message_text(
                "🗑 Price alert for *$" + sym + "* cancelled.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back to Token", callback_data="btt_" + contract)]])
            )
        else:
            await q.edit_message_text("No alert found for this token.", reply_markup=back_main())

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
        sym = h["symbol"]

        lines = ["🎯 *TARGETS  $" + sym + "*\n"]
        buttons = []

        if targets:
            for i, t in enumerate(targets):
                status = "✅ DONE" if t.get("triggered") else "⏳ WAITING"
                line = status + "  Sell " + str(int(t["pct"]*100)) + "% at " + str(t["x"]) + "x  (~" + money(avg * t["x"]) + ")"
                lines.append(line)
                if not t.get("triggered"):
                    buttons.append([InlineKeyboardButton(
                        "🗑 Cancel " + str(int(t["pct"]*100)) + "% @ " + str(t["x"]) + "x",
                        callback_data="as_del_" + str(i) + "_" + contract
                    )])
        else:
            lines.append("No auto-sell targets set.")

        lines.append("")
        if sl:
            lines.append("🛑 Stop Loss: " + str(sl) + "% drop  (~" + money(avg * (1 - sl/100)) + ")")
            buttons.append([InlineKeyboardButton("🗑 Cancel Stop Loss", callback_data="sl_del_" + contract)])
        else:
            lines.append("No stop loss set.")

        buttons.append([InlineKeyboardButton("🗑 Cancel ALL Targets", callback_data="cat_" + contract)])
        buttons.append([InlineKeyboardButton("◀ Back", callback_data="btt_" + contract)])

        await q.edit_message_text(
            "\n".join(lines), parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif cb.startswith("as_del_"):
        # Cancel individual auto-sell target: as_del_{index}_{contract}
        rest = cb[7:]
        idx_str, contract = rest.split("_", 1)
        if contract in ud["holdings"]:
            h = ud["holdings"][contract]
            targets = h.get("auto_sells", [])
            idx = int(idx_str)
            if 0 <= idx < len(targets):
                removed = targets.pop(idx)
                sym = h["symbol"]
                await q.edit_message_text(
                    "🗑 Auto-sell *" + str(int(removed["pct"]*100)) + "% @ " + str(removed["x"]) + "x* cancelled for $" + sym,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back to Targets", callback_data="vtg_" + contract)]])
                )
            else:
                await q.edit_message_text("Target not found.", reply_markup=back_main())
        else:
            await q.edit_message_text("Position not found.", reply_markup=back_main())

    elif cb.startswith("sl_del_"):
        contract = cb[7:]
        if contract in ud["holdings"]:
            ud["holdings"][contract]["stop_loss_pct"] = None
            sym = ud["holdings"][contract]["symbol"]
            await q.edit_message_text(
                "🗑 Stop loss cancelled for *$" + sym + "*",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back to Targets", callback_data="vtg_" + contract)]])
            )
        else:
            await q.edit_message_text("Position not found.", reply_markup=back_main())

    elif cb.startswith("cat_"):
        contract = cb[4:]
        if contract in ud["holdings"]:
            ud["holdings"][contract]["auto_sells"] = []
            ud["holdings"][contract]["stop_loss_pct"] = None
            sym = ud["holdings"][contract]["symbol"]
            await q.edit_message_text(
                "🗑 All targets & stop loss cancelled for *$" + sym + "*",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="btt_" + contract)]])
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

    # ── RISK CALCULATOR ────────────────────────────────────────────────────────
    elif cb == "cfg_riskcalc":
        ud["risk_calc"] = not ud.get("risk_calc", True)
        status = "ON ✅" if ud["risk_calc"] else "OFF ❌"
        await q.edit_message_text(
            "🧮 *RISK CALCULATOR*\n\nShows you the full risk/reward breakdown before every buy.\n\nStatus: *" + status + "*",
            parse_mode="Markdown", reply_markup=settings_kb(ud)
        )

    elif cb == "rc_yes":
        p = pending.get(u.id, {})
        if p.get("action") == "risk_confirm":
            pending.pop(u.id, None)
            contract = p["contract"]
            amount   = p["amount"]
            mood     = p.get("mood", "")
            await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
            await q.edit_message_text("Executing buy...")
            async with get_user_lock(u.id):
                result = await do_buy_core(ud, u.id, contract, amount, mood=mood)
            if isinstance(result, str):
                await q.edit_message_text(result, reply_markup=main_menu_kb())
                return
            info, tokens = result
            liq_warn = "\n\nWARNING: LOW LIQUIDITY" if info["liq"] < 50_000 else ""
            await q.edit_message_text(
                t(ud, "buy_exec",
                  name=info["name"], symbol=info["symbol"],
                  spent=money(amount), tokens=str(round(tokens, 4)),
                  price=money(info["price"]), mc=mc_str(info["mc"]),
                  liq=money(info["liq"]), cash=money(ud["balance"])
                ) + liq_warn,
                parse_mode="Markdown", reply_markup=buy_done_kb(contract)
            )
        else:
            await q.edit_message_text("No pending buy found.", reply_markup=back_main())

    elif cb == "rc_no":
        pending.pop(u.id, None)
        await q.edit_message_text("❌ Buy cancelled.", reply_markup=back_main())

    # ── LEADERBOARD ────────────────────────────────────────────────────────────
    elif cb == "v_leader":
        scores = []
        for uid2, d in users.items():
            if d.get("balance") is None:
                continue
            logs2 = trade_log.get(uid2, [])
            if not logs2:
                continue
            wins2  = [tr for tr in logs2 if tr["realized_pnl"] > 0]
            wr2    = round(len(wins2) / len(logs2) * 100, 1) if logs2 else 0
            tpnl2  = sum(tr["realized_pnl"] for tr in logs2)
            hv2    = sum(h["total_invested"] for h in d["holdings"].values())
            eq2    = d["balance"] + hv2 + d.get("savings", 0)
            sb2    = d.get("starting_balance", 5000) or 5000
            growth = round((eq2 - sb2) / sb2 * 100, 1) if sb2 > 0 else 0
            scores.append({
                "uid": uid2, "username": d["username"],
                "eq": eq2, "pnl": tpnl2, "wr": wr2,
                "trades": len(logs2), "growth": growth,
                "streak": d.get("best_streak", 0),
            })

        if not scores:
            await q.edit_message_text("🏆 *LEADERBOARD*\n\nNo traders with history yet.", parse_mode="Markdown", reply_markup=back_main())
            return

        scores.sort(key=lambda x: x["eq"], reverse=True)
        medals = ["🥇", "🥈", "🥉"]
        lines  = ["🏆 *GLOBAL LEADERBOARD*\n_(Ranked by Equity)_\n"]
        for i, s in enumerate(scores[:10]):
            medal  = medals[i] if i < 3 else str(i + 1) + "."
            me_tag = "  ← *YOU*" if s["uid"] == u.id else ""
            lines.append(
                medal + " *@" + s["username"] + "*" + me_tag + "\n"
                "  💰 " + money(s["eq"]) + "  |  " + str(s["growth"]) + "% growth\n"
                "  📊 " + str(s["trades"]) + " trades  WR:" + str(s["wr"]) + "%  🔥" + str(s["streak"]) + " streak\n"
            )
        my_rank = next((i + 1 for i, s in enumerate(scores) if s["uid"] == u.id), None)
        if my_rank and my_rank > 10:
            lines.append("\n📍 Your rank: #" + str(my_rank) + " of " + str(len(scores)))
        await q.edit_message_text(
            "\n".join(lines), parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 By PnL",      callback_data="lb_pnl"),
                 InlineKeyboardButton("🎯 By Win Rate",  callback_data="lb_wr")],
                [InlineKeyboardButton("🏠 Main Menu",   callback_data="mm")],
            ])
        )

    elif cb in ("lb_pnl", "lb_wr"):
        scores = []
        for uid2, d in users.items():
            if d.get("balance") is None:
                continue
            logs2 = trade_log.get(uid2, [])
            if not logs2:
                continue
            wins2 = [tr for tr in logs2 if tr["realized_pnl"] > 0]
            wr2   = round(len(wins2) / len(logs2) * 100, 1) if logs2 else 0
            tpnl2 = sum(tr["realized_pnl"] for tr in logs2)
            hv2   = sum(h["total_invested"] for h in d["holdings"].values())
            eq2   = d["balance"] + hv2 + d.get("savings", 0)
            scores.append({"uid": uid2, "username": d["username"], "eq": eq2, "pnl": tpnl2, "wr": wr2, "trades": len(logs2)})
        sort_key = "pnl" if cb == "lb_pnl" else "wr"
        label    = "PnL" if cb == "lb_pnl" else "Win Rate"
        scores.sort(key=lambda x: x[sort_key], reverse=True)
        medals = ["🥇", "🥈", "🥉"]
        lines  = ["🏆 *LEADERBOARD — by " + label + "*\n"]
        for i, s in enumerate(scores[:10]):
            medal  = medals[i] if i < 3 else str(i + 1) + "."
            val    = pstr(s["pnl"]) if cb == "lb_pnl" else str(s["wr"]) + "% WR"
            me_tag = "  ← *YOU*" if s["uid"] == u.id else ""
            lines.append(medal + " *@" + s["username"] + "*" + me_tag + "  " + val + "  (" + str(s["trades"]) + " trades)\n")
        await q.edit_message_text(
            "\n".join(lines), parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💰 By Equity",   callback_data="v_leader"),
                 InlineKeyboardButton("📊 By PnL",      callback_data="lb_pnl"),
                 InlineKeyboardButton("🎯 By Win Rate", callback_data="lb_wr")],
                [InlineKeyboardButton("🏠 Main Menu",   callback_data="mm")],
            ])
        )

    # ── SNIPER MODE v2 ─────────────────────────────────────────────────────────
    elif cb == "v_sniper":
        await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
        auto_on  = ud.get("sniper_auto", False)
        adv_on   = ud.get("sniper_advisory", False)
        status   = ("🟢 AUTO" if auto_on else "") + (" + " if auto_on and adv_on else "") + ("🧠 ADVISORY" if adv_on else "")
        if not status:
            status = "🔴 OFF"
        budget   = ud.get("sniper_daily_budget", 500.0)
        spent    = ud.get("sniper_daily_spent", 0.0)
        sf       = ud.get("sniper_filters", {})
        chains   = ud.get("sniper_chains", {})
        chain_str = "  ".join(
            ("✅" if v else "❌") + " " + k.upper()[:3]
            for k, v in chains.items()
        )
        log      = ud.get("sniper_log", [])
        bought_n = sum(1 for e in log if e.get("bought"))
        skip_n   = len(log) - bought_n
        await q.edit_message_text(
            "🎯 *AI SNIPER v2*\n\n"
            "Status: *" + status + "*\n\n"
            "⛓️ Chains: " + chain_str + "\n"
            "💰 Daily Budget: *" + money(budget) + "*  (spent: " + money(spent) + ")\n"
            "📊 Session: *" + str(bought_n) + " bought*, " + str(skip_n) + " skipped\n\n"
            "📡 *Scan:* New launches with complete profiles (Twitter + TG)\n\n"
            "🔧 *Filters:*\n"
            "  Min Score: *"    + str(sf.get("min_score",62))          + "/100*\n"
            "  Min Liq: *"      + money(sf.get("min_liq",20_000))       + "*\n"
            "  Min Liq%: *"     + str(sf.get("min_liq_pct",8))          + "% of MC*\n"
            "  MC Range: *"     + mc_str(sf.get("min_mc",30_000))       + "* → *" + mc_str(sf.get("max_mc",500_000)) + "*\n"
            "  Max Age: *"      + str(sf.get("max_age_h",2))            + "h* (fresh launches only)\n"
            "  Max Top10%: *"   + str(sf.get("max_top10_pct",55))       + "%* (whale trap guard)\n"
            "  Min LP Burn: *"  + str(sf.get("min_lp_burn",80))         + "%* (SOL only)\n"
            "  Min Buys/1h: *"  + str(sf.get("min_buys_h1",30))        + "* (spread volume)\n"
            "  Min Buy%: *"     + str(sf.get("min_buy_pct",55))         + "%* (buy pressure)\n"
            "  Vol/MC Cap: *"   + str(sf.get("max_vol_mc_ratio",6.0))   + "x* (wash trading filter)\n"
            "  Buy Amount: *"   + money(sf.get("buy_amount",100))       + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🤖 Auto Mode",        callback_data="sniper_auto_menu"),
                 InlineKeyboardButton("🧠 Advisory Mode",    callback_data="sniper_adv_menu")],
                [InlineKeyboardButton("⛓️ Chain Selector",   callback_data="sniper_chains_menu"),
                 InlineKeyboardButton("⚙️ Filters",          callback_data="sniper_filters_menu")],
                [InlineKeyboardButton("📋 Sniper Log",       callback_data="sniper_log_view"),
                 InlineKeyboardButton("💰 Daily Budget",     callback_data="sniper_budget_cfg")],
                [InlineKeyboardButton("👀 KOL Tracker",      callback_data="kol_menu")],
                [InlineKeyboardButton("◀ Back",              callback_data="mm")],
            ])
        )

    elif cb == "sniper_auto_menu":
        auto_on  = ud.get("sniper_auto", False)
        notify   = ud.get("sniper_auto_notify", True)
        sl_on    = ud.get("sniper_auto_sl", True)
        tp_on    = ud.get("sniper_auto_tp", True)
        sl_pct   = ud.get("sniper_auto_sl_pct", 40.0)
        tp_xs    = ud.get("sniper_auto_tp_x", [2.0, 5.0])
        tp_str   = " + ".join(str(x) + "x" for x in tp_xs)
        await q.edit_message_text(
            "🤖 *AUTO SNIPER MODE*\n\n"
            "AI analyzes every token. If it says SNIPE, the bot buys automatically,\n"
            "sets stop loss and take profit, and exits on dump detection.\n\n"
            "Status: *" + ("🟢 ON" if auto_on else "🔴 OFF") + "*\n"
            "Notifications: *" + ("ON 🔔" if notify else "OFF 🔕") + "*\n"
            "Auto Stop Loss: *" + ("ON — " + str(sl_pct) + "%" if sl_on else "OFF") + "*\n"
            "Auto Take Profit: *" + ("ON — " + tp_str if tp_on else "OFF") + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(("🔴 Disable Auto" if auto_on else "🟢 Enable Auto"), callback_data="sniper_auto_toggle")],
                [InlineKeyboardButton(("🔕 Mute Notifications" if notify else "🔔 Unmute Notifications"), callback_data="sniper_auto_notif")],
                [InlineKeyboardButton(("✅ Stop Loss ON" if sl_on else "❌ Stop Loss OFF"), callback_data="sniper_sl_toggle"),
                 InlineKeyboardButton("⚙️ SL %", callback_data="sniper_sl_pct_cfg")],
                [InlineKeyboardButton(("✅ Take Profit ON" if tp_on else "❌ Take Profit OFF"), callback_data="sniper_tp_toggle"),
                 InlineKeyboardButton("⚙️ TP Targets", callback_data="sniper_tp_cfg")],
                [InlineKeyboardButton("◀ Back", callback_data="v_sniper")],
            ])
        )

    elif cb == "sniper_adv_menu":
        adv_on   = ud.get("sniper_advisory", False)
        notify   = ud.get("sniper_adv_notify", True)
        ch_id    = ud.get("sniper_broadcast_channel")
        ch_name  = ud.get("sniper_broadcast_name", "")
        ch_line  = ("📡 Broadcast: *" + ch_name + "*") if ch_id else "📡 Broadcast: *Not set*"
        await q.edit_message_text(
            "🧠 *AI ADVISORY MODE*\n\n"
            "AI analyzes each token and sends a compact notification to your DM.\n"
            "Tap 👁 View Analysis to see the full report.\n"
            "You confirm or skip — full control stays with you.\n\n"
            "Status: *" + ("🟢 ON" if adv_on else "🔴 OFF") + "*\n\n"
            "📬 *Notification Mode:*\n"
            + ("🔔 *DM Mode* — signals sent to YOUR DM only\n   Channel is silent." if notify else
               "📡 *Channel Mode* — signals sent to CHANNEL only\n   Your DM receives nothing.") + "\n\n"
            + ch_line,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(("🔴 Disable Advisory" if adv_on else "🟢 Enable Advisory"), callback_data="sniper_adv_toggle")],
                [InlineKeyboardButton(("🔕 Mute DM Notifs" if notify else "🔔 Unmute DM Notifs"), callback_data="sniper_adv_notif")],
                [InlineKeyboardButton(("📡 Change Channel" if ch_id else "📡 Set Broadcast Channel"), callback_data="sniper_channel_setup")],
                [InlineKeyboardButton("🗑 Remove Channel", callback_data="sniper_channel_remove")] if ch_id else [],
                [InlineKeyboardButton("◀ Back", callback_data="v_sniper")],
            ])
        )

    elif cb == "sniper_auto_toggle":
        turning_on = not ud.get("sniper_auto", False)
        # Mutual exclusion: if enabling Auto, Advisory must be OFF
        if turning_on and ud.get("sniper_advisory", False):
            ud["sniper_advisory"] = False   # auto-disable Advisory mode
            switch_note = "\n⚠️ *Advisory Mode was switched OFF automatically.*"
        else:
            switch_note = ""
        ud["sniper_auto"] = turning_on
        auto_on = ud["sniper_auto"]
        notify  = ud.get("sniper_auto_notify", True)
        sl_on   = ud.get("sniper_auto_sl", True)
        tp_on   = ud.get("sniper_auto_tp", True)
        sl_pct  = ud.get("sniper_auto_sl_pct", 40.0)
        tp_xs   = ud.get("sniper_auto_tp_x", [2.0, 5.0])
        tp_str  = " + ".join(str(x) + "x" for x in tp_xs)
        await q.edit_message_text(
            "🤖 *AUTO SNIPER MODE*\n\n"
            "AI analyzes every token. If it says SNIPE, the bot buys automatically,\n"
            "sets stop loss and take profit, and exits on dump detection.\n\n"
            "Status: *" + ("🟢 ON" if auto_on else "🔴 OFF") + "*\n"
            "Notifications: *" + ("ON 🔔" if notify else "OFF 🔕") + "*\n"
            "Auto Stop Loss: *" + ("ON — " + str(sl_pct) + "%" if sl_on else "OFF") + "*\n"
            "Auto Take Profit: *" + ("ON — " + tp_str if tp_on else "OFF") + "*"
            + switch_note,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(("🔴 Disable Auto" if auto_on else "🟢 Enable Auto"), callback_data="sniper_auto_toggle")],
                [InlineKeyboardButton(("🔕 Mute Notifications" if notify else "🔔 Unmute Notifications"), callback_data="sniper_auto_notif")],
                [InlineKeyboardButton(("✅ Stop Loss ON" if sl_on else "❌ Stop Loss OFF"), callback_data="sniper_sl_toggle"),
                 InlineKeyboardButton("⚙️ SL %", callback_data="sniper_sl_pct_cfg")],
                [InlineKeyboardButton(("✅ Take Profit ON" if tp_on else "❌ Take Profit OFF"), callback_data="sniper_tp_toggle"),
                 InlineKeyboardButton("⚙️ TP Targets", callback_data="sniper_tp_cfg")],
                [InlineKeyboardButton("◀ Back", callback_data="v_sniper")],
            ])
        )

    elif cb == "sniper_adv_toggle":
        turning_on = not ud.get("sniper_advisory", False)
        # Mutual exclusion: if enabling Advisory, Auto must be OFF
        if turning_on and ud.get("sniper_auto", False):
            ud["sniper_auto"] = False   # auto-disable Auto mode
            switch_note = "\n\n⚠️ *Auto Mode was switched OFF automatically.*"
        else:
            switch_note = ""
        ud["sniper_advisory"] = turning_on
        adv_on = ud["sniper_advisory"]
        notify = ud.get("sniper_adv_notify", True)
        await q.edit_message_text(
            "🧠 *AI ADVISORY MODE*\n\n"
            "AI analyzes each token and sends you a full report with verdict, thesis,\n"
            "red flags, green flags and a suggested entry amount.\n"
            "You confirm or skip — full control stays with you.\n\n"
            "Status: *" + ("🟢 ON" if adv_on else "🔴 OFF") + "*\n"
            "Notifications: *" + ("ON 🔔" if notify else "OFF 🔕") + "*"
            + switch_note,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(("🔴 Disable Advisory" if adv_on else "🟢 Enable Advisory"), callback_data="sniper_adv_toggle")],
                [InlineKeyboardButton(("📡 Switch to Channel Mode" if notify else "🔔 Switch to DM Mode"), callback_data="sniper_adv_notif")],
                [InlineKeyboardButton("◀ Back", callback_data="v_sniper")],
            ])
        )

    elif cb == "sniper_auto_notif":
        ud["sniper_auto_notify"] = not ud.get("sniper_auto_notify", True)
        auto_on = ud.get("sniper_auto", False)
        notify  = ud["sniper_auto_notify"]
        sl_on   = ud.get("sniper_auto_sl", True)
        tp_on   = ud.get("sniper_auto_tp", True)
        sl_pct  = ud.get("sniper_auto_sl_pct", 40.0)
        tp_xs   = ud.get("sniper_auto_tp_x", [2.0, 5.0])
        tp_str  = " + ".join(str(x) + "x" for x in tp_xs)
        await q.edit_message_text(
            "🤖 *AUTO SNIPER MODE*\n\n"
            "AI analyzes every token. If it says SNIPE, the bot buys automatically,\n"
            "sets stop loss and take profit, and exits on dump detection.\n\n"
            "Status: *" + ("🟢 ON" if auto_on else "🔴 OFF") + "*\n"
            "Notifications: *" + ("ON 🔔" if notify else "OFF 🔕") + "*\n"
            "Auto Stop Loss: *" + ("ON — " + str(sl_pct) + "%" if sl_on else "OFF") + "*\n"
            "Auto Take Profit: *" + ("ON — " + tp_str if tp_on else "OFF") + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(("🔴 Disable Auto" if auto_on else "🟢 Enable Auto"), callback_data="sniper_auto_toggle")],
                [InlineKeyboardButton(("🔕 Mute Notifications" if notify else "🔔 Unmute Notifications"), callback_data="sniper_auto_notif")],
                [InlineKeyboardButton(("✅ Stop Loss ON" if sl_on else "❌ Stop Loss OFF"), callback_data="sniper_sl_toggle"),
                 InlineKeyboardButton("⚙️ SL %", callback_data="sniper_sl_pct_cfg")],
                [InlineKeyboardButton(("✅ Take Profit ON" if tp_on else "❌ Take Profit OFF"), callback_data="sniper_tp_toggle"),
                 InlineKeyboardButton("⚙️ TP Targets", callback_data="sniper_tp_cfg")],
                [InlineKeyboardButton("◀ Back", callback_data="v_sniper")],
            ])
        )

    elif cb == "sniper_adv_notif":
        ud["sniper_adv_notify"] = not ud.get("sniper_adv_notify", True)
        adv_on = ud.get("sniper_advisory", False)
        notify = ud["sniper_adv_notify"]
        await q.edit_message_text(
            "🧠 *AI ADVISORY MODE*\n\n"
            "AI analyzes each token and sends you a full report with verdict, thesis,\n"
            "red flags, green flags and a suggested entry amount.\n"
            "You confirm or skip — full control stays with you.\n\n"
            "Status: *" + ("🟢 ON" if adv_on else "🔴 OFF") + "*\n"
            "Notifications: *" + ("ON 🔔" if notify else "OFF 🔕") + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(("🔴 Disable Advisory" if adv_on else "🟢 Enable Advisory"), callback_data="sniper_adv_toggle")],
                [InlineKeyboardButton(("📡 Switch to Channel Mode" if notify else "🔔 Switch to DM Mode"), callback_data="sniper_adv_notif")],
                [InlineKeyboardButton("◀ Back", callback_data="v_sniper")],
            ])
        )

    elif cb == "sniper_channel_setup":
        # Ask user to paste their channel/group ID
        pending[u.id] = {"action": "sniper_channel_input"}
        await q.edit_message_text(
            "📡 *SET BROADCAST CHANNEL*\n\n"
            "The bot will post full AI signal cards to your channel or group.\n\n"
            "*How to get your channel/group ID:*\n"
            "1️⃣ Add @userinfobot to your channel/group\n"
            "2️⃣ It will reply with the ID (e.g. `-1001234567890`)\n"
            "3️⃣ Also make sure *Auracle-xbot* is an admin in the channel/group\n\n"
            "Then paste the ID below 👇\n\n"
            "_Example: -1001234567890_",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ Cancel", callback_data="sniper_adv_menu")]
            ])
        )

    elif cb == "sniper_channel_remove":
        ud["sniper_broadcast_channel"] = None
        ud["sniper_broadcast_name"] = ""
        ch_id  = None
        adv_on = ud.get("sniper_advisory", False)
        notify = ud.get("sniper_adv_notify", True)
        await q.edit_message_text(
            "🧠 *AI ADVISORY MODE*\n\n"
            "AI analyzes each token and sends a compact notification to your DM.\n"
            "Tap 👁 View Analysis to see the full report.\n"
            "You confirm or skip — full control stays with you.\n\n"
            "Status: *" + ("🟢 ON" if adv_on else "🔴 OFF") + "*\n"
            "DM Notifications: *" + ("ON 🔔" if notify else "OFF 🔕") + "*\n"
            "📡 Broadcast: *Removed ✅*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(("🔴 Disable Advisory" if adv_on else "🟢 Enable Advisory"), callback_data="sniper_adv_toggle")],
                [InlineKeyboardButton(("🔕 Mute DM Notifs" if notify else "🔔 Unmute DM Notifs"), callback_data="sniper_adv_notif")],
                [InlineKeyboardButton("📡 Set Broadcast Channel", callback_data="sniper_channel_setup")],
                [InlineKeyboardButton("◀ Back", callback_data="v_sniper")],
            ])
        )

    elif cb == "sniper_sl_toggle":
        ud["sniper_auto_sl"] = not ud.get("sniper_auto_sl", True)
        auto_on = ud.get("sniper_auto", False)
        notify  = ud.get("sniper_auto_notify", True)
        sl_on   = ud["sniper_auto_sl"]
        tp_on   = ud.get("sniper_auto_tp", True)
        sl_pct  = ud.get("sniper_auto_sl_pct", 40.0)
        tp_xs   = ud.get("sniper_auto_tp_x", [2.0, 5.0])
        tp_str  = " + ".join(str(x) + "x" for x in tp_xs)
        await q.edit_message_text(
            "🤖 *AUTO SNIPER MODE*\n\n"
            "AI analyzes every token. If it says SNIPE, the bot buys automatically,\n"
            "sets stop loss and take profit, and exits on dump detection.\n\n"
            "Status: *" + ("🟢 ON" if auto_on else "🔴 OFF") + "*\n"
            "Notifications: *" + ("ON 🔔" if notify else "OFF 🔕") + "*\n"
            "Auto Stop Loss: *" + ("ON — " + str(sl_pct) + "%" if sl_on else "OFF") + "*\n"
            "Auto Take Profit: *" + ("ON — " + tp_str if tp_on else "OFF") + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(("🔴 Disable Auto" if auto_on else "🟢 Enable Auto"), callback_data="sniper_auto_toggle")],
                [InlineKeyboardButton(("🔕 Mute Notifications" if notify else "🔔 Unmute Notifications"), callback_data="sniper_auto_notif")],
                [InlineKeyboardButton(("✅ Stop Loss ON" if sl_on else "❌ Stop Loss OFF"), callback_data="sniper_sl_toggle"),
                 InlineKeyboardButton("⚙️ SL %", callback_data="sniper_sl_pct_cfg")],
                [InlineKeyboardButton(("✅ Take Profit ON" if tp_on else "❌ Take Profit OFF"), callback_data="sniper_tp_toggle"),
                 InlineKeyboardButton("⚙️ TP Targets", callback_data="sniper_tp_cfg")],
                [InlineKeyboardButton("◀ Back", callback_data="v_sniper")],
            ])
        )

    elif cb == "sniper_tp_toggle":
        ud["sniper_auto_tp"] = not ud.get("sniper_auto_tp", True)
        auto_on = ud.get("sniper_auto", False)
        notify  = ud.get("sniper_auto_notify", True)
        sl_on   = ud.get("sniper_auto_sl", True)
        tp_on   = ud["sniper_auto_tp"]
        sl_pct  = ud.get("sniper_auto_sl_pct", 40.0)
        tp_xs   = ud.get("sniper_auto_tp_x", [2.0, 5.0])
        tp_str  = " + ".join(str(x) + "x" for x in tp_xs)
        await q.edit_message_text(
            "🤖 *AUTO SNIPER MODE*\n\n"
            "AI analyzes every token. If it says SNIPE, the bot buys automatically,\n"
            "sets stop loss and take profit, and exits on dump detection.\n\n"
            "Status: *" + ("🟢 ON" if auto_on else "🔴 OFF") + "*\n"
            "Notifications: *" + ("ON 🔔" if notify else "OFF 🔕") + "*\n"
            "Auto Stop Loss: *" + ("ON — " + str(sl_pct) + "%" if sl_on else "OFF") + "*\n"
            "Auto Take Profit: *" + ("ON — " + tp_str if tp_on else "OFF") + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(("🔴 Disable Auto" if auto_on else "🟢 Enable Auto"), callback_data="sniper_auto_toggle")],
                [InlineKeyboardButton(("🔕 Mute Notifications" if notify else "🔔 Unmute Notifications"), callback_data="sniper_auto_notif")],
                [InlineKeyboardButton(("✅ Stop Loss ON" if sl_on else "❌ Stop Loss OFF"), callback_data="sniper_sl_toggle"),
                 InlineKeyboardButton("⚙️ SL %", callback_data="sniper_sl_pct_cfg")],
                [InlineKeyboardButton(("✅ Take Profit ON" if tp_on else "❌ Take Profit OFF"), callback_data="sniper_tp_toggle"),
                 InlineKeyboardButton("⚙️ TP Targets", callback_data="sniper_tp_cfg")],
                [InlineKeyboardButton("◀ Back", callback_data="v_sniper")],
            ])
        )

    elif cb == "sniper_sl_pct_cfg":
        pending[u.id] = {"action": "sniper_sl_pct"}
        await q.edit_message_text(
            "🛑 Enter auto stop loss %\nExample: 35\n\nNote: AI tightens this to 20% if rug risk is HIGH.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_auto_menu")]])
        )

    elif cb == "sniper_tp_cfg":
        pending[u.id] = {"action": "sniper_tp_x"}
        await q.edit_message_text(
            "🎯 Enter take profit targets as X multiples:\nFormat: x1 x2 x3\nExample: 2 5 10\n\n"
            "Bot sells equal portions at each target.\n2 5 = 50% at 2x, 50% at 5x\n2 5 10 = 33% at each",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_auto_menu")]])
        )

    elif cb == "sniper_cfg_buys_h1":
        pending[u.id] = {"action": "sniper_buys_h1", "_prompt_msg_id": None}
        m = await q.edit_message_text(
            "📊 *MIN BUYS PER HOUR*\n\n"
            "Minimum number of buy transactions in the last hour.\n"
            "Current: *" + str(ud.get("sniper_filters", {}).get("min_buys_h1", 30)) + "*\n\n"
            "Enter a number (e.g. 20):",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Cancel", callback_data="sniper_filters_menu")]]),
        )
        pending[u.id]["_prompt_msg_id"] = m.message_id

    elif cb == "sniper_cfg_buy_pct":
        pending[u.id] = {"action": "sniper_buy_pct", "_prompt_msg_id": None}
        m = await q.edit_message_text(
            "📉 *MIN BUY PRESSURE %*\n\n"
            "Minimum % of transactions that must be buys (H1 window).\n"
            "Current: *" + str(ud.get("sniper_filters", {}).get("min_buy_pct", 52)) + "%*\n\n"
            "Enter a number 40-80 (e.g. 55):",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Cancel", callback_data="sniper_filters_menu")]]),
        )
        pending[u.id]["_prompt_msg_id"] = m.message_id

    elif cb == "sniper_cfg_vol_mc":
        pending[u.id] = {"action": "sniper_vol_mc", "_prompt_msg_id": None}
        m = await q.edit_message_text(
            "🚿 *VOL/MC RATIO CAP*\n\n"
            "Max allowed Volume/MC ratio. Above this = wash trading.\n"
            "Current: *" + str(ud.get("sniper_filters", {}).get("max_vol_mc_ratio", 8.0)) + "x*\n\n"
            "Enter a number (e.g. 6.0):",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Cancel", callback_data="sniper_filters_menu")]]),
        )
        pending[u.id]["_prompt_msg_id"] = m.message_id

    elif cb == "sniper_budget_cfg":
        pending[u.id] = {"action": "sniper_budget", "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "💰 Enter daily sniper budget in USD:\nExample: 300\n\nSniper stops buying once this is spent in a day.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="v_sniper")]])
        )

    elif cb == "sniper_cfg_score":
        pending[u.id] = {"action": "sniper_score", "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "📊 Enter minimum sniper score (0–100):\nExample: 45\n\nTokens below this score are skipped.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]])
        )

    elif cb == "sniper_cfg_liq":
        pending[u.id] = {"action": "sniper_liq", "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "💧 Enter minimum liquidity in USD:\nExample: 15000",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]])
        )

    elif cb == "sniper_cfg_mc":
        pending[u.id] = {"action": "sniper_mc", "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "📈 Enter MC range:\nFormat: min max\nExample: 20000 1000000",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]])
        )

    elif cb == "sniper_cfg_age":
        pending[u.id] = {"action": "sniper_age", "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "⏰ Enter max token age in hours:\nExample: 6\n\nRecommended: 3–6h for fresh launches.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]])
        )

    elif cb == "sniper_cfg_amt":
        pending[u.id] = {"action": "sniper_amt", "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "💵 Enter buy amount per snipe in USD:\nExample: 100",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]])
        )

    elif cb == "sniper_cfg_liq_pct":
        pending[u.id] = {"action": "sniper_liq_pct", "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "📉 *MIN LIQUIDITY % OF MC*\n\n"
            "Liquidity must be at least this % of market cap.\n"
            "Low liq% = easy rug pull.\n"
            "Current: *" + str(ud.get("sniper_filters", {}).get("min_liq_pct", 8)) + "%*\n\n"
            "Enter a number 3–30 (recommended: 8):",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Cancel", callback_data="sniper_filters_menu")]])
        )

    elif cb == "sniper_cfg_top10":
        pending[u.id] = {"action": "sniper_top10", "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "🐳 *MAX TOP 10 WALLETS %*\n\n"
            "Skip if top 10 wallets hold more than this % of supply.\n"
            "High concentration = whale dump risk.\n"
            "Current: *" + str(ud.get("sniper_filters", {}).get("max_top10_pct", 55)) + "%*\n\n"
            "Enter a number 30–80 (recommended: 55):",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Cancel", callback_data="sniper_filters_menu")]])
        )

    elif cb == "sniper_cfg_lp_burn":
        pending[u.id] = {"action": "sniper_lp_burn", "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "🔥 *MIN LP BURN % (Solana only)*\n\n"
            "LP must be at least this % burned.\n"
            "Unburned LP = dev can pull liquidity.\n"
            "Current: *" + str(ud.get("sniper_filters", {}).get("min_lp_burn", 80)) + "%*\n\n"
            "Enter a number 0–100 (recommended: 80):",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Cancel", callback_data="sniper_filters_menu")]])
        )

    elif cb == "sniper_chains_menu":
        chains = ud.get("sniper_chains", {
            "solana": True, "ethereum": True, "base": True, "bsc": True, "arbitrum": True
        })
        chain_icons = {"solana":"🟣","ethereum":"🔷","base":"🔵","bsc":"🟡","arbitrum":"🔶"}
        buttons = []
        for chain, enabled in chains.items():
            icon  = chain_icons.get(chain, "⚪")
            label = icon + " " + chain.upper()[:3] + " " + ("✅" if enabled else "❌")
            buttons.append([InlineKeyboardButton(label, callback_data="sniper_chain_" + chain)])
        buttons.append([InlineKeyboardButton("◀ Back", callback_data="v_sniper")])
        await q.edit_message_text(
            "⛓️ *CHAIN SELECTOR*\n\nToggle each chain on/off for the sniper.\nOnly tokens from active chains will be analyzed.",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif cb.startswith("sniper_chain_"):
        chain  = cb[13:]
        chains = ud.setdefault("sniper_chains", {
            "solana": True, "ethereum": True, "base": True, "bsc": True, "arbitrum": True
        })
        chains[chain] = not chains.get(chain, True)
        # Refresh chain selector in-place
        chain_icons = {"solana":"🟣","ethereum":"🔷","base":"🔵","bsc":"🟡","arbitrum":"🔶"}
        buttons = []
        for c, enabled in chains.items():
            icon  = chain_icons.get(c, "⚪")
            label = icon + " " + c.upper()[:3] + " " + ("✅" if enabled else "❌")
            buttons.append([InlineKeyboardButton(label, callback_data="sniper_chain_" + c)])
        buttons.append([InlineKeyboardButton("◀ Back", callback_data="v_sniper")])
        await q.edit_message_text(
            "⛓️ *CHAIN SELECTOR*\n\nToggle each chain on/off for the sniper.\nOnly tokens from active chains will be analyzed.",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif cb == "sniper_filters_menu":
        sf = ud.get("sniper_filters", {})
        await q.edit_message_text(
            "⚙️ *SNIPER FILTERS*\n\n"
            "Tokens must pass ALL filters before AI analyzes them.\n\n"
            "Min Score: *"    + str(sf.get("min_score",   62))          + "/100*\n"
            "Min Liq: *"      + money(sf.get("min_liq",       20_000))   + "*\n"
            "Min Liq%: *"     + str(sf.get("min_liq_pct",      8))       + "% of MC*\n"
            "MC Range: *"     + mc_str(sf.get("min_mc",        30_000))   + "* → *" + mc_str(sf.get("max_mc", 500_000)) + "*\n"
            "Max Age: *"      + str(sf.get("max_age_h",  2))              + "h* (fresh only)\n"
            "Max Top10%: *"   + str(sf.get("max_top10_pct",   55))        + "%*\n"
            "Min LP Burn: *"  + str(sf.get("min_lp_burn",     80))        + "%* (SOL only)\n"
            "Min Buys/1h: *"  + str(sf.get("min_buys_h1", 30))           + "*\n"
            "Min Buy%: *"     + str(sf.get("min_buy_pct",   55))          + "%*\n"
            "Vol/MC Cap: *"   + str(sf.get("max_vol_mc_ratio", 6.0))      + "x*\n"
            "Buy Amount: *"   + money(sf.get("buy_amount",100))           + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Min Score",     callback_data="sniper_cfg_score"),
                 InlineKeyboardButton("💧 Min Liq",       callback_data="sniper_cfg_liq")],
                [InlineKeyboardButton("📉 Min Liq%",      callback_data="sniper_cfg_liq_pct"),
                 InlineKeyboardButton("📈 MC Range",      callback_data="sniper_cfg_mc")],
                [InlineKeyboardButton("⏰ Max Age",       callback_data="sniper_cfg_age"),
                 InlineKeyboardButton("🐳 Max Top10%",    callback_data="sniper_cfg_top10")],
                [InlineKeyboardButton("🔥 Min LP Burn%",  callback_data="sniper_cfg_lp_burn"),
                 InlineKeyboardButton("📊 Min Buys/1h",   callback_data="sniper_cfg_buys_h1")],
                [InlineKeyboardButton("📉 Min Buy%",      callback_data="sniper_cfg_buy_pct"),
                 InlineKeyboardButton("🚿 Vol/MC Cap",    callback_data="sniper_cfg_vol_mc")],
                [InlineKeyboardButton("💵 Buy Amount",    callback_data="sniper_cfg_amt")],
                [InlineKeyboardButton("◀ Back",           callback_data="v_sniper")],
            ])
        )

    elif cb.startswith("tc_"):
        # KOL alert "Trade on Auracle" button — show token card
        contract = cb[3:]
        if contract:
            await _show_token_card(q, u, ud, ctx, contract)
        else:
            await q.answer("Invalid token address.")

    elif cb == "sniper_log_view":
        log = ud.get("sniper_log", [])
        if not log:
            await q.edit_message_text("📋 *SNIPER LOG*\n\nNo activity yet.", parse_mode="Markdown", reply_markup=back_main())
            return
        bought    = [e for e in log if e.get("bought")]
        skipped   = [e for e in log if not e.get("bought")]
        sniper_trades = [tr for tr in trade_log.get(u.id, []) if tr.get("mood") in ("AI-Sniper","Sniper")]
        b_wr = 0
        if sniper_trades:
            s_wins = [tr for tr in sniper_trades if tr["realized_pnl"] > 0]
            b_wr   = round(len(s_wins) / len(sniper_trades) * 100)

        # Show newest 10 first
        recent = list(reversed(log))[:10]
        verdict_emoji = {"SNIPE":"🟢","SKIP":"🔴","WAIT":"🟡"}
        buttons = []
        for i, e in enumerate(recent):
            ve     = verdict_emoji.get(e.get("verdict","?"), "⚪")
            bought_tag = " 💵" if e.get("bought") else ""
            conf   = e.get("confidence", 0)
            label  = ve + " $" + e.get("symbol","?") + "  conf:" + str(conf) + "/10  " + e.get("chain","?").upper()[:3] + bought_tag
            # Index in the full log (from end)
            full_idx = len(log) - 1 - list(reversed(log)).index(e)
            buttons.append([InlineKeyboardButton(label, callback_data="snp_log_detail_" + str(full_idx))])

        buttons.append([
            InlineKeyboardButton("🗑 Clear Log",    callback_data="sniper_log_clear"),
            InlineKeyboardButton("🔄 Reset Memory", callback_data="sniper_reset_memory"),
        ])
        buttons.append([InlineKeyboardButton("◀ Back", callback_data="v_sniper")])

        await q.edit_message_text(
            "📋 *SNIPER LOG*\n\n"
            "Analyzed: *" + str(len(log)) + "*  |  Bought: *" + str(len(bought)) + "*  |  Skipped: *" + str(len(skipped)) + "*\n"
            "Sniper Win Rate: *" + str(b_wr) + "%*\n\n"
            "Tap any token for full AI breakdown 👇",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif cb.startswith("snp_log_detail_"):
        idx = int(cb[15:])
        log = ud.get("sniper_log", [])
        if idx < 0 or idx >= len(log):
            await q.edit_message_text("Entry not found.", reply_markup=back_main())
            return
        e = log[idx]
        verdict_emoji = {"SNIPE":"🟢","SKIP":"🔴","WAIT":"🟡"}.get(e.get("verdict","?"),"⚪")
        rug_emoji     = {"LOW":"✅","MEDIUM":"⚠️","HIGH":"🚨","UNKNOWN":"❓"}.get(e.get("rug_risk","?"),"❓")
        mom_emoji     = {"STRONG":"🚀","MODERATE":"📈","WEAK":"📉","NEGATIVE":"💀","UNKNOWN":"❓"}.get(e.get("momentum","?"),"❓")
        soc_emoji     = {"GOOD":"✅","PARTIAL":"⚠️","NONE":"🚨","UNKNOWN":"❓"}.get(e.get("social","?"),"❓")
        conf          = e.get("confidence", 0)
        conf_bar      = "█" * conf + "░" * (10 - conf)
        red_flags     = "\n".join("  🚨 " + f for f in e.get("red_flags", [])) or "  None"
        green_flags   = "\n".join("  ✅ " + f for f in e.get("green_flags", [])) or "  None"
        hard_flags    = "\n".join("  🚨 " + f for f in e.get("hard_flags", [])) or ""
        ts            = e.get("timestamp","")[:16].replace("T"," ")
        bought_line   = "\n💵 *BOUGHT: " + money(e.get("amount", 0)) + "*" if e.get("bought") else ""
        skip_stage    = e.get("skip_stage", "")
        stage_line    = "\n⛔ *Filtered at:* " + skip_stage if skip_stage else ""

        hard_flags_block = ("\n🚨 *Hard Flags:*\n" + hard_flags + "\n") if hard_flags else ""

        txt = (
            "🔍 *AI SNIPER DETAIL*\n"
            "━━━━━━━━━━━━━━━━\n"
            "*$" + e.get("symbol","?") + "*  " + e.get("chain","?").upper() + "  " + ts + "\n"
            "MC: *" + mc_str(e.get("mc",0)) + "*  |  Liq: *" + money(e.get("liq",0)) + "*\n"
            "🧠 Sniper Score: *" + str(e.get("score",0)) + "/100*" + stage_line + "\n\n"
            + verdict_emoji + " *Verdict: " + e.get("verdict","?") + "*" + bought_line + "\n"
            "Confidence: *" + str(conf) + "/10*  `" + conf_bar + "`\n\n"
            "📝 *Why:*\n" + (e.get("thesis","No analysis available.") or "No analysis available.") + "\n\n"
            "━━━━━━━━━━━━━━━━\n"
            + hard_flags_block
            + rug_emoji + " Rug Risk: *" + e.get("rug_risk","?") + "*\n"
            + mom_emoji + " Momentum: *" + e.get("momentum","?") + "*\n"
            + soc_emoji + " Socials: *" + e.get("social","?") + "*\n\n"
            "🚩 *Red Flags:*\n" + red_flags + "\n\n"
            "💚 *Green Flags:*\n" + green_flags
        )
        contract = e.get("contract","")
        kb_rows = []
        if contract:
            kb_rows.append([InlineKeyboardButton("🔎 View Token Live", callback_data="btt_" + contract)])
        kb_rows.append([InlineKeyboardButton("◀ Back to Log", callback_data="sniper_log_view")])
        await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb_rows))


    # ── KOL / SMART WALLET TRACKER UI ─────────────────────────────────────────
    elif cb == "kol_menu":
        wallets    = ud.get("kol_wallets", [])
        alerts_on  = ud.get("kol_alerts_on", True)
        helius_set = bool(os.environ.get("HELIUS_API_KEY", ""))
        helius_line = "✅ Helius connected" if helius_set else "⚠️ *HELIUS_API_KEY not set* — add it in Railway Variables"

        wallet_lines = ""
        if wallets:
            wallet_lines = "\n\n*Tracked Wallets:*\n" + "\n".join(
                "  " + str(i+1) + ". *" + w.get("label", "Unnamed") + "*\n"
                "     `" + w.get("address","")[:20] + "...`  " + w.get("chain","sol").upper()
                for i, w in enumerate(wallets)
            )
        else:
            wallet_lines = "\n\n_No wallets tracked yet. Add one below._"

        btns = [
            [InlineKeyboardButton("➕ Add Wallet",      callback_data="kol_add"),
             InlineKeyboardButton("🗑 Remove Wallet",   callback_data="kol_remove_menu")],
            [InlineKeyboardButton(("🔕 Mute Alerts" if alerts_on else "🔔 Unmute Alerts"), callback_data="kol_toggle_alerts")],
            [InlineKeyboardButton("◀ Back",             callback_data="v_sniper")],
        ]
        await q.edit_message_text(
            "👀 *KOL / SMART WALLET TRACKER*\n\n"
            + helius_line + "\n\n"
            "Track up to *10 Solana wallets*. Get instant alerts when they ape into a new token.\n"
            "Alerts include token score, MC, liquidity, and one-tap trade button.\n\n"
            "Alerts: *" + ("🔔 ON" if alerts_on else "🔕 OFF") + "*"
            + wallet_lines,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(btns)
        )

    elif cb == "kol_toggle_alerts":
        ud["kol_alerts_on"] = not ud.get("kol_alerts_on", True)
        alerts_on  = ud["kol_alerts_on"]
        wallets    = ud.get("kol_wallets", [])
        helius_set = bool(os.environ.get("HELIUS_API_KEY", ""))
        helius_line = "✅ Helius connected" if helius_set else "⚠️ *HELIUS_API_KEY not set* — add it in Railway Variables"
        wallet_lines = ""
        if wallets:
            wallet_lines = "\n\n*Tracked Wallets:*\n" + "\n".join(
                "  " + str(i+1) + ". *" + w.get("label", "Unnamed") + "*\n"
                "     `" + w.get("address","")[:20] + "...`  " + w.get("chain","sol").upper()
                for i, w in enumerate(wallets)
            )
        else:
            wallet_lines = "\n\n_No wallets tracked yet. Add one below._"
        btns = [
            [InlineKeyboardButton("➕ Add Wallet",      callback_data="kol_add"),
             InlineKeyboardButton("🗑 Remove Wallet",   callback_data="kol_remove_menu")],
            [InlineKeyboardButton(("🔕 Mute Alerts" if alerts_on else "🔔 Unmute Alerts"), callback_data="kol_toggle_alerts")],
            [InlineKeyboardButton("◀ Back",             callback_data="v_sniper")],
        ]
        await q.edit_message_text(
            "👀 *KOL / SMART WALLET TRACKER*\n\n"
            + helius_line + "\n\n"
            "Track up to *10 Solana wallets*. Get instant alerts when they ape into a new token.\n"
            "Alerts include token score, MC, liquidity, and one-tap trade button.\n\n"
            "Alerts: *" + ("🔔 ON" if alerts_on else "🔕 OFF") + "*"
            + wallet_lines,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(btns)
        )

    elif cb == "kol_add":
        if len(ud.get("kol_wallets", [])) >= MAX_KOL_WALLETS:
            await q.edit_message_text(
                f"⚠️ *Max {MAX_KOL_WALLETS} wallets reached.*\nRemove one before adding another.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="kol_menu")]])
            )
            return
        pending[u.id] = {"action": "kol_add_wallet"}
        await q.edit_message_text(
            "👀 *ADD KOL WALLET*\n\n"
            "Send the wallet address you want to track.\n\n"
            "Format: `<address>` or `<address> <label>`\n\n"
            "Examples:\n"
            "`7xKXtg... ` ← address only\n"
            "`7xKXtg... CryptoWhale` ← with label\n\n"
            "_Solana wallets only (requires Helius API key)_",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Cancel", callback_data="kol_menu")]])
        )

    elif cb == "kol_remove_menu":
        wallets = ud.get("kol_wallets", [])
        if not wallets:
            await q.answer("No wallets to remove.")
            return
        btns = [
            [InlineKeyboardButton("🗑 " + w.get("label", w.get("address","")[:10]+"..."), callback_data="kol_del_" + str(i))]
            for i, w in enumerate(wallets)
        ]
        btns.append([InlineKeyboardButton("◀ Back", callback_data="kol_menu")])
        await q.edit_message_text(
            "🗑 *REMOVE KOL WALLET*\n\nTap a wallet to remove it:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(btns)
        )

    elif cb.startswith("kol_del_"):
        idx = int(cb[8:])
        wallets = ud.get("kol_wallets", [])
        if 0 <= idx < len(wallets):
            removed = wallets.pop(idx)
            label   = removed.get("label", removed.get("address","?")[:10])
            # Clear cached last sig
            _kol_last_sig.get(u.id, {}).pop(removed.get("address",""), None)
            await q.edit_message_text(
                "✅ *" + label + "* removed from KOL tracker.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="kol_menu")]])
            )
        else:
            await q.answer("Wallet not found.")

    elif cb == "sniper_log_clear":
        ud["sniper_log"] = []
        # NOTE: sniper_seen is intentionally NOT cleared here.
        # That would cause all previously seen tokens to flood back.
        # Use "Reset Memory" in sniper settings to also clear seen.
        await q.edit_message_text(
            "✅ *Log cleared.*\n\n"
            "_Note: Token memory is preserved — previously seen tokens won't flood back._\n"
            "Use *Reset Memory* in Sniper Settings to start completely fresh.",
            parse_mode="Markdown",
            reply_markup=back_main()
        )

    elif cb == "sniper_reset_memory":
        ud["sniper_log"]  = []
        ud["sniper_seen"] = {}
        await q.edit_message_text(
            "🔄 *Full reset done.*\n\n"
            "Log cleared + token memory wiped.\n"
            "The sniper will re-evaluate all tokens it sees next run.",
            parse_mode="Markdown",
            reply_markup=back_main()
        )

    # Advisory confirm / skip
    elif cb.startswith("snp_confirm_"):
        rest    = cb[12:]
        parts   = rest.rsplit("_", 1)
        contract = parts[0]
        amount   = float(parts[1]) if len(parts) > 1 else float(ud.get("sniper_filters",{}).get("buy_amount",100))
        sniper_bought = ud.setdefault("sniper_bought", [])
        if contract in sniper_bought:
            await q.edit_message_text("Already bought this token.", reply_markup=back_main())
            return
        sniper_bought.append(contract)
        if len(sniper_bought) > 500:
            ud["sniper_bought"] = sniper_bought[-500:]
        _sniper_daily_reset(ud)
        ud["sniper_daily_spent"] = ud.get("sniper_daily_spent", 0) + amount
        async with get_user_lock(u.id):
            result = await do_buy_core(ud, u.id, contract, amount, planned=True, mood="AI-Sniper")
        if isinstance(result, str):
            await q.edit_message_text(result, reply_markup=main_menu_kb())
        else:
            info2, tokens = result
            await q.edit_message_text(
                "✅ *ADVISORY BUY CONFIRMED*\n\n"
                "*$" + info2["symbol"] + "*\n"
                "Bought: *" + money(amount) + "*\n"
                "Price: *" + money(info2["price"]) + "*\n"
                "Cash left: *" + money(ud["balance"]) + "*",
                parse_mode="Markdown",
                reply_markup=buy_done_kb(contract)
            )

    elif cb.startswith("snp_view_"):
        # User tapped "👁 View Analysis" on compact pill — show full AI report
        contract = cb[9:]
        cached = _sniper_analysis_cache.get(u.id, {}).get(contract)
        if not cached:
            # Cache expired — fetch fresh data
            info2 = await get_token(contract)
            if not info2:
                await q.edit_message_text("⚠️ Token data expired. Paste the CA to view it live.", reply_markup=back_main())
                return
            await q.edit_message_text("⚠️ AI analysis expired. Tap the token CA to re-scan.", reply_markup=back_main())
            return
        info2, sc2, ai2 = cached["info"], cached["sc"], cached["ai"]
        report = _ai_report_text(info2, sc2, ai2, contract=contract)
        kb_rows = []
        # Always show View Token Card first
        kb_rows.append([InlineKeyboardButton("🪙 View Token Card", callback_data="btt_" + contract)])
        if ai2["verdict"] == "SNIPE":
            kb_rows.append([
                InlineKeyboardButton(
                    "✅ Buy " + money(ai2["suggested_amount"]),
                    callback_data="snp_confirm_" + contract + "_" + str(round(ai2["suggested_amount"], 2))
                ),
                InlineKeyboardButton("❌ Skip", callback_data="snp_skip_" + contract),
            ])
        else:
            kb_rows.append([InlineKeyboardButton("❌ Dismiss", callback_data="snp_skip_" + contract)])
        kb_rows.append([InlineKeyboardButton("🏠 Main Menu", callback_data="mm")])
        await q.edit_message_text(
            report, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb_rows)
        )

    elif cb.startswith("snp_skip_"):
        contract = cb[9:]
        await q.edit_message_text("❌ Token skipped.", reply_markup=back_main())

    # ── DCA BY MARKET CAP ──────────────────────────────────────────────────────
    # ══════════════════════════════════════════════════════════════════════
    # QUICK BUY
    # ══════════════════════════════════════════════════════════════════════
    elif cb.startswith("qb_") and not cb.startswith("qb_set_") and not cb.startswith("qb_amt_"):
        contract = cb[3:]
        qb_amt   = ud.get("quick_buy_amount", 100.0)
        if qb_amt > ud.get("balance", 0):
            await q.edit_message_text(
                "❌ Insufficient balance for Quick Buy of " + money(qb_amt) + "\n"
                "Balance: " + money(ud.get("balance", 0)),
                parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("◀ Back to Token", callback_data="btt_" + contract)]
                ])
            )
            return
        if ud.get("mood_tracking", True):
            pending[u.id] = {"action": "buy_mood", "contract": contract, "amount": qb_amt}
            await q.edit_message_text(
                "🧠 *MOOD CHECK*\n\nWhy are you buying this?\n\n"
                "1 - Research\n2 - Chart looks good\n3 - Community tip\n4 - FOMO\n5 - Gut feeling\n\nReply with a number:",
                parse_mode="Markdown", reply_markup=cancel_kb()
            )
        else:
            await _cleanup_token_view(ctx.bot, u.id, q.message.chat_id)
            await do_buy_query(q, ud, u.id, contract, qb_amt)

    elif cb.startswith("qb_set_"):
        # Show quick-buy amount picker (from within buy submenu)
        contract = cb[7:]
        qb_amt   = ud.get("quick_buy_amount", 100.0)
        await q.edit_message_text(
            "⚡ *SET QUICK BUY AMOUNT*\n\n"
            "Current: *$" + str(int(qb_amt)) + "*\n\n"
            "One tap on the token card will instantly buy this amount.\n"
            "Choose a new default:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("$25",   callback_data="qb_amt_25_"   + contract),
                 InlineKeyboardButton("$50",   callback_data="qb_amt_50_"   + contract),
                 InlineKeyboardButton("$100",  callback_data="qb_amt_100_"  + contract),
                 InlineKeyboardButton("$250",  callback_data="qb_amt_250_"  + contract)],
                [InlineKeyboardButton("$500",  callback_data="qb_amt_500_"  + contract),
                 InlineKeyboardButton("$1000", callback_data="qb_amt_1000_" + contract),
                 InlineKeyboardButton("✏️ Custom", callback_data="qb_custom_" + contract)],
                [InlineKeyboardButton("◀ Back", callback_data="bts_" + contract)],
            ])
        )

    elif cb.startswith("qb_amt_"):
        rest     = cb[7:]
        amt_str, contract = rest.split("_", 1)
        ud["quick_buy_amount"] = float(amt_str)
        await q.edit_message_text(
            "✅ Quick Buy set to *$" + amt_str + "*\n\n"
            "Tap ⚡ Quick Buy $" + amt_str + " on any token card to instantly buy.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ Back to Token", callback_data="btt_" + contract)]
            ])
        )

    elif cb.startswith("qb_custom_"):
        contract = cb[10:]
        pending[u.id] = {"action": "qb_custom_input", "contract": contract, "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text("Enter your custom Quick Buy amount in USD:", reply_markup=cancel_kb())


    elif cb == "wl_settings":
        milestone_on = ud.get("milestone_notif", True)
        await q.edit_message_text(
            "⚙️ *WATCHLIST SETTINGS*\n\n"
            "🔔 *Watchlist Milestones*\n"
            "Get notified when a watched token hits 2× 3× 5× 10× from\n"
            "the MC it had when you added it.\n\n"
            "Status: *" + ("🟢 ON" if milestone_on else "🔴 OFF") + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    ("🔴 Disable Watchlist Milestones" if milestone_on else "🟢 Enable Watchlist Milestones"),
                    callback_data="milestone_wl_toggle"
                )],
                [InlineKeyboardButton("◀ Back to Watchlist", callback_data="v_watchlist")],
            ])
        )

    elif cb == "milestone_wl_toggle":
        ud["milestone_notif"] = not ud.get("milestone_notif", True)
        on = ud["milestone_notif"]
        await q.edit_message_text(
            "🔔 Watchlist Milestones *" + ("enabled ✅" if on else "disabled ❌") + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ Back", callback_data="wl_settings")]
            ])
        )

    # ══════════════════════════════════════════════════════════════════════
    # MILESTONE NOTIFICATIONS (More menu)
    # ══════════════════════════════════════════════════════════════════════
    elif cb == "v_milestone_notif":
        ms_on   = ud.get("milestone_notif", True)
        dump_on = ud.get("milestone_notif_dump", True)
        await q.edit_message_text(
            "🚀 *MILESTONE NOTIFICATIONS*\n\n"
            "Get notified each time a holding hits a new multiplier.\n"
            "Each level fires *once* per position — no repeat spam.\n\n"
            "🚀 *Holdings Milestones:*  2× · 3× · 5× · 10× · 20× · 50×\n"
            "Status: *" + ("🟢 ON" if ms_on else "🔴 OFF") + "*\n\n"
            "🚨 *Dump Alert:*  fires once at –50%\n"
            "Status: *" + ("🟢 ON" if dump_on else "🔴 OFF") + "*\n\n"
            "👁 *Watchlist Milestones:* 2× 3× 5× 10× from add-time MC\n"
            "_(toggle in Watchlist → ⚙️ Settings)_",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    ("🔴 Disable Holdings Milestones" if ms_on else "🟢 Enable Holdings Milestones"),
                    callback_data="milestone_toggle"
                )],
                [InlineKeyboardButton(
                    ("🔴 Disable Dump Alert" if dump_on else "🟢 Enable Dump Alert"),
                    callback_data="milestone_dump_toggle"
                )],
                [InlineKeyboardButton("◀ Back to More", callback_data="v_more")],
            ])
        )

    elif cb == "milestone_toggle":
        ud["milestone_notif"] = not ud.get("milestone_notif", True)
        on = ud["milestone_notif"]
        await q.edit_message_text(
            "🚀 Holdings Milestones *" + ("enabled ✅" if on else "disabled ❌") + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="v_milestone_notif")]])
        )

    elif cb == "milestone_dump_toggle":
        ud["milestone_notif_dump"] = not ud.get("milestone_notif_dump", True)
        on = ud["milestone_notif_dump"]
        await q.edit_message_text(
            "🚨 Dump Alert *" + ("enabled ✅" if on else "disabled ❌") + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="v_milestone_notif")]])
        )

    # ══════════════════════════════════════════════════════════════════════
    # RUG PULL WARNING (More menu — OFF by default)
    # ══════════════════════════════════════════════════════════════════════
    elif cb == "v_rug_warn":
        rw_on  = ud.get("rug_warn_enabled", False)
        thresh = ud.get("rug_warn_threshold", 30)
        await q.edit_message_text(
            "🔥 *RUG PULL EARLY WARNING*\n\n"
            "Monitors liquidity on every token you hold.\n"
            "If LP drops *–" + str(thresh) + "% or more* within one scan cycle,\n"
            "you get an instant alert with a one-tap Sell Everything button.\n\n"
            "Status: *" + ("🟢 ON" if rw_on else "🔴 OFF") + "* (global OFF by default)\n"
            "Trigger threshold: *–" + str(thresh) + "% liq drop*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    ("🔴 Disable Rug Warning" if rw_on else "🟢 Enable Rug Warning"),
                    callback_data="rug_warn_toggle"
                )],
                [InlineKeyboardButton("Threshold: –20%", callback_data="rug_thresh_20"),
                 InlineKeyboardButton("Threshold: –30%", callback_data="rug_thresh_30"),
                 InlineKeyboardButton("Threshold: –50%", callback_data="rug_thresh_50")],
                [InlineKeyboardButton("◀ Back to More", callback_data="v_more")],
            ])
        )

    elif cb == "rug_warn_toggle":
        ud["rug_warn_enabled"] = not ud.get("rug_warn_enabled", False)
        on = ud["rug_warn_enabled"]
        await q.edit_message_text(
            "🔥 Rug Pull Warning *" + ("enabled ✅" if on else "disabled ❌") + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="v_rug_warn")]])
        )

    elif cb.startswith("rug_thresh_"):
        ud["rug_warn_threshold"] = int(cb[11:])
        thresh = ud["rug_warn_threshold"]
        await q.edit_message_text(
            "✅ Rug Warning threshold set to *–" + str(thresh) + "%*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="v_rug_warn")]])
        )

    # ══════════════════════════════════════════════════════════════════════
    # COPY TRADE REPLAY + SETTINGS
    # ══════════════════════════════════════════════════════════════════════
    elif cb == "copy_replay":
        logs = trade_log.get(u.id, [])
        closed = [t for t in logs if t.get("exit_price")]
        if not closed:
            await q.edit_message_text(
                "📽 *TRADE REPLAY*\n\nNo closed trades with replay data yet.\nClose a position to see its full timeline.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="v_copy")]])
            )
            return
        buttons = []
        for i, t2 in enumerate(reversed(closed[-10:])):
            real_i = len(closed) - 1 - i
            pnl_tag = "✅" if t2["realized_pnl"] >= 0 else "❌"
            lbl = pnl_tag + " $" + t2["symbol"] + "  " + str(round(t2.get("x",0),1)) + "x  " + t2["reason"]
            buttons.append([InlineKeyboardButton(lbl, callback_data="copy_replay_" + str(real_i))])
        buttons.append([InlineKeyboardButton("◀ Back", callback_data="v_copy")])
        await q.edit_message_text(
            "📽 *TRADE REPLAY*\n\nSelect a trade to review its full timeline:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif cb.startswith("copy_replay_") and cb[12:].isdigit():
        idx  = int(cb[12:])
        logs = trade_log.get(u.id, [])
        closed = [t2 for t2 in logs if t2.get("exit_price")]
        if idx >= len(closed):
            await q.edit_message_text("Trade not found.", reply_markup=back_main())
            return
        t2 = closed[idx]
        sym    = t2["symbol"]
        inv    = t2["invested"]
        ret    = t2["returned"]
        pnl    = t2["realized_pnl"]
        x_val  = t2.get("x", 0)
        reason = t2.get("reason", "manual")
        ep     = t2.get("exit_price", 0)
        ap     = t2.get("avg_price", 0)
        pp     = t2.get("peak_price", ep)
        hold_h = t2.get("hold_h", 0)
        bought_at = t2.get("bought_at", datetime.now())
        peak_x = round(pp / ap, 2) if ap > 0 else 0
        left_on_table = round((pp / ep - 1) * ret, 2) if ep > 0 and pp > ep else 0
        closed_at = t2.get("closed_at", datetime.now())
        entry_time = bought_at.strftime("%b %d %H:%M") if hasattr(bought_at,"strftime") else "?"
        exit_time  = closed_at.strftime("%b %d %H:%M") if hasattr(closed_at,"strftime") else "?"
        pnl_sign = "+" if pnl >= 0 else ""
        peak_note = ""
        if left_on_table > 1:
            peak_note = "\n_(left " + money(left_on_table) + " on the table — peak was " + str(peak_x) + "×)_"
        await q.edit_message_text(
            "📽 *TRADE REPLAY — $" + sym + "*\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "🟦 *ENTRY*  ·  " + entry_time + "\n"
            "  Invested: *" + money(inv) + "*  @  *" + money(ap) + "*\n\n"
            "⭐ *PEAK PRICE*\n"
            "  *" + money(pp) + "*  (" + str(peak_x) + "×)\n\n"
            "🔴 *EXIT*  ·  " + exit_time + "  ·  " + reason + "\n"
            "  Received: *" + money(ret) + "*  @  *" + money(ep) + "*\n\n"
            "📋 *RESULT*  ·  held *" + str(hold_h) + "h*\n"
            "  " + pnl_sign + money(pnl) + "  ·  *" + str(round(x_val,2)) + "×*"
            + peak_note,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ Back to Replay", callback_data="copy_replay")],
                [InlineKeyboardButton("🏠 Main Menu",     callback_data="mm")],
            ])
        )

    elif cb == "copy_settings":
        await q.edit_message_text(
            "⚙️ *COPY TRADE SETTINGS*\n\n"
            "📽 *Trade Replay* — view full timeline of any closed trade.\n"
            "  Access: Copy Trading → 📽 Replay\n\n"
            "⭐ *Peak vs Exit* — see how much you left on the table.\n\n"
            "All features are always on for closed trades.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📽 View Replays", callback_data="copy_replay")],
                [InlineKeyboardButton("◀ Back",          callback_data="v_copy")],
            ])
        )

    elif cb.startswith("dca_") and not cb.startswith("dca_cancel_") and not cb.startswith("dca_confirm_") and not cb.startswith("dca_addmore_") and not cb.startswith("dca_setmc_") and not cb.startswith("dca_setamt_") and not cb.startswith("dca_amt_quick_") and cb != "v_dca":
        contract = cb[4:]
        info = await get_token(contract)
        sym    = info["symbol"] if info else "?"
        cur_mc = mc_str(info["mc"]) if info else "unknown"
        existing = [d for d in ud.get("dca_orders", []) if d["contract"] == contract and not d.get("cancelled")]

        # Start fresh DCA session in pending
        pending[u.id] = {
            "action":   "dca_build",
            "contract": contract,
            "symbol":   sym,
            "targets":  [],  # list of {mc, amount} being built
        }

        ex_txt = ""
        if existing:
            ex_txt = "\n*Current plan:*\n"
            for tgt in existing[0].get("mc_targets", []):
                status = "✅" if tgt.get("triggered") else "⏳"
                ex_txt += status + " " + mc_str(tgt["mc"]) + " → buy " + money(tgt["amount"]) + "\n"

        await q.edit_message_text(
            "📉 *DCA BY MARKET CAP*\n\n"
            "*$" + sym + "*  |  Current MC: *" + cur_mc + "*\n" + ex_txt + "\n"
            "Build your DCA plan step by step.\n"
            "Tap *Set MC Target* to add your first trigger:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📈 Set MC Target", callback_data="dca_setmc_" + contract)],
                [InlineKeyboardButton("❌ Cancel Existing", callback_data="dca_cancel_" + contract)],
                [InlineKeyboardButton("◀ Back",            callback_data="btt_" + contract)],
            ])
        )

    elif cb.startswith("dca_setmc_"):
        contract = cb[10:]
        p = pending.get(u.id, {})
        if p.get("action") not in ("dca_build", "dca_addmore"):
            # Re-init if pending was lost
            info = await get_token(contract)
            pending[u.id] = {"action": "dca_build", "contract": contract,
                             "symbol": info["symbol"] if info else "?", "targets": []}
            p = pending[u.id]
        pending[u.id]["action"] = "dca_mc_input"
        targets = p.get("targets", [])
        step = len(targets) + 1
        await q.edit_message_text(
            "📈 *DCA TARGET " + str(step) + " — SET MC*\n\n"
            "Enter the market cap that should trigger this buy.\n\n"
            "Examples:\n"
            "  `500000`  = $500K\n"
            "  `1000000` = $1M\n"
            "  `5000000` = $5M",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ Back", callback_data="dca_" + contract)],
            ])
        )

    elif cb.startswith("dca_setamt_"):
        contract = cb[11:]
        p = pending.get(u.id, {})
        mc_val = p.get("pending_mc")
        if not mc_val:
            await q.edit_message_text("Session expired. Please restart DCA.", reply_markup=back_main())
            return
        pending[u.id]["action"] = "dca_amt_input"
        await q.edit_message_text(
            "💵 *DCA TARGET — SET BUY AMOUNT*\n\n"
            "MC trigger: *" + mc_str(mc_val) + "*\n\n"
            "How much USD to buy when this MC is hit?\n\n"
            "Examples:  `50`  `100`  `250`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("$50",  callback_data="dca_amt_quick_50_"  + contract),
                 InlineKeyboardButton("$100", callback_data="dca_amt_quick_100_" + contract),
                 InlineKeyboardButton("$250", callback_data="dca_amt_quick_250_" + contract)],
                [InlineKeyboardButton("$500", callback_data="dca_amt_quick_500_" + contract),
                 InlineKeyboardButton("◀ Back", callback_data="dca_setmc_" + contract)],
            ])
        )

    elif cb.startswith("dca_amt_quick_"):
        rest     = cb[14:]          # e.g. "100_<contract>"
        amt_str, contract = rest.split("_", 1)
        amt      = float(amt_str)
        p        = pending.get(u.id, {})
        mc_val   = p.get("pending_mc", 0)
        targets  = p.get("targets", [])
        if len(targets) >= MAX_DCA_TARGETS:
            await q.edit_message_text(f"❌ Max {MAX_DCA_TARGETS} DCA targets per token.", reply_markup=back_main())
            return
        targets.append({"mc": mc_val, "amount": amt, "triggered": False})
        pending[u.id]["targets"] = targets
        pending[u.id].pop("pending_mc", None)
        pending[u.id]["action"] = "dca_build"
        await _dca_show_plan(q, contract, pending[u.id])

    elif cb.startswith("dca_addmore_"):
        contract = cb[12:]
        p = pending.get(u.id, {})
        if not p.get("targets"):
            await q.edit_message_text("Session expired.", reply_markup=back_main())
            return
        pending[u.id]["action"] = "dca_mc_input"
        step = len(p["targets"]) + 1
        await q.edit_message_text(
            "📈 *DCA TARGET " + str(step) + " — SET MC*\n\n"
            "Enter the next MC trigger:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ Back", callback_data="dca_confirm_" + contract)],
            ])
        )

    elif cb.startswith("dca_confirm_"):
        contract = cb[12:]
        p        = pending.get(u.id, {})
        targets  = p.get("targets", [])
        sym      = p.get("symbol", "?")
        if not targets:
            await q.edit_message_text("No targets set.", reply_markup=back_main())
            return
        targets.sort(key=lambda x: x["mc"])
        ud["dca_orders"] = [d for d in ud.get("dca_orders", []) if d["contract"] != contract]
        ud["dca_orders"].append({
            "contract":   contract,
            "symbol":     sym,
            "mc_targets": targets,
            "created_at": datetime.now().isoformat(),
            "cancelled":  False,
        })
        pending.pop(u.id, None)
        lines = "\n".join(
            "  📍 Buy *" + money(tgt["amount"]) + "* at *" + mc_str(tgt["mc"]) + "* MC"
            for tgt in targets
        )
        await q.edit_message_text(
            "✅ *DCA PLAN SET — $" + sym + "*\n\n" + lines + "\n\n"
            "The bot will auto-buy at each MC milestone.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ Back to Token", callback_data="btt_" + contract)],
                [InlineKeyboardButton("🏠 Main Menu",    callback_data="mm")],
            ])
        )

    elif cb.startswith("dca_cancel_"):
        contract = cb[11:]
        before = len(ud.get("dca_orders", []))
        # Find symbol from dca_orders BEFORE removing it
        sym = contract[:8] + "..."
        for d in ud.get("dca_orders", []):
            if d["contract"] == contract and d.get("symbol"):
                sym = d["symbol"]
                break
        ud["dca_orders"] = [d for d in ud.get("dca_orders", []) if d["contract"] != contract]
        await q.edit_message_text(
            "✅ DCA orders for $" + sym + " cancelled.",
            reply_markup=back_main()
        )

    elif cb == "v_dca":
        orders = ud.get("dca_orders", [])
        if not orders:
            await q.edit_message_text("📉 *DCA ORDERS*\n\nNo active DCA orders.\nOpen a token and use the 📉 DCA by MC button.", parse_mode="Markdown", reply_markup=back_main())
            return
        lines = ["📉 *ACTIVE DCA ORDERS*\n"]
        for dca in orders:
            lines.append("*$" + dca["symbol"] + "*")
            for tgt in dca.get("mc_targets", []):
                status = "✅" if tgt.get("triggered") else "⏳"
                lines.append("  " + status + " " + mc_str(tgt["mc"]) + " → " + money(tgt["amount"]))
            lines.append("")
        await q.edit_message_text("\n".join(lines), parse_mode="Markdown", reply_markup=back_main())

    # ── CSV EXPORT ─────────────────────────────────────────────────────────────
    elif cb == "v_export":
        await q.edit_message_text("📁 Generating your trade history CSV...", reply_markup=back_more())
        await export_csv(ctx.bot, u.id, ud)

    # ── LANGUAGE SELECTOR ──────────────────────────────────────────────────────
    elif cb == "cfg_lang":
        await q.edit_message_text(
            "🌐 *SELECT LANGUAGE*\n\nChoose your preferred language:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🇬🇧 English",    callback_data="lang_en"),
                 InlineKeyboardButton("🇪🇸 Español",   callback_data="lang_es")],
                [InlineKeyboardButton("🇧🇷 Português", callback_data="lang_pt"),
                 InlineKeyboardButton("🇫🇷 Français",  callback_data="lang_fr")],
                [InlineKeyboardButton("🇨🇳 中文",       callback_data="lang_zh")],
                [InlineKeyboardButton("Back",           callback_data="v_settings")],
            ])
        )

    elif cb.startswith("lang_"):
        lang = cb[5:]
        if lang in TRANSLATIONS:
            ud["language"] = lang
            await q.edit_message_text(
                t(ud, "lang_set"), parse_mode="Markdown", reply_markup=settings_kb(ud)
            )
        else:
            await q.edit_message_text("Language not found.", reply_markup=back_main())

    # ── CHANNEL CARD AI ANALYSIS TOGGLE ────────────────────────────────────────
    # aiex_<contract> = expand (show AI panel)
    # aic_<contract>  = collapse (hide AI panel)
    elif cb.startswith("aiex_") or cb.startswith("aic_"):
        expanding = cb.startswith("aiex_")
        contract  = cb[5:] if expanding else cb[4:]   # "aiex_"=5 chars, "aic_"=4 chars
        cached    = _channel_ai_cache.get(contract)
        if not cached:
            await q.answer("Signal data expired — rescan the token.", show_alert=True)
            return
        info_c        = cached["info"]
        sc_c          = cached["sc"]
        ai_c          = cached["ai"]
        explorer_urls = cached.get("explorer_urls", {})
        bot_url       = cached.get("bot_url")

        base_text = _channel_card_text(info_c, sc_c, ai_c, contract)
        if expanding:
            full_text = base_text + _ai_section_text(info_c, sc_c, ai_c)
        else:
            full_text = base_text

        kb = _channel_card_kb(contract, bot_url, info_c, explorer_urls, expanded=expanding)
        try:
            await q.edit_message_text(
                full_text,
                parse_mode="Markdown",
                reply_markup=kb,
                disable_web_page_preview=True,
            )
        except Exception as _te:
            logger.debug(f"AI toggle edit failed: {_te}")
        await q.answer()


def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_start))
    app.add_handler(CallbackQueryHandler(btn))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.job_queue.run_repeating(checker_job, interval=PRICE_CHECK_INTERVAL, first=10)
    app.job_queue.run_repeating(sniper_job,      interval=300, first=60)
    app.job_queue.run_repeating(kol_tracker_job,       interval=300, first=90)   # KOL wallet tracker
    app.job_queue.run_repeating(channel_milestone_job, interval=300, first=120)  # Channel milestone tracker
    app.job_queue.run_repeating(autosave_job,          interval=120, first=120)  # DB autosave every 2 min
    app.job_queue.run_daily(daily_summary_job, time=__import__("datetime").time(23, 59))
    app.job_queue.run_daily(monthly_report_job, time=__import__("datetime").time(8, 0))

    # [CRIT-1] Load all persisted user data before starting
    load_all(users, trade_log)

    # [LOW-11] Graceful HTTP client shutdown
    async def _on_shutdown(application):
        global _http
        if _http and not _http.is_closed:
            await _http.aclose()
            logger.info("HTTP client closed cleanly.")

    app.post_shutdown = _on_shutdown

    logger.info("AURACLE_XBOT running...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
