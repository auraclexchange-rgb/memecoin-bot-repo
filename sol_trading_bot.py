#!/usr/bin/env python3
"""APEX SNIPER BOT - Advanced Paper Trading Bot"""

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

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env var not set. Add it in Railway → Variables.")

DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens/{}"
PRICE_CHECK_INTERVAL      = 20   # standard holdings checker
APEX_PRICE_CHECK_INTERVAL = 8    # APEX positions — faster, uses Helius when available
MAX_BALANCE = 10_000.0
MIN_BALANCE = 1.0
SNIPER_SEEN_EXPIRY_H = 0.08   # forget seen tokens after ~5 MIN (1 scan cycle)

# ── RugCheck rate limiter — max 3 concurrent calls to avoid 429s ─────────────
_rugcheck_semaphore = None

async def _get_rugcheck_semaphore():
    global _rugcheck_semaphore
    if _rugcheck_semaphore is None:
        _rugcheck_semaphore = _asyncio.Semaphore(3)
    return _rugcheck_semaphore
SNIPER_LOG_MAX = 200          # max sniper log entries per user

# ══════════════════════════════════════════════════════════════════════════════
# APEX — Autonomous Profit & Exit eXecution  (v1.0)
# ══════════════════════════════════════════════════════════════════════════════
APEX_TRAIL_ACTIVATE_X   = 1.5
APEX_TRAIL_PCT_EARLY    = 0.18
APEX_TRAIL_PCT_MID      = 0.15
APEX_TRAIL_PCT_HIGH     = 0.12
APEX_TRAIL_PCT_MOON     = 0.08
APEX_LOCK_2X_PCT        = 0.50
APEX_LOCK_5X_PCT        = 0.75
APEX_HEAT_SAFE          = 0.40
APEX_HEAT_CAUTION       = 0.60
APEX_HEAT_STOP          = 0.80
APEX_MAX_POSITIONS      = 999  # unlimited
APEX_DAILY_LOSS_LIMIT   = 0.20
APEX_CONFIRM_WAIT_S     = 45
APEX_DRAWDOWN_1_MULT    = 0.70
APEX_DRAWDOWN_2_MULT    = 0.50
APEX_DRAWDOWN_3_PAUSE   = 30
APEX_MIN_CONFIDENCE     = 3
APEX_SELF_LEARN_WINDOW  = 50
_apex_entry_queue: dict  = {}
_apex_paused_until: dict = {}
_apex_learn_memory: dict = {}
# Post-exit tracker: uid -> {contract -> {exit_price, exit_reason, exit_at, symbol,
#   entry_price, snapshots:[{ts,price,x_vs_exit,x_vs_entry,checked_at}]}}
_apex_post_exit: dict = {}
_user_locks:        dict = {}  # asyncio.Lock per uid — prevents concurrent buy/sell


import asyncio as _asyncio
import time as _time
import json as _json
import urllib.parse as _urlparse
import re as _re
from persistence import load_all, save_user, save_trade_log, autosave_job
_http: httpx.AsyncClient | None = None

async def get_http() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(timeout=10, limits=httpx.Limits(max_connections=20))
    return _http

# ── Token price cache (8s TTL) ───────────────────────────────────────────────
_token_cache: dict = {}
CACHE_TTL = 12.0
_ch_card_cache: dict = {}   # {contract[:32]: {channel_id, msg_id, info, sc, ai, expanded}}
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
    except Exception:
        pass
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

async def get_helius_pool_price(contract: str, pair_addr: str, api_key: str) -> dict:
    """
    Query Solana pool reserves directly via Helius RPC.
    Returns {price, liq, liq_drop_pct} in near-real-time (~400ms block time).
    Falls back gracefully if pool layout unreadable.
    """
    if not api_key or not pair_addr:
        return {}
    try:
        client   = await get_http()
        rpc_url  = f"https://mainnet.helius-rpc.com/?api-key={api_key}"

        # Step 1: get pool account info to find vault addresses
        pool_resp = await client.post(rpc_url, json={
            "jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
            "params": [pair_addr, {"encoding": "jsonParsed", "commitment": "confirmed"}]
        }, timeout=5)
        if pool_resp.status_code != 200:
            return {}

        pool_data = pool_resp.json().get("result", {}).get("value")
        if not pool_data:
            return {}

        # Step 2: get token accounts for this pool
        # Use getTokenAccountsByOwner to find what the pool holds
        ta_resp = await client.post(rpc_url, json={
            "jsonrpc": "2.0", "id": 2,
            "method": "getTokenAccountsByOwner",
            "params": [pair_addr,
                {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
                {"encoding": "jsonParsed", "commitment": "confirmed"}
            ]
        }, timeout=5)
        if ta_resp.status_code != 200:
            return {}

        accounts = ta_resp.json().get("result", {}).get("value", [])
        if not accounts:
            return {}

        # Parse token balances — find base (token) and quote (SOL wrapped / USDC)
        WSOL  = "So11111111111111111111111111111111111111112"
        USDC  = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        USDT  = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"

        base_amt  = None
        quote_usd = None

        for acct in accounts:
            info = acct.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
            mint    = info.get("mint", "")
            balance = float(info.get("tokenAmount", {}).get("uiAmount") or 0)

            if mint == contract:
                base_amt = balance
            elif mint == WSOL:
                # Use live SOL/USD price from cache (refreshed every 60s)
                sol_price = _sol_price_cache.get("price", 150.0)
                quote_usd = balance * sol_price
            elif mint in (USDC, USDT):
                quote_usd = balance

        if base_amt and base_amt > 0 and quote_usd and quote_usd > 0:
            price = quote_usd / base_amt
            liq   = quote_usd * 2  # total liq = 2× quote side
            return {"price": price, "liq": liq, "source": "helius_rpc"}

        return {}
    except Exception as e:
        logger.debug(f"Helius RPC pool price failed: {e}")
        return {}


async def get_helius_rug_signal(contract: str, api_key: str) -> dict:
    """
    Check last 5 transactions on the token for large liquidity removals.
    Returns {rug_detected: bool, reason: str}
    """
    if not api_key:
        return {"rug_detected": False}
    try:
        client = await get_http()
        url    = f"https://api.helius.xyz/v1/addresses/{contract}/transactions"
        r = await client.get(url, params={
            "api-key": api_key, "limit": "5",
            "type": "REMOVE_LIQUIDITY"
        }, timeout=5)
        if r.status_code != 200:
            return {"rug_detected": False}

        txns = r.json()
        if not isinstance(txns, list) or not txns:
            return {"rug_detected": False}

        # Any REMOVE_LIQUIDITY in last 5 txns = rug signal
        for tx in txns:
            desc = tx.get("description", "").lower()
            if "remove" in desc or "withdraw" in desc:
                return {
                    "rug_detected": True,
                    "reason": f"Liquidity removal detected on-chain"
                }
        return {"rug_detected": False}
    except Exception as e:
        logger.debug(f"Helius rug signal check failed: {e}")
        return {"rug_detected": False}



# ── LANGUAGE SYSTEM ───────────────────────────────────────────────────────────
TRANSLATIONS: dict = {
    "en": {
        "welcome":        "👋 Welcome to *APEX SNIPER BOT*!\n\nAdvanced multi-chain paper trading bot.\n\nSet your starting balance:\nMin: $1  |  Max: $10,000\n\nEnter your starting balance:",
        "welcome_back":   "⚡ *APEX SNIPER BOT*\n\nWelcome back, *{username}*!\n💰 Balance: *{balance}*\n💎 Savings: *{savings}*\n\nPaste any crypto CA to trade 👇",
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
        "welcome":        "👋 ¡Bienvenido a *APEX SNIPER BOT*!\n\nBot avanzado de trading simulado multi-cadena.\n\nConfigura tu saldo inicial:\nMín: $1  |  Máx: $10,000\n\nIngresa tu saldo inicial:",
        "welcome_back":   "⚡ *APEX SNIPER BOT*\n\n¡Bienvenido de nuevo, *{username}*!\n💰 Saldo: *{balance}*\n💎 Ahorros: *{savings}*\n\nPega cualquier CA para operar 👇",
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
        "welcome":        "👋 Bem-vindo ao *APEX SNIPER BOT*!\n\nBot avançado de trading simulado multi-chain.\n\nDefina seu saldo inicial:\nMín: $1  |  Máx: $10,000\n\nDigite seu saldo inicial:",
        "welcome_back":   "⚡ *APEX SNIPER BOT*\n\nBem-vindo de volta, *{username}*!\n💰 Saldo: *{balance}*\n💎 Poupança: *{savings}*\n\nCole qualquer CA para negociar 👇",
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
        "welcome":        "👋 Bienvenue sur *APEX SNIPER BOT*!\n\nBot de trading papier multi-chaîne avancé.\n\nDéfinissez votre solde de départ:\nMin: $1  |  Max: $10 000\n\nEntrez votre solde de départ:",
        "welcome_back":   "⚡ *APEX SNIPER BOT*\n\nBienvenue, *{username}*!\n💰 Solde: *{balance}*\n💎 Épargne: *{savings}*\n\nCollez n'importe quelle CA pour trader 👇",
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
        "welcome":        "👋 欢迎使用 *APEX SNIPER BOT*!\n\n高级多链模拟交易机器人。\n\n设置起始余额:\n最低: $1  |  最高: $10,000\n\n请输入起始余额:",
        "welcome_back":   "⚡ *APEX SNIPER BOT*\n\n欢迎回来，*{username}*！\n💰 余额: *{balance}*\n💎 储蓄: *{savings}*\n\n粘贴任意合约地址开始交易 👇",
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
_rf_locks: dict = {}       # uid -> asyncio.Lock — prevents concurrent refresh calls
_ohlcv_cache: dict = {}    # contract -> {data, ts} — 30s TTL, avoids redundant GeckoTerminal fetches
OHLCV_CACHE_TTL = 30.0
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
_kol_last_sig:  dict = {}      # uid -> {wallet_addr -> last_sig}
_kol_hot_contracts: dict = {}  # contract -> [{label, sol_spent, ts}] — KOL recent buys
_buy_pct_prev:  dict = {}      # contract -> buy_pct_h1 from last sniper cycle (velocity)
_sol_price_history: list = []  # rolling 12-entry SOL price log (one per sniper_job run)
_rug_liq_prev: dict = {}            # uid -> {contract -> last_liq} for rug pull detection
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
        draw.text((W-40, 40), "APEX SNIPER BOT", font=font_brand, fill=(185,200,230), anchor="rm")

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


async def enrich_dev_wallet_history(contract: str, info: dict, api_key: str) -> dict:
    """
    Checks the deployer wallet's on-chain history via Helius.
    Looks at past tokens launched by same wallet — serial rugger detection.
    Returns: {dev_rug_count, dev_token_count, dev_rug_rate, dev_flags, dev_risk}
    """
    result = {
        "dev_rug_count":  0,
        "dev_token_count": 0,
        "dev_rug_rate":   0.0,
        "dev_flags":      [],
        "dev_risk":       "UNKNOWN",
    }
    if not api_key:
        return result

    try:
        client = await get_http()
        rpc_url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"

        # Step 1: get token mint account to find deployer/mint authority
        mint_resp = await client.post(rpc_url, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "getAccountInfo",
            "params": [contract, {"encoding": "jsonParsed", "commitment": "confirmed"}]
        }, timeout=6)

        if mint_resp.status_code != 200:
            return result

        mint_data = mint_resp.json().get("result", {}).get("value", {})
        parsed    = mint_data.get("data", {}).get("parsed", {}).get("info", {}) if mint_data else {}

        # Get mint authority (deployer) — if None, was already renounced
        deployer = parsed.get("mintAuthority") or parsed.get("freezeAuthority")
        if not deployer:
            # Try RugCheck stored data
            deployer = info.get("deployer") or info.get("creator")
        if not deployer:
            result["dev_risk"] = "LOW"  # can't find deployer = likely renounced = good
            return result

        # Step 2: get recent transactions from deployer wallet
        tx_resp = await client.post(rpc_url, json={
            "jsonrpc": "2.0", "id": 2,
            "method": "getSignaturesForAddress",
            "params": [deployer, {"limit": 50, "commitment": "confirmed"}]
        }, timeout=6)

        if tx_resp.status_code != 200:
            return result

        sigs = tx_resp.json().get("result", []) or []

        # Step 3: use Helius enhanced API to check what tokens this wallet created
        hist_resp = await client.get(
            f"https://api.helius.xyz/v1/addresses/{deployer}/transactions",
            params={"api-key": api_key, "limit": "50", "type": "CREATE_MINT"},
            timeout=7
        )

        tokens_launched = []
        if hist_resp.status_code == 200:
            txns = hist_resp.json() or []
            for tx in txns:
                for ti in tx.get("tokenTransfers", []):
                    mint = ti.get("mint", "")
                    if mint and mint != contract and mint not in tokens_launched:
                        tokens_launched.append(mint)

        result["dev_token_count"] = len(tokens_launched)

        if not tokens_launched:
            # No prior tokens — could be fresh wallet (red flag) or legit
            if len(sigs) < 5:
                result["dev_flags"].append("Fresh wallet — very few on-chain transactions")
                result["dev_risk"] = "MEDIUM"
            else:
                result["dev_risk"] = "LOW"
            return result

        # Step 4: check each prior token's current state (are they dead?)
        rug_count = 0
        checked   = 0
        for prev_contract in tokens_launched[:8]:  # check up to 8 prior tokens
            try:
                prev_info = await get_token(prev_contract)
                checked  += 1
                if not prev_info:
                    rug_count += 1  # can't fetch = likely dead/rugged
                    continue
                prev_liq = prev_info.get("liq", 0)
                prev_mc  = prev_info.get("mc",  0)
                prev_age = prev_info.get("age_h", 0)
                # Dead token: liq < $500, mc < $5K, and older than 2h
                if prev_liq < 500 and prev_mc < 5_000 and prev_age > 2:
                    rug_count += 1
            except Exception:
                rug_count += 1  # error = assume dead
            await _asyncio.sleep(0.1)  # don't hammer API

        if checked > 0:
            rug_rate = rug_count / checked
            result["dev_rug_count"] = rug_count
            result["dev_rug_rate"]  = round(rug_rate, 2)

            if rug_rate >= 0.8:
                result["dev_flags"].append(f"🚨 SERIAL RUGGER — {rug_count}/{checked} prior tokens dead")
                result["dev_risk"] = "HIGH"
            elif rug_rate >= 0.5:
                result["dev_flags"].append(f"⚠️ Dev rugged {rug_count}/{checked} prior tokens")
                result["dev_risk"] = "MEDIUM"
            elif rug_rate >= 0.3:
                result["dev_flags"].append(f"Dev has {rug_count} prior failed tokens")
                result["dev_risk"] = "MEDIUM"
            else:
                result["dev_risk"] = "LOW"
                if checked >= 3:
                    result["dev_flags"].append(f"✅ Dev history clean ({checked} prior tokens checked)")
        else:
            result["dev_risk"] = "LOW"

        return result

    except Exception as e:
        logger.debug(f"Dev wallet history check failed: {e}")
        return result


async def enrich_wallet_clustering(contract: str, top_holders: list, api_key: str) -> dict:
    """
    Checks if top holder wallets share a common funding source.
    If multiple top holders funded from same wallet = coordinated/insider group.
    Returns: {cluster_detected, cluster_pct, cluster_flags}
    """
    result = {
        "cluster_detected": False,
        "cluster_pct":      0.0,
        "cluster_flags":    [],
    }
    if not api_key or not top_holders:
        return result

    try:
        client  = await get_http()
        rpc_url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"

        funding_sources: dict = {}  # source_wallet -> [holder_addr, ...]
        holder_pcts:     dict = {}  # holder_addr -> pct

        for holder in top_holders[:8]:  # check top 8 holders
            addr = holder.get("addr_full") or holder.get("addr", "")
            pct  = holder.get("pct", 0)
            if not addr or len(addr) < 30:
                continue
            holder_pcts[addr] = pct

            # Get first few transactions of this wallet = funding source
            tx_resp = await client.post(rpc_url, json={
                "jsonrpc": "2.0", "id": 1,
                "method": "getSignaturesForAddress",
                "params": [addr, {"limit": 5, "commitment": "confirmed"}]
            }, timeout=5)

            if tx_resp.status_code != 200:
                continue

            sigs = tx_resp.json().get("result", []) or []
            if not sigs:
                continue

            # Get the oldest (first) transaction = funding tx
            oldest_sig = sigs[-1].get("signature", "")
            if not oldest_sig:
                continue

            tx_detail = await client.post(rpc_url, json={
                "jsonrpc": "2.0", "id": 2,
                "method": "getTransaction",
                "params": [oldest_sig, {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}]
            }, timeout=5)

            if tx_detail.status_code != 200:
                continue

            tx = tx_detail.json().get("result", {})
            if not tx:
                continue

            # Find who funded this wallet (sender in first tx)
            accounts = tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
            if accounts:
                funder = accounts[0].get("pubkey", "") if isinstance(accounts[0], dict) else str(accounts[0])
                if funder and funder != addr:
                    funding_sources.setdefault(funder, []).append(addr)

            await _asyncio.sleep(0.1)

        # Analyse clustering
        for funder, holders_from_same in funding_sources.items():
            if len(holders_from_same) >= 2:
                clustered_pct = sum(holder_pcts.get(h, 0) for h in holders_from_same)
                if clustered_pct >= 10:
                    result["cluster_detected"] = True
                    result["cluster_pct"]      = round(clustered_pct, 1)
                    result["cluster_flags"].append(
                        f"🚨 {len(holders_from_same)} wallets funded from same source — hold {clustered_pct:.1f}%"
                    )

        return result

    except Exception as e:
        logger.debug(f"Wallet clustering check failed: {e}")
        return result


async def enrich_volume_pattern(contract: str, api_key: str) -> dict:
    """
    Analyses individual swap transactions to detect manufactured volume.
    Real organic pumps: random sizes, many wallets, irregular timing.
    Fake pumps: round numbers, same wallets, regular intervals.
    Returns: {vol_organic_score, vol_flags, round_number_pct, wallet_repeat_pct}
    """
    result = {
        "vol_organic_score": 7,
        "vol_flags":         [],
        "round_number_pct":  0.0,
        "wallet_repeat_pct": 0.0,
    }
    if not api_key:
        return result

    try:
        client = await get_http()
        r = await client.get(
            f"https://api.helius.xyz/v1/addresses/{contract}/transactions",
            params={"api-key": api_key, "limit": "50", "type": "SWAP"},
            timeout=8
        )
        if r.status_code != 200:
            return result

        txns = r.json() or []
        if len(txns) < 5:
            return result

        amounts       = []
        wallets       = []
        timestamps    = []
        score         = 7

        for tx in txns:
            fee_payer = tx.get("feePayer", "")
            wallets.append(fee_payer)
            ts = tx.get("timestamp", 0)
            timestamps.append(ts)

            # Get SOL amount moved
            native = tx.get("nativeTransfers", []) or []
            sol_moved = sum(
                abs(t.get("amount", 0))
                for t in native
                if t.get("fromUserAccount") == fee_payer or t.get("toUserAccount") == fee_payer
            ) / 1e9
            if sol_moved > 0:
                amounts.append(sol_moved)

        if not amounts:
            return result

        # ── Check 1: Round number buys (fake organic) ─────────────────────────
        round_count = sum(1 for a in amounts if round(a, 1) == a and a > 0.1)
        round_pct   = round(round_count / len(amounts) * 100, 1)
        result["round_number_pct"] = round_pct
        if round_pct > 70:
            result["vol_flags"].append(f"⚠️ {round_pct}% round-number buys — likely bot activity")
            score -= 2

        # ── Check 2: Wallet repeat rate (same wallets buying repeatedly) ──────
        from collections import Counter
        wallet_counts  = Counter(wallets)
        repeat_wallets = sum(1 for w, c in wallet_counts.items() if c >= 3)
        repeat_pct     = round(repeat_wallets / max(len(set(wallets)), 1) * 100, 1)
        result["wallet_repeat_pct"] = repeat_pct
        if repeat_pct > 40:
            result["vol_flags"].append(f"⚠️ {repeat_pct}% wallets trading 3+ times — wash trading")
            score -= 2
        elif repeat_pct < 10:
            score += 1  # many unique wallets = organic

        # ── Check 3: Timing regularity (bots trade at fixed intervals) ────────
        if len(timestamps) >= 6:
            timestamps_sorted = sorted(timestamps)
            intervals = [timestamps_sorted[i+1] - timestamps_sorted[i]
                         for i in range(len(timestamps_sorted)-1) if timestamps_sorted[i] > 0]
            if intervals:
                avg_interval = sum(intervals) / len(intervals)
                # Low variance = bot (all trades equally spaced)
                variance = sum((x - avg_interval)**2 for x in intervals) / len(intervals)
                std_dev  = variance ** 0.5
                cv       = std_dev / avg_interval if avg_interval > 0 else 1
                if cv < 0.2:  # coefficient of variation < 20% = very regular = bot
                    result["vol_flags"].append("⚠️ Highly regular trade timing — bot pattern")
                    score -= 2
                elif cv > 0.8:  # high variance = organic irregular human buying
                    score += 1

        # ── Check 4: Unique buyer count ───────────────────────────────────────
        unique_buyers = len(set(wallets))
        if unique_buyers >= 20:
            score += 1
        elif unique_buyers < 8:
            result["vol_flags"].append(f"Only {unique_buyers} unique wallets in last 50 swaps")
            score -= 2

        result["vol_organic_score"] = max(0, min(10, score))
        return result

    except Exception as e:
        logger.debug(f"Volume pattern analysis failed: {e}")
        return result


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
            url   = s.get("url","")
            if stype in ("twitter","x") and not twitter:
                twitter = url
            elif stype == "telegram" and not telegram:
                telegram = url
        for w in websites_raw:
            url = w.get("url","")
            if url and not website:
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
        no_mint    = None
        freeze     = None
        lp_burn    = None
        top10      = None
        top20      = None
        insider    = None
        dev_pct_rc = None
        rug_risks  = []
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
                    # Top holders
                    holders = rc_data.get("topHolders") or []
                    if holders:
                        top10  = round(sum(float(h.get("pct", 0)) for h in holders[:10]), 1)
                        top20  = round(sum(float(h.get("pct", 0)) for h in holders[:20]), 1)
                        dev_h  = holders[0] if holders else {}
                        dev_pct_rc = round(float(dev_h.get("pct", 0)), 2) if dev_h else None
                    # Insider / dev %
                    insider_pct = rc_data.get("insiderNetworkStats", {}).get("insiderPct", None)
                    if insider_pct is not None:
                        insider = round(float(insider_pct), 1)
                    # Risk flags
                    for risk in rc_data.get("risks", []):
                        lvl  = risk.get("level", "").lower()
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
            "top10_pct":   top10,
            "top20_pct":   top20,
            "dev_pct_rc":  dev_pct_rc,
            "insider_pct": insider,
            "rug_risks":   rug_risks,
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
    Sniper scoring — tightened rug filters.
    Max 100 pts. Hard flags auto-fail.
    """
    score     = 0
    strengths = []
    warnings  = []
    flags     = []

    age_h        = info.get("age_h") or 0
    liq          = info.get("liq", 0)
    mc           = info.get("mc", 0)
    liq_pct      = info.get("liq_pct", 0)
    buy_pct      = info.get("buy_pct", 50)
    buy_pct_h1   = info.get("buy_pct_h1", buy_pct)
    buy_pct_m5   = info.get("buy_pct_m5", buy_pct)
    buys         = info.get("buys", 0)
    sells        = info.get("sells", 0)
    buys_h1      = info.get("buys_h1", 0)
    sells_h1     = info.get("sells_h1", 0)
    vol_h1       = info.get("vol_h1", 0)
    vol_m5       = info.get("vol_m5", 0)
    ch_m5        = info.get("ch_m5", 0)
    ch_h1        = info.get("ch_h1", 0)
    twitter      = info.get("twitter", "")
    telegram     = info.get("telegram", "")
    website      = info.get("website", "")
    no_mint      = info.get("no_mint")
    no_freeze    = info.get("no_freeze")
    lp_burn      = info.get("lp_burn")
    top10_pct    = info.get("top10_pct")
    insider_pct  = info.get("insider_pct")
    rug_risks    = info.get("rug_risks", []) or []
    pf_curve     = info.get("pf_curve")
    pf_replies   = info.get("pf_replies", 0) or 0
    pf_graduated = info.get("pf_graduated", False)
    maker_pct    = info.get("maker_pct")
    top3_vol_pct = info.get("top3_vol_pct")
    boost_amount = info.get("boost_amount", 0) or 0
    maker_count      = info.get("maker_count") or 0
    dev_pct          = float(info.get("pf_dev_pct") or info.get("dev_pct_rc") or 0)
    is_solana        = info.get("chain", "").lower() in ("solana", "sol")
    # New enrichment fields
    dev_rug_rate     = info.get("dev_rug_rate", -1.0)
    dev_risk         = info.get("dev_risk", "UNKNOWN")
    dev_flags_new    = info.get("dev_flags", [])
    cluster_detected = info.get("cluster_detected", False)
    cluster_pct      = info.get("cluster_pct", 0.0)
    vol_organic      = info.get("vol_organic_score", -1)
    vol_flags_new    = info.get("vol_flags", [])
    tw_fresh         = info.get("tw_is_fresh_acct", False)
    tw_age_d         = info.get("tw_account_age_d")

    # ── RUG RISK PARSING ──────────────────────────────────────────────────────
    danger_str   = [r for r in rug_risks if isinstance(r, str)]
    bundle_flag  = any("bundle" in r.lower() for r in danger_str)
    dev_sold     = any("deployer sold" in r.lower() or "creator sold" in r.lower() for r in danger_str)
    copycat      = any("copycat" in r.lower() for r in danger_str)
    honeypot     = any("honeypot" in r.lower() for r in danger_str)

    # INSTANT HARD FLAGS — these are rug signals, not warnings
    if honeypot:
        flags.append("🚨 Honeypot detected — cannot sell")
    if bundle_flag:
        flags.append("🚨 Bundle sniped at launch — insider pump")
    if dev_sold:
        flags.append("🚨 Dev already sold — abandoned")
    if copycat:
        flags.append("🚨 Copycat token")
    if no_mint is False:
        flags.append("🚨 Mint authority active — supply can inflate")
    if liq < 8_000 and not pf_curve:
        flags.append(f"🚨 Liq ${liq:,.0f} — too thin to trade")
    if liq_pct < 5 and mc > 0 and not pf_curve:
        flags.append(f"🚨 Liq only {liq_pct:.1f}% of MC — drain risk")
    if top10_pct is not None and top10_pct > 50:
        flags.append(f"🚨 Top10 wallets hold {top10_pct:.1f}% — whale trap")
    if insider_pct is not None and insider_pct > 15:
        flags.append(f"🚨 Insider/dev holds {insider_pct:.1f}% — dump risk")
    if dev_pct > 10:
        flags.append(f"🚨 DEV wallet holds {dev_pct:.1f}%")
    if top3_vol_pct is not None and top3_vol_pct > 65:
        flags.append(f"🚨 Top 3 wallets = {top3_vol_pct:.0f}% of volume — wash trade")
    vol_mc_ratio = (vol_h1 / mc) if mc > 0 else 0
    if vol_mc_ratio > 5:
        flags.append(f"🚨 Vol/MC={vol_mc_ratio:.1f}x — likely wash trading")
    if maker_count > 0 and maker_count < 30:
        flags.append(f"🚨 Only {maker_count} unique wallets — too few real buyers")
    # Dev wallet history flags
    if dev_risk == "HIGH":
        flags.append(f"🚨 Serial rugger — dev wallet history shows rug pattern")
    elif dev_risk == "MEDIUM" and dev_rug_rate >= 0.5:
        flags.append(f"⚠️ Dev rugged {round(dev_rug_rate*100)}% of prior tokens")
    # Wallet clustering flags
    if cluster_detected and cluster_pct >= 20:
        flags.append(f"🚨 Wallet cluster: {cluster_pct}% held by coordinated insiders")
    # Twitter fresh account flag
    if tw_fresh:
        flags.append("🚨 Twitter account created same week as token — scam signal")
    # Volume pattern flags
    if vol_organic >= 0 and vol_organic <= 3:
        flags.append(f"🚨 Volume pattern analysis: likely manufactured ({vol_organic}/10 organic)")

    # ── CATEGORY 1: SAFETY (0–30 pts) ────────────────────────────────────────
    if is_solana:
        if lp_burn is not None:
            if lp_burn >= 90:   score += 12; strengths.append(f"🔒 LP burned {lp_burn}%")
            elif lp_burn >= 50: score += 8;  strengths.append(f"LP {lp_burn}% burned")
            elif lp_burn >= 20: score += 3;  warnings.append(f"LP only {lp_burn}% burned")
            else:               warnings.append(f"LP barely burned ({lp_burn}%)")
        else:
            score += 3
    else:
        score += 6

    if no_mint is True:   score += 8; strengths.append("✅ No mint authority")
    if no_freeze is True: score += 5; strengths.append("✅ No freeze authority")
    elif no_freeze is False: warnings.append("Freeze authority enabled")

    # Healthy top10 bonus
    if top10_pct is not None:
        if top10_pct <= 20:   score += 5; strengths.append(f"🟢 Low concentration (top10={top10_pct:.0f}%)")
        elif top10_pct <= 35: score += 3
        elif top10_pct <= 50: warnings.append(f"Top10 at {top10_pct:.0f}% — watch for dump")

    # DEV green signal
    if dev_pct > 0 and dev_pct <= 3:
        score += 3; strengths.append(f"Low dev holding ({dev_pct:.1f}%)")
    # Dev history bonus
    if dev_risk == "LOW" and dev_rug_rate >= 0:
        score += 5; strengths.append(f"✅ Dev history clean — no prior rugs")
    elif dev_risk == "MEDIUM":
        score -= 5
    # Wallet clustering penalty
    if cluster_detected:
        score -= 8; warnings.append(f"Coordinated wallets hold {cluster_pct}%")
    elif cluster_pct == 0 and top10_pct is not None:
        score += 2; strengths.append("✅ No wallet clustering detected")
    # Volume organics bonus
    if vol_organic >= 8:
        score += 4; strengths.append(f"Organic volume pattern ({vol_organic}/10)")
    elif vol_organic >= 6:
        score += 2
    elif 0 <= vol_organic <= 4:
        score -= 4; warnings.append(f"Suspicious volume pattern ({vol_organic}/10)")
    # Twitter account age
    if tw_age_d is not None:
        if tw_age_d > 365:
            score += 3; strengths.append(f"Established Twitter ({tw_age_d}d old)")
        elif tw_age_d < 7:
            score -= 5; warnings.append(f"Twitter only {tw_age_d}d old")

    # ── CATEGORY 2: LAUNCH TIMING (0–20 pts) ─────────────────────────────────
    if pf_curve is not None and is_solana:
        if pf_graduated:
            score += 12; strengths.append("🎓 Graduated to Raydium — proven demand")
        elif 30 <= pf_curve <= 65:
            score += 20; strengths.append(f"⚡ Sweet spot curve ({pf_curve}%)")
        elif 15 <= pf_curve < 30:
            score += 12; strengths.append(f"Early curve ({pf_curve}%) — fresh")
        elif 65 < pf_curve < 85:
            score += 8;  warnings.append(f"Curve {pf_curve}% — near graduation")
        elif pf_curve >= 85 and not pf_graduated:
            score += 5;  warnings.append(f"Curve {pf_curve}% — graduation imminent, may dump")
        elif pf_curve < 5:
            warnings.append(f"Curve only {pf_curve}% — very early, mostly bots")
        else:
            score += 4
    else:
        if age_h < 0.25:    score += 4;  warnings.append("Under 15min — bots still active")
        elif age_h < 0.5:   score += 16; strengths.append("🔥 Very fresh (15–30min)")
        elif age_h < 1.5:   score += 20; strengths.append("⚡ Optimal window (30min–1.5h)")
        elif age_h < 3:     score += 12; strengths.append("Early entry (1.5–3h)")
        elif age_h < 5:     score += 6;  warnings.append(f"Getting late ({round(age_h,1)}h)")
        elif age_h < 6:     score += 2;  warnings.append(f"Nearly too old ({round(age_h,1)}h)")

    # ── CATEGORY 3: SOCIAL ATTENTION (0–15 pts) ──────────────────────────────
    social_pts   = 0
    social_count = sum([bool(twitter), bool(telegram), bool(website)])
    if social_count >= 3:
        social_pts += 8; strengths.append("Full socials (TW+TG+Web)")
    elif social_count == 2:
        social_pts += 5; strengths.append("Twitter + Telegram confirmed")
    elif social_count == 1:
        social_pts += 1; warnings.append("Only 1 social — incomplete profile")
    # No socials handled as flag in pre-filter

    if boost_amount >= 50:  social_pts += 5; strengths.append(f"High boost ({boost_amount:.0f} SOL)")
    elif boost_amount >= 20: social_pts += 3; strengths.append(f"Boost active ({boost_amount:.0f} SOL)")
    elif boost_amount >= 5:  social_pts += 1

    if pf_replies >= 100: social_pts += 4; strengths.append(f"Active community ({pf_replies} replies)")
    elif pf_replies >= 40: social_pts += 2

    score += min(social_pts, 15)

    # ── CATEGORY 4: ENTRY MC (0–15 pts) ──────────────────────────────────────
    if 20_000 <= mc <= 80_000:
        score += 15; strengths.append(f"🎯 Micro cap ({mc_str(mc)})")
    elif 80_000 < mc <= 300_000:
        score += 12; strengths.append(f"Good entry ({mc_str(mc)})")
    elif 300_000 < mc <= 800_000:
        score += 7;  warnings.append(f"Mid MC ({mc_str(mc)}) — less upside")
    elif 800_000 < mc <= 2_000_000:
        score += 3;  warnings.append(f"High MC ({mc_str(mc)})")
    elif mc < 20_000:
        score += 3;  warnings.append(f"Very low MC — ultra risky")
    else:
        warnings.append(f"Already pumped ({mc_str(mc)})")

    # ── CATEGORY 5: ORGANIC SPREAD (0–10 pts) ────────────────────────────────
    h1_tx   = buys_h1 + sells_h1
    h1_buys = buys_h1 if h1_tx > 0 else buys
    eff_age = max(age_h, 0.25)
    buyers_per_hour = h1_buys / min(eff_age, 1)

    if buyers_per_hour >= 80:   score += 10; strengths.append(f"🚀 Viral ({int(buyers_per_hour)}/hr buyers)")
    elif buyers_per_hour >= 40: score += 8;  strengths.append(f"Strong spread ({int(buyers_per_hour)}/hr)")
    elif buyers_per_hour >= 15: score += 5
    elif buyers_per_hour >= 5:  score += 2;  warnings.append(f"Low buyer rate ({int(buyers_per_hour)}/hr)")
    else:                        warnings.append("Very few buyers — thin interest")

    avg_buy_size = (vol_h1 / h1_buys) if h1_buys > 0 else 0
    if 0 < avg_buy_size < 200:
        score += 3; strengths.append(f"Retail organic (avg ${avg_buy_size:.0f})")
    elif avg_buy_size > 2_000:
        warnings.append(f"Large avg buy (${avg_buy_size:,.0f}) — whale dominated")

    # Healthy holder count
    if maker_count >= 200: score += 2; strengths.append(f"Healthy holders ({maker_count})")
    elif maker_count >= 80: score += 1

    # ── CATEGORY 6: BUY PRESSURE + MOMENTUM (0–10 pts) ───────────────────────
    momentum_pts = 0
    if buy_pct_h1 >= 65:   momentum_pts += 6; strengths.append(f"Dominant buy pressure ({buy_pct_h1}%)")
    elif buy_pct_h1 >= 57: momentum_pts += 4
    elif buy_pct_h1 >= 53: momentum_pts += 2
    else: warnings.append(f"Weak buy pressure H1 ({buy_pct_h1}%)")

    if buy_pct_m5 >= 62 and ch_m5 > 0:
        momentum_pts += 4; strengths.append(f"Accelerating now (5m:{ch_m5:+.1f}%)")
    elif ch_m5 > 0 and ch_h1 > 0 and ch_h1 < 150:
        momentum_pts += 2; strengths.append(f"Building (5m:{ch_m5:+.1f}% 1h:{ch_h1:+.1f}%)")
    elif ch_h1 >= 200:
        momentum_pts += 0; warnings.append(f"Parabolic (+{ch_h1:.0f}%) — late entry")
    elif ch_m5 < -10 and ch_h1 < -10:
        warnings.append(f"Dumping ({ch_m5:.1f}% / {ch_h1:.1f}%)")

    vol_h1_per_5m = (vol_h1 / 12) if vol_h1 > 0 else 0
    if vol_m5 > vol_h1_per_5m * 2 and vol_m5 > 500:
        momentum_pts += 2; strengths.append("Volume spike now 🔥")

    if maker_pct is not None:
        if maker_pct >= 60:   momentum_pts += 3; strengths.append(f"Healthy maker dist ({maker_pct}%)")
        elif maker_pct >= 50: momentum_pts += 1
        elif maker_pct < 35:  warnings.append(f"Few unique buyers ({maker_pct}%) — possible shill")

    score += min(momentum_pts, 10)

    # ── CATEGORY 7: PUMPFUN BONUS (0–5 pts) ──────────────────────────────────
    if is_solana and pf_curve is not None:
        if pf_graduated: score += 5
        elif pf_curve > 40 and age_h < 2:
            score += 3; strengths.append(f"Fast curve fill ({pf_curve}% in {round(age_h,1)}h)")

    # ── CATEGORY 8: VELOCITY BONUS (0–8 pts) ───────────────────────────────────
    velocity = info.get("buy_pct_velocity", 0.0)
    if velocity >= 10:
        score += 8; strengths.append(f"🚀 Buy pressure accelerating (+{velocity:.0f}%)")
    elif velocity >= 5:
        score += 5; strengths.append(f"📈 Momentum building (+{velocity:.0f}%)")
    elif velocity >= 2:
        score += 2
    elif velocity <= -8:
        score -= 8; warnings.append(f"📉 Buy pressure fading ({velocity:.0f}%)")
    elif velocity <= -4:
        score -= 4; warnings.append(f"Buy pressure declining ({velocity:.0f}%)")

    # ── CATEGORY 9: KOL SIGNAL BONUS (0–20 pts) ──────────────────────────────
    kol_count  = info.get("kol_buy_count", 0)
    kol_sol    = info.get("kol_sol_total", 0)
    kol_labels = info.get("kol_labels", [])
    if kol_count >= 2:
        score += 20; strengths.append(f"👀 {kol_count} KOL wallets bought ({kol_sol:.1f} SOL)")
    elif kol_count == 1:
        _kl = kol_labels[0] if kol_labels else "KOL wallet"
        if kol_sol >= 1.0:
            score += 15; strengths.append(f"👀 {_kl} bought {kol_sol:.1f} SOL")
        else:
            score += 8;  strengths.append(f"👀 KOL wallet entry detected")

    # ── CATEGORY 10: CHAIN-SPECIFIC AGE ADJUSTMENT ───────────────────────────
    _chain_low = info.get("chain", "").lower()
    if _chain_low in ("solana", "sol"):
        if age_h > 2.0:   score -= 8;  warnings.append(f"SOL token {round(age_h,1)}h old — rug window open")
        elif age_h > 1.0: score -= 3
    elif _chain_low in ("ethereum", "eth"):
        if age_h < 0.5:   score -= 5;  warnings.append("ETH token under 30min — bots active")
        elif age_h <= 6:  score += 3
    elif _chain_low in ("bsc",):
        score -= 5;  warnings.append("BSC — higher bot/rug base rate")

    # ── HARD FLAG PENALTIES ───────────────────────────────────────────────────
    flag_count = len(flags)
    if flag_count >= 3:   score = max(0, score - 45)
    elif flag_count == 2: score = max(0, score - 25)
    elif flag_count == 1: score = max(0, score - 12)

    score = min(score, 100)

    # ── Verdict ───────────────────────────────────────────────────────────────
    if flag_count >= 2:
        verdict = "SKIP"
    elif score >= 72:
        verdict = "SNIPE"
    elif score >= 54:
        verdict = "WAIT"
    else:
        verdict = "SKIP"

    return {
        "score":     score,
        "verdict":   verdict,
        "strengths": strengths[:5],
        "warnings":  warnings[:4],
        "flags":     flags,
        "icon":      "🟢" if score >= 72 else "🟡" if score >= 54 else "🔴",
    }





def score_token(info: dict) -> dict:
    """APEX token scoring for manual CA scans (distinct from sniper_score)."""
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
            # ── APEX Mode ─────────────────────────────────────────────────────
            "apex_mode":              False,
            "apex_vault":             0.0,
            "apex_session_start_bal": 0.0,
            "apex_daily_pnl":         0.0,
            "apex_daily_date":        None,
            "apex_consec_losses":     0,
            "apex_total_trades":      0,
            "apex_total_wins":        0,
            "apex_learn_threshold":   3,
            "apex_learn_score_min":   30,
            "apex_phase":             "learning",  # learning / calibrating / optimised
            "apex_size_mult":         1.0,          # position size multiplier — self-tuned
            "apex_max_positions_learned": 999,      # unlimited in learning
            "apex_vault_trade_on":    False,        # vault trades as separate balance
            "apex_vault_pnl":         0.0,          # lifetime vault P&L
            # ── Equity history: daily snapshot {date, equity, balance, pnl}
            # appended by daily_summary_job at 23:59. Used for balance curve display.
            "equity_history":         [],
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
                "min_score":        35,      # GemTools-style: low threshold, volume over quality
                "min_liq":          5_000,   # micro-caps have thin liq — $5K min
                "min_mc":           10_000,  # calls as low as $16K MC
                "max_mc":           500_000, # raised from 200K — catches more mid-cap calls
                "max_age_h":        6.0,     # tightened — fresh tokens only, reduces noise
                "buy_amount":       20,      # lowered default for safer paper trading start
                "min_buys_h1":      10,      # micro-caps have lower buy counts
                "min_buy_pct":      45,      # relaxed from 50 — more signals, still bullish
                "max_vol_mc_ratio": 10.0,    # micro-caps spike hard
                "min_liq_pct":      3,       # very small MC tokens have low liq%
                "max_top10_pct":    28,      # GemTools max seen: 26.5%
                "min_lp_burn":      50,      # relaxed for micro-caps
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
        save_user(uid, users[uid])
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


# ── POSITION HISTORY HELPERS ──────────────────────────────────────────────────
# These read from h["price_history"] (all positions) and h["sr_history"] (APEX).
# Both are populated by checker_job every cycle. sr_history is preferred for
# APEX positions because it carries volume data; price_history is the fallback
# for manual/sniper positions that don't have sr_history.

def _position_sparkline(h: dict) -> str:
    """
    Build a 10-char Unicode block sparkline from price history.

    Reads sr_history first (APEX positions, richer data), falls back to
    price_history (all positions). Returns "" if fewer than 3 snapshots exist.

    Output example:  ▁▂▃▅▇█▇▅▃▂   (price path left→right, oldest→newest)
    """
    BLOCKS = "▁▂▃▄▅▆▇█"
    # Prefer sr_history (APEX) — same price field name
    history = h.get("sr_history") or h.get("price_history") or []
    if len(history) < 3:
        return ""
    # Sample up to 10 evenly-spaced points
    n       = len(history)
    step    = max(1, n // 10)
    sampled = [history[i]["price"] for i in range(0, n, step)][-10:]
    lo, hi  = min(sampled), max(sampled)
    if hi == lo:
        return "▄" * len(sampled)   # flat line
    return "".join(
        BLOCKS[min(7, int((p - lo) / (hi - lo) * 7))]
        for p in sampled
    )


def _position_history_line(h: dict, current_price: float) -> str:
    """
    Build the single history line shown under each position in the positions list.

    Sparkline chars (▁▂▃▄▅▆▇█) are NOT used here — they render as blank boxes
    on many Android fonts when outside a monospace code block. The full sparkline
    is available on the token card 📜 History toggle inside a code block.

    Format:  Peak 3.4x  Low 0.82x
    """
    avg = h.get("avg_price", 0)
    if avg <= 0:
        return ""
    peak_p = h.get("peak_price", current_price)
    peak_x = round(peak_p / avg, 2) if avg > 0 else 0
    history = h.get("sr_history") or h.get("price_history") or []
    if history:
        low_p = min(snap["price"] for snap in history)
        low_x = round(low_p / avg, 2) if avg > 0 else 0
        low_txt = "  Low " + str(low_x) + "x" if low_x < 0.99 else ""
    else:
        low_txt = ""
    if peak_x < 1.01 and not low_txt:
        return ""
    return "  Peak " + str(peak_x) + "x" + low_txt



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
    # ── Vault-funded position: proceeds return to vault, not balance ─
    _vault_funded_amt = h.get("vault_funded_amt", 0.0)
    _vault_funded = h.get("vault_funded", False) and _vault_funded_amt > 0
    if _vault_funded:
        # Proceeds go back to vault; excess profit also to vault
        ud["apex_vault"] = ud.get("apex_vault", 0.0) + usd
        ud["apex_vault_pnl"] = ud.get("apex_vault_pnl", 0.0) + realized
    else:
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

    # ── APEX vault reconciliation on full close ───────────────────────────────
    # h["apex_vault_reserved"] holds the sum of 2x/5x milestone reservation
    # amounts set by apex_run_position_manager. We credit the real vault now,
    # scaled by the position's actual outcome so phantom money is impossible:
    #   - If position closed at a profit  → credit the reserved amount in full
    #   - If position closed at break-even or loss → credit nothing (no phantom gain)
    # The vault balance accumulates only real, confirmed profits this way.
    if closed:
        reserved = h.get("apex_vault_reserved", 0.0)
        if reserved > 0 and realized > 0:
            ud["apex_vault"] = ud.get("apex_vault", 0.0) + reserved
        # apex_vault_locked and apex_vault_reserved stay on the now-deleted holding;
        # Python garbage-collects them. The vault balance itself is preserved.
    save_user(uid, ud)
    if closed:
        save_trade_log(uid, trade_log.get(uid, []))
    return {
        "received":   usd,
        "realized":   realized,
        "closed":     closed,
        "hold_h":     round(hold_h, 1),
        "auto_saved": auto_saved,
    }


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

    # ── Step 1: Delete previous chart if one exists ───────────────────────────
    prev_chart_id = chart_msg_ids.get(uid)
    if prev_chart_id:
        try:
            await ctx.bot.delete_message(chat_id=chat_id, message_id=prev_chart_id)
        except Exception:
            pass
        chart_msg_ids.pop(uid, None)

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
            # ── OHLCV cache (30s TTL) — skip GeckoTerminal if data is fresh ──
            _cached_ohlcv = _ohlcv_cache.get(pair_addr)
            if _cached_ohlcv and (_time.time() - _cached_ohlcv["ts"]) < OHLCV_CACHE_TTL:
                ohlcv = _cached_ohlcv["data"]
            else:
                ohlcv = await _asyncio.wait_for(fetch_ohlcv(pair_addr, chain_id), timeout=6)
                _ohlcv_cache[pair_addr] = {"data": ohlcv, "ts": _time.time()}
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
        [InlineKeyboardButton("👛 Wallet",         callback_data="v_wallet"),
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
         InlineKeyboardButton("🧠 Score",          callback_data="tks_" + contract),
         InlineKeyboardButton("📜 History",        callback_data="th_"  + contract)],
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

    # ── APEX: position manager (trailing, profit lock, threats) ───────────────
    for _apex_uid, _apex_ud in list(users.items()):
        if _apex_ud.get("balance") is None:
            continue
        if _apex_ud.get("apex_mode") or any(
            h.get("mood") in ("APEX", "AI-Sniper")
            for h in _apex_ud.get("holdings", {}).values()
        ):
            await apex_run_position_manager(app, _apex_uid, _apex_ud)
    # ── APEX: entry confirmation queue ────────────────────────────────────────
    for _apex_uid, _apex_ud in list(users.items()):
        if _apex_uid in _apex_entry_queue:
            try:
                await apex_process_entry_queue(app, _apex_uid, _apex_ud)
            except Exception as _apex_qe:
                logger.error(f"APEX entry queue error for {_apex_uid}: {_apex_qe}", exc_info=True)


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

            # ── Skip ALL APEX/AI-Sniper positions — apex_run_position_manager
            # and apex_checker_job own these exclusively. Do NOT gate this on
            # apex_trail_stop being set: before 1.5x the trail is None and both
            # loops would fire on the same position causing a double-sell race.
            if h.get("mood") in ("APEX", "AI-Sniper", "APEX-DCA"):
                continue

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
                # ── Log auto-sell event so the trigger price and PnL are
                # never lost (the target gets deleted from auto_sells on close)
                import time as _tas
                h.setdefault("auto_sell_history", []).append({
                    "x":    t["x"],
                    "pct":  t["pct"],
                    "price": price,
                    "pnl":  result["realized"],
                    "ts":   _tas.time(),
                })
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

                # ── Append price snapshot to universal history ────────────────
                # APEX positions also have sr_history (vol-aware, used by S/R engine).
                # price_history is simpler and works for ALL position types —
                # it's what the positions screen sparkline reads from.
                import time as _th
                _now_ts = _th.time()
                ph = h.setdefault("price_history", [])
                ph.append({
                    "price": cur_price,
                    "mc":    hi.get("mc", 0),
                    "ts":    _now_ts,
                })
                if len(ph) > 500:
                    h["price_history"] = ph[-500:]

                # ── Append liquidity snapshot ─────────────────────────────────
                # liq_at_buy is fixed at entry. liq_history shows the full curve
                # so you can see liquidity drain before it triggers a threat.
                lh = h.setdefault("liq_history", [])
                lh.append({
                    "liq": hi.get("liq", 0),
                    "ts":  _now_ts,
                })
                if len(lh) > 500:
                    h["liq_history"] = lh[-500:]

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

        # ── Daily equity snapshot ─────────────────────────────────────────────
        # Saved regardless of whether user trades today. This gives a complete
        # equity curve including idle days with open positions.
        _eq = (
            ud.get("balance", 0)
            + sum(h.get("total_invested", 0) for h in ud.get("holdings", {}).values())
            + ud.get("savings", 0)
            + ud.get("apex_vault", 0.0)
        )
        _day_pnl = sum(t["realized_pnl"] for t in today_trades) if today_trades else 0.0
        ud.setdefault("equity_history", []).append({
            "date":    today.isoformat(),
            "equity":  round(_eq, 4),
            "balance": round(ud.get("balance", 0), 4),
            "pnl":     round(_day_pnl, 4),
        })
        # Keep 365 days
        if len(ud["equity_history"]) > 365:
            ud["equity_history"] = ud["equity_history"][-365:]
        # APEX report if user has apex mode or any APEX trades today
        if ud.get("apex_mode") or any(t.get("mood") in ("APEX","AI-Sniper") for t in today_trades):
            await apex_daily_report(ctx.bot, uid, ud)
            continue
        if not today_trades:
            continue
        wins = [t for t in today_trades if t["realized_pnl"] > 0]
        tpnl = sum(t["realized_pnl"] for t in today_trades)
        wr = round(len(wins)/len(today_trades)*100) if today_trades else 0
        try:
            await ctx.bot.send_message(
                chat_id=uid, parse_mode="Markdown",
                text=(
                    "\U0001f4c5 *DAILY SUMMARY*\n\n"
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
        if t.get("mood") == "AI-Sniper"
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
    Rule-based token analysis — derives verdict from sniper_score data.
    No API key required. Falls back gracefully if Anthropic API is unavailable.
    """
    sf      = ud.get("sniper_filters", {})
    max_buy = float(sf.get("buy_amount", 100))
    bal     = ud.get("balance", 0)

    score      = sc.get("score", 0)
    flags      = sc.get("flags", [])
    strengths  = sc.get("strengths", [])
    warnings   = sc.get("warnings", [])
    flag_count = len(flags)

    symbol    = info.get("symbol", "?")
    mc        = info.get("mc", 0)
    age_h     = round(info.get("age_h") or 0, 1)
    liq       = info.get("liq", 0)
    buy_pct   = info.get("buy_pct_h1", info.get("buy_pct", 50))
    ch_h1     = info.get("ch_h1", 0)
    ch_m5     = info.get("ch_m5", 0)
    lp_burn   = info.get("lp_burn")
    no_mint   = info.get("no_mint")
    no_freeze = info.get("no_freeze")
    pf_curve  = info.get("pf_curve")
    insider   = info.get("insider_pct")
    top10     = info.get("top10_pct")
    dev_pct   = float(info.get("pf_dev_pct") or info.get("dev_pct_rc") or 0)
    vol_h1    = info.get("vol_h1", 0)
    buys_h1   = info.get("buys_h1", 0)

    # ── Verdict + Confidence ──────────────────────────────────────────────────
    if flag_count >= 3 or score < 30:
        verdict    = "SKIP"
        confidence = max(1, min(3, 10 - score // 10))
    elif score >= 45 and flag_count <= 1:
        verdict    = "SNIPE"
        confidence = min(10, max(4, score // 10))
    elif score >= 35:
        verdict    = "WAIT"
        confidence = min(6, max(3, score // 12))
    else:
        verdict    = "SKIP"
        confidence = max(1, score // 15)

    # ── Rug Risk ──────────────────────────────────────────────────────────────
    if flag_count >= 2:
        rug_risk = "HIGH"
    elif flag_count == 1:
        rug_risk = "HIGH"
    elif (top10 and top10 > 40) or (insider and insider > 8) or (dev_pct and dev_pct > 5):
        rug_risk = "MEDIUM"
    elif lp_burn and lp_burn >= 90 and no_mint is True and no_freeze is not False:
        rug_risk = "LOW"
    else:
        rug_risk = "MEDIUM"

    # ── Momentum ─────────────────────────────────────────────────────────────
    if buy_pct >= 65 and ch_m5 > 0 and ch_h1 > 0:
        momentum = "STRONG"
    elif buy_pct >= 55 and ch_h1 > 0:
        momentum = "MODERATE"
    elif buy_pct < 48 or ch_h1 < -10:
        momentum = "NEGATIVE"
    else:
        momentum = "WEAK"

    # ── Social Score ─────────────────────────────────────────────────────────
    soc_count = sum([bool(info.get("twitter")), bool(info.get("telegram")), bool(info.get("website"))])
    if soc_count >= 3:   social_score = "GOOD"
    elif soc_count >= 1: social_score = "PARTIAL"
    else:                social_score = "NONE"

    # ── Thesis ───────────────────────────────────────────────────────────────
    thesis_parts = []

    if verdict == "SNIPE":
        thesis_parts.append(f"${symbol} shows strong entry signals at {mc_str(mc)} MC.")
        if lp_burn and lp_burn >= 90:
            thesis_parts.append(f"LP fully burned ({lp_burn}%) — rug protection confirmed.")
        if buy_pct >= 60:
            thesis_parts.append(f"Buy pressure solid at {buy_pct}% with {buys_h1} H1 buys.")
        if pf_curve and 30 <= pf_curve <= 65:
            thesis_parts.append(f"Pump.fun curve at {pf_curve}% — sweet spot for entry.")
    elif verdict == "WAIT":
        thesis_parts.append(f"${symbol} has potential but needs confirmation.")
        if warnings:
            thesis_parts.append(warnings[0] + ".")
        if ch_m5 <= 0:
            thesis_parts.append("Wait for 5m green candle before entry.")
        else:
            thesis_parts.append(f"Monitor momentum — 1h change {ch_h1:+.1f}%.")
    else:  # SKIP
        if flags:
            thesis_parts.append(f"Skipping — {flags[0].replace('🚨 ', '')}.")
        elif score < 45:
            thesis_parts.append(f"Score too low ({score}/100) — does not meet entry criteria.")
        else:
            thesis_parts.append("Setup does not meet entry criteria.")
        if warnings:
            thesis_parts.append(warnings[0] + ".")

    thesis = " ".join(thesis_parts) or f"Score {score}/100. {sc.get('verdict', '')}."

    # ── Red / Green flags ─────────────────────────────────────────────────────
    red_flags   = [f.replace("🚨 ", "") for f in flags[:4]]
    if warnings: red_flags += [warnings[0]]

    green_flags = [s.replace("✅ ", "").replace("🔒 ", "").replace("🎯 ", "").replace("🚀 ", "").replace("⚡ ", "") for s in strengths[:4]]

    # ── Suggested entry amount ────────────────────────────────────────────────
    if verdict == "SNIPE":
        confidence_factor = confidence / 10
        suggested = round(min(max_buy * confidence_factor, max_buy, bal), 2)
    elif verdict == "WAIT":
        suggested = round(min(max_buy * 0.5, bal), 2)
    else:
        suggested = 0.0

    # ── Try real AI if key available + credits ───────────────────────────────
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key and verdict != "SKIP":
        try:
            history_ctx = _build_history_context(ud, uid)
            prompt = (
                f"Token: ${symbol} | MC: {mc_str(mc)} | Age: {age_h}h | Score: {score}/100\n"
                f"LP: {lp_burn}% burned | Mint: {'disabled' if no_mint else 'ACTIVE'} | Buy%: {buy_pct}%\n"
                f"Flags: {'; '.join(flags) or 'None'} | Strengths: {'; '.join(strengths[:3]) or 'None'}\n"
                f"Curve: {pf_curve}% | Insiders: {insider}% | Top10: {top10}%\n"
                f"Socials: TW={'yes' if info.get('twitter') else 'no'} TG={'yes' if info.get('telegram') else 'no'}\n"
                f"History: {history_ctx}\n\n"
                "Give a 2-sentence sniper verdict. Reply ONLY in JSON:\n"
                '{"verdict":"SNIPE"|"SKIP"|"WAIT","confidence":<1-10>,"thesis":"<2 sentences>",'
                f'"suggested_amount":<float max {max_buy}>,"red_flags":[],"green_flags":[],'
                '"rug_risk":"LOW"|"MEDIUM"|"HIGH","momentum":"STRONG"|"MODERATE"|"WEAK"|"NEGATIVE",'
                '"social_score":"GOOD"|"PARTIAL"|"NONE"}'
            )
            # Rate limit
            now_ts = datetime.now().timestamp()
            if not hasattr(ai_analyze_token, "_call_times"):
                ai_analyze_token._call_times = []
            ai_analyze_token._call_times = [t for t in ai_analyze_token._call_times if now_ts - t < 60]
            if len(ai_analyze_token._call_times) < 4:
                ai_analyze_token._call_times.append(now_ts)
                client = await get_http()
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"Content-Type":"application/json","x-api-key":api_key,"anthropic-version":"2023-06-01"},
                    json={"model":"claude-haiku-4-5-20251001","max_tokens":400,"messages":[{"role":"user","content":prompt}]},
                    timeout=10.0
                )
                if resp.status_code == 200:
                    raw  = resp.json()["content"][0]["text"].strip().replace("```json","").replace("```","")
                    result = _json.loads(raw)
                    if result.get("verdict") in ("SNIPE","SKIP","WAIT"):
                        result["suggested_amount"] = min(float(result.get("suggested_amount", suggested)), max_buy, bal)
                        return result
        except Exception as _ai_err:
            logger.debug(f"AI call skipped (using rule-based): {_ai_err}")

    return {
        "verdict":          verdict,
        "confidence":       confidence,
        "suggested_amount": suggested,
        "thesis":           thesis,
        "red_flags":        red_flags[:4],
        "green_flags":      green_flags[:4],
        "rug_risk":         rug_risk,
        "momentum":         momentum,
        "social_score":     social_score,
    }


async def _broadcast_to_channel(bot, channel_id: int, info: dict, sc: dict, ai: dict, contract: str, uid: int = 0) -> bool:
    """Post channel card v3. AI shown by default. 3 buttons only."""
    try:
        text   = _ai_report_text(info, sc, ai, contract=contract, expanded=True)
        chain  = info.get("chain", "solana").lower().replace(" ", "")
        symbol = info.get("symbol", "TOKEN")
        dex_ch = {"solana":"solana","ethereum":"ethereum","base":"base","bsc":"bsc","arbitrum":"arbitrum"}.get(chain, chain)

        bot_url = ("https://t.me/" + _bot_username + "?start=" + contract) if _bot_username else None
        dex_url = "https://dexscreener.com/" + dex_ch + "/" + contract
        gt_url  = "https://www.geckoterminal.com/" + dex_ch + "/pools/" + contract
        view_url = dex_url  # primary view link

        def _build_kb(expanded: bool) -> InlineKeyboardMarkup:
            rows = []
            if bot_url:
                rows.append([InlineKeyboardButton("⚡ Buy on APEX Sniper", url=bot_url)])
            rows.append([InlineKeyboardButton("🔍 View Token Live ↗", url=view_url)])
            _score = sc.get("score", 0)
            label = f"🧠 {_score}/100 ▲" if expanded else f"🧠 {_score}/100 ▼"
            rows.append([InlineKeyboardButton(label, callback_data="ch_ai_" + contract[:32])])
            return InlineKeyboardMarkup(rows)

        msg = await bot.send_message(
            chat_id=channel_id, text=text, parse_mode="HTML",
            reply_markup=_build_kb(True),
            disable_web_page_preview=True
        )
        _ch_card_cache[contract[:32]] = {
            "channel_id": channel_id, "message_id": msg.message_id,
            "info": info, "sc": sc, "ai": ai, "contract": contract,
            "expanded": True, "bot_url": bot_url,
            "dex_url": dex_url, "gt_url": gt_url, "view_url": view_url,
        }
        if uid: _register_channel_call(uid, contract, info, channel_id)
        return True
    except Exception as e:
        logger.warning(f"Broadcast to channel {channel_id} failed: {e}")
        return False

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

                # ── Feature 3: Feed KOL buys into sniper score ────────
                for _kb in new_buys[:3]:
                    _km = _kb.get("mint", "")
                    if _km:
                        _kol_hot_contracts.setdefault(_km, []).append({
                            "label":     wallet_label,
                            "sol_spent": _kb.get("sol_spent", 0),
                            "ts":        _time.time(),
                        })
                        # Keep max 10 entries per contract, expire after 2h
                        _kol_hot_contracts[_km] = [
                            e for e in _kol_hot_contracts[_km]
                            if _time.time() - e["ts"] < 7200
                        ][-10:]

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
                            [InlineKeyboardButton("⚡ Trade on APEX Sniper", callback_data="tc_" + mint)],
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
                        ms_text = (
                            f"<b>{rocket} ${symbol} — {milestone_x}x</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"📊 MC: <b>{mc_str(entry_mc)}</b> → <b>{mc_str(cur_mc)}</b>\n"
                            f"⏱ Time to {milestone_x}x: <b>{elapsed_str}</b>\n"
                        )
                        chain = info.get("chain", "solana").lower()
                        dex_chain = {"solana":"solana","ethereum":"ethereum","base":"base","bsc":"bsc","arbitrum":"arbitrum"}.get(chain, chain)
                        buy_url = ("https://t.me/" + _bot_username + "?start=" + contract) if _bot_username else f"https://dexscreener.com/{dex_chain}/{contract}"
                        orig_msg_id = call_data.get("message_id", 0)
                        try:
                            await ctx.bot.send_message(
                                chat_id=ch_id,
                                text=ms_text,
                                parse_mode="HTML",
                                reply_to_message_id=orig_msg_id if orig_msg_id else None,
                                reply_markup=InlineKeyboardMarkup([
                                    [InlineKeyboardButton("⚡ Buy on APEX Sniper", url=buy_url)],
                                ]),
                                disable_web_page_preview=True
                            )
                            logger.info(f"Milestone {milestone_x}x posted for ${symbol} to channel {ch_id}")
                        except Exception as _me:
                            logger.warning(f"Milestone post failed for {contract}: {_me}")

            except Exception as _ce:
                logger.warning(f"Milestone tracker error for {contract}: {_ce}")


def _register_channel_call(uid: int, contract: str, info: dict, channel_id: int):
    """Record a token call to the channel so milestone_job can track it."""
    user_calls = _channel_calls.setdefault(uid, {})
    if contract not in user_calls:   # don't reset if already tracking
        user_calls[contract] = {
            "symbol":        info.get("symbol", "?"),
            "entry_mc":      info.get("mc", 0),
            "entry_price":   info.get("price", 0),
            "called_at":     datetime.now().isoformat(),
            "channel_id":    channel_id,
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
            if (ud.get("sniper_auto") or ud.get("sniper_advisory") or ud.get("apex_mode"))
            and ud.get("balance", 0) > 0
        ]
        if not active_users:
            return

        # ── Feature 2: SOL market condition check ─────────────────────
        try:
            _sol_now = await get_sol_price()
            _sol_price_history.append((_time.time(), _sol_now))
            # Keep only last 12 entries (~1 hour of 5-min runs)
            if len(_sol_price_history) > 12:
                _sol_price_history.pop(0)
            # Bearish if SOL dropped >4% vs 30 min ago (6 entries back)
            _sol_bearish = False
            if len(_sol_price_history) >= 6:
                _sol_30m_ago = _sol_price_history[-6][1]
                if _sol_30m_ago > 0 and _sol_now < _sol_30m_ago * 0.96:
                    _sol_bearish = True
                    logger.info(f"SOL market bearish — {_sol_30m_ago:.2f} → {_sol_now:.2f} (-{round((1-_sol_now/_sol_30m_ago)*100,1)}%). APEX entries paused.")
        except Exception:
            _sol_bearish = False

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

                # ── Feature 1: Velocity — buy% acceleration/deceleration ──
                _cur_bp = info.get("buy_pct_h1", info.get("buy_pct", 50))
                _prev_bp = _buy_pct_prev.get(contract)
                if _prev_bp is not None:
                    info["buy_pct_velocity"] = round(_cur_bp - _prev_bp, 1)
                else:
                    info["buy_pct_velocity"] = 0.0
                _buy_pct_prev[contract] = _cur_bp
                # Expire old velocity entries (keep cache under 2000)
                if len(_buy_pct_prev) > 2000:
                    _stale = list(_buy_pct_prev.keys())[:200]
                    for _k in _stale: _buy_pct_prev.pop(_k, None)

                # ── Feature 3: KOL boost — inject signal before scoring ──
                _kol_hits = [
                    e for e in _kol_hot_contracts.get(contract, [])
                    if _time.time() - e["ts"] < 3600  # only last 1h
                ]
                info["kol_buy_count"]  = len(_kol_hits)
                info["kol_sol_total"]  = round(sum(e["sol_spent"] for e in _kol_hits), 2)
                info["kol_labels"]     = list({e["label"] for e in _kol_hits})

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
                    skip_reason  = None
                    age_h        = info.get("age_h") or 0
                    buys_h1      = info.get("buys_h1", 0)
                    sells_h1     = info.get("sells_h1", 0)
                    buy_pct_h1   = info.get("buy_pct_h1", info.get("buy_pct", 50))
                    vol_h1       = info.get("vol_h1", 0)
                    mc           = info.get("mc", 1)
                    liq          = info.get("liq", 0)
                    is_pumpfun   = info.get("pf_curve") is not None
                    pf_curve_val = info.get("pf_curve") or 0
                    vol_mc_ratio = (vol_h1 / mc) if mc > 0 else 0
                    maker_count  = info.get("maker_count") or 0

                    # ── Use saved filter settings directly ──
                    eff_min_score   = int(sf.get("min_score",   35))
                    eff_min_liq     = float(sf.get("min_liq",    5_000))
                    eff_min_mc      = float(sf.get("min_mc",    10_000))
                    eff_max_mc      = float(sf.get("max_mc",   200_000))
                    eff_max_age     = float(sf.get("max_age_h",   72.0))
                    eff_min_buys    = int(sf.get("min_buys_h1",    10))
                    eff_min_buy_pct = int(sf.get("min_buy_pct",    50))
                    eff_max_vol_mc  = float(sf.get("max_vol_mc_ratio", 10.0))

                    # 1. Hard flags = instant skip, no exceptions
                    if sc.get("flags"):
                        skip_reason = "Hard flag: " + sc["flags"][0]

                    # 2. Score threshold
                    elif sc["score"] < eff_min_score:
                        skip_reason = f"Score too low ({sc['score']}/100 < {eff_min_score})"

                    # 3. Liquidity
                    elif is_pumpfun and liq < 5_000 and pf_curve_val < 10:
                        skip_reason = f"Pump.fun too early — liq ${liq:,.0f} / curve {pf_curve_val}%"
                    elif not is_pumpfun and liq < eff_min_liq:
                        skip_reason = f"Liq too low (${liq:,.0f})"

                    # 4. MC range
                    elif not (eff_min_mc <= mc <= eff_max_mc):
                        skip_reason = f"MC out of range ({mc_str(mc)})"

                    # 5. Age
                    elif age_h > eff_max_age:
                        skip_reason = f"Too old ({round(age_h,1)}h)"

                    # 6. Activity
                    elif buys_h1 < eff_min_buys and (not is_pumpfun or buys_h1 < 20):
                        skip_reason = f"Low activity H1 ({buys_h1} buys)"

                    # 7. Buy pressure
                    elif buy_pct_h1 < eff_min_buy_pct:
                        skip_reason = f"Sell pressure H1 ({buy_pct_h1}% buys)"

                    # 8. Wash trading
                    elif vol_mc_ratio > eff_max_vol_mc:
                        skip_reason = f"Wash trade signal (vol/MC={round(vol_mc_ratio,1)}x)"

                    # 9. No socials — soft penalty (−10 score) instead of hard skip
                    #    Pump.fun tokens and early launches often lack socials on DexScreener.
                    #    Hard-skipping them was the #1 cause of 183 skipped / 0 advisory signals.
                    _no_socials = not info.get("twitter") and not info.get("telegram")
                    if _no_socials:
                        sc["score"] = max(0, sc.get("score", 0) - 10)
                        sc.setdefault("red_flags", []).append("No socials (−10 score)")
                        # Re-check score threshold after penalty
                        if sc["score"] < eff_min_score:
                            skip_reason = f"Score too low after no-socials penalty ({sc['score']}/100)"

                    # 10. Too few unique holders (only if not already skipping)
                    if not skip_reason and maker_count > 0 and maker_count < 25:
                        skip_reason = f"Too few holders ({maker_count})"

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
                        # ── Option 3: Skip reason counter ─────────────────
                        # Bucket the reason into a short category key for display
                        _reason_key = (
                            "hard_flag"    if "Hard flag"    in skip_reason else
                            "score"        if "Score"        in skip_reason else
                            "liquidity"    if "Liq"          in skip_reason else
                            "mc_range"     if "MC out"       in skip_reason else
                            "age"          if "old"          in skip_reason else
                            "low_activity" if "activity"     in skip_reason else
                            "sell_pressure"if "Sell pressure"in skip_reason else
                            "wash_trade"   if "Wash"         in skip_reason else
                            "no_socials"   if "socials"      in skip_reason else
                            "few_holders"  if "holders"      in skip_reason else
                            "other"
                        )
                        skip_counts = ud.setdefault("sniper_skip_counts", {})
                        skip_counts[_reason_key] = skip_counts.get(_reason_key, 0) + 1
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

                    # ── Meta enrichment (description, Twitter, holders) ───
                    try:
                        _meta = await enrich_token_meta(info, item)
                        info.update(_meta)
                    except Exception as _me:
                        logger.debug(f"Meta enrichment error: {_me}")
                    try:
                        _http = await get_http()
                        _tw = await enrich_twitter_momentum(info, _http)
                        info.update(_tw)
                    except Exception as _te:
                        logger.debug(f"Twitter enrichment error: {_te}")
                    try:
                        _http = await get_http()
                        _hd = await enrich_holder_distribution(contract, info.get("chain",""), _http)
                        info.update(_hd)
                    except Exception as _hde:
                        logger.debug(f"Holder distribution error: {_hde}")

                    # ── Helius deep enrichment (Solana only) ──────────────────
                    if helius_key and info.get("chain","").lower() in ("solana","sol"):
                        # Dev wallet history — serial rugger detection
                        try:
                            _dev = await enrich_dev_wallet_history(contract, info, helius_key)
                            info.update(_dev)
                        except Exception as _de:
                            logger.debug(f"Dev history error: {_de}")

                        # Wallet clustering — coordinated insider detection
                        try:
                            _top_h = info.get("holder_distribution", [])
                            _clust = await enrich_wallet_clustering(contract, _top_h, helius_key)
                            info.update(_clust)
                        except Exception as _ce:
                            logger.debug(f"Wallet clustering error: {_ce}")

                        # Volume pattern — bot/wash trade detection
                        try:
                            _vp = await enrich_volume_pattern(contract, helius_key)
                            info.update(_vp)
                        except Exception as _ve:
                            logger.debug(f"Volume pattern error: {_ve}")

                        # Re-score with all new data
                        sc = sniper_score(info)

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
                    # MODE 0 — APEX AUTONOMOUS ENGINE
                    # ════════════════════════════════════════════════════
                    # During learning, also act on WAIT verdict — need trade data
                    _apex_verdict_ok = ai["verdict"] == "SNIPE" or (
                        ai["verdict"] == "WAIT" and apex_get_phase(ud) == "learning"
                    )
                    if ud.get("apex_mode") and _apex_verdict_ok:
                        apex_reset_daily(ud)
                        _ok = True
                        _apex_phase = apex_get_phase(ud)
                        if _apex_phase == "learning":
                            # Learning: heat check + SOL market condition
                            if apex_capital_heat(ud) >= APEX_HEAT_STOP:         _ok = False
                            elif _sol_bearish:                                   _ok = False
                        else:
                            # Calibrating / Optimised: apply learned gates
                            if apex_is_paused(uid):                              _ok = False
                            elif apex_is_daily_loss_halted(ud):                  _ok = False
                            elif apex_capital_heat(ud) >= APEX_HEAT_STOP:        _ok = False
                            elif apex_count_positions(ud) >= ud.get("apex_max_positions_learned", APEX_MAX_POSITIONS): _ok = False
                            elif ai.get("confidence",0) < ud.get("apex_learn_threshold", APEX_MIN_CONFIDENCE): _ok = False
                            elif sc.get("score",0) < ud.get("apex_learn_score_min", 35):                        _ok = False
                        if _ok:
                            # Guard: don't overwrite an already-queued entry (resets 45s timer)
                            _already_queued = contract in _apex_entry_queue.get(uid, {})
                            if not _already_queued:
                                base_amt = ud.get("sniper_filters", {}).get("buy_amount", 50.0)
                                _apex_entry_queue.setdefault(uid, {})[contract] = {
                                    "info": info, "sc": sc, "ai": ai,
                                    "queued_at": datetime.now(), "base_amount": base_amt,
                                }
                                # Mark as seen so sniper won't re-scan it next cycle
                                seen_map[contract] = _time.time()
                                logger.info(f"APEX queued {contract} ({info.get('symbol','?')}) for {uid}")
                            else:
                                logger.debug(f"APEX: {contract} already in queue for {uid}, skipping")

                    # ════════════════════════════════════════════════════
                    # MODE 1 — FULL AUTO
                    # ════════════════════════════════════════════════════
                    if ud.get("sniper_auto") and not ud.get("apex_mode") and ai["verdict"] == "SNIPE":
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
                            if ai["verdict"] in ("SNIPE", "WAIT"):
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
                            else:
                                # No channel configured — fall back to DM silently
                                # so the user still gets signals instead of nothing
                                if ai["verdict"] in ("SNIPE", "WAIT"):
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
                                        logger.error(f"Advisory fallback DM error {uid}: {_ae}")

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
            # Skip positions managed by APEX (it has own threat detection)
            if h.get("mood") == "APEX":
                continue
            # Skip AI-Sniper positions that APEX has taken over (apex_trail_stop set)
            if h.get("mood") == "AI-Sniper" and h.get("apex_trail_stop") is not None:
                continue
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

    # Handle deep link: /start CA — from channel "Buy on APEX Sniper" button
    payload = (ctx.args[0] if ctx.args else "").strip()
    if payload and len(payload) > 20 and ud.get("balance") is not None:
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
            "👋 Welcome to *APEX SNIPER BOT*!\n\n"
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
        "⚡ *APEX SNIPER BOT*\n\n"
        "Welcome back, *" + ud["username"] + "*!\n"
        "💰 Balance: *" + money(ud["balance"]) + "*\n"
        "💎 Savings: *" + money(ud["savings"]) + "*\n\n"
        "Paste any crypto CA to trade 👇"
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_menu_kb())
    else:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=main_menu_kb())


def _get_user_lock(uid: int):
    """Get or create a per-user asyncio lock (prevents concurrent buy/sell races)."""
    if uid not in _user_locks:
        import asyncio as _al
        _user_locks[uid] = _al.Lock()
    return _user_locks[uid]


def apex_get_phase(ud: dict) -> str:
    """
    Returns the APEX learning phase for a user.
    - 'learning'  : < 20 total APEX trades — gather data, relaxed gates, calibrate every 5 trades
    - 'active'    : ≥ 20 trades — full learned gates applied, calibrate every 10 trades
    """
    total = ud.get("apex_total_trades", 0)
    if total < 20:
        return "learning"
    return "active"


def apex_capital_heat(ud: dict) -> float:
    # ── IMPORTANT: only count APEX/AI-Sniper holdings as "deployed" capital.
    # Manual paper trades must NOT inflate heat or they will incorrectly block
    # APEX entries and reduce position sizes even when APEX capital is free.
    balance  = ud.get("balance", 0)
    deployed = sum(
        h.get("total_invested", 0)
        for h in ud.get("holdings", {}).values()
        if h.get("mood") in ("APEX", "AI-Sniper", "APEX-DCA")
    )
    total = balance + sum(h.get("total_invested", 0) for h in ud.get("holdings", {}).values())
    return (deployed / total) if total > 0 else 0.0


def apex_position_size(ud: dict, ai_confidence: int, base_amount: float) -> float:
    heat = apex_capital_heat(ud)
    if heat >= APEX_HEAT_STOP:
        return 0.0
    if ai_confidence >= 9:
        conf_mult = 1.0
    elif ai_confidence >= 7:
        conf_mult = 0.70
    elif ai_confidence >= 5:
        conf_mult = 0.50
    else:
        return 0.0
    if heat >= APEX_HEAT_CAUTION:
        heat_mult = 0.50
    elif heat >= APEX_HEAT_SAFE:
        heat_mult = 0.75
    else:
        heat_mult = 1.0
    consec = ud.get("apex_consec_losses", 0)
    dd_mult = APEX_DRAWDOWN_2_MULT if consec >= 2 else APEX_DRAWDOWN_1_MULT if consec >= 1 else 1.0
    # During learning phase, ignore drawdown multiplier — need full data
    _phase = apex_get_phase(ud)
    if _phase == "learning":
        dd_mult = 1.0
    size_mult = ud.get("apex_size_mult", 1.0)
    return max(1.0, round(base_amount * conf_mult * heat_mult * dd_mult * size_mult, 2))


def apex_trail_pct(current_x: float) -> float:
    if current_x >= 10.0: return APEX_TRAIL_PCT_MOON
    if current_x >= 5.0:  return APEX_TRAIL_PCT_HIGH
    if current_x >= 2.0:  return APEX_TRAIL_PCT_MID
    return APEX_TRAIL_PCT_EARLY


def apex_check_threat(info: dict, h: dict) -> str:
    """
    Rug/dump threat detector — runs every 8–15s.

    SMART RED THRESHOLDS (entry-aligned + peak-aware):
    ─────────────────────────────────────────────────
    Base stop is read from h["stop_loss_pct"] which APEX sets at entry
    based on the AI rug-risk assessment:
        HIGH rug risk  → 12%   tight, suspicious token
        MEDIUM         → 18%   moderate room
        LOW            → 22%   healthy token, needs space

    Peak-aware expansion: once a token has proven buyers exist by
    reaching a peak above entry, it earns additional room proportional
    to how high it went — because deep retraces after a pump are normal:
        Peak 1.2x–2x   → +8%  extra room  (e.g. LOW: 22% → 30%)
        Peak 2x+        → +15% extra room  (e.g. LOW: 22% → 37%)
        Peak never > 1.2x → no expansion  (token never showed strength)

    HARD FLOORS — never change regardless of peak or base stop:
        Liq drain -25%                  → RED always (LP removal)
        Buy% below 20%                  → RED always (panic)
        Liq -15% + buy% < 40%           → RED always (coordinated dump)
        Volume spike 4x + buy% < 30%    → RED always (bundle sell)
    """
    price      = info.get("price", 0)
    avg        = h.get("avg_price", price)
    liq        = info.get("liq", 0)
    liq_at_buy = h.get("liq_at_buy", liq)
    vol_m5     = info.get("vol_m5", 0)
    vol_h1     = info.get("vol_h1", 1)
    buy_pct_m5 = info.get("buy_pct_m5", info.get("buy_pct", 50))
    buy_pct_h1 = info.get("buy_pct_h1", info.get("buy_pct", 50))
    drop_pct   = (price - avg) / avg * 100 if avg > 0 else 0
    liq_drop   = ((liq_at_buy - liq) / liq_at_buy * 100) if liq_at_buy > 0 else 0
    avg_5m_vol = vol_h1 / 12 if vol_h1 > 0 else 0

    # ── Compute smart RED threshold ───────────────────────────────────────────
    # Base: read the per-token stop set at entry (rug-risk aligned)
    base_stop = h.get("stop_loss_pct", 15.0)   # fallback 15% for legacy positions

    # Peak-aware expansion: token proved buyers exist → earn more retrace room
    peak       = h.get("apex_peak_price", avg)
    cx_peak    = (peak / avg) if avg > 0 else 1.0
    if cx_peak >= 2.0:
        # Strong token — peaked at 2x+, deep retraces before continuation normal
        red_threshold = -(base_stop + 15.0)
    elif cx_peak >= 1.2:
        # Showed strength — peaked 1.2x–2x, give room for healthy retrace
        red_threshold = -(base_stop + 8.0)
    else:
        # Never proved itself — use base stop as-is
        red_threshold = -base_stop

    # ══ HARD FLOOR RED — rug signals, never bypassed by smart threshold ═══════
    if liq_drop >= 25:                                          return "RED"
    if buy_pct_m5 < 20:                                        return "RED"
    if liq_drop >= 15 and buy_pct_m5 < 40:                     return "RED"
    if avg_5m_vol > 0 and vol_m5 >= avg_5m_vol * 4 and buy_pct_m5 < 30: return "RED"

    # ══ SMART PRICE RED — entry-aligned + peak-aware ══════════════════════════
    if drop_pct <= red_threshold:                               return "RED"
    # Combined signal: price breach + sell pressure (tighter than hard floor)
    if drop_pct <= (red_threshold * 0.7) and buy_pct_m5 < 40: return "RED"

    # ══ ORANGE — tighten trail stop ══════════════════════════════════════════
    orange_threshold = red_threshold * 0.55   # ~55% of RED threshold
    sigs = 0
    if drop_pct <= orange_threshold:                           sigs += 1
    if liq_drop >= 10:                                         sigs += 1
    if buy_pct_m5 < 40:                                        sigs += 1
    if buy_pct_h1 < 42:                                        sigs += 1
    if avg_5m_vol > 0 and vol_m5 >= avg_5m_vol * 2.5 and buy_pct_m5 < 45: sigs += 1
    if sigs >= 2: return "ORANGE"

    # ══ YELLOW — watch closely ════════════════════════════════════════════════
    yellow_threshold = red_threshold * 0.30
    warn = 0
    if drop_pct <= yellow_threshold:                           warn += 1
    if liq_drop >= 5:                                          warn += 1
    if buy_pct_m5 < 48:                                        warn += 1
    if buy_pct_h1 < 45:                                        warn += 1
    if warn >= 2 or sigs >= 1: return "YELLOW"
    return "CLEAR"


def apex_reset_daily(ud: dict) -> None:
    import time as _t
    today = _t.strftime("%Y-%m-%d")
    if ud.get("apex_daily_date") != today:
        ud["apex_daily_date"]         = today
        ud["apex_daily_pnl"]          = 0.0
        ud["apex_session_start_bal"]  = (
            ud.get("balance", 0)
            + sum(h.get("total_invested", 0) for h in ud.get("holdings", {}).values())
            + ud.get("apex_vault", 0.0)
        )


def apex_is_paused(uid: int) -> bool:
    from datetime import datetime as _dt
    pause_until = _apex_paused_until.get(uid)
    if pause_until and _dt.now() < pause_until:
        return True
    _apex_paused_until.pop(uid, None)
    return False


def apex_is_daily_loss_halted(ud: dict) -> bool:
    apex_reset_daily(ud)
    start = ud.get("apex_session_start_bal", ud.get("balance", 1))
    if start is None or start <= 0:
        return True   # FIX: zero/None balance → halt trading, don't keep trying
    return ud.get("apex_daily_pnl", 0.0) <= -(start * APEX_DAILY_LOSS_LIMIT)


def apex_count_positions(ud: dict) -> int:
    return sum(1 for h in ud.get("holdings", {}).values() if h.get("mood") in ("APEX", "AI-Sniper"))


def apex_learn_record(uid: int, entry: dict) -> None:
    # Also write to module global for fast in-session access
    mem = _apex_learn_memory.setdefault(uid, [])
    mem.append(entry)
    if len(mem) > APEX_SELF_LEARN_WINDOW * 2:
        _apex_learn_memory[uid] = mem[-APEX_SELF_LEARN_WINDOW:]
    # Persist inside ud so it survives restarts
    if uid in users:
        ud_mem = users[uid].setdefault("apex_memory", [])
        ud_mem.append(entry)
        if len(ud_mem) > APEX_SELF_LEARN_WINDOW * 2:
            users[uid]["apex_memory"] = ud_mem[-APEX_SELF_LEARN_WINDOW:]


def apex_self_calibrate(ud: dict, uid: int) -> dict:
    mem = _apex_learn_memory.get(uid, [])
    if len(mem) < 10:
        return {}
    recent = mem[-APEX_SELF_LEARN_WINDOW:]
    wins   = [t for t in recent if t.get("pnl", 0) > 0]
    wr     = len(wins) / len(recent) if recent else 0
    changes = {}
    cur_conf = ud.get("apex_learn_threshold", APEX_MIN_CONFIDENCE)
    conf_buckets = {}
    for t in recent:
        b = (t.get("confidence", 5) // 2) * 2
        conf_buckets.setdefault(b, []).append(t.get("pnl", 0))
    losing = [b for b, pnls in conf_buckets.items() if sum(pnls) < 0 and len(pnls) >= 3]
    if losing:
        new_thresh = min(max(losing) + 2, 8)
        if new_thresh != cur_conf:
            ud["apex_learn_threshold"] = new_thresh
            changes["apex_learn_threshold"] = {"old": cur_conf, "new": new_thresh,
                "reason": f"Conf<={max(losing)} trades losing ({len(losing)} buckets)"}
    cur_score = ud.get("apex_learn_score_min", 45)
    score_low = [t.get("pnl", 0) for t in recent if t.get("score", 50) < 40]
    if len(score_low) >= 5 and sum(score_low) < 0 and cur_score < 50:
        new_score_floor = min(50, cur_score + 5)
        ud["apex_learn_score_min"] = new_score_floor
        changes["apex_learn_score_min"] = {"old": cur_score, "new": new_score_floor,
            "reason": f"Low-score trades ({len(score_low)}) consistently losing — tightening gradually"}
    elif wr > 0.6 and cur_score > 45:
        new_s = max(45, cur_score - 3)
        if new_s != cur_score:
            ud["apex_learn_score_min"] = new_s
            changes["apex_learn_score_min"] = {"old": cur_score, "new": new_s,
                "reason": f"High WR {round(wr*100)}% — relaxing score filter"}
    return changes


# ══════════════════════════════════════════════════════════════════════════════
# APEX ASYNC FUNCTIONS (need bot context — injected into bot.py scope)
# ══════════════════════════════════════════════════════════════════════════════

async def apex_run_position_manager(app, uid: int, ud: dict, positions_due: list = None) -> None:
    from datetime import timedelta
    async with _get_user_lock(uid):
        for contract, h in list(ud.get("holdings", {}).items()):
            if h.get("mood") not in ("APEX", "AI-Sniper", "APEX-DCA"):
                continue
            # Skip if not due for check this cycle (adaptive interval)
            if positions_due is not None and contract not in positions_due:
                continue
            try:
                info = await get_token(contract)
                if not info:
                    continue
                # ── Helius RPC: faster live price + on-chain rug signal ────
                _helius_key = os.environ.get("HELIUS_API_KEY", "")
                if _helius_key and info.get("chain","").lower() in ("solana","sol"):
                    _pair = h.get("pair_addr", "") or info.get("pair_addr", "")
                    if _pair:
                        _live = await get_helius_pool_price(contract, _pair, _helius_key)
                        if _live.get("price"):
                            info["price"] = _live["price"]
                            info["liq"]   = _live.get("liq", info["liq"])
                            # ── Write Helius price back to shared cache so all
                            # callers in this cycle see the same price, not stale
                            # DexScreener data from up to 12s ago.
                            if contract in _token_cache:
                                _token_cache[contract]["data"]["price"] = info["price"]
                                _token_cache[contract]["data"]["liq"]   = info["liq"]
                    # Check for on-chain liquidity removal (instant rug signal)
                    _rug_sig = await get_helius_rug_signal(contract, _helius_key)
                    if _rug_sig.get("rug_detected"):
                        logger.warning(f"Helius rug signal: {contract} — {_rug_sig['reason']}")
                        info["_helius_rug"] = True
                price = info["price"]
                avg   = h.get("avg_price", price)
                if avg <= 0:
                    continue
                # Skip if position already fully exited (race condition guard)
                if h.get("amount", 0) <= 0:
                    continue
                cx = price / avg
                if price > h.get("apex_peak_price", avg):
                    h["apex_peak_price"] = price
                peak   = h.get("apex_peak_price", price)
                threat = apex_check_threat(info, h)
                # ── Only record threat transition when the level changes ──────────
                # Logging every cycle would be noise. This gives you the exact
                # moment and price at which each escalation/de-escalation happened.
                _prev_threat = h.get("apex_threat", "CLEAR")
                if threat != _prev_threat:
                    import time as _tth
                    h.setdefault("threat_history", []).append({
                        "from": _prev_threat,
                        "to":   threat,
                        "cx":   round(cx, 3),
                        "price": price,
                        "ts":   _tth.time(),
                    })
                h["apex_threat"] = threat
                # ── S/R candle recording (for zone tracking) ─────────────────────
                apex_sr_record_candle(h, price, info.get("mc", 0),
                                      info.get("vol_m5", 0),
                                      info.get("buy_pct_m5", info.get("buy_pct", 50)))

                # ── RED: emergency exit ───────────────────────────────────────────
                if threat == "RED" or info.get("_helius_rug"):
                    cv     = h["amount"] * price
                    result = sell_core(ud, uid, contract, cv, price, "apex_threat_red")
                    ud["apex_daily_pnl"] = ud.get("apex_daily_pnl", 0) + result["realized"]
                    if result["realized"] < 0:
                        ud["apex_consec_losses"] = ud.get("apex_consec_losses", 0) + 1
                        if ud["apex_consec_losses"] >= 3:
                            _apex_paused_until[uid] = datetime.now() + timedelta(minutes=APEX_DRAWDOWN_3_PAUSE)
                    else:
                        ud["apex_consec_losses"] = 0
                        ud["apex_total_wins"] = ud.get("apex_total_wins", 0) + 1
                    ud["apex_total_trades"] = ud.get("apex_total_trades", 0) + 1
                    apex_learn_record(uid, {"score": h.get("apex_entry_score", 0),
                        "confidence": h.get("apex_entry_conf", 0), "verdict": "SNIPE",
                        "outcome_x": round(cx, 3), "reason": "apex_threat_red", "pnl": result["realized"]})
                    # ── Self-calibrate on RED exits too, not just trail exits.
                    # Rug/dump losses are the most important signal for the learning engine.
                    _cal_freq = 5 if apex_get_phase(ud) == "learning" else 10
                    if ud.get("apex_total_trades", 0) % _cal_freq == 0:
                        apex_self_calibrate(ud, uid)
                    _apex_last_check.get(uid, {}).pop(contract, None)
                    # ── Register post-exit tracker ────────────────────────────
                    _apex_post_exit.setdefault(uid, {})[contract] = {
                        "symbol":      h["symbol"],
                        "exit_price":  price,
                        "entry_price": avg,
                        "exit_reason": "apex_threat_red",
                        "exit_x":      round(cx, 3),
                        "exit_at":     _time.time(),
                        "snapshots":   [],   # filled by apex_checker_job at 30m/1h/4h
                    }
                    try:
                        await app.bot.send_message(chat_id=uid, parse_mode="Markdown",
                            text=("*\U0001f6a8 APEX \u2014 THREAT RED \u2014 EMERGENCY EXIT*\n\n"
                                  "*$" + h["symbol"] + "*\n"
                                  "Threat: *\U0001f534 Critical dump/rug signal*\n"
                                  "Exit: *" + str(round(cx, 2)) + "x*  |  PnL: *" + pstr(result["realized"]) + "*\n"
                                  "Cash: *" + money(ud["balance"]) + "*"),
                            reply_markup=main_menu_kb())
                    except Exception:
                        pass
                    continue

                # ── Profit lock milestones at 2x and 5x ──────────────────────────
                # DESIGN NOTE: These milestones are RESERVATIONS, not cash transfers.
                # We record the intended lock amount on the holding in
                # h["apex_vault_reserved"] so sell_core can credit the real vault
                # proportionally when the position closes. ud["apex_vault"] must
                # never be incremented here — that would create phantom money if
                # the position later loses back the gain before closing.
                vault_locks = h.setdefault("apex_vault_locked", {})
                cv          = h["amount"] * price
                entry_val   = h.get("total_invested", cv)
                if cx >= 5.0 and "5x" not in vault_locks:
                    profit   = cv - entry_val
                    lock_amt = max(0.0, profit * APEX_LOCK_5X_PCT)
                    if lock_amt > 0:
                        vault_locks["5x"] = lock_amt
                        # Store total reserved on holding for sell_core to consume
                        h["apex_vault_reserved"] = h.get("apex_vault_reserved", 0.0) + lock_amt
                        try:
                            await app.bot.send_message(chat_id=uid, parse_mode="Markdown",
                                text=("\U0001f3e6 *APEX \u2014 PROFIT MILESTONE (5x)*\n\n"
                                      "*$" + h["symbol"] + "* hit *5x*!\n"
                                      "Reserving: *" + money(lock_amt) + "* \u2192 Vault on close\n"
                                      "\U0001f512 Will lock when position closes\n"
                                      "Trail continues\u2026"),
                                reply_markup=main_menu_kb())
                        except Exception:
                            pass
                elif cx >= 2.0 and "2x" not in vault_locks:
                    profit   = cv - entry_val
                    lock_amt = max(0.0, profit * APEX_LOCK_2X_PCT)
                    if lock_amt > 0:
                        vault_locks["2x"] = lock_amt
                        # Store total reserved on holding for sell_core to consume
                        h["apex_vault_reserved"] = h.get("apex_vault_reserved", 0.0) + lock_amt
                        try:
                            await app.bot.send_message(chat_id=uid, parse_mode="Markdown",
                                text=("\U0001f3e6 *APEX \u2014 PROFIT MILESTONE (2x)*\n\n"
                                      "*$" + h["symbol"] + "* hit *2x*!\n"
                                      "Reserving: *" + money(lock_amt) + "* \u2192 Vault on close\n"
                                      "\U0001f512 Will lock when position closes\n"
                                      "Trail now active \u2014 riding the move\u2026"),
                                reply_markup=main_menu_kb())
                        except Exception:
                            pass

                # ── Trailing stop ─────────────────────────────────────────────────
                if cx >= APEX_TRAIL_ACTIVATE_X:
                    tpct = apex_trail_pct(cx)
                    if threat == "ORANGE":
                        tpct = min(tpct, 0.06)
                    elif threat == "YELLOW":
                        tpct = min(tpct, 0.10)
                    # ── S/R multiplier: tighten trail near resistance zones ──
                    sr_mult = apex_sr_trail_multiplier(h, info.get("mc", 0), info.get("vol_m5", 0))
                    tpct    = tpct * sr_mult
                    tpct    = max(tpct, 0.04)   # floor: never tighter than 4%
                    trail_stop = peak * (1.0 - tpct)
                    h["apex_trail_stop"] = trail_stop
                    h["apex_trail_pct"]  = tpct
                    if price <= trail_stop:
                        cv2    = h["amount"] * price
                        if cv2 < 0.01:
                            continue
                        result = sell_core(ud, uid, contract, cv2, price, "apex_trail_exit")
                        ud["apex_daily_pnl"] = ud.get("apex_daily_pnl", 0) + result["realized"]
                        if result["realized"] < 0:
                            ud["apex_consec_losses"] = ud.get("apex_consec_losses", 0) + 1
                            if ud["apex_consec_losses"] >= 3:
                                _apex_paused_until[uid] = datetime.now() + timedelta(minutes=APEX_DRAWDOWN_3_PAUSE)
                        else:
                            ud["apex_consec_losses"] = 0
                            ud["apex_total_wins"] = ud.get("apex_total_wins", 0) + 1
                        ud["apex_total_trades"] = ud.get("apex_total_trades", 0) + 1
                        apex_learn_record(uid, {"score": h.get("apex_entry_score", 0),
                            "confidence": h.get("apex_entry_conf", 0), "verdict": "SNIPE",
                            "outcome_x": round(cx, 3), "reason": "apex_trail_exit", "pnl": result["realized"]})
                        if ud.get("apex_total_trades", 0) % 10 == 0:
                            apex_self_calibrate(ud, uid)
                        vault_total = sum(vault_locks.values())
                        try:
                            await app.bot.send_message(chat_id=uid, parse_mode="Markdown",
                                text=("\U0001f4c8 *APEX \u2014 TRAIL EXIT*\n\n"
                                      "*$" + h["symbol"] + "*\n"
                                      "Peak: *" + str(round(peak / avg, 2)) + "x*  \u2192  Exit: *" + str(round(cx, 2)) + "x*\n"
                                      "Trail triggered at *" + str(round(tpct * 100, 1)) + "%* below peak\n"
                                      "PnL: *" + pstr(result["realized"]) + "*\n"
                                      "\U0001f512 Vault gains: *" + money(vault_total) + "*\n"
                                      "Cash: *" + money(ud["balance"]) + "*"),
                                reply_markup=main_menu_kb())
                        except Exception:
                            pass
                        # ── Register post-exit tracker ────────────────────
                        _apex_post_exit.setdefault(uid, {})[contract] = {
                            "symbol":      h["symbol"],
                            "exit_price":  price,
                            "entry_price": avg,
                            "exit_reason": "apex_trail_exit",
                            "exit_x":      round(cx, 3),
                            "exit_at":     _time.time(),
                            "snapshots":   [],
                        }
                        continue

                # ── Smart DCA at support (before momentum exit check) ────────────
                _dca_range = APEX_DCA_MIN_CX <= cx <= APEX_DCA_MAX_CX
                if _dca_range and cx < APEX_TRAIL_ACTIVATE_X:
                    try:
                        await apex_try_smart_dca(app, uid, ud, contract, h, info)
                    except Exception as _dca_err:
                        logger.debug(f"APEX DCA error: {_dca_err}")

                # ── Momentum decay early exit — staged partial exit ────────────────
                if cx < 1.2:
                    bpm5 = info.get("buy_pct_m5", info.get("buy_pct", 50))
                    if bpm5 < 45:
                        import time as _mdt
                        _partial_ts = h.get("apex_partial_exit_ts", 0)
                        _now_ts     = _time.time()
                        if _partial_ts == 0:
                            # First decay signal — sell 40%, wait 60s before full exit
                            _cv3p = h["amount"] * price * 0.40
                            if _cv3p >= 0.50:
                                sell_core(ud, uid, contract, _cv3p, price, "apex_momentum_decay_partial")
                                h["apex_partial_exit_ts"] = _now_ts
                                try:
                                    await app.bot.send_message(chat_id=uid, parse_mode="Markdown",
                                        text=("⚠️ *APEX — Momentum Warning*\n\n"
                                              "*$" + h["symbol"] + "*\n"
                                              "Buy pressure: *" + str(bpm5) + "%* at *" + str(round(cx,2)) + "x*\n"
                                              "Sold 40% — watching for recovery (60s)"),
                                        reply_markup=main_menu_kb())
                                except Exception: pass
                            continue
                        elif _now_ts - _partial_ts < 180:   # 3-minute grace window (was 60s)
                            continue
                        # 3 min passed and still decaying — exit remainder
                        h.pop("apex_partial_exit_ts", None)
                        cv3    = h["amount"] * price
                        result = sell_core(ud, uid, contract, cv3, price, "apex_momentum_decay")
                        ud["apex_daily_pnl"] = ud.get("apex_daily_pnl", 0) + result["realized"]
                        if result["realized"] < 0:
                            ud["apex_consec_losses"] = ud.get("apex_consec_losses", 0) + 1
                        else:
                            ud["apex_consec_losses"] = 0
                            ud["apex_total_wins"] = ud.get("apex_total_wins", 0) + 1
                        ud["apex_total_trades"] = ud.get("apex_total_trades", 0) + 1
                        # ── Pause check must read the UPDATED consec_losses value
                        if ud["apex_consec_losses"] >= 3:
                            _apex_paused_until[uid] = datetime.now() + timedelta(minutes=APEX_DRAWDOWN_3_PAUSE)
                        apex_learn_record(uid, {"score": h.get("apex_entry_score", 0),
                            "confidence": h.get("apex_entry_conf", 0), "verdict": "SNIPE",
                            "outcome_x": round(cx, 3), "reason": "apex_momentum_decay", "pnl": result["realized"]})
                        if ud.get("apex_total_trades", 0) % 10 == 0:
                            apex_self_calibrate(ud, uid)
                        try:
                            await app.bot.send_message(chat_id=uid, parse_mode="Markdown",
                                text=("\u26a0\ufe0f *APEX \u2014 EARLY EXIT (Momentum Decay)*\n\n"
                                      "*$" + h["symbol"] + "*\n"
                                      "M5 buy pressure: *" + str(bpm5) + "%* \u2014 thesis broken\n"
                                      "Exit: *" + str(round(cx, 2)) + "x*  |  PnL: *" + pstr(result["realized"]) + "*\n"
                                      "Small loss taken early \u2014 protecting capital"),
                                reply_markup=main_menu_kb())
                        except Exception:
                            pass
                        # ── Register post-exit tracker ────────────────────
                        _apex_post_exit.setdefault(uid, {})[contract] = {
                            "symbol":      h["symbol"],
                            "exit_price":  price,
                            "entry_price": avg,
                            "exit_reason": "apex_momentum_decay",
                            "exit_x":      round(cx, 3),
                            "exit_at":     _time.time(),
                            "snapshots":   [],
                        }
                        continue

                # ── No exit this cycle: persist state changes (apex_threat,
                # apex_peak_price, apex_trail_stop, sr_history) so they survive
                # a bot restart. sell_core handles saves on exits; we handle it
                # here for the non-exit path.
                save_user(uid, ud)
            except Exception as _e:
                logger.warning(f"APEX position manager {uid}/{contract}: {_e}")


async def apex_process_entry_queue(app, uid: int, ud: dict) -> None:
    queue = _apex_entry_queue.get(uid, {})
    if not queue:
        return
    from datetime import timedelta
    now = datetime.now()
    processed = []
    async with _get_user_lock(uid):
      for contract, entry in list(queue.items()):
        if (now - entry.get("queued_at", now)).total_seconds() < APEX_CONFIRM_WAIT_S:
            continue
        processed.append(contract)
        info_orig = entry["info"]
        sc        = entry["sc"]
        ai        = entry["ai"]
        base_amt  = entry.get("base_amount", 50.0)
        try:
            info_live = await get_token(contract)
        except Exception:
            info_live = None
        if not info_live:
            continue
        bpct_now = info_live.get("buy_pct_m5", info_live.get("buy_pct", 50))
        mc_runup  = info_live.get("mc", 0) / max(info_orig.get("mc", 1), 1)
        if bpct_now < 50:
            logger.info(f"APEX queue: {contract} rejected — buy% faded to {bpct_now}%")
            continue
        if mc_runup > 2.0:
            logger.info(f"APEX queue: {contract} rejected — already {round(mc_runup,1)}x since signal")
            continue
        # Guard: don't double-buy if already in holdings
        if contract in ud.get("holdings", {}):
            logger.info(f"APEX queue: {contract} already in holdings — skipping duplicate buy")
            continue
        buy_amt = apex_position_size(ud, ai.get("confidence", 5), base_amt)
        if buy_amt < 1.0:
            continue
        _eq_phase = apex_get_phase(ud)
        if _eq_phase != "learning":
            if apex_count_positions(ud) >= ud.get("apex_max_positions_learned", APEX_MAX_POSITIONS):
                continue
            if apex_is_daily_loss_halted(ud):
                continue
        budget  = ud.get("sniper_daily_budget", 500.0)
        spent   = ud.get("sniper_daily_spent", 0.0)
        _vault_trade = ud.get("apex_vault_trade_on", False)
        _vault_bal   = ud.get("apex_vault", 0.0)
        if _vault_trade and _vault_bal >= buy_amt:
            # Vault-funded entry — deduct from vault, not balance
            buy_amt = min(buy_amt, _vault_bal, budget - spent)
            if buy_amt < 1.0:
                continue
            ud["apex_vault"] = _vault_bal - buy_amt
            ud["sniper_daily_spent"] = spent + buy_amt
            result = await do_buy_core(ud, uid, contract, buy_amt, planned=True, mood="APEX")
            # Tag holding as vault-funded so sell_core routes proceeds back
            _new_h = ud.get("holdings", {}).get(contract, {})
            if _new_h:
                _new_h["vault_funded"]     = True
                _new_h["vault_funded_amt"] = buy_amt
        else:
            buy_amt = min(buy_amt, ud.get("balance", 0), budget - spent)
            if buy_amt < 1.0:
                continue
            ud["sniper_daily_spent"] = spent + buy_amt
            result = await do_buy_core(ud, uid, contract, buy_amt, planned=True, mood="APEX")
        if not isinstance(result, tuple):
            logger.warning(f"APEX buy failed: {result}")
            continue
        info_post, _ = result
        h = ud["holdings"].get(contract, {})
        if not h:
            continue
        h["apex_peak_price"]   = info_post["price"]
        h["apex_trail_stop"]   = None
        h["apex_trail_pct"]    = APEX_TRAIL_PCT_EARLY
        h["apex_threat"]       = "CLEAR"
        h["apex_vault_locked"] = {}
        h["apex_entry_score"]  = sc.get("score", 0)
        h["apex_entry_conf"]   = ai.get("confidence", 0)
        h["liq_at_buy"]        = info_post.get("liq", 0)
        h["pair_addr"]         = info_post.get("pair_addr", "")
        # Tighter stop loss — rugs move fast
        _rug = ai.get("rug_risk", "LOW")
        _apex_sl = 12.0 if _rug == "HIGH" else (18.0 if _rug == "MEDIUM" else 22.0)
        h["stop_loss_pct"] = _apex_sl
        import time as _tsla
        h.setdefault("stop_loss_history", []).append({
            "old":    None,
            "new":    _apex_sl,
            "source": "apex_entry",
            "cx":     1.0,
            "ts":     _tsla.time(),
        })
        # S/R + DCA tracking fields
        h["sr_history"]        = []
        h["sr_peak_vol"]       = 0.0
        h["sr_peak_visit_vol"] = 0.0
        h["sr_buy_pct_dipped"] = False
        h["apex_dca_count"]    = 0
        h["apex_last_dca_ts"]  = 0.0
        h["apex_dca_history"]  = []
        ud.setdefault("sniper_bought", []).append(contract)
        ud.setdefault("sniper_log", []).append({
            "contract":   contract, "symbol": info_post["symbol"],
            "chain":      info_post.get("chain", "?"), "mc": info_post["mc"],
            "score":      sc.get("score", 0), "verdict": "SNIPE",
            "confidence": ai.get("confidence", 0), "rug_risk": ai.get("rug_risk", "?"),
            "thesis":     ai.get("thesis", ""), "timestamp": datetime.now().isoformat(),
            "bought":     True, "amount": buy_amt, "mode": "APEX",
        })
        if len(ud["sniper_log"]) > SNIPER_LOG_MAX:
            ud["sniper_log"] = ud["sniper_log"][-SNIPER_LOG_MAX:]
        heat = round(apex_capital_heat(ud) * 100, 1)
        try:
            await app.bot.send_message(chat_id=uid, parse_mode="Markdown",
                text=("\U0001f3af *APEX ENTRY CONFIRMED*\n\n"
                      "*$" + info_post["symbol"] + "*  " + info_post.get("chain", "").upper() + "\n"
                      "Confidence: *" + str(ai.get("confidence", 0)) + "/10*  |  Score: *" + str(sc.get("score", 0)) + "/100*\n"
                      "Rug Risk: *" + ai.get("rug_risk", "?") + "*\n\n"
                      "\U0001f4dd " + ai.get("thesis", "\u2014") + "\n\n"
                      "\U0001f4b5 Bought: *" + money(buy_amt) + "*\n"
                      "Entry MC: *" + mc_str(info_post["mc"]) + "*\n"
                      "Heat: *" + str(heat) + "%*  |  Positions: *" + str(apex_count_positions(ud)) + "/∞*\n"
                      "\U0001f6d1 SL: *" + str(h["stop_loss_pct"]) + "%*  |  \U0001f501 Trail: *activates at 1.5x*"),
                reply_markup=main_menu_kb())
        except Exception as _ne:
            logger.error(f"APEX entry notify: {_ne}")
    for c in processed:
        queue.pop(c, None)
    if not queue:
        _apex_entry_queue.pop(uid, None)


async def apex_daily_report(bot, uid: int, ud: dict) -> None:
    today = datetime.now().date()
    logs  = trade_log.get(uid, [])
    apex_trades = [t for t in logs
                   if t.get("closed_at", datetime.min).date() == today
                   and t.get("mood") in ("APEX", "AI-Sniper")]
    if not apex_trades:
        try:
            await bot.send_message(chat_id=uid, parse_mode="Markdown",
                text=("\U0001f4c5 *APEX DAILY REPORT*\n\n"
                      "No APEX trades executed today.\n"
                      "Vault: *" + money(ud.get("apex_vault", 0)) + "*\n"
                      "Balance: *" + money(ud.get("balance", 0)) + "*"),
                reply_markup=main_menu_kb())
        except Exception:
            pass
        return
    wins  = [t for t in apex_trades if t.get("realized_pnl", 0) > 0]
    losses= [t for t in apex_trades if t.get("realized_pnl", 0) <= 0]
    total_pnl = sum(t.get("realized_pnl", 0) for t in apex_trades)
    wr   = round(len(wins) / len(apex_trades) * 100) if apex_trades else 0
    aw   = sum(t.get("realized_pnl", 0) for t in wins) / len(wins) if wins else 0
    al   = sum(t.get("realized_pnl", 0) for t in losses) / len(losses) if losses else 0
    best = max(apex_trades, key=lambda t: t.get("realized_pnl", 0))
    wrst = min(apex_trades, key=lambda t: t.get("realized_pnl", 0))
    reasons = {}
    for t in apex_trades:
        r = t.get("reason", "manual")
        reasons[r] = reasons.get(r, 0) + 1
    reason_lines = []
    reason_icons = {"apex_trail_exit": "\U0001f4c8", "apex_momentum_decay": "\u26a0\ufe0f",
                    "apex_threat_red": "\U0001f6a8", "stop_loss": "\U0001f6d1",
                    "apex_threat_orange": "\U0001f7e0", "manual": "\U0001f4cc"}
    for r, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
        icon = reason_icons.get(r, "\u25aa\ufe0f")
        reason_lines.append("  " + icon + " " + r.replace("apex_", "").replace("_", " ") + ": " + str(cnt))
    learn_changes = apex_self_calibrate(ud, uid)
    learn_txt = ""
    if learn_changes:
        learn_txt = "\n\n\U0001f9e0 *SELF-LEARNING ADJUSTMENTS:*\n"
        for key, chg in learn_changes.items():
            label = key.replace("apex_learn_", "").replace("_", " ").title()
            learn_txt += "  " + label + ": *" + str(chg["old"]) + "* \u2192 *" + str(chg["new"]) + "*\n"
            learn_txt += "  _" + chg["reason"] + "_\n"
    mem     = _apex_learn_memory.get(uid, [])
    mem_wr  = round(len([m for m in mem if m.get("pnl", 0) > 0]) / len(mem) * 100) if mem else 0
    life_pnl= sum(m.get("pnl", 0) for m in mem)
    try:
        await bot.send_message(chat_id=uid, parse_mode="Markdown",
            text=("\U0001f4c5 *APEX DAILY REPORT \u2014 " + str(today) + "*\n\n"
                  "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                  "\U0001f3af *TODAY'S PERFORMANCE*\n"
                  "Trades: *" + str(len(apex_trades)) + "*  (" + str(len(wins)) + "W / " + str(len(losses)) + "L)\n"
                  "Win Rate: *" + str(wr) + "%*\n"
                  "Total PnL: *" + pstr(total_pnl) + "*\n"
                  "Avg Win: *" + money(aw) + "*  |  Avg Loss: *" + money(abs(al)) + "*\n\n"
                  "\U0001f3c6 Best: *$" + best.get("symbol", "?") + "* " + pstr(best.get("realized_pnl", 0)) + " (" + str(round(best.get("x", 0), 2)) + "x)\n"
                  "\U0001f494 Worst: *$" + wrst.get("symbol", "?") + "* " + pstr(wrst.get("realized_pnl", 0)) + " (" + str(round(wrst.get("x", 0), 2)) + "x)\n\n"
                  "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                  "\U0001f6aa *EXIT BREAKDOWN*\n" + "\n".join(reason_lines) + "\n\n"
                  "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                  "\U0001f3e6 *CAPITAL STATUS*\n"
                  "Balance: *" + money(ud.get("balance", 0)) + "*\n"
                  "Vault (locked): *" + money(ud.get("apex_vault", 0)) + "*\n"
                  "Heat: *" + str(round(apex_capital_heat(ud) * 100, 1)) + "%*\n\n"
                  "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                  "\U0001f9e0 *LIFETIME LEARNING (" + str(len(mem)) + " trades)*\n"
                  "All-time WR: *" + str(mem_wr) + "%*\n"
                  "All-time PnL: *" + pstr(life_pnl) + "*\n"
                  "Min Confidence: *" + str(ud.get("apex_learn_threshold", APEX_MIN_CONFIDENCE)) + "/10*\n"
                  "Min Score: *" + str(ud.get("apex_learn_score_min", 45)) + "/100*"
                  + learn_txt),
            reply_markup=main_menu_kb())
    except Exception as _rpe:
        logger.error(f"APEX daily report: {_rpe}")

    # ── JSON trade log export ─────────────────────────────────────────────────
    try:
        import json as _json_mod
        import io   as _io_mod

        all_apex = [t for t in trade_log.get(uid, [])
                    if t.get("mood") in ("APEX", "AI-Sniper", "APEX-DCA")]

        # Attach post-exit snapshot data to each trade in the export
        post_exits = _apex_post_exit.get(uid, {})
        export_trades = []
        for t in all_apex:
            entry = {
                "symbol":        t.get("symbol"),
                "contract":      t.get("contract"),
                "chain":         t.get("chain"),
                "mood":          t.get("mood"),
                "invested":      round(t.get("invested", 0), 4),
                "returned":      round(t.get("returned", 0), 4),
                "realized_pnl":  round(t.get("realized_pnl", 0), 4),
                "x":             t.get("x"),
                "hold_h":        t.get("hold_h"),
                "exit_reason":   t.get("reason"),
                "bought_at":     t["bought_at"].isoformat() if hasattr(t.get("bought_at"), "isoformat") else str(t.get("bought_at","")),
                "closed_at":     t["closed_at"].isoformat() if hasattr(t.get("closed_at"), "isoformat") else str(t.get("closed_at","")),
                "avg_entry_price": t.get("avg_price"),
                "exit_price":    t.get("exit_price"),
                "peak_price":    t.get("peak_price"),
                # Post-exit snapshots: what the token did AFTER APEX sold
                "post_exit_snapshots": post_exits.get(t.get("contract",""), {}).get("snapshots", []),
            }
            export_trades.append(entry)

        export_payload = {
            "export_date":   str(datetime.now().date()),
            "generated_at":  datetime.now().isoformat(),
            "summary": {
                "total_trades":   len(all_apex),
                "wins":           len([t for t in all_apex if t.get("realized_pnl",0) > 0]),
                "losses":         len([t for t in all_apex if t.get("realized_pnl",0) <= 0]),
                "total_pnl":      round(sum(t.get("realized_pnl",0) for t in all_apex), 4),
                "vault_balance":  round(ud.get("apex_vault", 0), 4),
                "current_balance":round(ud.get("balance", 0), 4),
            },
            "exit_breakdown": reasons,
            "learning_state": {
                "total_trades":    ud.get("apex_total_trades", 0),
                "phase":           apex_get_phase(ud),
                "min_confidence":  ud.get("apex_learn_threshold", 3),
                "min_score":       ud.get("apex_learn_score_min", 35),
                "size_mult":       ud.get("apex_size_mult", 1.0),
            },
            "trades": export_trades,
        }

        json_bytes = _json_mod.dumps(export_payload, indent=2, default=str).encode("utf-8")
        filename   = "apex_trades_" + str(datetime.now().date()) + ".json"
        await bot.send_document(
            chat_id=uid,
            document=_io_mod.BytesIO(json_bytes),
            filename=filename,
            caption=(
                "📁 *APEX Trade Log — " + str(datetime.now().date()) + "*\n"
                "Contains all APEX trades with post-exit snapshots.\n"
                "Use this to analyse which exits were correct."
            ),
            parse_mode="Markdown",
        )
    except Exception as _je:
        logger.error(f"APEX JSON export error: {_je}")

# ══ APEX S/R ENGINE + SMART DCA ══════════════════════════════════════════
APEX_SR_HISTORY_MAX    = 48
APEX_SR_ZONE_PROXIMITY = 0.07
APEX_SR_VOL_THRESHOLD  = 0.70
APEX_SR_DOUBLE_TOP_VOL = 0.65
APEX_SR_BREAKOUT_VOL   = 1.30

# ── S/R ENGINE CONSTANTS ──────────────────────────────────────────────────────
APEX_SR_HISTORY_MAX    = 48      # max candle snapshots stored per position (48 × 30s = 24 min)
APEX_SR_ZONE_PROXIMITY = 0.07    # 7% — how close to a zone before it's "active"
APEX_SR_VOL_THRESHOLD  = 0.70    # resistance zone vol must be ≥70% of peak vol to be significant
APEX_SR_DOUBLE_TOP_VOL = 0.65    # second resistance attempt with <65% vol of first = double top signal
APEX_SR_BREAKOUT_VOL   = 1.30    # vol spike 30%+ above resistance zone = breakout confirmation

# ── SMART DCA CONSTANTS ───────────────────────────────────────────────────────
APEX_DCA_MIN_DIP        = 0.15   # position must have pulled back ≥15% from peak to consider DCA
APEX_DCA_MIN_CX         = 0.75   # only DCA if position is still ≥0.75x (don't average into -25%+)
APEX_DCA_MAX_CX         = 1.30   # don't DCA if still near ATH (not a real dip)
APEX_DCA_BUY_PCT_FLOOR  = 45.0   # buy% must have dipped below this before recovery
APEX_DCA_BUY_PCT_RECOV  = 52.0   # buy% must recover above this to confirm bounce
APEX_DCA_MAX_HEAT       = 0.65   # don't DCA if heat ≥65%
APEX_DCA_SIZE_MULT      = 0.50   # DCA size = 50% of original position size
APEX_DCA_MAX_PER_POS    = 1      # max 1 DCA per position


# ══════════════════════════════════════════════════════════════════════════════
# S/R ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def apex_sr_record_candle(h: dict, price: float, mc: float, vol_m5: float, buy_pct: float) -> None:
    """
    Called every checker cycle (30s) for each APEX position.
    Stores a candle snapshot: {mc, price, vol, buy_pct, ts}
    Maintains a rolling window of APEX_SR_HISTORY_MAX snapshots.
    Separately tracks the volume-weighted MC levels for S/R zone calculation.
    """
    import time as _t
    history = h.setdefault("sr_history", [])
    history.append({
        "mc":      mc,
        "price":   price,
        "vol":     vol_m5,
        "buy_pct": buy_pct,
        "ts":      _t.time(),
    })
    if len(history) > APEX_SR_HISTORY_MAX:
        h["sr_history"] = history[-APEX_SR_HISTORY_MAX:]

    # Update peak vol tracker (used for resistance zone significance scoring)
    if vol_m5 > h.get("sr_peak_vol", 0):
        h["sr_peak_vol"] = vol_m5


def apex_sr_compute_zones(h: dict) -> dict:
    """
    Analyses sr_history to identify significant S/R zones.

    Returns:
      resistance_zones : list[{mc, vol, strength}] — sorted by vol desc
      support_zones    : list[{mc, vol, strength}]
      active_resistance: float | None — nearest resistance MC above current
      active_support   : float | None — nearest support MC below current
    """
    history   = h.get("sr_history", [])
    peak_vol  = h.get("sr_peak_vol", 1)
    avg_price = h.get("avg_price", 0)
    result    = {
        "resistance_zones":  [],
        "support_zones":     [],
        "active_resistance": None,
        "active_support":    None,
    }

    if len(history) < 5 or peak_vol == 0:
        return result

    # ── Find volume-weighted levels ────────────────────────────────────────────
    # Group snapshots into MC buckets (5% width) and sum volume in each bucket
    bucket_size = 0.05   # 5% MC buckets
    buckets: dict = {}
    for snap in history:
        mc  = snap["mc"]
        vol = snap["vol"]
        if mc <= 0:
            continue
        # Bucket key = round to nearest 5%
        base   = avg_price * h.get("amount", 1)   # reference — use entry MC
        entry_mc = h.get("avg_cost_mc", mc)
        if entry_mc <= 0:
            entry_mc = mc
        bucket_key = round(mc / (entry_mc * bucket_size)) * (entry_mc * bucket_size)
        b = buckets.setdefault(bucket_key, {"mc": bucket_key, "vol": 0, "buy_pcts": [], "count": 0})
        b["vol"]        += vol
        b["buy_pcts"].append(snap["buy_pct"])
        b["count"]      += 1

    if not buckets:
        return result

    # ── Score each bucket ──────────────────────────────────────────────────────
    sorted_buckets = sorted(buckets.values(), key=lambda x: -x["vol"])
    current_mc     = history[-1]["mc"] if history else 0
    entry_mc_ref   = h.get("avg_cost_mc", current_mc) or current_mc

    for b in sorted_buckets:
        vol_ratio   = b["vol"] / peak_vol if peak_vol > 0 else 0
        avg_buy_pct = sum(b["buy_pcts"]) / len(b["buy_pcts"]) if b["buy_pcts"] else 50
        strength    = round(vol_ratio * 10, 1)   # 0–10 strength score

        if vol_ratio < 0.30:
            continue   # ignore low-volume zones

        zone = {"mc": b["mc"], "vol": b["vol"], "vol_ratio": vol_ratio,
                "avg_buy_pct": avg_buy_pct, "strength": strength}

        if b["mc"] > current_mc:
            # Above current price = resistance
            result["resistance_zones"].append(zone)
        else:
            # Below current price = support
            result["support_zones"].append(zone)

    # Sort: resistance by mc ascending (nearest first), support by mc descending (nearest first)
    result["resistance_zones"].sort(key=lambda x: x["mc"])
    result["support_zones"].sort(key=lambda x: -x["mc"])

    # Nearest active zones
    if result["resistance_zones"]:
        result["active_resistance"] = result["resistance_zones"][0]["mc"]
    if result["support_zones"]:
        result["active_support"] = result["support_zones"][0]["mc"]

    return result


def apex_sr_trail_multiplier(h: dict, current_mc: float, current_vol: float) -> float:
    """
    Returns a trail tightening multiplier based on S/R proximity.
    1.0 = no change to trail
    <1.0 = trail tightened (multiply trail_pct by this)

    Examples:
      0.5 → trail cut in half (e.g. 18% → 9%)
      0.4 → trail at 40% of normal (e.g. 18% → 7.2%)
      1.0 → no change
    """
    zones      = apex_sr_compute_zones(h)
    res_mc     = zones.get("active_resistance")
    peak_vol   = h.get("sr_peak_vol", 1) or 1
    history    = h.get("sr_history", [])

    if not res_mc or res_mc <= 0:
        return 1.0   # no data — no change

    distance_pct = (res_mc - current_mc) / current_mc if current_mc > 0 else 1.0

    # ── Double top detection ───────────────────────────────────────────────────
    # If we've visited resistance before and current vol < APEX_SR_DOUBLE_TOP_VOL × peak
    peak_visit_vol = h.get("sr_peak_visit_vol", 0)
    if peak_visit_vol > 0:
        vol_ratio = current_vol / peak_visit_vol if peak_visit_vol > 0 else 1.0
        if vol_ratio < APEX_SR_DOUBLE_TOP_VOL and distance_pct < APEX_SR_ZONE_PROXIMITY:
            # Double top forming — aggressively tighten
            return 0.35   # trail at 35% of normal

    # ── Breakout detection ─────────────────────────────────────────────────────
    if (distance_pct < APEX_SR_ZONE_PROXIMITY and
        current_vol > peak_vol * APEX_SR_BREAKOUT_VOL):
        # High volume through resistance = breakout — don't tighten
        h["sr_peak_visit_vol"] = current_vol   # update peak visit vol
        return 1.0

    # ── Approaching resistance ─────────────────────────────────────────────────
    if distance_pct < APEX_SR_ZONE_PROXIMITY:
        # Inside the resistance zone
        # Record visit volume (for double top detection on next approach)
        if current_vol > h.get("sr_peak_visit_vol", 0):
            h["sr_peak_visit_vol"] = current_vol
        # Tighten proportionally to how close we are
        closeness = 1.0 - (distance_pct / APEX_SR_ZONE_PROXIMITY)   # 0–1
        return max(0.30, 1.0 - (closeness * 0.55))   # floor: 30% of trail minimum

    elif distance_pct < APEX_SR_ZONE_PROXIMITY * 2:
        # Approaching — start pre-tightening
        closeness = 1.0 - (distance_pct / (APEX_SR_ZONE_PROXIMITY * 2))
        return max(0.70, 1.0 - (closeness * 0.25))   # up to 25% reduction

    return 1.0   # far from resistance — no change


# ══════════════════════════════════════════════════════════════════════════════
# SMART DCA ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def apex_dca_should_consider(h: dict, info: dict, ud: dict) -> tuple:
    """
    Fast pre-check: should we even evaluate DCA for this position?
    Returns (True, reason) or (False, reason).
    """
    # Already DCA'd max times?
    if h.get("apex_dca_count", 0) >= APEX_DCA_MAX_PER_POS:
        return False, "Max DCA reached for this position"

    # Already DCA'd recently (30 min cooldown)?
    last_dca = h.get("apex_last_dca_ts", 0)
    import time as _t
    if _t.time() - last_dca < 1800:
        return False, "DCA cooldown active"

    avg   = h.get("avg_price", 0)
    price = info.get("price", 0)
    peak  = h.get("apex_peak_price", price)

    if avg <= 0 or price <= 0:
        return False, "Invalid price data"

    cx        = price / avg
    peak_cx   = peak / avg if avg > 0 else 1.0
    pullback  = (peak - price) / peak if peak > 0 else 0

    # Position too underwater — don't average down into a failing trade
    if cx < APEX_DCA_MIN_CX:
        return False, f"Position too underwater ({round(cx,2)}x < {APEX_DCA_MIN_CX}x min)"

    # Position still too close to ATH — not a real dip
    if pullback < APEX_DCA_MIN_DIP:
        return False, f"Dip too small ({round(pullback*100,1)}% < {round(APEX_DCA_MIN_DIP*100)}% min)"

    # Not a huge winner yet — no point DCA-ing at 1.05x after 20% pullback
    if peak_cx < 1.40:
        return False, f"Peak too low ({round(peak_cx,2)}x) — DCA reserved for meaningful winners"

    # Capital heat check
    heat = apex_capital_heat(ud)   # defined in bot.py scope
    if heat >= APEX_DCA_MAX_HEAT:
        return False, f"Heat too high ({round(heat*100,1)}%)"

    # ── Liquidity health — liq drained 15%+ since buy = token is dying ──────────
    liq_now    = info.get("liq", 0)
    liq_at_buy = h.get("liq_at_buy", liq_now)
    if liq_at_buy > 0:
        liq_drain = (liq_at_buy - liq_now) / liq_at_buy
        if liq_drain >= 0.15:
            return False, f"Liq drained {round(liq_drain*100,1)}% since entry — token may be dead"

    # ── Token age — if it's over 4h old and losing, it won't recover ─────────
    age_h = info.get("age_h") or 0
    if age_h > 4.0 and cx < 1.0:
        return False, f"Token {round(age_h,1)}h old and underwater — no recovery expected"

    # ── Must have a green M5 candle with real volume to confirm life ──────────
    ch_m5  = info.get("ch_m5", 0)
    vol_m5 = info.get("vol_m5", 0)
    vol_h1 = info.get("vol_h1", 0)
    avg_5m = vol_h1 / 12 if vol_h1 > 0 else 0
    if ch_m5 <= 0:
        return False, f"No green M5 candle — price still falling, not a bounce"
    if avg_5m > 0 and vol_m5 < avg_5m * 0.3:
        return False, f"M5 volume dead ({round(vol_m5)} vs avg {round(avg_5m)}) — no real buying"

    # ── Sell domination check — if sells overwhelming, this is breakdown ──────
    buy_pct_m5 = info.get("buy_pct_m5", info.get("buy_pct", 50))
    if buy_pct_m5 < 40:
        return False, f"M5 sell dominated ({buy_pct_m5}% buys) — not safe to add"

    # ── Balance check — need at least $5 to DCA ───────────────────────────────
    orig_invested = h.get("total_invested", 0) + sum(
        d.get("amount", 0) for d in h.get("apex_dca_history", [])
    )
    dca_size = max(5.0, orig_invested * APEX_DCA_SIZE_MULT)
    if ud.get("balance", 0) < dca_size:
        return False, "Insufficient balance for DCA"

    return True, f"Pre-check passed (cx={round(cx,2)}x, pullback={round(pullback*100,1)}%)"


def apex_dca_confirm_bounce(h: dict, info: dict) -> tuple:
    """
    4-condition confirmation that this is a real bounce, not a breakdown.
    All 4 must pass for DCA to execute.

    Returns (True, signals_dict) or (False, signals_dict)
    """
    signals = {
        "buy_pct_m5":        info.get("buy_pct_m5", info.get("buy_pct", 50)),
        "buy_pct_h1":        info.get("buy_pct_h1", info.get("buy_pct", 50)),
        "vol_m5":            info.get("vol_m5", 0),
        "vol_h1":            info.get("vol_h1", 1),
        "threat":            h.get("apex_threat", "CLEAR"),
        "at_support":        False,
        "buy_pct_was_low":   h.get("sr_buy_pct_dipped", False),
        "conditions_met":    [],
        "conditions_failed": [],
    }

    # ── Condition 1: Threat is not RED or ORANGE ──────────────────────────────
    if signals["threat"] in ("RED", "ORANGE"):
        signals["conditions_failed"].append(f"Threat level {signals['threat']} — not a safe dip")
        return False, signals
    signals["conditions_met"].append("Threat level safe (CLEAR/YELLOW)")

    # ── Condition 2: Buy pressure dipped then recovered (bounce confirmation) ──
    # Track if buy_pct previously dipped below floor
    bpm5 = signals["buy_pct_m5"]
    if bpm5 < APEX_DCA_BUY_PCT_FLOOR:
        h["sr_buy_pct_dipped"] = True
        signals["buy_pct_was_low"] = True

    if h.get("sr_buy_pct_dipped") and bpm5 >= APEX_DCA_BUY_PCT_RECOV:
        signals["conditions_met"].append(f"Buy pressure recovered: {bpm5}% (was below {APEX_DCA_BUY_PCT_FLOOR}%)")
        h["sr_buy_pct_dipped"] = False   # reset after confirmation
    elif not h.get("sr_buy_pct_dipped"):
        # Hasn't dipped yet — no recovery to confirm
        signals["conditions_failed"].append(f"No buy pressure dip detected yet (m5={bpm5}%)")
    else:
        signals["conditions_failed"].append(f"Buy pressure not yet recovered ({bpm5}% < {APEX_DCA_BUY_PCT_RECOV}%)")

    # ── Condition 3: At or near support zone ─────────────────────────────────
    zones        = apex_sr_compute_zones(h)
    active_sup   = zones.get("active_support")
    current_mc   = info.get("mc", 0)

    if active_sup and current_mc > 0:
        dist_to_sup = abs(current_mc - active_sup) / current_mc
        if dist_to_sup <= APEX_SR_ZONE_PROXIMITY * 1.5:   # 10.5% proximity
            signals["at_support"] = True
            signals["conditions_met"].append(f"At support zone (MC {_mc_str(current_mc)} near {_mc_str(active_sup)})")
        else:
            signals["conditions_failed"].append(f"Not near support zone (dist={round(dist_to_sup*100,1)}%)")
    else:
        # No computed support yet — use entry MC as reference
        entry_mc = h.get("avg_cost_mc", current_mc) or current_mc
        if entry_mc > 0:
            dist = abs(current_mc - entry_mc) / entry_mc
            if dist <= 0.12:   # within 12% of entry MC = near entry support
                signals["at_support"] = True
                signals["conditions_met"].append(f"Near entry MC ({_mc_str(current_mc)}) — key level")
            else:
                signals["conditions_failed"].append("No support zone data yet")
        else:
            signals["conditions_failed"].append("No support zone data available")

    # ── Condition 4: Volume signature suggests accumulation not panic ──────────
    avg_5m_vol = signals["vol_h1"] / 12 if signals["vol_h1"] > 0 else 0
    vol_m5     = signals["vol_m5"]
    if avg_5m_vol > 0:
        vol_ratio = vol_m5 / avg_5m_vol
        if vol_ratio < 0.4:
            signals["conditions_failed"].append(f"Volume too low ({round(vol_ratio,1)}x avg) — no accumulation")
        elif vol_ratio > 3.0 and signals["buy_pct_m5"] < 50:
            signals["conditions_failed"].append(f"High vol + sell pressure ({vol_ratio:.1f}x avg, {bpm5}% buys) — panic")
        else:
            signals["conditions_met"].append(f"Volume healthy ({round(vol_ratio,1)}x avg baseline)")
    else:
        signals["conditions_met"].append("Volume check passed (no baseline)")

    # ── All 4 must pass ────────────────────────────────────────────────────────
    if signals["conditions_failed"]:
        return False, signals
    return True, signals


def _mc_str(mc: float) -> str:
    """Format MC for display."""
    if mc >= 1_000_000:
        return f"${mc/1_000_000:.1f}M"
    return f"${mc/1_000:.0f}K"


async def apex_try_smart_dca(app, uid: int, ud: dict, contract: str, h: dict, info: dict) -> bool:
    """
    Main DCA orchestrator. Called from apex_run_position_manager when
    position is in a dip. Returns True if DCA was executed.
    """
    # ── Pre-check ──────────────────────────────────────────────────────────────
    ok, pre_reason = apex_dca_should_consider(h, info, ud)
    if not ok:
        logger.debug(f"APEX DCA pre-check failed {contract}: {pre_reason}")
        return False

    # ── Bounce confirmation ────────────────────────────────────────────────────
    confirmed, signals = apex_dca_confirm_bounce(h, info)
    if not confirmed:
        failed = " | ".join(signals["conditions_failed"])
        logger.debug(f"APEX DCA not confirmed {contract}: {failed}")
        return False

    # ── Calculate DCA size ─────────────────────────────────────────────────────
    orig_invested = h.get("total_invested", 0)
    dca_size      = round(max(5.0, orig_invested * APEX_DCA_SIZE_MULT), 2)
    dca_size      = min(dca_size, ud.get("balance", 0))

    if dca_size < 2.0:
        return False

    # ── Execute DCA buy ────────────────────────────────────────────────────────
    import time as _t
    result = await do_buy_core(ud, uid, contract, dca_size, planned=True, mood="APEX-DCA")
    if not isinstance(result, tuple):
        logger.warning(f"APEX DCA buy failed: {result}")
        return False

    info_post, _ = result

    # ── Update DCA tracking ────────────────────────────────────────────────────
    h["apex_dca_count"]   = h.get("apex_dca_count", 0) + 1
    h["apex_last_dca_ts"] = _t.time()
    h["sr_buy_pct_dipped"]= False   # reset bounce tracker

    dca_history = h.setdefault("apex_dca_history", [])
    dca_history.append({
        "amount":    dca_size,
        "price":     info_post["price"],
        "mc":        info_post["mc"],
        "ts":        _t.time(),
        "signals":   {
            "buy_pct_m5": signals["buy_pct_m5"],
            "at_support":  signals["at_support"],
            "met":         signals["conditions_met"],
        }
    })

    # ── Notify user ────────────────────────────────────────────────────────────
    price   = info_post["price"]
    avg_new = h.get("avg_price", price)
    cx_new  = price / h.get("avg_price", price) if h.get("avg_price") else 1.0
    met_str = "\n".join(f"  \u2705 {c}" for c in signals["conditions_met"])

    try:
        await app.bot.send_message(
            chat_id=uid, parse_mode="Markdown",
            text=(
                "\U0001f4c9 *APEX SMART DCA EXECUTED*\n\n"
                "*$" + h["symbol"] + "*\n"
                "Added *" + money(dca_size) + "* at support\n"
                "New avg cost: *" + money(avg_new) + "*\n"
                "New avg MC: *" + _mc_str(h.get("avg_cost_mc", 0)) + "*\n"
                "Position now: *" + str(round(cx_new, 2)) + "x*\n\n"
                "*Why DCA'd:*\n" + met_str + "\n\n"
                "Trail resets at 1.5x from new avg entry\n"
                "Cash: *" + money(ud.get("balance", 0)) + "*"
            ),
            reply_markup=main_menu_kb()
        )
    except Exception as _ne:
        logger.error(f"APEX DCA notify: {_ne}")

    return True


# ══ META ENRICHMENT ══════════════════════════════════════════════════════
# ── Known rug template fingerprints ──────────────────────────────────────────
# These are phrases/patterns that appear repeatedly across serial rug operations.
# NOT penalising meme content — only recycled scam templates.
_RUG_PHRASES = [
    "100x guaranteed",
    "1000x guaranteed",
    "guaranteed returns",
    "risk-free",
    "can't go down",
    "based on sol",  # recycled template phrase
    "next big thing guaranteed",
    "buy now before it's too late",
    "last chance to get in",
    "team is fully doxxed",   # ironically a scam signal when unprovable
    "audit passed 100%",
    "lp locked forever",      # often copy-paste lie
    "no team tokens",         # copy-paste claim
    "renounced and locked",   # template phrase
    "the next pepe",
    "the next shib",
    "the next doge",
    "fairlaunch, no presale",  # recycled template
    "community owned",        # overused filler
    "diamond hands only",
    "to the moon guaranteed",
    "ape in now",
    "buy the dip now",
]

_PRESSURE_WORDS = [
    "hurry", "urgent", "last chance", "dont miss", "don't miss",
    "buy now", "ape now", "fomo", "limited time", "act fast",
    "selling fast", "almost gone",
]


async def enrich_token_meta(info: dict, item: dict) -> dict:
    """
    Analyses token description and metadata for rug fingerprints.
    Does NOT penalise for having no utility — this is memecoin context.
    Looks for: copy-paste templates, identity mismatches, pressure language,
    ghost metadata, suspicious name/description mismatches.

    Returns dict with keys:
      meta_description   : str — the raw description text (truncated)
      meta_flags         : list[str] — specific rug fingerprints detected
      meta_score         : int — 0 to 10 (10 = cleanest, 0 = most suspicious)
      meta_identity_ok   : bool — name/ticker/description tell coherent story
      meta_is_ghost      : bool — description is blank or one word
      meta_pressure_lang : bool — uses scammy urgency language
    """
    result = {
        "meta_description":    "",
        "meta_flags":          [],
        "meta_score":          7,          # default neutral-positive
        "meta_identity_ok":    True,
        "meta_is_ghost":       False,
        "meta_pressure_lang":  False,
    }

    # ── Gather raw text fields ────────────────────────────────────────────────
    desc     = (item.get("_pf_description") or item.get("description") or
                info.get("description") or "").strip()
    name     = (info.get("name") or "").strip().lower()
    symbol   = (info.get("symbol") or "").strip().lower()
    twitter  = info.get("twitter", "")
    telegram = info.get("telegram", "")

    result["meta_description"] = desc[:300] if desc else ""

    desc_lower = desc.lower()
    flags      = []
    score      = 7

    # ── Ghost metadata ────────────────────────────────────────────────────────
    if len(desc) <= 3:
        result["meta_is_ghost"] = True
        flags.append("Ghost description (blank or 1 word)")
        score -= 1   # soft penalty only — many legit memecoins have minimal desc

    # ── Rug template phrases ──────────────────────────────────────────────────
    matched_phrases = [p for p in _RUG_PHRASES if p in desc_lower]
    if matched_phrases:
        flags.append(f"Rug template phrase: '{matched_phrases[0]}'")
        score -= len(matched_phrases) * 2
        if len(matched_phrases) >= 2:
            flags.append(f"{len(matched_phrases)} template phrases (serial rugger pattern)")

    # ── Pressure language ─────────────────────────────────────────────────────
    pressure_hits = [p for p in _PRESSURE_WORDS if p in desc_lower]
    if pressure_hits:
        result["meta_pressure_lang"] = True
        flags.append(f"Pressure language: '{pressure_hits[0]}'")
        score -= 2

    # ── Identity coherence check ──────────────────────────────────────────────
    # Does the name/symbol appear in the description at all?
    # A team that wrote their own description almost always mentions their token.
    if desc and len(desc) > 20:
        name_in_desc   = (name in desc_lower or symbol in desc_lower or
                          symbol.replace("$","") in desc_lower)
        if not name_in_desc:
            # Only flag if description is substantial (not ghost)
            # Could be a meme token that's intentionally abstract
            pass   # soft — don't penalise, just note

    # ── Mismatched Twitter handle ─────────────────────────────────────────────
    if twitter:
        # Extract handle from URL
        handle = twitter.rstrip("/").split("/")[-1].lower().replace("@","")
        # Flag if handle shares no characters with name or symbol
        handle_clean = handle.replace("_","").replace("-","")
        sym_clean    = symbol.replace("$","").lower()
        name_clean   = "".join(c for c in name if c.isalpha())
        # Check for complete mismatch — handle has zero overlap with token identity
        if (len(handle_clean) > 3 and len(sym_clean) > 2 and
            handle_clean not in sym_clean and sym_clean not in handle_clean and
            name_clean not in handle_clean and handle_clean not in name_clean and
            not any(c in handle_clean for c in sym_clean[:3])):
            flags.append(f"Twitter handle mismatch: @{handle} vs ${symbol}")
            result["meta_identity_ok"] = False
            score -= 2

    # ── Suspiciously long or copy-heavy description ───────────────────────────
    if len(desc) > 800:
        # Very long descriptions on memecoins are often plagiarised from utility tokens
        flags.append("Unusually long description for a memecoin (possible copy-paste)")
        score -= 1

    # ── Score floor/ceiling ───────────────────────────────────────────────────
    result["meta_flags"] = flags
    result["meta_score"] = max(0, min(10, score))
    return result


async def enrich_twitter_momentum(info: dict, http_client) -> dict:
    """
    Checks Twitter/X signals for the token without requiring API key.
    Uses public nitter instances and Twitter oEmbed to get:
      - Account creation date (old = better)
      - Follower count
      - Recent tweet count / activity
      - Whether handle matches the token
      - Basic bot pattern detection (all tweets same structure)

    Returns dict:
      tw_followers      : int | None
      tw_account_age_d  : int | None — days old
      tw_recent_tweets  : int | None — tweets in last 7 days
      tw_is_fresh_acct  : bool — created within 7 days of token launch
      tw_momentum_score : int — 0–10
      tw_flags          : list[str]
      tw_verified       : bool — account exists and is accessible
    """
    result = {
        "tw_followers":       None,
        "tw_account_age_d":   None,
        "tw_recent_tweets":   None,
        "tw_is_fresh_acct":   False,
        "tw_momentum_score":  5,
        "tw_flags":           [],
        "tw_verified":        False,
    }

    twitter_url = info.get("twitter", "")
    if not twitter_url:
        result["tw_flags"].append("No Twitter link")
        result["tw_momentum_score"] = 3
        return result

    # Extract handle
    handle = twitter_url.rstrip("/").split("/")[-1].replace("@","").strip()
    if not handle or len(handle) < 2:
        result["tw_flags"].append("Unparseable Twitter URL")
        return result

    flags = []
    score = 5

    # ── Try Twitter oEmbed (public, no API key needed) ─────────────────────
    try:
        oembed_url = f"https://publish.twitter.com/oembed?url=https://twitter.com/{handle}&omit_script=true"
        r = await http_client.get(oembed_url, timeout=6)
        if r.status_code == 200:
            result["tw_verified"] = True
            score += 1   # account exists
        elif r.status_code == 404:
            flags.append("Twitter account not found / suspended")
            result["tw_momentum_score"] = 0
            result["tw_flags"] = flags
            return result
    except Exception:
        pass   # oEmbed unavailable — don't fail the whole enrichment

    # ── Try nitter for richer data ─────────────────────────────────────────
    nitter_instances = [
        "https://nitter.privacydev.net",
        "https://nitter.poast.org",
    ]
    profile_html = None
    for nitter in nitter_instances:
        try:
            r = await http_client.get(f"{nitter}/{handle}", timeout=7,
                                      headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200 and "tweet" in r.text.lower():
                profile_html = r.text
                break
        except Exception:
            continue

    if profile_html:
        import re as _re
        result["tw_verified"] = True

        # Extract follower count
        fol_match = _re.search(r'([\d,]+)\s*Followers', profile_html, _re.IGNORECASE)
        if fol_match:
            try:
                followers = int(fol_match.group(1).replace(",",""))
                result["tw_followers"] = followers
                if followers < 50:
                    flags.append(f"Very low followers: {followers}")
                    score -= 2
                elif followers < 300:
                    flags.append(f"Low followers: {followers}")
                    score -= 1
                elif followers > 5000:
                    score += 2   # real following
                elif followers > 1000:
                    score += 1
            except ValueError:
                pass

        # Account join date
        join_match = _re.search(r'Joined\s+(\w+\s+\d{4})', profile_html, _re.IGNORECASE)
        if join_match:
            try:
                from datetime import datetime as _dt
                joined = _dt.strptime(join_match.group(1), "%B %Y")
                age_d  = (_dt.now() - joined).days
                result["tw_account_age_d"] = age_d
                if age_d < 7:
                    result["tw_is_fresh_acct"] = True
                    flags.append(f"Account created {age_d}d ago — very fresh")
                    score -= 3
                elif age_d < 30:
                    flags.append(f"Account created {age_d}d ago")
                    score -= 1
                elif age_d > 365:
                    score += 2   # established account
            except Exception:
                pass

        # Count recent tweets (rough — count tweet containers in HTML)
        tweet_count = len(_re.findall(r'class="tweet-content"', profile_html))
        result["tw_recent_tweets"] = tweet_count
        if tweet_count == 0:
            flags.append("No recent tweets visible")
            score -= 2
        elif tweet_count >= 5:
            score += 1   # active posting

        # Bot pattern detection — if all tweet texts start identically
        tweet_texts = _re.findall(r'class="tweet-content[^"]*"[^>]*>(.*?)</div>', profile_html, _re.DOTALL)
        if len(tweet_texts) >= 3:
            # Strip HTML tags for comparison
            clean = [_re.sub(r'<[^>]+>', '', t).strip()[:30] for t in tweet_texts]
            # If 80%+ start with same 20 chars = bot pattern
            prefixes = [c[:20] for c in clean if len(c) >= 20]
            if prefixes and len(set(prefixes)) == 1 and len(prefixes) >= 3:
                flags.append("Bot pattern: all tweets identical structure")
                score -= 3

    else:
        # Nitter failed — can't get details, slight uncertainty penalty
        if result["tw_verified"]:
            pass   # account exists but can't get details
        else:
            flags.append("Twitter account unverifiable")
            score -= 1

    result["tw_flags"]           = flags
    result["tw_momentum_score"]  = max(0, min(10, score))
    return result


async def enrich_holder_distribution(contract: str, chain: str, http_client) -> dict:
    """
    Fetches top holder distribution.
    Solana: uses Solscan public API
    EVM:    uses DexScreener token page data

    Returns dict:
      holder_top10_pct     : float | None — % held by top 10 wallets
      holder_top1_pct      : float | None — % held by single largest wallet
      holder_fresh_pct     : float | None — % held by wallets <7 days old
      holder_count         : int | None
      holder_flags         : list[str]
      holder_score         : int — 0–10
      holder_distribution  : list[dict] — [{rank, pct, address_short, age_d}]
    """
    result = {
        "holder_top10_pct":    None,
        "holder_top1_pct":     None,
        "holder_fresh_pct":    None,
        "holder_count":        None,
        "holder_flags":        [],
        "holder_score":        6,
        "holder_distribution": [],
    }

    chain_lower = (chain or "").lower()
    flags = []
    score = 6

    # ── Solana: Solscan public API ────────────────────────────────────────────
    if chain_lower in ("solana", "sol"):
        try:
            url = f"https://public-api.solscan.io/token/holders?tokenAddress={contract}&limit=20&offset=0"
            r = await http_client.get(url, timeout=8,
                                      headers={"accept": "application/json",
                                               "User-Agent": "Mozilla/5.0"})
            if r.status_code == 200:
                data = r.json()
                holders_raw = data.get("data", [])
                total_supply = data.get("total", 1) or 1

                if holders_raw:
                    top_pcts = []
                    dist     = []
                    for i, h in enumerate(holders_raw[:10]):
                        amt = h.get("amount", 0)
                        pct = round(amt / total_supply * 100, 2) if total_supply > 0 else 0
                        top_pcts.append(pct)
                        addr = h.get("address", "")
                        dist.append({
                            "rank":  i + 1,
                            "pct":   pct,
                            "addr":  addr[:6] + "…" + addr[-4:] if len(addr) > 10 else addr,
                        })

                    result["holder_distribution"] = dist
                    result["holder_top10_pct"]    = round(sum(top_pcts), 2)
                    result["holder_top1_pct"]     = top_pcts[0] if top_pcts else None
                    result["holder_count"]        = data.get("total", None)

                    top10 = result["holder_top10_pct"]
                    top1  = result["holder_top1_pct"] or 0

                    # Score top10 concentration
                    if top10 > 60:
                        flags.append(f"Top 10 wallets hold {top10}% — extreme concentration")
                        score -= 4
                    elif top10 > 40:
                        flags.append(f"Top 10 wallets hold {top10}% — high concentration")
                        score -= 2
                    elif top10 < 20:
                        score += 2   # well distributed

                    # Score top1
                    if top1 > 20:
                        flags.append(f"Single wallet holds {top1}% — whale risk")
                        score -= 3
                    elif top1 > 10:
                        flags.append(f"Largest wallet: {top1}%")
                        score -= 1

                    # Check if top wallets all created around same time (coordinated)
                    # We can detect this if top 5 all have similar addresses (heuristic)
                    if len(dist) >= 3:
                        # If top 3 wallets each hold >8%, likely coordinated
                        top3_each = [d["pct"] for d in dist[:3]]
                        if all(p > 8 for p in top3_each):
                            flags.append("Top 3 wallets each >8% — possible coordinated hold")
                            score -= 2

        except Exception as _se:
            logger.debug(f"Solscan holder fetch failed: {_se}")

    # ── EVM: DexScreener token page ────────────────────────────────────────────
    elif chain_lower in ("ethereum", "eth", "base", "bsc", "arbitrum"):
        try:
            # DexScreener token page has holder data in its JSON payload
            url = f"https://io.dexscreener.com/dex/log/amm/v2/update/tokens/{contract}"
            r = await http_client.get(url, timeout=7,
                                      headers={"User-Agent": "Mozilla/5.0",
                                               "Referer": "https://dexscreener.com"})
            if r.status_code == 200:
                data     = r.json()
                holders  = data.get("holders", {})
                top_list = holders.get("items", [])
                if top_list:
                    pcts = [h.get("percentage", 0) for h in top_list[:10]]
                    result["holder_top10_pct"] = round(sum(pcts), 2)
                    result["holder_top1_pct"]  = pcts[0] if pcts else None
                    result["holder_count"]     = holders.get("totalCount")
                    result["holder_distribution"] = [
                        {"rank": i+1, "pct": p,
                         "addr": top_list[i].get("address","?")[:6] + "…"}
                        for i, p in enumerate(pcts)
                    ]
                    top10 = result["holder_top10_pct"]
                    top1  = result["holder_top1_pct"] or 0
                    if top10 > 60:
                        flags.append(f"Top 10 hold {top10}% — extreme concentration")
                        score -= 4
                    elif top10 > 40:
                        flags.append(f"Top 10 hold {top10}% — high concentration")
                        score -= 2
                    elif top10 < 20:
                        score += 2
                    if top1 > 20:
                        flags.append(f"Single wallet holds {top1}%")
                        score -= 3
                    elif top1 > 10:
                        flags.append(f"Largest wallet: {top1}%")
                        score -= 1
        except Exception as _eve:
            logger.debug(f"EVM holder fetch failed: {_eve}")

    result["holder_flags"] = flags
    result["holder_score"] = max(0, min(10, score))
    return result


def _build_meta_prompt_block(info: dict) -> str:
    """
    Builds the META INTELLIGENCE section for the AI prompt.
    Only included if enrichment data is present.
    """
    lines = []

    # ── Token description ─────────────────────────────────────────────────────
    desc = info.get("meta_description", "")
    if desc:
        lines.append(f"Description: \"{desc[:200]}\"")
    else:
        lines.append("Description: [BLANK]")

    # ── Metadata flags ────────────────────────────────────────────────────────
    meta_flags = info.get("meta_flags", [])
    meta_score = info.get("meta_score", 7)
    lines.append(f"Metadata integrity score: {meta_score}/10")
    if meta_flags:
        lines.append("Metadata flags: " + " | ".join(meta_flags))
    if info.get("meta_is_ghost"):
        lines.append("⚠️ Ghost metadata — team put no effort into identity")
    if info.get("meta_pressure_lang"):
        lines.append("⚠️ Pressure/scam language detected in description")
    if not info.get("meta_identity_ok", True):
        lines.append("⚠️ Twitter handle doesn't match token identity")

    # ── Twitter momentum ──────────────────────────────────────────────────────
    tw_score    = info.get("tw_momentum_score")
    tw_flags    = info.get("tw_flags", [])
    tw_verified = info.get("tw_verified", False)
    tw_fol      = info.get("tw_followers")
    tw_age      = info.get("tw_account_age_d")
    tw_fresh    = info.get("tw_is_fresh_acct", False)

    if tw_score is not None:
        lines.append(f"\nTwitter momentum score: {tw_score}/10")
        if tw_verified:
            lines.append("Account status: ✅ Verified accessible")
            if tw_fol is not None:
                lines.append(f"Followers: {tw_fol:,}")
            if tw_age is not None:
                lines.append(f"Account age: {tw_age} days old")
            if tw_fresh:
                lines.append("⚠️ Account created same week as token — likely dedicated scam account")
        else:
            lines.append("Account status: ❌ Unverifiable / suspended")
        if tw_flags:
            lines.append("Twitter flags: " + " | ".join(tw_flags))

    # ── Holder distribution ───────────────────────────────────────────────────
    h_top10 = info.get("holder_top10_pct")
    h_top1  = info.get("holder_top1_pct")
    h_count = info.get("holder_count")
    h_score = info.get("holder_score")
    h_flags = info.get("holder_flags", [])
    h_dist  = info.get("holder_distribution", [])

    if h_top10 is not None:
        lines.append(f"\nHolder distribution score: {h_score}/10")
        lines.append(f"Total holders: {h_count:,}" if h_count else "Total holders: unknown")
        lines.append(f"Top 10 wallets control: {h_top10}% of supply")
        if h_top1:
            lines.append(f"Largest single wallet: {h_top1}%")
        if h_dist:
            dist_str = "  ".join(f"#{d['rank']}:{d['pct']}%" for d in h_dist[:5])
            lines.append(f"Top 5: {dist_str}")
        if h_flags:
            lines.append("Holder flags: " + " | ".join(h_flags))
    else:
        lines.append("\nHolder distribution: Not available")

    return "\n".join(lines)


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
            # ── History lists — appended by checker_job / event handlers ──────
            # price_history : {price, mc, ts}         — every checker cycle (20s)
            # liq_history   : {liq, ts}               — every checker cycle (20s)
            # stop_loss_history : {old, new, source, cx, ts} — every SL change
            # auto_sell_history : {x, pct, price, pnl, ts}   — every TP trigger
            # threat_history    : {from, to, cx, ts}          — APEX only, on change
            "price_history":      [],
            "liq_history":        [],
            "stop_loss_history":  [],
            "auto_sell_history":  [],
            "threat_history":     [],
        }
    if planned:
        ud["planned"] += 1
    else:
        ud["impulse"] += 1

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

    save_user(uid, ud)
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
                assert amt > 0
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
                        [InlineKeyboardButton("Yes, Continue", callback_data="ot_yes_" + contract + "_" + str(amount) + "_" + mood)],
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
                    h = ud["holdings"][contract]
                    import time as _tslc
                    h.setdefault("stop_loss_history", []).append({
                        "old":    h.get("stop_loss_pct"),
                        "new":    pct,
                        "source": "user_custom",
                        "cx":     None,
                        "ts":     _tslc.time(),
                    })
                    h["stop_loss_pct"] = pct
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
                symbol = p.get("symbol", "?")
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
                assert amt > 0
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
                    text="✅ *APEX SNIPER BOT connected!*\n\nAI Sniper signals will be posted here.",
                    parse_mode="Markdown"
                )
                # Try to get chat name
                try:
                    chat_info = await ctx.bot.get_chat(ch_id)
                    ch_name = chat_info.title or str(ch_id)
                except Exception:
                    ch_name = str(ch_id)
                ud["sniper_broadcast_channel"] = ch_id
                ud["sniper_broadcast_name"] = ch_name
                del pending[u.id]
                save_user(u.id, ud)
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
                    "Make sure *apex_sniper_bot* is an admin in the channel/group, then try again.\n\n"
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
                if "_competitions" not in globals():
                    globals()["_competitions"] = {}
                globals()["_competitions"][code] = comp
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
            _comps = globals().get("_competitions", {})
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
                save_user(u.id, ud)
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

        elif action == "sniper_budget":
            try:
                val = float(text.replace("$", "").replace(",", ""))
                assert val > 0
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

        elif action == "apex_vault_withdraw_amt":
            vault = ud.get("apex_vault", 0.0)
            raw   = text.strip().upper().replace("$", "").replace(",", "")
            try:
                if raw == "MAX":
                    amt = vault
                else:
                    amt = float(raw)
                assert amt > 0, "must be positive"
                assert amt <= vault + 0.001, "exceeds vault balance"
                amt = min(amt, vault)
                ud["apex_vault"] -= amt
                ud["balance"]    += amt
                save_user(u.id, ud)
                pending.pop(u.id, None)
                await _clean()
                await message.reply_text(
                    "\u2705 *VAULT WITHDRAWAL CONFIRMED*\n\n"
                    "Withdrawn: *" + money(amt) + "*\n"
                    "Trading Balance: *" + money(ud["balance"]) + "*\n"
                    "Vault Remaining: *" + money(ud["apex_vault"]) + "*",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("\U0001f3e6 Vault", callback_data="apex_vault_menu"),
                        InlineKeyboardButton("\U0001f3e0 Menu",  callback_data="mm"),
                    ]])
                )
            except AssertionError:
                await _clean()
                await message.reply_text(
                    "\u274c Invalid amount. Vault balance: *" + money(vault) + "*\n"
                    "Enter a number up to " + money(vault) + " or type MAX.",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\u274c Cancel", callback_data="apex_vault_menu")]])
                )
                pending[u.id] = {"action": "apex_vault_withdraw_amt"}
            except Exception:
                await _clean()
                await message.reply_text(
                    "\u274c Invalid input. Enter a number (e.g. 50) or MAX.",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\u274c Cancel", callback_data="apex_vault_menu")]])
                )
                pending[u.id] = {"action": "apex_vault_withdraw_amt"}

    # No pending (or pending was cleared for CA) — treat as CA
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
    ud["last_chain"] = info.get("chain", "solana")
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
    filename = "apex_sniper_trades_" + datetime.now().strftime("%Y%m%d") + ".csv"
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
    ud = get_user(u.id, u.username or u.first_name)
    cb = q.data

    if cb == "mm":
        pending.pop(u.id, None)
        # ── Delete any floating chart image from the last token card view ─────
        _prev_chart = chart_msg_ids.pop(u.id, None)
        if _prev_chart:
            try:
                await ctx.bot.delete_message(chat_id=q.message.chat_id, message_id=_prev_chart)
            except Exception:
                pass
        if ud.get("balance") is None:
            await cmd_start(update, ctx)
            return
        await q.edit_message_text(
            "⚡ *APEX SNIPER BOT*\n\nWelcome back, *" + ud["username"] + "*!\n"
            "💰 Balance: *" + money(ud["balance"]) + "*\n"
            "💎 Savings: *" + money(ud["savings"]) + "*\n"
            "🏦 Vault: *" + money(ud.get("apex_vault", 0.0)) + "*\n\n"
            "Paste any CA to trade 👇",
            parse_mode="Markdown", reply_markup=main_menu_kb()
        )

    elif cb == "v_trade":
        await q.edit_message_text(
            "⚡ *BUY and SELL NOW*\n\nPaste any Solana, ETH, BSC or Base contract address in the chat to get started.",
            parse_mode="Markdown", reply_markup=back_main()
        )

    elif cb == "v_pos":
        # ── Delete any floating chart image from the last token card view ─────
        _prev_chart = chart_msg_ids.pop(u.id, None)
        if _prev_chart:
            try:
                await ctx.bot.delete_message(chat_id=q.message.chat_id, message_id=_prev_chart)
            except Exception:
                pass
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
                cv    = h["amount"] * info["price"]
                cx    = info["price"] / h["avg_price"] if h["avg_price"] > 0 else 0
                ppnl  = cv - h["total_invested"]
                sl    = h.get("stop_loss_pct")
                targets = [t for t in h.get("auto_sells", []) if not t.get("triggered")]
                # ── Hold time
                held_h   = (datetime.now() - h.get("bought_at", datetime.now())).total_seconds() / 3600
                held_txt = "  ⏱" + age_str(held_h)
                # ── Stop loss / auto-sell indicators
                sl_txt = "  🛑" + str(sl) + "%" if sl else ""
                as_txt = "  🎯" + str(len(targets)) + " TP" if targets else ""
                # ── APEX threat badge (only shown when not CLEAR)
                threat     = h.get("apex_threat", "")
                threat_txt = ""
                if threat == "RED":    threat_txt = "  🔴"
                elif threat == "ORANGE": threat_txt = "  🟠"
                elif threat == "YELLOW": threat_txt = "  🟡"
                # ── History line: sparkline + peak x + low x
                hist_line = _position_history_line(h, info["price"])
                # ── Mood badge for APEX/Sniper positions
                mood = h.get("mood", "")
                mood_txt = "  ⚡APEX" if mood in ("APEX", "APEX-DCA") else ("  🎯Sniper" if mood == "AI-Sniper" else "")
                # ── Build the position block
                lines.append(
                    "*$" + h["symbol"] + "*" + mood_txt + "  " + str(round(cx, 2)) + "x" + threat_txt + "\n"
                    "  " + money(cv) + "  " + pstr(ppnl) + held_txt + sl_txt + as_txt + "\n"
                    + (hist_line + "\n" if hist_line else "")
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

    elif cb == "v_wallet":
        bal    = ud.get("balance", 0)
        sav    = ud.get("savings", 0)
        vault  = ud.get("apex_vault", 0.0)
        asp    = str(ud["auto_save_pct"]) + "% of profits" if ud.get("auto_save_pct") else "not set"
        total  = bal + sav + vault
        # In-position reserved (vault milestones not yet closed)
        reserved = sum(
            h.get("apex_vault_reserved", 0)
            for h in ud.get("holdings", {}).values()
        )
        await q.edit_message_text(
            "👛 *WALLET*\n\n"
            "💵 Trading Balance:  *" + money(bal)   + "*\n"
            "💰 Savings:          *" + money(sav)   + "*\n"
            "🏦 APEX Vault:       *" + money(vault) + "*\n"
            + ("🔒 Pending vault:    *" + money(reserved) + "*\n" if reserved > 0 else "")
            + "─────────────────────────\n"
            "📊 Total:            *" + money(total) + "*\n\n"
            "Auto-save: *" + asp + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💰 Savings",          callback_data="v_savings"),
                 InlineKeyboardButton("🏦 APEX Vault",       callback_data="apex_vault_menu")],
                [InlineKeyboardButton("🏠 Main Menu",        callback_data="mm")],
            ])
        )

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
            "APEX SNIPER BOT TRADE\n"
            "$" + trade["symbol"] + "  " + str(round(trade.get("x", 0), 2)) + "x\n"
            + ("+" if pnl_positive else "") + money(trade["realized_pnl"]) + "\n"
            "Held: " + str(trade["hold_h"]) + "h\n"
            "Paper Trading | APEX SNIPER BOT"
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
            "APEX SNIPER BOT - TRADER PROFILE\n"
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
            "Trade on APEX SNIPER BOT"
        )
        await q.edit_message_text(
            "🌐 *PUBLIC PROFILE*\n\nCopy and share this card:\n\n" + profile_card,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="mm")]])
        )


    # ── WATCHLIST ──────────────────────────────────────────────────────────
    elif cb == "v_watchlist":
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
        ref_link = f"https://t.me/apex_sniper_bot?start=ref_{u.id}"
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
        await q.edit_message_text(
            "📋 *MORE FEATURES*\n\nSelect a feature:",
            parse_mode="Markdown",
            reply_markup=more_menu_kb()
        )

    elif cb == "v_help":
        await q.edit_message_text(
            "📖 *APEX SNIPER BOT HELP*\n\n"
            "Welcome to APEX SNIPER BOT — your paper trading terminal!\n\n"
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
            "💬 *Support:* @apex_sniper_support",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📖 Full Docs", url="https://apex_sniper_bot.gitbook.io/apex_sniper_bot-docs/")],
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
        _comps = globals().get("_competitions", {})
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
        _comps = globals().get("_competitions", {})
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
            ud["balance"] -= bet
        if "_competitions" not in globals():
            globals()["_competitions"] = {}
        globals()["_competitions"][code] = comp
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
        _comps = globals().get("_competitions", {})
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
        # ── Dedup: drop concurrent refresh taps for the same user ─────────────
        if u.id not in _rf_locks:
            _rf_locks[u.id] = _asyncio.Lock()
        if _rf_locks[u.id].locked():
            # Already refreshing — silently ignore the extra tap
            return
        async with _rf_locks[u.id]:
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
            await do_buy_query(q, ud, u.id, contract, amount)

    elif cb.startswith("bc_"):
        contract = cb[3:]
        pending[u.id] = {"action": "buy_custom", "contract": contract, "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text("Enter buy amount in USD:", reply_markup=cancel_kb())

    elif cb.startswith("sp_"):
        rest = cb[3:]
        pct_str, contract = rest.split("_", 1)
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
            "📊 *APEX SCORE*\n\n"
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

    elif cb.startswith("th_"):
        # ── Token card — Position History toggle ──────────────────────────────
        # Reads all history lists on the holding and renders a single clean screen.
        # Each section only appears if it has data — no blank sections.
        contract = cb[3:]
        h        = ud.get("holdings", {}).get(contract)

        if not h:
            await q.edit_message_text(
                "📜 *POSITION HISTORY*\n\n"
                "You don't hold this token yet.\n"
                "History starts recording from the moment you buy.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="btt_" + contract)]])
            )
            return

        sym  = h.get("symbol", "?")
        avg  = h.get("avg_price", 0)
        lines = ["📜 *POSITION HISTORY — $" + sym + "*\n"]

        # ── 1. Price path ─────────────────────────────────────────────────────
        ph = h.get("sr_history") or h.get("price_history") or []
        if ph:
            spark  = _position_sparkline(h)
            lo_p   = min(s["price"] for s in ph)
            hi_p   = max(s["price"] for s in ph)
            lo_x   = round(lo_p / avg, 2) if avg > 0 else 0
            hi_x   = round(hi_p / avg, 2) if avg > 0 else 0
            # Time tracked: first snapshot to last
            elapsed_m = round((ph[-1]["ts"] - ph[0]["ts"]) / 60) if len(ph) > 1 else 0
            elapsed_txt = (str(elapsed_m) + "m") if elapsed_m < 60 else (str(round(elapsed_m / 60, 1)) + "h")
            lines.append(
                "📈 *Price Path*\n"
                + ("`" + spark + "`\n" if spark else "")
                + "  Peak: *" + str(hi_x) + "x*   Low: *" + str(lo_x) + "x*\n"
                + "  Tracking: *" + str(len(ph)) + " snapshots · " + elapsed_txt + "*\n"
            )

        # ── 2. Liquidity path ─────────────────────────────────────────────────
        lh = h.get("liq_history", [])
        if len(lh) >= 3:
            BLOCKS   = "▁▂▃▄▅▆▇█"
            liq_vals = [s["liq"] for s in lh]
            # Sample up to 10 evenly spaced points
            n      = len(liq_vals)
            step   = max(1, n // 10)
            samp   = liq_vals[::step][-10:]
            lo_l, hi_l = min(samp), max(samp)
            if hi_l > lo_l:
                liq_spark = "".join(BLOCKS[min(7, int((v - lo_l) / (hi_l - lo_l) * 7))] for v in samp)
            else:
                liq_spark = "▄" * len(samp)
            liq_entry = h.get("liq_at_buy", lh[0]["liq"])
            liq_now   = lh[-1]["liq"]
            liq_chg   = round((liq_now - liq_entry) / liq_entry * 100, 1) if liq_entry > 0 else 0
            liq_icon  = "🚨" if liq_chg <= -20 else ("⚠️" if liq_chg <= -10 else "✅")
            lines.append(
                "💧 *Liquidity Path*\n"
                + "`" + liq_spark + "`\n"
                + "  Entry: *" + money(liq_entry) + "*   Now: *" + money(liq_now) + "*\n"
                + "  Change: " + liq_icon + " *" + ("+" if liq_chg >= 0 else "") + str(liq_chg) + "%*\n"
            )

        # ── 3. Stop loss changes ──────────────────────────────────────────────
        slh = h.get("stop_loss_history", [])
        if slh:
            _source_labels = {
                "apex_entry":      "APEX entry",
                "user_button":     "you (button)",
                "user_custom":     "you (custom)",
                "user_removed":    "you (removed)",
                "user_cancel_all": "you (cancel all)",
            }
            sl_lines = []
            for s in slh[-5:]:   # show last 5 changes max
                old_txt = str(s["old"]) + "%" if s["old"] is not None else "none"
                new_txt = str(s["new"]) + "%" if s["new"] is not None else "removed"
                who     = _source_labels.get(s["source"], s["source"])
                cx_txt  = ("  · " + str(s["cx"]) + "x") if s.get("cx") else ""
                sl_lines.append("  " + old_txt + " → *" + new_txt + "*  · " + who + cx_txt)
            lines.append(
                "🛑 *Stop Loss History* (" + str(len(slh)) + ")\n"
                + "\n".join(sl_lines) + "\n"
            )

        # ── 4. Auto-sell hits ─────────────────────────────────────────────────
        ash = h.get("auto_sell_history", [])
        if ash:
            as_lines = []
            for s in ash:
                as_lines.append(
                    "  " + str(s["x"]) + "x target · sold "
                    + str(int(s["pct"] * 100)) + "% · "
                    + money(s["price"]) + " · PnL " + pstr(s["pnl"])
                )
            lines.append(
                "🎯 *Auto-Sell Hits* (" + str(len(ash)) + ")\n"
                + "\n".join(as_lines) + "\n"
            )

        # ── 5. Threat history (APEX only) ─────────────────────────────────────
        thr = h.get("threat_history", [])
        if thr:
            _threat_icons = {"CLEAR": "🟢", "YELLOW": "🟡", "ORANGE": "🟠", "RED": "🔴"}
            thr_lines = []
            for s in thr[-6:]:   # show last 6 transitions
                fi = _threat_icons.get(s["from"], "⚪")
                ti = _threat_icons.get(s["to"],   "⚪")
                thr_lines.append(
                    "  " + fi + " " + s["from"] + " → " + ti + " " + s["to"]
                    + "  · " + str(s["cx"]) + "x · " + money(s["price"])
                )
            lines.append(
                "🔴 *Threat History* (" + str(len(thr)) + ")\n"
                + "\n".join(thr_lines) + "\n"
            )

        # ── 6. DCA history (APEX only) ────────────────────────────────────────
        dca = h.get("apex_dca_history", [])
        if dca:
            dca_lines = []
            for s in dca:
                dca_lines.append(
                    "  +" + money(s["amount"]) + " @ " + money(s["price"])
                    + " · MC " + mc_str(s.get("mc", 0))
                )
            lines.append(
                "⚡ *DCA History* (" + str(len(dca)) + ")\n"
                + "\n".join(dca_lines) + "\n"
            )

        # ── No history at all yet ─────────────────────────────────────────────
        if len(lines) == 1:
            lines.append("No history recorded yet.\nData starts accumulating once the checker runs.")

        text = "\n".join(lines)
        if len(text) > 4096:
            text = text[:4092] + "…"

        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back to Token", callback_data="btt_" + contract)]])
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
            h = ud["holdings"][contract]
            import time as _tsld
            h.setdefault("stop_loss_history", []).append({
                "old":    h.get("stop_loss_pct"),
                "new":    None,
                "source": "user_removed",
                "cx":     None,
                "ts":     _tsld.time(),
            })
            h["stop_loss_pct"] = None
            sym = h["symbol"]
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
            h = ud["holdings"][contract]
            import time as _tcat
            if h.get("stop_loss_pct") is not None:
                h.setdefault("stop_loss_history", []).append({
                    "old":    h["stop_loss_pct"],
                    "new":    None,
                    "source": "user_cancel_all",
                    "cx":     None,
                    "ts":     _tcat.time(),
                })
            h["auto_sells"] = []
            h["stop_loss_pct"] = None
            sym = h["symbol"]
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
            h = ud["holdings"][contract]
            import time as _tsls
            h.setdefault("stop_loss_history", []).append({
                "old":    h.get("stop_loss_pct"),
                "new":    pct,
                "source": "user_button",
                "cx":     round((h.get("avg_price", 1) and 1), 3),
                "ts":     _tsls.time(),
            })
            h["stop_loss_pct"] = pct
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
            h = ud["holdings"][contract]
            import time as _tslr
            h.setdefault("stop_loss_history", []).append({
                "old":    h.get("stop_loss_pct"),
                "new":    None,
                "source": "user_removed",
                "cx":     None,
                "ts":     _tslr.time(),
            })
            h["stop_loss_pct"] = None
            sym = h["symbol"]
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
            await q.edit_message_text("Executing buy...")
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
        auto_on  = ud.get("sniper_auto", False)
        adv_on   = ud.get("sniper_advisory", False)
        apex_on  = ud.get("apex_mode", False)
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

        # ── Skip reason breakdown ─────────────────────────────────────────────
        skip_counts = ud.get("sniper_skip_counts", {})
        _skip_icons = {
            "hard_flag":    "🚩",
            "score":        "📉",
            "liquidity":    "💧",
            "mc_range":     "📊",
            "age":          "⏰",
            "low_activity": "😴",
            "sell_pressure":"📛",
            "wash_trade":   "🔄",
            "no_socials":   "👻",
            "few_holders":  "👥",
            "other":        "❓",
        }
        _skip_labels = {
            "hard_flag":    "Hard flag",
            "score":        "Score too low",
            "liquidity":    "Low liquidity",
            "mc_range":     "MC out of range",
            "age":          "Too old",
            "low_activity": "Low activity",
            "sell_pressure":"Sell pressure",
            "wash_trade":   "Wash trading",
            "no_socials":   "No socials (−10 pts)",
            "few_holders":  "Few holders",
            "other":        "Other",
        }
        if skip_counts:
            top_skips = sorted(skip_counts.items(), key=lambda x: -x[1])[:4]
            skip_detail = "  " + "  ·  ".join(
                _skip_icons.get(k, "❓") + " " + _skip_labels.get(k, k) + ": " + str(v)
                for k, v in top_skips
            )
        else:
            skip_detail = "  _No skip data yet_"
        if apex_on:
            mode_line = "⚡ *APEX ENGINE — ACTIVE*"
        elif auto_on:
            mode_line = "🟢 *AUTO MODE — ACTIVE*  _(legacy)_"
        elif adv_on:
            mode_line = "🧠 *ADVISORY MODE — ACTIVE*"
        else:
            mode_line = "🔴 *SNIPER OFF*"

        await q.edit_message_text(
            "🎯 *AI SNIPER*\n"
            "━━━━━━━━━━━━━━━━━━\n"
            + mode_line + "\n\n"
            "⛓️ Chains: " + chain_str + "\n"
            "💰 Budget: *" + money(budget) + "*  (spent: " + money(spent) + ")\n"
            "📊 Session: *" + str(bought_n) + " bought*  ·  " + str(skip_n) + " skipped\n"
            + (skip_detail + "\n" if skip_n > 0 else "") +
            "\n"
            "⚡ *APEX* — Full autonomous engine. Trailing exits, threat\n"
            "  detection, vault locking, self-calibration. _Recommended._\n\n"
            "🧠 *Advisory* — AI flags signals to your DM or channel.\n"
            "  You confirm every trade manually.\n\n"
            "🔧 *Active Filters:*\n"
            "  Score ≥ *" + str(sf.get("min_score", 35)) + "*  ·  "
            "Liq ≥ *" + money(sf.get("min_liq", 5_000)) + "*  ·  "
            "MC *" + mc_str(sf.get("min_mc", 10_000)) + "–" + mc_str(sf.get("max_mc", 500_000)) + "*\n"
            "  Buy amt: *" + money(sf.get("buy_amount", 20)) + "*  ·  "
            "Age ≤ *" + str(sf.get("max_age_h", 6.0)) + "h*  ·  "
            "Buy% ≥ *" + str(sf.get("min_buy_pct", 45)) + "%*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "⚡ APEX Engine" + (" ✅" if apex_on else " — Tap to configure"),
                    callback_data="apex_menu"
                )],
                [InlineKeyboardButton("🧠 Advisory Mode", callback_data="sniper_adv_menu")],
                [InlineKeyboardButton("⛓️ Chains",  callback_data="sniper_chains_menu"),
                 InlineKeyboardButton("⚙️ Filters", callback_data="sniper_filters_menu")],
                [InlineKeyboardButton("📋 Sniper Log",  callback_data="sniper_log_view"),
                 InlineKeyboardButton("💰 Budget",      callback_data="sniper_budget_cfg")],
                [InlineKeyboardButton("👀 KOL Tracker", callback_data="kol_menu")],
                [InlineKeyboardButton("🔩 Manual / Legacy Mode", callback_data="sniper_auto_menu")],
                [InlineKeyboardButton("◀ Back", callback_data="mm")],
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
            "🔩 *MANUAL / LEGACY MODE*\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "⚠️ _This mode is superseded by APEX._\n"
            "_APEX has smarter exits, trailing stops, vault locking,_\n"
            "_and self-calibration. Use APEX instead._\n\n"
            "Status: *" + ("🟢 ON" if auto_on else "🔴 OFF") + "*\n\n"
            "Buys automatically on SNIPE verdict with fixed SL/TP.\n"
            "No active position management after entry.\n\n"
            "Stop Loss: *" + ("ON — " + str(sl_pct) + "%" if sl_on else "OFF") + "*\n"
            "Take Profit: *" + ("ON — " + tp_str if tp_on else "OFF") + "*\n"
            "Notifications: *" + ("ON 🔔" if notify else "OFF 🔕") + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⚡ Switch to APEX Instead", callback_data="apex_menu")],
                [InlineKeyboardButton(("🔴 Disable" if auto_on else "🟢 Enable (Legacy)"), callback_data="sniper_auto_toggle")],
                [InlineKeyboardButton(("✅ Stop Loss ON" if sl_on else "❌ Stop Loss OFF"), callback_data="sniper_sl_toggle"),
                 InlineKeyboardButton("⚙️ SL %", callback_data="sniper_sl_pct_cfg")],
                [InlineKeyboardButton(("✅ Take Profit ON" if tp_on else "❌ Take Profit OFF"), callback_data="sniper_tp_toggle"),
                 InlineKeyboardButton("⚙️ TP Targets", callback_data="sniper_tp_cfg")],
                [InlineKeyboardButton(("🔕 Mute Notifs" if notify else "🔔 Unmute Notifs"), callback_data="sniper_auto_notif")],
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
            "3️⃣ Also make sure *apex_sniper_bot* is an admin in the channel/group\n\n"
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
            "Current: *" + str(ud.get("sniper_filters", {}).get("max_vol_mc_ratio", 10.0)) + "x*\n\n"
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
        skip_counts = ud.get("sniper_skip_counts", {})
        _skip_icons = {"hard_flag":"🚩","score":"📉","liquidity":"💧","mc_range":"📊",
                       "age":"⏰","low_activity":"😴","sell_pressure":"📛",
                       "wash_trade":"🔄","no_socials":"👻","few_holders":"👥","other":"❓"}
        _skip_labels = {"hard_flag":"Hard flag","score":"Score","liquidity":"Liq",
                        "mc_range":"MC range","age":"Too old","low_activity":"Activity",
                        "sell_pressure":"Buy%","wash_trade":"Wash trade",
                        "no_socials":"No socials","few_holders":"Holders","other":"Other"}
        if skip_counts:
            top = sorted(skip_counts.items(), key=lambda x: -x[1])[:5]
            skip_txt = "\n\n🔍 *Why tokens are being skipped:*\n" + "\n".join(
                "  " + _skip_icons.get(k,"❓") + " " + _skip_labels.get(k,k) + ": *" + str(v) + "*"
                for k, v in top
            )
        else:
            skip_txt = ""
        await q.edit_message_text(
            "⚙️ *SNIPER FILTERS*\n\n"
            "Tokens must pass ALL filters before AI analyzes them.\n"
            "_No socials = −10 score penalty (not a hard skip)_\n\n"
            "Min Score: *"    + str(sf.get("min_score",   35))         + "/100*\n"
            "Min Liq: *"      + money(sf.get("min_liq",        5_000))  + "*\n"
            "MC Range: *"     + mc_str(sf.get("min_mc",        10_000)) + "* → *" + mc_str(sf.get("max_mc", 500_000)) + "*\n"
            "Max Age: *"      + str(sf.get("max_age_h",  6.0))          + "h*\n"
            "Min Buys/1h: *"  + str(sf.get("min_buys_h1", 10))         + "*\n"
            "Min Buy%: *"     + str(sf.get("min_buy_pct",   45))        + "%*\n"
            "Vol/MC Cap: *"   + str(sf.get("max_vol_mc_ratio", 10.0))   + "x*\n"
            "Buy Amount: *"   + money(sf.get("buy_amount", 20))         + "*"
            + skip_txt,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Min Score",     callback_data="sniper_cfg_score"),
                 InlineKeyboardButton("💧 Min Liq",       callback_data="sniper_cfg_liq")],
                [InlineKeyboardButton("📈 MC Range",      callback_data="sniper_cfg_mc"),
                 InlineKeyboardButton("⏰ Max Age",       callback_data="sniper_cfg_age")],
                [InlineKeyboardButton("📊 Min Buys/1h",   callback_data="sniper_cfg_buys_h1"),
                 InlineKeyboardButton("📉 Min Buy%",      callback_data="sniper_cfg_buy_pct")],
                [InlineKeyboardButton("🚿 Vol/MC Cap",    callback_data="sniper_cfg_vol_mc"),
                 InlineKeyboardButton("💵 Buy Amount",    callback_data="sniper_cfg_amt")],
                [InlineKeyboardButton("🔄 Reset to Recommended", callback_data="sniper_filters_reset")],
                [InlineKeyboardButton("◀ Back",           callback_data="v_sniper")],
            ])
        )

    elif cb.startswith("tc_"):
        # KOL alert "Trade on APEX Sniper" button — show token card
        contract = cb[3:]
        if contract:
            await _show_token_card(q, u, ud, ctx, contract)
        else:
            await q.answer("Invalid token address.")

    elif cb == "sniper_filters_reset":
        ud["sniper_filters"] = {
            "min_score":        35,
            "min_liq":          5_000,
            "min_mc":           10_000,
            "max_mc":           500_000,
            "max_age_h":        6.0,
            "buy_amount":       20,
            "min_buys_h1":      10,
            "min_buy_pct":      45,
            "max_vol_mc_ratio": 10.0,
            "min_liq_pct":      3,
            "max_top10_pct":    28,
            "min_lp_burn":      50,
        }
        ud["sniper_skip_counts"] = {}   # reset skip counters too
        save_user(u.id, ud)
        await q.answer("✅ Filters reset to recommended defaults!", show_alert=True)
        # Re-render filters menu
        sf = ud["sniper_filters"]
        await q.edit_message_text(
            "⚙️ *SNIPER FILTERS — RESET DONE*\n\n"
            "Filters restored to recommended defaults.\n"
            "_No socials = −10 score penalty (not a hard skip)_\n\n"
            "Min Score: *35/100*\n"
            "Min Liq: *$5,000*\n"
            "MC Range: *$10K* → *$500K*\n"
            "Max Age: *6.0h*\n"
            "Min Buys/1h: *10*\n"
            "Min Buy%: *45%*\n"
            "Vol/MC Cap: *10.0x*\n"
            "Buy Amount: *$20*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Min Score",     callback_data="sniper_cfg_score"),
                 InlineKeyboardButton("💧 Min Liq",       callback_data="sniper_cfg_liq")],
                [InlineKeyboardButton("📈 MC Range",      callback_data="sniper_cfg_mc"),
                 InlineKeyboardButton("⏰ Max Age",       callback_data="sniper_cfg_age")],
                [InlineKeyboardButton("📊 Min Buys/1h",   callback_data="sniper_cfg_buys_h1"),
                 InlineKeyboardButton("📉 Min Buy%",      callback_data="sniper_cfg_buy_pct")],
                [InlineKeyboardButton("🚿 Vol/MC Cap",    callback_data="sniper_cfg_vol_mc"),
                 InlineKeyboardButton("💵 Buy Amount",    callback_data="sniper_cfg_amt")],
                [InlineKeyboardButton("🔄 Reset to Recommended", callback_data="sniper_filters_reset")],
                [InlineKeyboardButton("◀ Back",           callback_data="v_sniper")],
            ])
        )

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

        skip_counts = ud.get("sniper_skip_counts", {})
        _skip_icons = {"hard_flag":"🚩","score":"📉","liquidity":"💧","mc_range":"📊",
                       "age":"⏰","low_activity":"😴","sell_pressure":"📛",
                       "wash_trade":"🔄","no_socials":"👻","few_holders":"👥","other":"❓"}
        _skip_labels_short = {"hard_flag":"Flag","score":"Score","liquidity":"Liq",
                              "mc_range":"MC","age":"Age","low_activity":"Activity",
                              "sell_pressure":"Buy%","wash_trade":"Wash",
                              "no_socials":"Socials","few_holders":"Holders","other":"Other"}
        if skip_counts:
            top_skips = sorted(skip_counts.items(), key=lambda x: -x[1])[:5]
            skip_breakdown = "\n🔍 *Skip reasons:*  " + "  ".join(
                _skip_icons.get(k,"❓") + _skip_labels_short.get(k,k) + ":" + str(v)
                for k, v in top_skips
            ) + "\n"
        else:
            skip_breakdown = ""

        await q.edit_message_text(
            "📋 *SNIPER LOG*\n\n"
            "Analyzed: *" + str(len(log)) + "*  |  Bought: *" + str(len(bought)) + "*  |  Skipped: *" + str(len(skipped)) + "*\n"
            "Sniper Win Rate: *" + str(b_wr) + "%*\n"
            + skip_breakdown +
            "\nTap any token for full AI breakdown 👇",
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
        if len(ud.get("kol_wallets", [])) >= 10:
            await q.edit_message_text(
                "⚠️ *Max 10 wallets reached.*\nRemove one before adding another.",
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

    # ── APEX VAULT MENU ───────────────────────────────────────────────────────
    elif cb == "apex_vault_menu":
        vault        = ud.get("apex_vault", 0.0)
        balance      = ud.get("balance", 0.0)
        vault_pnl    = ud.get("apex_vault_pnl", 0.0)
        vault_trade  = ud.get("apex_vault_trade_on", False)
        total_locked = sum(
            sum(h.get("apex_vault_locked", {}).values())
            for h in ud.get("holdings", {}).values()
        )
        # Count open vault-funded positions
        vault_pos_val = sum(
            h.get("total_invested", 0) for h in ud.get("holdings", {}).values()
            if h.get("vault_funded")
        )
        vt_label = "⚡ Vault Trading: ON — tap to disable" if vault_trade else "💤 Vault Trading: OFF — tap to enable"
        kb = [
            [InlineKeyboardButton(vt_label,                    callback_data="apex_vault_trade_toggle")],
            [InlineKeyboardButton("💸 Withdraw to Balance",    callback_data="apex_vault_withdraw")],
            [InlineKeyboardButton("💰 Withdraw All to Balance",callback_data="apex_vault_withdraw_all")],
            [InlineKeyboardButton("📊 Vault History",          callback_data="apex_vault_history")],
            [InlineKeyboardButton("⬅️ Back",                   callback_data="mm")],
        ]
        vault_trade_line = (
            "\n⚡ *Vault Trading ON* — APEX spends vault balance first\n"
            "  Active vault positions: *" + money(vault_pos_val) + "*\n"
            "  Vault P&L (all time): *" + pstr(vault_pnl) + "*\n"
        ) if vault_trade else "\n💤 Vault Trading OFF — vault is protected savings\n"
        await q.edit_message_text(
            "🏦 *APEX PROFIT VAULT*\n\n"
            "💰 Vault Balance: *" + money(vault) + "*\n"
            "📈 In-position locked: *" + money(total_locked) + "*\n"
            "💵 Main Balance: *" + money(balance) + "*\n"
            + vault_trade_line +
            "\nVault funds are profits locked at 2x and 5x milestones.\n"
            "Withdrawing moves them to your main trading balance.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    elif cb == "apex_vault_withdraw":
        vault = ud.get("apex_vault", 0.0)
        if vault < 0.01:
            await q.edit_message_text(
                "🏦 *APEX VAULT*\n\nVault is empty — no profits locked yet.\n\n"
                "Profits lock to vault at 2x and 5x milestones in APEX mode.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="apex_vault_menu")]])
            )
            return
        pending[u.id] = {"action": "apex_vault_withdraw_amt", "_prompt_msg_id": q.message.message_id}
        await q.edit_message_text(
            "🏦 *VAULT WITHDRAWAL*\n\n"
            "Vault balance: *" + money(vault) + "*\n\n"
            "Enter amount to withdraw to your trading balance:\n"
            "_(e.g. 50, 200.50, or type MAX for everything)_",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="apex_vault_menu")]])
        )

    elif cb == "apex_vault_withdraw_all":
        vault = ud.get("apex_vault", 0.0)
        if vault < 0.01:
            await q.edit_message_text(
                "🏦 *APEX VAULT*\n\nVault is empty.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="apex_vault_menu")]])
            )
            return
        ud["balance"]    += vault
        ud["apex_vault"]  = 0.0
        await q.edit_message_text(
            "✅ *VAULT WITHDRAWN*\n\n"
            "Moved *" + money(vault) + "* from vault to trading balance.\n\n"
            "💵 New Trading Balance: *" + money(ud["balance"]) + "*\n"
            "🏦 Vault: *$0.00*\n\n"
            "⚠️ These funds are now available for APEX to trade with.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="apex_vault_menu")]])
        )

    elif cb == "apex_vault_trade_toggle":
        vault       = ud.get("apex_vault", 0.0)
        current     = ud.get("apex_vault_trade_on", False)
        new_state   = not current
        if new_state and vault < 1.0:
            await q.edit_message_text(
                "🏦 *VAULT TRADING*\n\n"
                "Your vault is empty — nothing to trade with yet.\n\n"
                "Vault fills when APEX positions close at 2x or 5x profit milestones.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="apex_vault_menu")]])
            )
            return
        ud["apex_vault_trade_on"] = new_state
        save_user(u.id, ud)
        state_word = "ENABLED" if new_state else "DISABLED"
        msg = (
            "⚡ *VAULT TRADING " + state_word + "*\n\n"
            + ("APEX will now draw from vault balance first before touching your main balance.\n\n"
               "💰 Vault available: *" + money(vault) + "*\n"
               "Profits from vault trades return to vault — self-sustaining cycle."
               if new_state else
               "Vault is protected again.\nAPEX will only trade from your main balance.")
        )
        await q.edit_message_text(
            msg, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="apex_vault_menu")]])
        )

    elif cb == "apex_vault_history":
        uid_v   = u.id
        logs    = trade_log.get(uid_v, [])
        # Collect all trades where vault was locked
        vault_events = []
        for h in ud.get("holdings", {}).values():
            for milestone, amt in h.get("apex_vault_locked", {}).items():
                vault_events.append(("🔒 " + milestone + " lock (open pos)", amt, h.get("symbol","?")))
        apex_logs = [t for t in logs if t.get("mood") in ("APEX","AI-Sniper")]
        total_locked_hist = sum(e[1] for e in vault_events)
        if not vault_events and not apex_logs:
            await q.edit_message_text(
                "🏦 *VAULT HISTORY*\n\nNo vault locks yet.\n\n"
                "Profits lock at 2x and 5x in APEX mode.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="apex_vault_menu")]])
            )
            return
        lines_out = ["🏦 *VAULT HISTORY*\n"]
        if vault_events:
            lines_out.append("*Open position locks:*")
            for label, amt, sym in vault_events:
                lines_out.append("  " + label + " $" + sym + " → " + money(amt))
        if apex_logs:
            lines_out.append("\n*Closed APEX trades:*")
            for t in apex_logs[-8:]:
                x = round(t.get("x", 0), 2)
                pnl = t.get("realized_pnl", 0)
                lines_out.append(
                    "  $" + t.get("symbol","?") + "  " + str(x) + "x  " + pstr(pnl)
                )
        lines_out.append("\n💰 Current vault: *" + money(ud.get("apex_vault",0)) + "*")
        await q.edit_message_text(
            "\n".join(lines_out),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="apex_vault_menu")]])
        )


    # ── APEX MODE MENU ────────────────────────────────────────────────────────
    elif cb == "apex_menu":
        apex_on   = ud.get("apex_mode", False)
        vault     = ud.get("apex_vault", 0.0)
        try:
            heat  = apex_capital_heat(ud)
        except Exception:
            heat  = 0.0
        try:
            positions = apex_count_positions(ud)
        except Exception:
            positions = 0
        try:
            paused = apex_is_paused(u.id)
        except Exception:
            paused = False
        try:
            halted = apex_is_daily_loss_halted(ud)
        except Exception:
            halted = False
        daily_pnl  = ud.get("apex_daily_pnl", 0.0)
        total_tr   = ud.get("apex_total_trades", 0)
        total_wins = ud.get("apex_total_wins", 0)
        wr         = round(total_wins / total_tr * 100) if total_tr > 0 else 0

        if apex_on and paused:
            state_line = "\u23f8\ufe0f *PAUSED* (cooling down after 3 losses)"
        elif apex_on and halted:
            state_line = "\U0001f6d1 *HALTED* (daily loss limit reached)"
        elif apex_on:
            state_line = "\u26a1 *ON*"
        else:
            state_line = "\U0001f534 *OFF*"

        heat_pct  = round(heat * 100)
        heat_bar  = "\u2593" * int(heat * 10) + "\u2591" * (10 - int(heat * 10))
        heat_icon = "\U0001f7e2" if heat < 0.4 else ("\U0001f7e1" if heat < 0.65 else "\U0001f534")
        toggle_label = "\U0001f534 Disable APEX" if apex_on else "\u26a1 Enable APEX"

        kb = [
            [InlineKeyboardButton(toggle_label, callback_data="apex_toggle")],
            [InlineKeyboardButton("\U0001f3e6 Vault \u2014 " + money(vault), callback_data="apex_vault_menu")],
            [InlineKeyboardButton("\U0001f4ca APEX Stats",    callback_data="apex_stats"),
             InlineKeyboardButton("\U0001f4cb APEX Log",      callback_data="apex_log_view")],
            [InlineKeyboardButton("\u2699\ufe0f APEX Settings", callback_data="apex_settings_menu")],
            [InlineKeyboardButton("\u25c0 Back",              callback_data="v_sniper")],
        ]
        text = (
            "\u26a1 *APEX MODE*\n"
            "_Autonomous Profit & Exit eXecution_\n\n"
            "Status: " + state_line + "\n\n"
            "\U0001f4ca *Live Stats*\n"
            "  Open positions: *" + str(positions) + "/∞*\n"
            "  " + heat_icon + " Capital heat: *" + heat_bar + "* " + str(heat_pct) + "%\n"
            "  Today PnL: *" + pstr(daily_pnl) + "*\n"
            "  All-time: *" + str(total_tr) + " trades*  *" + str(wr) + "% WR*\n\n"
            "\U0001f3e6 *Vault:* " + money(vault) + "\n\n"
            "\u26a1 APEX features:\n"
            "  \u2022 45s entry confirmation\n"
            "  \u2022 Dynamic position sizing\n"
            "  \u2022 Trailing stop (no fixed TP)\n"
            "  \u2022 S/R-aware trail tightening\n"
            "  \u2022 Smart DCA at support\n"
            "  \u2022 Drawdown protection\n"
            "  \u2022 Profit vault (2x/5x locks)\n"
            "  \u2022 Self-learning thresholds"
        )
        await q.edit_message_text(text, parse_mode="Markdown",
                                  reply_markup=InlineKeyboardMarkup(kb))

    elif cb == "apex_toggle":
        apex_on = ud.get("apex_mode", False)
        if not apex_on:
            ud["sniper_auto"]              = False
            ud["apex_mode"]                = True
            ud["apex_session_start_bal"]   = ud.get("balance", 0)
            try:
                apex_reset_daily(ud)
            except Exception:
                pass
            msg = (
                "\u26a1 *APEX MODE ENABLED*\n\n"
                "Auto Mode disabled \u2014 APEX is now in control.\n\n"
                "APEX will:\n"
                "\u2022 Wait 45s before buying (live confirmation)\n"
                "\u2022 Size positions by confidence \u00d7 heat\n"
                "\u2022 Trail exits \u2014 no fixed take profits\n"
                "\u2022 Tighten trail near S/R zones\n"
                "\u2022 DCA into support on winning positions\n"
                "\u2022 Lock profits to vault at 2x and 5x\n"
                "\u2022 Pause after 3 losses, halt at -20% daily\n\n"
                "\U0001f4b0 Starting balance: *" + money(ud["apex_session_start_bal"]) + "*\n"
                "\U0001f3e6 Vault: *" + money(ud.get("apex_vault", 0)) + "*"
            )
        else:
            ud["apex_mode"] = False
            msg = (
                "\U0001f534 *APEX MODE DISABLED*\n\n"
                "Open APEX positions will continue to be managed until they close.\n\n"
                "Switch to Auto or Advisory Mode from the sniper menu."
            )
        await q.edit_message_text(
            msg, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\u26a1 APEX Menu",  callback_data="apex_menu")],
                [InlineKeyboardButton("\U0001f3af Sniper", callback_data="v_sniper")],
                [InlineKeyboardButton("\U0001f3e0 Menu",   callback_data="mm")],
            ])
        )

    elif cb == "apex_stats":
        total_tr   = ud.get("apex_total_trades", 0)
        total_wins = ud.get("apex_total_wins", 0)
        wr         = round(total_wins / total_tr * 100) if total_tr > 0 else 0
        vault      = ud.get("apex_vault", 0.0)
        daily_pnl  = ud.get("apex_daily_pnl", 0.0)
        conf_thr   = ud.get("apex_learn_threshold", 5)
        score_min  = ud.get("apex_learn_score_min", 45)
        consec_l   = ud.get("apex_consec_losses", 0)
        try:
            paused = apex_is_paused(u.id)
            halted = apex_is_daily_loss_halted(ud)
        except Exception:
            paused = halted = False

        apex_logs = [t for t in trade_log.get(u.id, [])
                     if t.get("mood") in ("APEX", "APEX-DCA", "AI-Sniper")]
        exit_counts: dict = {}
        for t in apex_logs:
            r = t.get("reason", "manual")
            exit_counts[r] = exit_counts.get(r, 0) + 1
        reason_parts = [r + ": " + str(n)
                        for r, n in sorted(exit_counts.items(), key=lambda x: -x[1])]
        reason_str = "  " + "\n  ".join(reason_parts) if reason_parts else "  No closed trades yet"

        status_str = "\u23f8\ufe0f Paused" if paused else ("\U0001f6d1 Halted" if halted else "\u2705 Active")
        text = (
            "\U0001f4ca *APEX LIFETIME STATS*\n\n"
            "Trades: *" + str(total_tr) + "*  Wins: *" + str(total_wins) + "*  WR: *" + str(wr) + "%*\n"
            "Today PnL: *" + pstr(daily_pnl) + "*\n"
            "Consecutive losses: *" + str(consec_l) + "*\n"
            "Status: " + status_str + "\n\n"
            "\U0001f3e6 *Vault:* " + money(vault) + "\n\n"
            "\U0001f9e0 *Self-learned thresholds:*\n"
            "  Min confidence: *" + str(conf_thr) + "/10*\n"
            "  Min score: *" + str(score_min) + "/100*\n\n"
            "\U0001f4e4 *Exit breakdown:*\n" + reason_str
        )
        await q.edit_message_text(
            text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\u2b05\ufe0f Back", callback_data="apex_menu")]
            ])
        )

    elif cb == "apex_settings_menu":
        conf_thr  = ud.get("apex_learn_threshold", 5)
        score_min = ud.get("apex_learn_score_min", 45)
        text = (
            "\u2699\ufe0f *APEX SETTINGS*\n\n"
            "Auto-tuned by self-learning engine every 10 trades.\n\n"
            "Min confidence: *" + str(conf_thr) + "/10*\n"
            "Min score: *" + str(score_min) + "/100*\n"
            "Max open positions: *Unlimited*\n"
            "Daily loss halt: *20%*\n"
            "Drawdown pause: *30 min* after 3 losses\n"
            "Trail activates at: *1.5x*\n"
            "Vault locks: *2x \u2192 50%*  |  *5x \u2192 75%*\n"
            "DCA: *1\u00d7 per position at support*\n\n"
            "_Thresholds adjust automatically based on what\u2019s working._"
        )
        await q.edit_message_text(
            text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\u2b05\ufe0f Back", callback_data="apex_menu")]
            ])
        )

    elif cb == "apex_log_view":
        apex_logs = [t for t in trade_log.get(u.id, [])
                     if t.get("mood") in ("APEX", "APEX-DCA", "AI-Sniper")]
        open_apex = [(ca, h) for ca, h in ud.get("holdings", {}).items()
                     if h.get("mood") in ("APEX", "APEX-DCA", "AI-Sniper")]
        if not apex_logs and not open_apex:
            text = "\U0001f4cb *APEX LOG*\n\nNo APEX trades yet."
        else:
            lines = ["\U0001f4cb *APEX LOG*\n"]
            if open_apex:
                lines.append("*Open positions:*")
                for ca, h in open_apex[:4]:
                    cx    = round((h.get("price", h.get("avg_price", 0)) / h.get("avg_price", 1)), 2) if h.get("avg_price") else 0
                    lines.append("  $" + h.get("symbol", "?") + "  " + str(cx) + "x  inv " + money(h.get("total_invested", 0)))
                lines.append("")
            if apex_logs:
                lines.append("*Closed trades (last 8):*")
                for t in apex_logs[-8:]:
                    x   = round(t.get("x", 0), 2)
                    pnl = t.get("realized_pnl", 0)
                    sym = t.get("symbol", "?")
                    rea = t.get("reason", "")
                    dca = " +DCA" if t.get("apex_dca") else ""
                    lines.append("  $" + sym + "  " + str(x) + "x  " + pstr(pnl) + dca
                                 + ("  (" + rea + ")" if rea else ""))
            text = "\n".join(lines)
        await q.edit_message_text(
            text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("\u2b05\ufe0f Back", callback_data="apex_menu")]
            ])
        )


    # ── CHANNEL CARD AI TOGGLE ────────────────────────────────────────────────
    elif cb.startswith("ch_ai_"):
        ca_key = cb[6:]
        card   = _ch_card_cache.get(ca_key)
        if not card:
            return  # q.answer() already called at top of btn()
        card["expanded"] = not card["expanded"]
        expanded = card["expanded"]
        info     = card["info"];  sc_c = card["sc"];  ai_c = card["ai"]
        contract = card["contract"]
        bot_url  = card.get("bot_url")
        view_url = card.get("view_url") or card.get("dex_url", "")
        score    = sc_c.get("score", 0)

        new_text = _ai_report_text(info, sc_c, ai_c, contract=contract, expanded=expanded)

        rows = []
        if bot_url:
            rows.append([InlineKeyboardButton("⚡ Buy on APEX Sniper", url=bot_url)])
        rows.append([InlineKeyboardButton("🔍 View Token Live ↗", url=view_url)])
        label = f"🧠 {score}/100 ▲" if expanded else f"🧠 {score}/100 ▼"
        rows.append([InlineKeyboardButton(label, callback_data="ch_ai_" + ca_key)])

        try:
            await q.edit_message_text(
                text=new_text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(rows),
                disable_web_page_preview=True
            )
        except Exception as err:
            logger.warning(f"ch_ai toggle failed: {err}")



# Tracks last check time per position for adaptive interval
_apex_last_check: dict = {}   # uid -> {contract -> timestamp}

async def apex_post_exit_tracker(app) -> None:
    """
    Runs inside apex_checker_job every cycle.
    For each recently exited APEX position, fetches current price at
    30min / 1h / 4h intervals after exit and records what the token did.
    This data feeds the daily report and JSON export so you can see
    whether RED exits were rugs or healthy retraces you missed.

    Snapshot checkpoints: 30min, 1h, 4h after exit.
    Entries expire after 5 hours.
    """
    CHECKPOINTS = [1800, 3600, 14400]   # 30m, 1h, 4h in seconds
    EXPIRY      = 18000                  # 5h — stop tracking after this
    now_ts      = _time.time()

    for uid, exits in list(_apex_post_exit.items()):
        for contract, rec in list(exits.items()):
            try:
                age = now_ts - rec["exit_at"]
                # Expire old entries
                if age > EXPIRY:
                    del exits[contract]
                    continue

                snapshots    = rec["snapshots"]
                snaps_taken  = [s["checkpoint_s"] for s in snapshots]

                # Which checkpoints are due?
                due = [cp for cp in CHECKPOINTS if cp not in snaps_taken and age >= cp]
                if not due:
                    continue

                info = await get_token(contract)
                if not info:
                    continue

                cur_price    = info.get("price", 0)
                exit_price   = rec["exit_price"]
                entry_price  = rec["entry_price"]
                if cur_price <= 0 or exit_price <= 0:
                    continue

                x_vs_exit  = round(cur_price / exit_price, 3)
                x_vs_entry = round(cur_price / entry_price, 3) if entry_price > 0 else 0

                for cp in due:
                    snapshots.append({
                        "checkpoint_s": cp,
                        "checkpoint_label": "30m" if cp == 1800 else ("1h" if cp == 3600 else "4h"),
                        "price":        cur_price,
                        "x_vs_exit":    x_vs_exit,
                        "x_vs_entry":   x_vs_entry,
                        "mc":           info.get("mc", 0),
                        "checked_at":   now_ts,
                    })

                # Alert user if token pumped significantly after APEX sold
                # Only alert once, on first snapshot that shows meaningful gain
                if not rec.get("pump_alerted") and x_vs_exit >= 1.5:
                    rec["pump_alerted"] = True
                    label = "30m" if age < 3600 else ("1h" if age < 14400 else "4h")
                    try:
                        await app.bot.send_message(
                            chat_id=uid, parse_mode="Markdown",
                            text=(
                                "📊 *POST-EXIT INSIGHT*\n\n"
                                "*$" + rec["symbol"] + "* pumped after APEX sold\n\n"
                                "Exit reason: *" + rec["exit_reason"].replace("apex_","").replace("_"," ") + "*\n"
                                "Exit at: *" + str(rec["exit_x"]) + "x*\n"
                                "Now (" + label + " later): *" + str(round(x_vs_entry, 2)) + "x vs entry*\n"
                                "Price since exit: *+" + str(round((x_vs_exit - 1) * 100, 1)) + "%*\n\n"
                                "_This data is saved in your daily JSON export._"
                            )
                        )
                    except Exception:
                        pass

            except Exception as _pe:
                logger.debug(f"Post-exit tracker error {contract}: {_pe}")


async def apex_checker_job(ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Adaptive APEX position checker.
    CLEAR threat  → 15s interval (saves credits)
    YELLOW threat → 8s interval
    ORANGE/RED    → 4s interval (maximum urgency)
    Helius on-chain rug signal checked every cycle regardless.
    """
    import time as _t
    now = _t.time()
    try:
        for uid, ud in list(users.items()):
            holdings = ud.get("holdings", {})
            apex_holdings = {
                c: h for c, h in holdings.items()
                if h.get("mood") in ("APEX", "AI-Sniper", "APEX-DCA") and h.get("amount", 0) > 0
            }
            if not apex_holdings:
                continue

            user_checks = _apex_last_check.setdefault(uid, {})

            # Determine which positions actually need checking this cycle
            positions_due = []
            for contract, h in apex_holdings.items():
                threat    = h.get("apex_threat", "CLEAR")
                last_chk  = user_checks.get(contract, 0)
                elapsed   = now - last_chk

                # Adaptive interval based on current threat
                if threat in ("RED", "ORANGE"):
                    interval = 4
                elif threat == "YELLOW":
                    interval = 8
                else:
                    interval = 15   # CLEAR — save credits

                if elapsed >= interval:
                    positions_due.append(contract)

            if not positions_due:
                continue

            # Run position manager — it will only act on positions_due
            # Pass due list so it skips positions not yet due
            await apex_run_position_manager(ctx.application, uid, ud,
                                            positions_due=positions_due)

            # Update last check timestamps
            for contract in positions_due:
                user_checks[contract] = now

    except Exception as e:
        logger.error(f"apex_checker_job crashed: {e}", exc_info=True)

    # ── Post-exit snapshot collector (runs every apex_checker cycle) ──────────
    try:
        await apex_post_exit_tracker(ctx.application)
    except Exception as _pet:
        logger.debug(f"Post-exit tracker error: {_pet}")


def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_start))
    app.add_handler(CallbackQueryHandler(btn))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.job_queue.run_repeating(checker_job,      interval=PRICE_CHECK_INTERVAL,      first=10)
    app.job_queue.run_repeating(apex_checker_job, interval=APEX_PRICE_CHECK_INTERVAL, first=15)
    app.job_queue.run_repeating(sniper_job,      interval=300, first=60)
    app.job_queue.run_repeating(kol_tracker_job,       interval=300, first=90)   # KOL wallet tracker
    app.job_queue.run_repeating(channel_milestone_job, interval=300, first=120)  # Channel milestone tracker
    app.job_queue.run_daily(daily_summary_job, time=__import__("datetime").time(23, 59))
    # APEX midnight self-calibration
    async def _apex_midnight_calibrate(ctx2):
        for _uid2, _ud2 in list(users.items()):
            if _ud2.get("apex_mode"):
                apex_self_calibrate(_ud2, _uid2)
    app.job_queue.run_daily(_apex_midnight_calibrate, time=__import__("datetime").time(0, 5))
    app.job_queue.run_daily(monthly_report_job, time=__import__("datetime").time(8, 0))
    app.job_queue.run_repeating(autosave_job, interval=120, first=120)  # DB autosave every 2 min

    # Load all persisted user data before starting
    load_all(users, trade_log)
    # Warm APEX in-memory learning from persisted data
    for _uid, _ud in users.items():
        _mem = _ud.get("apex_memory")
        if _mem:
            _apex_learn_memory[_uid] = list(_mem)

    # Graceful HTTP client shutdown
    async def _on_shutdown(application):
        global _http
        if _http and not _http.is_closed:
            await _http.aclose()
            logger.info("HTTP client closed cleanly.")
    app.post_shutdown = _on_shutdown

    logger.info("APEX SNIPER BOT running...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
