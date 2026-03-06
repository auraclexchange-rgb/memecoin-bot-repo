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

BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens/{}"
PRICE_CHECK_INTERVAL = 30
MAX_BALANCE = 10_000.0
MIN_BALANCE = 1.0

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

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

def generate_trade_card(symbol: str, chain: str, pnl_str: str, x_val: str, held_h: str, bought_str: str, position_str: str, username: str, pnl_pct: str, pnl_positive: bool) -> "io.BytesIO | None":
    if not PILLOW_OK:
        return None
    try:
        import os
        W, H = 1100, 580
        img = Image.new("RGB", (W, H), color=(8, 10, 18))
        draw = ImageDraw.Draw(img)
        # Find fonts — bundled with bot, fallback to system
        _dir = os.path.dirname(os.path.abspath(__file__))
        _bold = None
        _regular = None
        for _path in [
            os.path.join(_dir, "DejaVuSans-Bold.ttf"),
            os.path.join("/app", "DejaVuSans-Bold.ttf"),
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ]:
            if os.path.exists(_path):
                _bold = _path
                break
        for _path in [
            os.path.join(_dir, "DejaVuSans.ttf"),
            os.path.join("/app", "DejaVuSans.ttf"),
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ]:
            if os.path.exists(_path):
                _regular = _path
                break
        try:
            if not _bold or not _regular:
                raise Exception("Fonts not found")
            font_token = ImageFont.truetype(_bold,    86)
            font_pill  = ImageFont.truetype(_bold,    68)
            font_label = ImageFont.truetype(_regular, 30)
            font_value = ImageFont.truetype(_bold,    30)
            font_brand = ImageFont.truetype(_bold,    24)
            font_user  = ImageFont.truetype(_bold,    28)
            font_tiny  = ImageFont.truetype(_regular, 19)
            font_badge = ImageFont.truetype(_bold,    22)
        except Exception:
            font_token = font_pill = font_label = font_value = font_brand = font_user = font_tiny = font_badge = ImageFont.load_default()
        chain_short = {"solana":"SOL","sol":"SOL","ethereum":"ETH","eth":"ETH","base":"BASE","bsc":"BNB","bnb":"BNB","arbitrum":"ARB","arb":"ARB","polygon":"MATIC","matic":"MATIC","avalanche":"AVAX","avax":"AVAX","sui":"SUI"}
        chain_label = chain_short.get(chain.lower(), chain.upper()[:4])
        chain_colors = {"SOL":(153,69,255),"ETH":(98,126,234),"BASE":(0,82,255),"BNB":(243,186,47),"ARB":(40,160,240),"MATIC":(130,71,229),"AVAX":(232,65,66),"SUI":(78,122,255)}
        badge_col = chain_colors.get(chain_label, (80,100,160))
        for y in range(H):
            t = y/H
            draw.line([(0,y),(W,y)], fill=(int(8+t*4),int(10+t*6),int(18+t*14)))
        glow_col = (0,45,20) if pnl_positive else (45,6,6)
        for i in range(100,0,-1):
            draw.rectangle([0,0,i*2,H], fill=glow_col)
        base_dir = os.path.dirname(os.path.abspath(__file__))
        char_file = "win_char.jpg" if pnl_positive else "loss_char.jpg"
        char_path = os.path.join(base_dir, char_file)
        if not os.path.exists(char_path):
            char_path = os.path.join("/app", char_file)
        if os.path.exists(char_path):
            char = Image.open(char_path).convert("RGBA")
            char_h = H
            char_w = int(char.width * char_h / char.height)
            char = char.resize((char_w, char_h), Image.LANCZOS)
            char_x = W - char_w - 5
            char_y = 0
            img.paste(char, (char_x, char_y), char)
            overlay = Image.new("RGBA", (W, H), (0,0,0,0))
            ov_draw = ImageDraw.Draw(overlay)
            fade_start = char_x
            fade_end = char_x + 180
            for x in range(max(0, fade_start), min(fade_end, W)):
                t = (x - fade_start) / (fade_end - fade_start)
                alpha = int((1 - t) * 200)
                ov_draw.line([(x,0),(x,H)], fill=(8,10,18,alpha))
            img = Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")
            draw = ImageDraw.Draw(img)
        badge_w = len(chain_label)*15+30
        draw.rounded_rectangle([38,22,38+badge_w,58], radius=12, fill=badge_col)
        draw.text((38+badge_w//2,40), chain_label, font=font_badge, fill=(255,255,255), anchor="mm")
        draw.text((W-40,40), "AURACLE_XBOT", font=font_brand, fill=(185,200,230), anchor="rm")
        draw.text((38,75), "$"+symbol, font=font_token, fill=(240,245,255))
        clean_pnl      = pnl_str.lstrip("$")
        clean_bought   = bought_str.lstrip("$")
        clean_position = position_str.lstrip("$")
        pill_col = (0,200,105) if pnl_positive else (205,38,38)
        txt_col  = (5,15,8)   if pnl_positive else (255,235,235)
        prefix   = "+"        if pnl_positive else "-"
        draw.rounded_rectangle([38,195,590,298], radius=20, fill=pill_col)
        draw.text((68,246), chain_label+"  "+prefix+"$"+clean_pnl, font=font_pill, fill=txt_col, anchor="lm")
        pnl_col = (0,220,120) if pnl_positive else (220,75,75)
        stats = [("PNL",prefix+pnl_pct,pnl_col),("Bought",chain_label+" - $"+clean_bought,(195,210,235)),("Position",chain_label+" - $"+clean_position,(195,210,235)),("Held",held_h,(195,210,235))]
        for i,(label,value,vcol) in enumerate(stats):
            y = 322+i*50
            draw.text((38,y), label, font=font_label, fill=(125,140,170))
            draw.text((370,y), value, font=font_value, fill=vcol)
        draw.line([(38,H-68),(630,H-68)], fill=(35,45,68), width=1)
        ax,ay = 55,H-34
        draw.ellipse([ax-22,ay-22,ax+22,ay+22], fill=(50,70,140))
        draw.text((ax,ay), (username[0].upper() if username else "A"), font=font_tiny, fill=(200,220,255), anchor="mm")
        draw.text((ax+32,ay), "@"+username, font=font_user, fill=(200,215,240), anchor="lm")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        return buf
    except Exception as e:
        logger.error("Card generation error: " + str(e))
        return None


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

        return {
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
                "min_score":  55,
                "min_liq":    30_000,
                "min_mc":     50_000,
                "max_mc":     10_000_000,
                "max_age_h":  72,
                "buy_amount": 100,
            },
            "sniper_bought":    [],
            "sniper_log":       [],   # history of every sniper decision
            # DCA by Market Cap
            "dca_orders":       [],
            # Language
            "language":         "en",
        }
        trade_log[uid] = []
    return users[uid]


async def fetch_ohlcv(pair_addr: str, chain_id: str) -> list:
    try:
        url = (
            f"https://api.geckoterminal.com/api/v2/networks/{chain_id}"
            f"/pools/{pair_addr}/ohlcv/minute?aggregate=5&limit=60"
        )
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(url, headers={"Accept": "application/json"})
            if r.status_code == 200:
                return r.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
    except Exception:
        pass
    return []


def generate_price_chart(info: dict, ohlcv: list):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as patches
        import numpy as np
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
        import logging
        logging.getLogger(__name__).warning(f"Chart error: {e}")
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
        # Track mood performance
        mood = h.get("mood", "")
        if mood:
            if mood not in ud.get("mood_stats", {}):
                ud.setdefault("mood_stats", {})[mood] = {"trades": 0, "wins": 0, "pnl": 0.0}
            ud["mood_stats"][mood]["trades"] += 1
            ud["mood_stats"][mood]["wins"] += 1
            ud["mood_stats"][mood]["pnl"] += realized
    else:
        ud["trade_hours"][hour]["losses"] += 1
        ud["consec_losses"] = ud.get("consec_losses", 0) + 1
        mood = h.get("mood", "")
        if mood:
            if mood not in ud.get("mood_stats", {}):
                ud.setdefault("mood_stats", {})[mood] = {"trades": 0, "wins": 0, "pnl": 0.0}
            ud["mood_stats"][mood]["trades"] += 1
            ud["mood_stats"][mood]["pnl"] += realized

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
        e = "🚀" if v >= 20 else "📈" if v >= 5 else "🟢" if v >= 0 else "🔴" if v >= -10 else "💀"
        return e + " *" + ("+" if v >= 0 else "") + str(round(v, 1)) + "%*"

    held = contract in ud["holdings"]
    pos_line = ""
    if held:
        h = ud["holdings"][contract]
        cv = h["amount"] * info["price"]
        cx = info["price"] / h["avg_price"] if h.get("avg_price", 0) > 0 else 0
        ppnl = cv - h["total_invested"]
        pnl_e = "💚" if ppnl >= 0 else "🔴"
        pos_line = (
            "\n━━━━━━━━━━━━━━━━\n"
            "📌 *YOUR POSITION*\n"
            "💼 Value: *" + money(cv) + "*  |  *" + str(round(cx, 2)) + "x*\n"
            + pnl_e + " PnL: *" + pstr(ppnl) + "*"
        )

    buy_pct  = info.get("buy_pct", 50)
    sell_pct = 100 - buy_pct
    pressure = "🟢 Buying" if buy_pct >= 55 else "🔴 Selling" if sell_pct >= 55 else "⚖️ Neutral"
    liq_warn = "\n🚨 *WARNING: LOW LIQUIDITY — HIGH RISK*" if info["liq"] < 50_000 else ""

    sc_line = ""
    if sc:
        score_e = "🟢" if sc["score"] >= 70 else "🟡" if sc["score"] >= 45 else "🔴"
        grade_e = "✅" if "GREEN" in sc["verdict"] else "⚠️" if "YELLOW" in sc["verdict"] else "🚫"
        sc_line = (
            "\n━━━━━━━━━━━━━━━━\n"
            + score_e + " *AURACLE SCORE: " + str(sc["score"]) + "/100*\n"
            + grade_e + " *" + sc["verdict"] + "*\n"
        )
        if sc["strengths"]:
            sc_line += "\n💪 *Strengths:*\n" + "\n".join("  ✅ " + s for s in sc["strengths"])
        if sc["warnings"]:
            sc_line += "\n\n⚠️ *Warnings:*\n" + "\n".join("  🚨 " + w for w in sc["warnings"])

    age_h = info.get("age_h", 0)
    age_e = "🆕" if age_h < 1 else "⏰"

    # Build social links line
    twitter  = info.get("twitter", "")
    telegram = info.get("telegram", "")
    website  = info.get("website", "")
    social_parts = []
    if twitter:
        social_parts.append("🐦 [Twitter](" + twitter + ")")
    if telegram:
        social_parts.append("💬 [Telegram](" + telegram + ")")
    if website:
        social_parts.append("🌐 [Website](" + website + ")")
    social_line = ""
    if social_parts:
        social_line = "\n━━━━━━━━━━━━━━━━\n🔗 *Socials:*  " + "  •  ".join(social_parts)

    # Build dynamic X search links using combined OR query (same format as user specified)
    import urllib.parse, re
    _name   = str(info.get("name",""))
    _symbol = str(info.get("symbol",""))
    # Combined OR query: (Name OR $SYMBOL OR CA OR url:CA)
    combined_q  = f"({_name} OR ${_symbol} OR {contract} OR url:{contract})"
    q_combined  = urllib.parse.quote(combined_q)
    q_ca_only   = urllib.parse.quote(contract)
    q_name_only = urllib.parse.quote(_name)
    q_sym_only  = urllib.parse.quote("$" + _symbol)
    base = "https://x.com/search?f=live&q={}&src=typed_query"
    # Safe display text (strip markdown-breaking chars)
    safe_name   = re.sub(r'[_*\[\]()~`>#+\-=|{}.!]', '', _name)[:15].strip() or "Name"
    safe_symbol = re.sub(r'[_*\[\]()~`>#+\-=|{}.!]', '', _symbol).strip() or "SYM"
    x_all    = f"[🔍 All]({base.format(q_combined)})"
    x_ca     = f"[📌 CA]({base.format(q_ca_only)})"
    x_name   = f"[📛 {safe_name}]({base.format(q_name_only)})"
    x_symbol = f"[🔤 ${safe_symbol}]({base.format(q_sym_only)})"
    x_line   = f"\n━━━━━━━━━━━━━━━━\n🔍 *Search 𝕏:* {x_all}  {x_ca}  {x_name}  {x_symbol}"

    # ATH line
    ath_price = info.get("ath_price", 0)
    ath_mc    = info.get("ath_mc", 0)
    ath_line  = ""
    if ath_price and ath_price > 0:
        ath_line = "\n🏆 *ATH:* $" + str(ath_price) + ("  (" + mc_str(ath_mc) + " MC)" if ath_mc else "")

    try:
        return (
            "🪙 *" + str(info.get("name","Unknown")) + "* ($" + str(info.get("symbol","???")) + ")\n"
            + "⛓ " + str(chain_icon(info.get("chain","SOL"))) + " " + str(info.get("chain","SOL")).upper()
            + "  🏦 " + str(info.get("dex","")).upper() + "\n"
            + "`" + contract + "`\n"
            + "━━━━━━━━━━━━━━━━\n"
            + "💲 *Price:* $" + str(info.get("price",0)) + "\n"
            + "📊 *Market Cap:* " + mc_str(info.get("mc",0)) + "\n"
            + "💧 *Liquidity:* " + money(info.get("liq",0)) + " (" + str(info.get("liq_pct",0)) + "%)\n"
            + age_e + " *Age:* " + age_str(age_h)
            + ath_line + "\n"
            + "━━━━━━━━━━━━━━━━\n"
            + "📉 *Price Changes*\n"
            + "5m: " + fc(info.get("ch_m5",0)) + "   1h: " + fc(info.get("ch_h1",0)) + "\n"
            + "6h: " + fc(info.get("ch_h6",0)) + "   24h: " + fc(info.get("ch_h24",0)) + "\n"
            + "━━━━━━━━━━━━━━━━\n"
            + "📈 *Vol 24h:* " + money(info.get("vol_h24",0)) + "\n"
            + "🛒 *Buys:* " + str(info.get("buys",0)) + " (" + str(buy_pct) + "%)"
            + "  🏃 *Sells:* " + str(info.get("sells",0)) + " (" + str(sell_pct) + "%)\n"
            + "⚡ *Pressure:* " + pressure
            + pos_line + sc_line + liq_warn
            + social_line
            + x_line
        )
    except Exception as e:
        return f"🪙 *{info.get('name','Token')}* (${info.get('symbol','???')})\n`{contract}`\n\nPrice: ${info.get('price',0)}"


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
        [InlineKeyboardButton("🔗 Referrals",      callback_data="v_referrals"),
         InlineKeyboardButton("👤 Profile",        callback_data="v_profile")],
        [InlineKeyboardButton("📁 Export CSV",     callback_data="v_export"),
         InlineKeyboardButton("📖 Help & Docs",    callback_data="v_help")],
        [InlineKeyboardButton("🏠 Main Menu",      callback_data="mm")],
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
        InlineKeyboardButton("🔔 Alert",        callback_data="pal_" + contract),
        InlineKeyboardButton("👁 Watchlist",    callback_data="wl_" + contract),
    ])
    rows.append([
        InlineKeyboardButton("📉 DCA by MC",   callback_data="dca_" + contract),
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
    # External links — chain aware
    chain_raw = ud.get("last_chain", "solana").lower()
    chain_map_dex  = {"solana":"solana","sol":"solana","ethereum":"ethereum","eth":"ethereum",
                      "bsc":"bsc","bnb":"bsc","base":"base","arbitrum":"arbitrum","arb":"arbitrum"}
    chain_map_gmgn = {"solana":"sol","sol":"sol","ethereum":"eth","eth":"eth",
                      "bsc":"bsc","bnb":"bsc","base":"base"}
    dex_chain  = chain_map_dex.get(chain_raw, "solana")
    gmgn_chain = chain_map_gmgn.get(chain_raw, "sol")
    dex_url    = f"https://dexscreener.com/{dex_chain}/{contract}"
    gmgn_url   = f"https://gmgn.ai/{gmgn_chain}/token/{contract}"
    pump_url   = f"https://pump.fun/{contract}"
    link_row = [
        InlineKeyboardButton("📊 DEX",  url=dex_url),
        InlineKeyboardButton("🔍 GMGN", url=gmgn_url),
    ]
    if chain_raw in ("solana","sol"):
        link_row.append(InlineKeyboardButton("🎯 PUMP", url=pump_url))
    rows.append(link_row)
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
    for uid, ud in list(users.items()):
        if ud.get("balance") is None:
            continue
    await bundle_sell_detector(app)
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


async def sniper_scan() -> list:
    """Fetch recently listed token profiles from DexScreener."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://api.dexscreener.com/token-profiles/latest/v1",
                                 headers={"Accept": "application/json"})
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    return data
    except Exception as e:
        logger.warning(f"Sniper scan error: {e}")
    return []


def _sniper_chain_id(chain: str) -> str:
    """Normalise DexScreener chainId to sniper_chains key."""
    return {
        "solana": "solana", "sol": "solana",
        "ethereum": "ethereum", "eth": "ethereum",
        "base": "base",
        "bsc": "bsc", "bnb": "bsc",
        "arbitrum": "arbitrum", "arb": "arbitrum",
    }.get(chain.lower(), "")


def _sniper_daily_reset(ud: dict):
    """Reset daily sniper budget if it's a new day."""
    today = datetime.now().date()
    if ud.get("sniper_daily_date") != today:
        ud["sniper_daily_spent"] = 0.0
        ud["sniper_daily_date"] = today


def _build_history_context(ud: dict) -> str:
    """Summarise user's past sniper trades for AI context."""
    sniper_trades = [
        t for t in trade_log.get(
            next((k for k, v in users.items() if v is ud), 0), []
        )
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


async def ai_analyze_token(info: dict, sc: dict, ud: dict) -> dict:
    """
    Call Claude to analyze a token for sniper quality.
    Returns dict with: verdict, confidence, suggested_amount, thesis, red_flags, green_flags
    """
    history_ctx = _build_history_context(ud)
    bal          = ud.get("balance", 1000)
    sf           = ud.get("sniper_filters", {})
    max_buy      = float(sf.get("buy_amount", 100))

    prompt = f"""You are an expert crypto trader and risk analyst. Analyze this token for a sniper buy and give a structured verdict.

TOKEN DATA:
Symbol: ${info['symbol']} ({info.get('name','?')})
Chain: {info.get('chain','?')}
Price: ${info.get('price',0):.10g}
Market Cap: ${info.get('mc',0):,.0f}
Liquidity: ${info.get('liq',0):,.0f} ({info.get('liq_pct',0)}% of MC)
Age: {round(info.get('age_h',0),1)} hours old
24h Volume: ${info.get('vol_h24',0):,.0f}
1h Volume: ${info.get('vol_h1',0):,.0f}
5m Volume: ${info.get('vol_m5',0):,.0f}
Price Change 5m: {info.get('ch_m5',0)}%
Price Change 1h: {info.get('ch_h1',0)}%
Price Change 6h: {info.get('ch_h6',0)}%
Price Change 24h: {info.get('ch_h24',0)}%
Buys 24h: {info.get('buys',0)} ({info.get('buy_pct',50)}%)
Sells 24h: {info.get('sells',0)} ({100-info.get('buy_pct',50)}%)
Twitter: {info.get('twitter','none')}
Telegram: {info.get('telegram','none')}
Website: {info.get('website','none')}
AURACLE Score: {sc['score']}/100 — {sc['verdict']}
AURACLE Strengths: {', '.join(sc.get('strengths',[]))}
AURACLE Warnings: {', '.join(sc.get('warnings',[]))}

USER'S SNIPER HISTORY (learn from this):
{history_ctx}

User balance: ${bal:,.2f}
Max configured buy per snipe: ${max_buy}

ANALYZE:
1. Rug/honeypot risk: Look for suspicious patterns (very new + massive volume, liquidity ratio too low, sell pressure dominant)
2. Momentum: Is volume accelerating? Are buys increasing? Price trend direction?
3. Social presence: Does it have Twitter/TG/website? No socials on a new token is a major red flag.
4. Pattern match: Does this resemble past winning sniper trades in the history above?
5. Suggested entry: Based on risk level and user balance, how much should they put in? Be conservative on high-risk tokens.

Respond ONLY in this exact JSON format (no markdown, no explanation outside JSON):
{{
  "verdict": "SNIPE" | "SKIP" | "WAIT",
  "confidence": <1-10 integer>,
  "suggested_amount": <float, max {max_buy}>,
  "thesis": "<2-3 sentence plain English explanation of your decision>",
  "red_flags": ["<flag1>", "<flag2>"],
  "green_flags": ["<flag1>", "<flag2>"],
  "rug_risk": "LOW" | "MEDIUM" | "HIGH",
  "momentum": "STRONG" | "MODERATE" | "WEAK" | "NEGATIVE",
  "social_score": "GOOD" | "PARTIAL" | "NONE"
}}"""

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"Content-Type": "application/json"},
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 600,
                    "messages": [{"role": "user", "content": prompt}],
                }
            )
            if resp.status_code == 200:
                raw = resp.json()
                text = raw["content"][0]["text"].strip()
                # Strip any accidental markdown fences
                text = text.replace("```json", "").replace("```", "").strip()
                import json as _json
                result = _json.loads(text)
                # Clamp suggested_amount
                result["suggested_amount"] = min(
                    float(result.get("suggested_amount", max_buy)),
                    max_buy,
                    ud.get("balance", 0)
                )
                return result
    except Exception as e:
        logger.warning(f"AI token analysis failed: {e}")

    # Fallback if AI call fails
    return {
        "verdict": "WAIT",
        "confidence": 0,
        "suggested_amount": max_buy,
        "thesis": "AI analysis unavailable. Manual review recommended.",
        "red_flags": ["AI call failed"],
        "green_flags": [],
        "rug_risk": "UNKNOWN",
        "momentum": "UNKNOWN",
        "social_score": "UNKNOWN",
    }


def _ai_report_text(info: dict, sc: dict, ai: dict) -> str:
    """Build the full AI advisory report message."""
    verdict_emoji = {"SNIPE": "🟢", "SKIP": "🔴", "WAIT": "🟡"}.get(ai["verdict"], "⚪")
    rug_emoji     = {"LOW": "✅", "MEDIUM": "⚠️", "HIGH": "🚨"}.get(ai["rug_risk"], "❓")
    mom_emoji     = {"STRONG": "🚀", "MODERATE": "📈", "WEAK": "📉", "NEGATIVE": "💀"}.get(ai["momentum"], "❓")
    soc_emoji     = {"GOOD": "✅", "PARTIAL": "⚠️", "NONE": "🚨"}.get(ai["social_score"], "❓")
    conf_bar      = "█" * ai["confidence"] + "░" * (10 - ai["confidence"])

    red_flags   = "\n".join("  🚨 " + f for f in ai.get("red_flags", [])) or "  None detected"
    green_flags = "\n".join("  ✅ " + f for f in ai.get("green_flags", [])) or "  None detected"

    return (
        "🤖 *AI SNIPER ANALYSIS*\n"
        "━━━━━━━━━━━━━━━━\n"
        "*$" + info["symbol"] + "*  |  " + info.get("chain","?").upper() + "  |  MC: " + mc_str(info["mc"]) + "\n\n"
        + verdict_emoji + " *Verdict: " + ai["verdict"] + "*\n"
        "🎯 Confidence: *" + str(ai["confidence"]) + "/10*  `" + conf_bar + "`\n"
        "💵 Suggested Buy: *" + money(ai["suggested_amount"]) + "*\n\n"
        "📝 *Thesis:*\n" + ai["thesis"] + "\n\n"
        "━━━━━━━━━━━━━━━━\n"
        + rug_emoji + " Rug Risk: *" + ai["rug_risk"] + "*\n"
        + mom_emoji + " Momentum: *" + ai["momentum"] + "*\n"
        + soc_emoji + " Socials: *" + ai["social_score"] + "*\n\n"
        "🚩 *Red Flags:*\n" + red_flags + "\n\n"
        "💪 *Green Flags:*\n" + green_flags + "\n\n"
        "📊 AURACLE Score: *" + str(sc["score"]) + "/100* — " + sc["verdict"]
    )


def _sniper_auto_kb(contract: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 Set Auto-Sell", callback_data="asm_" + contract),
         InlineKeyboardButton("🛑 Stop Loss",     callback_data="slm_" + contract)],
        [InlineKeyboardButton("📝 Journal",        callback_data="jnl_" + contract),
         InlineKeyboardButton("🏠 Main Menu",      callback_data="mm")],
    ])


async def sniper_job(ctx: ContextTypes.DEFAULT_TYPE):
    """Main AI sniper job — runs every 5 minutes."""
    # Collect users with either mode active
    active_users = [
        (uid, ud) for uid, ud in users.items()
        if (ud.get("sniper_auto") or ud.get("sniper_advisory")) and ud.get("balance", 0) > 0
    ]
    if not active_users:
        return

    raw_tokens = await sniper_scan()
    seen: set = set()

    for item in raw_tokens:
        contract = item.get("tokenAddress", "")
        if not contract or contract in seen:
            continue
        seen.add(contract)

        info = await get_token(contract)
        if not info:
            continue

        token_chain = _sniper_chain_id(info.get("chain", ""))
        sc          = score_token(info)

        for uid, ud in active_users:
            try:
                # ── Chain filter ───────────────────────────────────────────
                chains = ud.get("sniper_chains", {})
                if token_chain and not chains.get(token_chain, False):
                    continue

                # ── Basic pre-filters ──────────────────────────────────────
                sf = ud.get("sniper_filters", {})
                if sc["score"] < int(sf.get("min_score", 55)):
                    continue
                if info["liq"] < float(sf.get("min_liq", 30_000)):
                    continue
                if not (float(sf.get("min_mc", 50_000)) <= info["mc"] <= float(sf.get("max_mc", 10_000_000))):
                    continue
                if info.get("age_h") and info["age_h"] > float(sf.get("max_age_h", 72)):
                    continue

                # ── Deduplicate ────────────────────────────────────────────
                sniper_bought = ud.setdefault("sniper_bought", [])
                if contract in sniper_bought:
                    continue

                # ── Daily budget reset + check ─────────────────────────────
                _sniper_daily_reset(ud)
                budget    = ud.get("sniper_daily_budget", 500.0)
                spent     = ud.get("sniper_daily_spent", 0.0)
                max_buy   = float(sf.get("buy_amount", 100))
                if spent + max_buy > budget:
                    logger.info(f"Sniper daily budget hit for {uid}")
                    continue

                # ── AI Analysis ────────────────────────────────────────────
                ai = await ai_analyze_token(info, sc, ud)

                # Log every decision
                ud.setdefault("sniper_log", []).append({
                    "contract":  contract,
                    "symbol":    info["symbol"],
                    "chain":     info.get("chain", "?"),
                    "mc":        info["mc"],
                    "score":     sc["score"],
                    "verdict":   ai["verdict"],
                    "confidence":ai["confidence"],
                    "rug_risk":  ai["rug_risk"],
                    "timestamp": datetime.now().isoformat(),
                    "bought":    False,
                })

                # ══════════════════════════════════════════════════════════
                # MODE 1 — FULL AUTO
                # ══════════════════════════════════════════════════════════
                if ud.get("sniper_auto") and ai["verdict"] == "SNIPE":
                    buy_amt = min(ai["suggested_amount"], ud["balance"], budget - spent)
                    if buy_amt < 1:
                        continue
                    sniper_bought.append(contract)
                    ud["sniper_daily_spent"] = spent + buy_amt
                    ud["sniper_log"][-1]["bought"] = True
                    ud["sniper_log"][-1]["amount"] = buy_amt

                    result = await do_buy_core(ud, uid, contract, buy_amt, planned=True, mood="AI-Sniper")
                    if isinstance(result, tuple):
                        info2, _ = result
                        h = ud["holdings"].get(contract, {})

                        # Auto stop loss
                        if ud.get("sniper_auto_sl") and h:
                            sl_pct = ud.get("sniper_auto_sl_pct", 40.0)
                            # Tighten SL if rug risk is HIGH
                            if ai["rug_risk"] == "HIGH":
                                sl_pct = min(sl_pct, 20.0)
                            h["stop_loss_pct"] = sl_pct

                        # Auto take profit
                        if ud.get("sniper_auto_tp") and h:
                            tp_xs = ud.get("sniper_auto_tp_x", [2.0, 5.0])
                            pct_each = round(1.0 / len(tp_xs), 2)
                            h["auto_sells"] = [
                                {"pct": pct_each, "x": x, "triggered": False}
                                for x in tp_xs
                            ]
                            # Last target gets the remainder
                            if h["auto_sells"]:
                                h["auto_sells"][-1]["pct"] = round(1.0 - pct_each * (len(tp_xs) - 1), 2)

                        if ud.get("sniper_auto_notify", True):
                            sl_line = f"\n🛑 Stop Loss: {ud.get('sniper_auto_sl_pct',40)}%" if ud.get("sniper_auto_sl") else ""
                            tp_line = ""
                            if ud.get("sniper_auto_tp"):
                                tp_parts = [f"{int(pct_each*100)}% at {x}x" for x in tp_xs]
                                tp_line = "\n🎯 TP: " + "  |  ".join(tp_parts)
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

                # ══════════════════════════════════════════════════════════
                # MODE 2 — ADVISORY
                # ══════════════════════════════════════════════════════════
                if ud.get("sniper_advisory") and ud.get("sniper_adv_notify", True):
                    # Send even SKIP/WAIT if notify is on, but only SNIPE if confidence >= 6
                    if ai["verdict"] == "SNIPE" or ai["confidence"] >= 6:
                        report = _ai_report_text(info, sc, ai)
                        kb_rows = []
                        if ai["verdict"] == "SNIPE":
                            kb_rows.append([
                                InlineKeyboardButton(
                                    "✅ Buy " + money(ai["suggested_amount"]),
                                    callback_data="snp_confirm_" + contract + "_" + str(round(ai["suggested_amount"], 2))
                                ),
                                InlineKeyboardButton("❌ Skip", callback_data="snp_skip_" + contract),
                            ])
                        else:
                            kb_rows.append([InlineKeyboardButton("❌ Dismiss", callback_data="snp_skip_" + contract)])
                        kb_rows.append([InlineKeyboardButton("🏠 Main Menu", callback_data="mm")])
                        try:
                            await ctx.bot.send_message(
                                chat_id=uid, parse_mode="Markdown",
                                text=report,
                                reply_markup=InlineKeyboardMarkup(kb_rows)
                            )
                        except Exception as _ae:
                            logger.error(f"Advisory notify error {uid}: {_ae}")

            except Exception as _ue:
                logger.warning(f"Sniper job error for user {uid}: {_ue}")


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
    message = update.message          # shorthand used throughout handler
    text = message.text.strip()
    p = pending.get(u.id)

    if p:
        action = p.get("action", "")
        if not action:
            # Pending exists but no action — clear it and treat as CA
            pending.pop(u.id, None)
            p = None
        elif len(text) > 30 and action not in ("set_balance", "comp_bet", "comp_join", "acc_new"):
            # Looks like a CA pasted while in a non-CA pending state — clear and scan
            pending.pop(u.id, None)
            p = None

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

        elif action == "ch_custom_target":
            try:
                target = float(text)
                pending[u.id] = {"action": "ch_custom_days", "target": target}
                await message.reply_text(f"✅ Target: {money(target)}\n\nNow enter number of days for the challenge:\nExample: 30")
            except:
                await message.reply_text("❌ Enter a valid number. Example: 10000")

        elif action == "ch_custom_days":
            try:
                days = int(text)
                target = pending[u.id].get("target", 10000)
                start_eq = ud["balance"] + sum(h["total_invested"] for h in ud["holdings"].values())
                ud["challenge"] = {"start_eq": start_eq, "target_eq": target, "days": days, "started": datetime.now().isoformat()}
                del pending[u.id]
                await message.reply_text(f"🎯 *Challenge Started!*\n\n{money(start_eq)} → {money(target)} in {days} days\n\nGood luck!", parse_mode="Markdown", reply_markup=main_menu_kb())
            except:
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



        elif action == "wl_waiting":
            # User typed something while waiting for watchlist choice — ignore, remind them
            await message.reply_text(
                "👇 Please tap a button above to set your alert, or tap Cancel.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Alert by Price",        callback_data="wl_add_price")],
                    [InlineKeyboardButton("Alert by Market Cap",   callback_data="wl_add_mc")],
                    [InlineKeyboardButton("No Alert — Just Watch", callback_data="mm")],
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
            except:
                await message.reply_text("❌ Enter a valid price. Example: 0.00005")

        elif action == "wl_target_mc":
            try:
                target = float(text)
                contract = pending[u.id].get("contract","")
                if contract and ud.get("watchlist", {}).get(contract):
                    ud["watchlist"][contract]["target_mc"] = target
                del pending[u.id]
                await message.reply_text(f"✅ MC alert set at {mc_str(target)}", reply_markup=main_menu_kb())
            except:
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
                pending.pop(u.id, None)
                await update.message.reply_text("✅ Min score set to *" + str(val) + "/100*", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Enter a number between 0 and 100", reply_markup=cancel_kb())
            return

        elif action == "sniper_liq":
            try:
                val = float(text.replace("$", "").replace(",", ""))
                assert val >= 0
                ud.setdefault("sniper_filters", {})["min_liq"] = val
                pending.pop(u.id, None)
                await update.message.reply_text("✅ Min liquidity set to *" + money(val) + "*", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Enter a number like 50000", reply_markup=cancel_kb())
            return

        elif action == "sniper_mc":
            try:
                parts = text.replace("$", "").replace(",", "").split()
                assert len(parts) == 2
                min_mc, max_mc = float(parts[0]), float(parts[1])
                assert 0 < min_mc < max_mc
                ud.setdefault("sniper_filters", {})["min_mc"] = min_mc
                ud["sniper_filters"]["max_mc"] = max_mc
                pending.pop(u.id, None)
                await update.message.reply_text(
                    "✅ MC range: *" + mc_str(min_mc) + "* → *" + mc_str(max_mc) + "*",
                    parse_mode="Markdown", reply_markup=back_main()
                )
            except Exception:
                await update.message.reply_text("❌ Format: min max\nExample: 50000 5000000", reply_markup=cancel_kb())
            return

        elif action == "sniper_age":
            try:
                val = float(text)
                assert val > 0
                ud.setdefault("sniper_filters", {})["max_age_h"] = val
                pending.pop(u.id, None)
                await update.message.reply_text("✅ Max age: *" + str(val) + "h*", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Enter hours like 24", reply_markup=cancel_kb())
            return

        elif action == "sniper_amt":
            try:
                val = float(text.replace("$", "").replace(",", ""))
                assert val > 0
                ud.setdefault("sniper_filters", {})["buy_amount"] = val
                pending.pop(u.id, None)
                await update.message.reply_text("✅ Buy amount: *" + money(val) + "* per snipe", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Enter a number like 150", reply_markup=cancel_kb())
            return

        elif action == "sniper_sl_pct":
            try:
                val = float(text.replace("%", ""))
                assert 5 <= val <= 95
                ud["sniper_auto_sl_pct"] = val
                pending.pop(u.id, None)
                await update.message.reply_text("✅ Auto SL set to *" + str(val) + "%* drop", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Enter a % between 5 and 95", reply_markup=cancel_kb())
            return

        elif action == "sniper_tp_x":
            try:
                parts = text.replace("x","").split()
                xs = [float(p) for p in parts]
                assert all(x > 1 for x in xs) and len(xs) >= 1
                xs.sort()
                ud["sniper_auto_tp_x"] = xs
                pending.pop(u.id, None)
                await update.message.reply_text(
                    "✅ Take profit targets: *" + "  |  ".join(str(x) + "x" for x in xs) + "*",
                    parse_mode="Markdown", reply_markup=back_main()
                )
            except Exception:
                await update.message.reply_text("❌ Format: 2 5 10", reply_markup=cancel_kb())
            return

        elif action == "sniper_budget":
            try:
                val = float(text.replace("$", "").replace(",", ""))
                assert val > 0
                ud["sniper_daily_budget"] = val
                pending.pop(u.id, None)
                await update.message.reply_text("✅ Daily sniper budget: *" + money(val) + "*", parse_mode="Markdown", reply_markup=back_main())
            except Exception:
                await update.message.reply_text("❌ Enter a number like 300", reply_markup=cancel_kb())
            return

        # ── DCA BY MC INPUT ────────────────────────────────────────────────────
        elif action == "dca_input":
            contract = p["contract"]
            sym      = p.get("symbol", "?")
            try:
                parts = text.replace("$", "").replace(",", "").split()
                assert len(parts) >= 2 and len(parts) % 2 == 0
                mc_targets = []
                for i in range(0, len(parts), 2):
                    mc_val  = float(parts[i])
                    buy_amt = float(parts[i + 1])
                    assert mc_val > 0 and buy_amt > 0
                    mc_targets.append({"mc": mc_val, "amount": buy_amt, "triggered": False})
                mc_targets.sort(key=lambda x: x["mc"])
                # Remove old DCA for same contract
                ud["dca_orders"] = [d for d in ud.get("dca_orders", []) if d["contract"] != contract]
                ud["dca_orders"].append({
                    "contract":   contract,
                    "symbol":     sym,
                    "mc_targets": mc_targets,
                    "created_at": datetime.now().isoformat(),
                    "cancelled":  False,
                })
                pending.pop(u.id, None)
                lines = "\n".join(
                    "  📍 Buy *" + money(tgt["amount"]) + "* when MC hits *" + mc_str(tgt["mc"]) + "*"
                    for tgt in mc_targets
                )
                await update.message.reply_text(
                    t(ud, "dca_set", symbol=sym, lines=lines),
                    parse_mode="Markdown", reply_markup=back_main()
                )
            except Exception:
                await update.message.reply_text(
                    "❌ Format: mc1 amount1 mc2 amount2\nExample: 500000 100 1000000 200",
                    reply_markup=cancel_kb()
                )
            return

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
    ud["last_chain"] = info.get("chain", "solana")
    # Send price chart with real candlesticks
    try:
        # Get pair address for OHLCV
        ohlcv = []
        pair_addr = info.get("pair_addr", "")
        chain_id  = info.get("chain", "solana").lower()
        chain_map = {"solana": "solana", "sol": "solana", "ethereum": "eth",
                     "eth": "eth", "bsc": "bsc", "bnb": "bsc",
                     "base": "base", "arbitrum": "arbitrum"}
        chain_id = chain_map.get(chain_id, chain_id)
        if pair_addr:
            ohlcv = await fetch_ohlcv(pair_addr, chain_id)
        chart_buf = generate_price_chart(info, ohlcv)
        if chart_buf:
            await update.message.reply_photo(
                photo=chart_buf,
                caption=f"📊 *${info['symbol']}* — {info.get('chain','SOL').upper()} | MC: {mc_str(info['mc'])}",
                parse_mode="Markdown"
            )
    except Exception as chart_err:
        logger.warning(f"Chart error: {chart_err}")
    # Send token card
    try:
        card_txt = token_card(info, contract, ud, sc)
        await msg.edit_text(card_txt, parse_mode="Markdown", reply_markup=buy_kb(contract, ud))
    except Exception as card_err:
        logger.error(f"Token card error: {card_err}")
        await msg.edit_text(f"❌ Error loading token: {card_err}", reply_markup=back_main())


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
        rest = cb[3:]
        if rest == "all":
            ud["limit_orders"] = []
            ud["price_alerts"] = []
            await q.edit_message_text("✅ All orders and alerts cancelled.", reply_markup=back_main())
        else:
            try:
                idx = int(rest)
                if 0 <= idx < len(ud["limit_orders"]):
                    cancelled_sym = ud["limit_orders"][idx].get("symbol", "?")
                    ud["limit_orders"].pop(idx)
                    await q.edit_message_text(
                        "✅ Order for $" + cancelled_sym + " cancelled.",
                        reply_markup=back_main()
                    )
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
                    await context.bot.send_photo(chat_id=ch_id, photo=card, caption=caption)
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
        wl = ud.get("watchlist", {})
        if not wl:
            await q.edit_message_text(
                "👁 *WATCHLIST*\n\nNo tokens being watched.\nPaste a CA then use the Watchlist button to add.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("◀ Back", callback_data="mm")],
                ]))
        else:
            txt = "👁 *WATCHLIST* — " + str(len(wl)) + " token" + ("s" if len(wl) != 1 else "") + "\n\nTap any token to view live data:\n"
            buttons = []
            for ca, w in list(wl.items()):
                sym      = w.get("symbol", "?")
                added_mc = mc_str(w.get("added_mc", 0))
                tp       = w.get("target_price")
                tm       = w.get("target_mc")
                alert_tag = " 🔔" if (tp or tm) else ""
                buttons.append([InlineKeyboardButton(
                    "🪙 $" + sym + "  |  Added: " + added_mc + alert_tag,
                    callback_data="btt_" + ca
                )])
            buttons.append([InlineKeyboardButton("◀ Back", callback_data="mm")])
            await q.edit_message_text(txt, parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(buttons))

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
                        except:
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

    elif cb == "wl_add_price":
        pending[u.id] = {"action": "wl_target_price", "contract": pending.get(u.id, {}).get("contract", "")}
        await q.edit_message_text("👁 Enter target PRICE to alert:\nExample: 0.00005", reply_markup=cancel_kb())

    elif cb == "wl_add_mc":
        pending[u.id] = {"action": "wl_target_mc", "contract": pending.get(u.id, {}).get("contract", "")}
        await q.edit_message_text("👁 Enter target MARKET CAP to alert:\nExample: 100000 (=$100K)", reply_markup=cancel_kb())

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
                [InlineKeyboardButton("Alert by Price",      callback_data="wl_add_price")],
                [InlineKeyboardButton("Alert by Market Cap", callback_data="wl_add_mc")],
                [InlineKeyboardButton("No Alert — Just Watch", callback_data="mm")],
            ]))

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
            "Filters:\n"
            "  Min Score: *" + str(sf.get("min_score",55)) + "/100*\n"
            "  Min Liq: *" + money(sf.get("min_liq",30_000)) + "*\n"
            "  MC Range: *" + mc_str(sf.get("min_mc",50_000)) + "* → *" + mc_str(sf.get("max_mc",10_000_000)) + "*\n"
            "  Max Age: *" + str(sf.get("max_age_h",72)) + "h*\n"
            "  Buy Amount: *" + money(sf.get("buy_amount",100)) + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🤖 Auto Mode",        callback_data="sniper_auto_menu"),
                 InlineKeyboardButton("🧠 Advisory Mode",    callback_data="sniper_adv_menu")],
                [InlineKeyboardButton("⛓️ Chain Selector",   callback_data="sniper_chains_menu"),
                 InlineKeyboardButton("⚙️ Filters",          callback_data="sniper_filters_menu")],
                [InlineKeyboardButton("📋 Sniper Log",       callback_data="sniper_log_view"),
                 InlineKeyboardButton("💰 Daily Budget",     callback_data="sniper_budget_cfg")],
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
        adv_on  = ud.get("sniper_advisory", False)
        notify  = ud.get("sniper_adv_notify", True)
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
                [InlineKeyboardButton(("🔕 Mute Notifications" if notify else "🔔 Unmute Notifications"), callback_data="sniper_adv_notif")],
                [InlineKeyboardButton("◀ Back", callback_data="v_sniper")],
            ])
        )

    elif cb == "sniper_auto_toggle":
        ud["sniper_auto"] = not ud.get("sniper_auto", False)
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

    elif cb == "sniper_adv_toggle":
        ud["sniper_advisory"] = not ud.get("sniper_advisory", False)
        adv_on = ud["sniper_advisory"]
        notify = ud.get("sniper_adv_notify", True)
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
                [InlineKeyboardButton(("🔕 Mute Notifications" if notify else "🔔 Unmute Notifications"), callback_data="sniper_adv_notif")],
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
                [InlineKeyboardButton(("🔕 Mute Notifications" if notify else "🔔 Unmute Notifications"), callback_data="sniper_adv_notif")],
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

    elif cb == "sniper_budget_cfg":
        pending[u.id] = {"action": "sniper_budget"}
        await q.edit_message_text(
            "💰 Enter daily sniper budget in USD:\nExample: 300\n\nSniper stops buying once this is spent in a day.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="v_sniper")]])
        )

    elif cb == "sniper_cfg_score":
        pending[u.id] = {"action": "sniper_score"}
        await q.edit_message_text(
            "📊 Enter minimum AURACLE score (0–100):\nExample: 65",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]])
        )

    elif cb == "sniper_cfg_liq":
        pending[u.id] = {"action": "sniper_liq"}
        await q.edit_message_text(
            "💧 Enter minimum liquidity in USD:\nExample: 75000",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]])
        )

    elif cb == "sniper_cfg_mc":
        pending[u.id] = {"action": "sniper_mc"}
        await q.edit_message_text(
            "📈 Enter MC range:\nFormat: min max\nExample: 100000 5000000",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]])
        )

    elif cb == "sniper_cfg_age":
        pending[u.id] = {"action": "sniper_age"}
        await q.edit_message_text(
            "⏰ Enter max token age in hours:\nExample: 24",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ Back", callback_data="sniper_filters_menu")]])
        )

    elif cb == "sniper_cfg_amt":
        pending[u.id] = {"action": "sniper_amt"}
        await q.edit_message_text(
            "💵 Enter buy amount per snipe in USD:\nExample: 150",
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
        await q.edit_message_text(
            "⚙️ *SNIPER FILTERS*\n\n"
            "Tokens must pass ALL filters before AI analyzes them.\n\n"
            "Min Score: *" + str(sf.get("min_score",55)) + "/100*\n"
            "Min Liq: *" + money(sf.get("min_liq",30_000)) + "*\n"
            "MC Range: *" + mc_str(sf.get("min_mc",50_000)) + "* → *" + mc_str(sf.get("max_mc",10_000_000)) + "*\n"
            "Max Age: *" + str(sf.get("max_age_h",72)) + "h*\n"
            "Buy Amount: *" + money(sf.get("buy_amount",100)) + "*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Min Score",   callback_data="sniper_cfg_score"),
                 InlineKeyboardButton("💧 Min Liq",     callback_data="sniper_cfg_liq")],
                [InlineKeyboardButton("📈 MC Range",    callback_data="sniper_cfg_mc"),
                 InlineKeyboardButton("⏰ Max Age",     callback_data="sniper_cfg_age")],
                [InlineKeyboardButton("💵 Buy Amount",  callback_data="sniper_cfg_amt")],
                [InlineKeyboardButton("◀ Back",         callback_data="v_sniper")],
            ])
        )

    elif cb == "sniper_log_view":
        log = ud.get("sniper_log", [])
        if not log:
            await q.edit_message_text("📋 *SNIPER LOG*\n\nNo activity yet.", parse_mode="Markdown", reply_markup=back_main())
            return
        bought = [e for e in log if e.get("bought")]
        skipped = [e for e in log if not e.get("bought")]
        b_wr = 0
        sniper_trades = [tr for tr in trade_log.get(u.id, []) if tr.get("mood") in ("AI-Sniper","Sniper")]
        if sniper_trades:
            s_wins = [tr for tr in sniper_trades if tr["realized_pnl"] > 0]
            b_wr   = round(len(s_wins) / len(sniper_trades) * 100)
        lines = [
            "📋 *SNIPER LOG*\n",
            "Total analyzed: *" + str(len(log)) + "*",
            "Bought: *" + str(len(bought)) + "*  |  Skipped: *" + str(len(skipped)) + "*",
            "Sniper Win Rate: *" + str(b_wr) + "%*\n",
        ]
        for e in reversed(log[-10:]):
            verdict_e = {"SNIPE":"🟢","SKIP":"🔴","WAIT":"🟡"}.get(e.get("verdict","?"),"⚪")
            bought_e  = "💵" if e.get("bought") else "–"
            lines.append(
                verdict_e + " *$" + e.get("symbol","?") + "*  " + e.get("chain","?").upper()[:3]
                + "  conf:" + str(e.get("confidence","?"))
                + "  rug:" + e.get("rug_risk","?")
                + "  " + bought_e
            )
        await q.edit_message_text(
            "\n".join(lines), parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑 Clear Log", callback_data="sniper_log_clear")],
                [InlineKeyboardButton("◀ Back",      callback_data="v_sniper")],
            ])
        )

    elif cb == "sniper_log_clear":
        ud["sniper_log"] = []
        await q.edit_message_text("✅ Sniper log cleared.", reply_markup=back_main())

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

    elif cb.startswith("snp_skip_"):
        contract = cb[9:]
        await q.edit_message_text("❌ Token skipped.", reply_markup=back_main())

    # ── DCA BY MARKET CAP ──────────────────────────────────────────────────────
    elif cb.startswith("dca_"):
        contract = cb[4:]
        info = await get_token(contract)
        sym  = info["symbol"] if info else "?"
        cur_mc = mc_str(info["mc"]) if info else "unknown"
        existing = [d for d in ud.get("dca_orders", []) if d["contract"] == contract and not d.get("cancelled")]
        ex_txt = ""
        if existing:
            ex_txt = "\n\n*Existing DCA for $" + sym + ":*\n"
            for tgt in existing[0].get("mc_targets", []):
                status = "✅ Done" if tgt.get("triggered") else "⏳ Waiting"
                ex_txt += "  " + status + "  " + mc_str(tgt["mc"]) + " → " + money(tgt["amount"]) + "\n"
        pending[u.id] = {"action": "dca_input", "contract": contract, "symbol": sym}
        await q.edit_message_text(
            "📉 *DCA BY MARKET CAP*\n\n"
            "*$" + sym + "*  |  Current MC: *" + cur_mc + "*\n" + ex_txt + "\n\n"
            "Enter your DCA plan as MC target + buy amount pairs:\n\n"
            "Format: *mc1 amount1 mc2 amount2 ...*\n\n"
            "Example:\n`500000 100 1000000 200 5000000 300`\n\n"
            "This buys $100 at $500K MC, $200 at $1M MC, $300 at $5M MC.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel DCA Orders", callback_data="dca_cancel_" + contract)],
                [InlineKeyboardButton("Back", callback_data="btt_" + contract)],
            ])
        )

    elif cb.startswith("dca_cancel_"):
        contract = cb[11:]
        before = len(ud.get("dca_orders", []))
        ud["dca_orders"] = [d for d in ud.get("dca_orders", []) if d["contract"] != contract]
        sym = contract[:8] + "..."
        for h in ud["holdings"].values():
            if h.get("symbol"):
                sym = h["symbol"]
                break
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


def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_start))
    app.add_handler(CallbackQueryHandler(btn))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.job_queue.run_repeating(checker_job, interval=PRICE_CHECK_INTERVAL, first=10)
    app.job_queue.run_repeating(sniper_job,  interval=300, first=60)
    app.job_queue.run_daily(daily_summary_job, time=__import__("datetime").time(23, 59))
    app.job_queue.run_daily(monthly_report_job, time=__import__("datetime").time(8, 0))
    logger.info("AURACLE_XBOT running...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
