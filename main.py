import requests
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup


# ======================================================================
#                               КОМАНДА /start
# ======================================================================

async def start(update, context):
    keyboard = [
        [InlineKeyboardButton("📊 Все USDT пары", callback_data="all_pairs"),
         InlineKeyboardButton("🟥 Отрицательные ставки", callback_data="negative"),
         InlineKeyboardButton("🟩 Положительные ставки", callback_data="positive")],
        [InlineKeyboardButton("👑 ТОП-5 фандингов", callback_data="top_5")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Бот запущен! Выберите действие:",
        reply_markup=reply_markup
    )


# ======================================================================
#                          ОДИНОЧНЫЕ ФУНКЦИИ FUNDING
# ======================================================================

def get_binance():
    try:
        url = "https://fapi.binance.com/fapi/v1/premiumIndex"
        data = requests.get(url).json()
        res = []
        for i in data:
            symbol = i["symbol"]
            if symbol.endswith("USDT"):
                res.append((symbol, float(i["lastFundingRate"]) * 100, "Binance", "8h"))
        return res
    except:
        return []


def get_bybit():
    try:
        url = "https://api.bybit.com/v5/market/tickers?category=linear"
        data = requests.get(url).json()
        res = []
        for item in data["result"]["list"]:
            symbol = item["symbol"]
            if symbol.endswith("USDT"):
                res.append((symbol, float(item["fundingRate"]) * 100, "Bybit", "8h"))
        return res
    except:
        return []


def get_okx():
    try:
        url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
        pairs = requests.get(url).json()["data"]
        res = []
        for p in pairs:
            inst = p["instId"]
            if inst.endswith("-USDT-SWAP"):
                fr_url = f"https://www.okx.com/api/v5/public/funding-rate?instId={inst}"
                d = requests.get(fr_url).json()
                try:
                    fr = float(d["data"][0]["fundingRate"]) * 100
                    symbol = inst.replace("-USDT-SWAP", "USDT")
                    res.append((symbol, fr, "OKX", "8h"))
                except:
                    pass
        return res
    except:
        return []


def get_deribit():
    res = []
    for inst in ["BTC-PERPETUAL", "ETH-PERPETUAL"]:
        try:
            url = f"https://www.deribit.com/api/v2/public/get_funding_rate_value?instrument_name={inst}"
            d = requests.get(url).json()
            fr = float(d["result"]["funding_rate"]) * 100
            symbol = inst.replace("-PERPETUAL", "USDT")
            res.append((symbol, fr, "Deribit", "1h"))
        except:
            pass
    return res


def get_bitmex():
    try:
        url = "https://www.bitmex.com/api/v1/instrument?symbol=&columns=symbol,fundingRate"
        data = requests.get(url).json()
        res = []
        for item in data:
            s = item["symbol"]
            if "USDT" in s:
                res.append((s, float(item["fundingRate"]) * 100, "BitMEX", "8h"))
        return res
    except:
        return []


# ======================================================================
#                               КОМАНДА /fundingall
# ======================================================================

async def funding_all(update, context):
    await update.message.reply_text("⏳ Собираю данные со всех бирж...")

    all_pairs = []
    all_pairs.extend(get_binance())
    all_pairs.extend(get_bybit())
    all_pairs.extend(get_okx())
    all_pairs.extend(get_deribit())
    all_pairs.extend(get_bitmex())

    all_pairs.sort(key=lambda x: x[1], reverse=True)

    msg = "📊 *Funding всех USDT-пар (ТОП-50)*\n\n"

    for symbol, fr, exch, period in all_pairs[:50]:
        msg += f"{symbol}: {fr:.4f}% ({exch}) ⏱ {period}\n"

    await update.message.reply_text(msg, parse_mode="Markdown")


# ======================================================================
#                            КНОПКИ MENU
# ======================================================================

async def button(update, context):
    query = update.callback_query
    await query.answer()

    if query.data == "all_pairs":
        await funding_all(update, context)

    elif query.data == "negative":
        # Отрицательные ставки
        all_pairs = []
        all_pairs.extend(get_binance())
        all_pairs.extend(get_bybit())
        all_pairs.extend(get_okx())
        all_pairs.extend(get_deribit())
        all_pairs.extend(get_bitmex())

        # Фильтруем только отрицательные
        negative_pairs = [pair for pair in all_pairs if pair[1] < 0]
        negative_pairs.sort(key=lambda x: x[1])

        msg = "📊 *Отрицательные ставки*\n\n"
        for symbol, fr, exch, period in negative_pairs[:20]:
            msg += f"{symbol}: {fr:.4f}% ({exch}) ⏱ {period}\n"

        await update.message.reply_text(msg, parse_mode="Markdown")

    elif query.data == "positive":
        # Положительные ставки
        all_pairs = []
        all_pairs.extend(get_binance())
        all_pairs.extend(get_bybit())
        all_pairs.extend(get_okx())
        all_pairs.extend(get_deribit())
        all_pairs.extend(get_bitmex())

        # Фильтруем только положительные
        positive_pairs = [pair for pair in all_pairs if pair[1] > 0]
        positive_pairs.sort(key=lambda x: x[1], reverse=True)

        msg = "📊 *Положительные ставки*\n\n"
        for symbol, fr, exch, period in positive_pairs[:20]:
            msg += f"{symbol}: {fr:.4f}% ({exch}) ⏱ {period}\n"

        await update.message.reply_text(msg, parse_mode="Markdown")

    elif query.data == "top_5":
        # ТОП 5 положительных и ТОП 5 отрицательных
        all_pairs = []
        all_pairs.extend(get_binance())
        all_pairs.extend(get_bybit())
        all_pairs.extend(get_okx())
        all_pairs.extend(get_deribit())
        all_pairs.extend(get_bitmex())

        negative_pairs = [pair for pair in all_pairs if pair[1] < 0]
        negative_pairs.sort(key=lambda x: x[1])

        positive_pairs = [pair for pair in all_pairs if pair[1] > 0]
        positive_pairs.sort(key=lambda x: x[1], reverse=True)

        msg = "📊 *ТОП 5 Фандингов*\n\n"

        msg += "🟥 *ТОП-5 Отрицательных ставок*\n"
        for symbol, fr, exch, period in negative_pairs[:5]:
            msg += f"{symbol}: {fr:.4f}% ({exch}) ⏱ {period}\n"

        msg += "\n🟩 *ТОП-5 Положительных ставок*\n"
        for symbol, fr, exch, period in positive_pairs[:5]:
            msg += f"{symbol}: {fr:.4f}% ({exch}) ⏱ {period}\n"

        await update.message.reply_text(msg, parse_mode="Markdown")


# ======================================================================
#                               ЗАПУСК БОТА
# ======================================================================

BOT_TOKEN = "8329955590:AAGk1Nu1LUHhBWQ7bqeorTctzhxie69Wzf0"

app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CallbackQueryHandler(button))

app.run_polling()
