import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, CallbackContext

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
                res.append((symbol, float(i["lastFundingRate"]) * 100, "Binance"))
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
                res.append((symbol, float(item["fundingRate"]) * 100, "Bybit"))
        return res
    except:
        return []


def get_okx():
    try:
        url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
        pairs = requests.get(url).json()["data"]

        res = []
        for p in pairs:
            inst = p["instId"]  # example: BTC-USDT-SWAP
            if inst.endswith("-USDT-SWAP"):
                fr_url = f"https://www.okx.com/api/v5/public/funding-rate?instId={inst}"
                d = requests.get(fr_url).json()
                try:
                    fr = float(d["data"][0]["fundingRate"]) * 100
                    symbol = inst.replace("-USDT-SWAP", "USDT")
                    res.append((symbol, fr, "OKX"))
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
            res.append((symbol, fr, "Deribit"))
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
                res.append((s, float(item["fundingRate"]) * 100, "BitMEX"))
        return res
    except:
        return []


# ======================================================================
#                               КОМАНДЫ
# ======================================================================

# Команда "/start"
async def start(update: Update, context: CallbackContext):
    msg = "Добро пожаловать в бот для отслеживания фандинга на криптобиржах! Выберите нужную команду:"
    keyboard = [
        [InlineKeyboardButton("Топ 5 фандингов (Положительные)", callback_data="top_positive_funding")],
        [InlineKeyboardButton("Топ 5 фандингов (Отрицательные)", callback_data="top_negative_funding")],
        [InlineKeyboardButton("Все пары с фандингом", callback_data="funding_all")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(msg, reply_markup=reply_markup)


# Команда для получения фандинга для всех торговых пар
async def funding_all(update: Update, context: CallbackContext):
    await update.message.reply_text("⏳ Собираю данные со всех бирж...")

    all_pairs = []
    all_pairs.extend(get_binance())
    all_pairs.extend(get_bybit())
    all_pairs.extend(get_okx())
    all_pairs.extend(get_deribit())
    all_pairs.extend(get_bitmex())

    # Сортировка по funding от максимального к минимальному
    all_pairs.sort(key=lambda x: x[1], reverse=True)

    msg = "📊 *Funding всех USDT-пар (ТОП-50)*\n\n"

    for symbol, fr, exch in all_pairs[:50]:
        msg += f"{symbol}: {fr:.4f}% ({exch})\n"

    await update.message.reply_text(msg, parse_mode="Markdown")


# Команда для получения Топ-5 фандингов с положительным фандингом
async def top_positive_funding(update: Update, context: CallbackContext):
    await update.message.reply_text("⏳ Собираю данные по положительному фандингу...")

    all_pairs = []
    all_pairs.extend(get_binance())
    all_pairs.extend(get_bybit())
    all_pairs.extend(get_okx())
    all_pairs.extend(get_deribit())
    all_pairs.extend(get_bitmex())

    # Сортировка по фандингу от максимального к минимальному
    positive_pairs = [pair for pair in all_pairs if pair[1] > 0]
    positive_pairs.sort(key=lambda x: x[1], reverse=True)

    msg = "📊 *Топ-5 положительных фандингов*:\n\n"

    for symbol, fr, exch in positive_pairs[:5]:
        msg += f"{symbol}: {fr:.4f}% ({exch})\n"

    await update.message.reply_text(msg, parse_mode="Markdown")


# Команда для получения Топ-5 фандингов с отрицательным фандингом
async def top_negative_funding(update: Update, context: CallbackContext):
    await update.message.reply_text("⏳ Собираю данные по отрицательному фандингу...")

    all_pairs = []
    all_pairs.extend(get_binance())
    all_pairs.extend(get_bybit())
    all_pairs.extend(get_okx())
    all_pairs.extend(get_deribit())
    all_pairs.extend(get_bitmex())

    # Сортировка по фандингу от минимального к максимальному
    negative_pairs = [pair for pair in all_pairs if pair[1] < 0]
    negative_pairs.sort(key=lambda x: x[1])

    msg = "📊 *Топ-5 отрицательных фандингов*:\n\n"

    for symbol, fr, exch in negative_pairs[:5]:
        msg += f"{symbol}: {fr:.4f}% ({exch})\n"

    await update.message.reply_text(msg, parse_mode="Markdown")


# ======================================================================
#                               ЗАПУСК БОТА С WEBHOOK
# ======================================================================

BOT_TOKEN = "8329955590:AAGk1Nu1LUHhBWQ7bqeorTctzhxie69Wzf0"  # Замените на свой токен

# Создание приложения
app = ApplicationBuilder().token(BOT_TOKEN).build()

# Добавление обработчиков команд
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("funding_all", funding_all))
app.add_handler(CommandHandler("top_positive_funding", top_positive_funding))
app.add_handler(CommandHandler("top_negative_funding", top_negative_funding))

# Настройка webhook для Render (порт 8443)
app.run_webhook(listen="0.0.0.0", port=8443, url_path="webhook")
