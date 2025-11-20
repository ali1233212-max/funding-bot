import requests
from telegram.ext import ApplicationBuilder, CommandHandler

# ---- Команда /start ----
async def start(update, context):
    await update.message.reply_text("Бот запущен! Команда: /funding")


# ---- Функции получения funding ----
def get_binance():
    try:
        url = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"
        data = requests.get(url).json()
        return float(data["lastFundingRate"]) * 100
    except:
        return None

def get_bybit():
    try:
        url = "https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT"
        data = requests.get(url).json()
        return float(data["result"]["list"][0]["fundingRate"]) * 100
    except:
        return None

def get_okx():
    try:
        url = "https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP"
        data = requests.get(url).json()
        return float(data["data"][0]["fundingRate"]) * 100
    except:
        return None

def get_deribit():
    try:
        url = "https://www.deribit.com/api/v2/public/get_funding_rate_value?instrument_name=BTC-PERPETUAL"
        data = requests.get(url).json()
        return float(data["result"]["funding_rate"]) * 100
    except:
        return None

def get_bitmex():
    try:
        url = "https://www.bitmex.com/api/v1/instrument?symbol=XBTUSDT&columns=fundingRate"
        data = requests.get(url).json()
        return float(data[0]["fundingRate"]) * 100
    except:
        return None


# ---- Команда /funding ----
async def funding(update, context):
    binance = get_binance()
    bybit = get_bybit()
    okx = get_okx()
    deribit = get_deribit()
    bitmex = get_bitmex()

    msg = "📊 *Funding Rate BTCUSDT*\n\n"

    msg += f"🟡 Binance:   {binance:.4f}%\n" if binance is not None else "🟡 Binance:   ❌ ошибка\n"
    msg += f"🟣 Bybit:     {bybit:.4f}%\n" if bybit is not None else "🟣 Bybit:     ❌ ошибка\n"
    msg += f"🔵 OKX:       {okx:.4f}%\n" if okx is not None else "🔵 OKX:       ❌ ошибка\n"
    msg += f"🟠 Deribit:   {deribit:.4f}%\n" if deribit is not None else "🟠 Deribit:   ❌ ошибка\n"
    msg += f"⚫ BitMEX:    {bitmex:.4f}%\n" if bitmex is not None else "⚫ BitMEX:    ❌ ошибка\n"

    await update.message.reply_text(msg, parse_mode="Markdown")


# ---- Запуск бота ----
BOT_TOKEN = "8329955590:AAH63Ax6WmTjESyLVvqEPTE5ibutOiK_rCM"

app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("funding", funding))

app.run_polling()
