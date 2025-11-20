import requests
from telegram.ext import ApplicationBuilder, CommandHandler


# ======================================================================
#                               КОМАНДА /start
# ======================================================================

async def start(update, context):
    await update.message.reply_text(
        "Бот запущен! Доступные команды:\n"
        "/funding — funding BTCUSDT по биржам\n"
        "/fundingall — funding всех USDT-пар\n"
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
            inst = p["instId"]
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
#                               КОМАНДА /funding
# ======================================================================

async def funding(update, context):
    bb = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT").json()
    binance_fr = float(bb["lastFundingRate"]) * 100

    byb = requests.get("https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT").json()
    bybit_fr = float(byb["result"]["list"][0]["fundingRate"]) * 100

    okx = requests.get("https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP").json()
    okx_fr = float(okx["data"][0]["fundingRate"]) * 100

    der = requests.get("https://www.deribit.com/api/v2/public/get_funding_rate_value?instrument_name=BTC-PERPETUAL").json()
    der_fr = float(der["result"]["funding_rate"]) * 100

    bitm = requests.get("https://www.bitmex.com/api/v1/instrument?symbol=XBTUSDT&columns=fundingRate").json()
    bitmex_fr = float(bitm[0]["fundingRate"]) * 100

    msg = f"""
📊 *Funding Rate BTCUSDT*

🟡 Binance:   {binance_fr:.4f}%
🟣 Bybit:     {bybit_fr:.4f}%
🔵 OKX:       {okx_fr:.4f}%
🟠 Deribit:   {der_fr:.4f}%
⚫ BitMEX:    {bitmex_fr:.4f}%
"""
    await update.message.reply_text(msg, parse_mode="Markdown")


# ======================================================================
#                        КОМАНДА /fundingall — ВСЕ ПАРЫ
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

    for symbol, fr, exch in all_pairs[:50]:
        msg += f"{symbol}: {fr:.4f}% ({exch})\n"

    await update.message.reply_text(msg, parse_mode="Markdown")


# ======================================================================
#                               ЗАПУСК БОТА
# ======================================================================

BOT_TOKEN = "8329955590:AAGk1Nu1LUHhBWQ7bqeorTctzhxie69Wzf0"

app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("funding", funding))
app.add_handler(CommandHandler("fundingall", funding_all))

app.run_polling()
