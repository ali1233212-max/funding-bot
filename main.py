import asyncio
import aiohttp
import logging
from datetime import datetime
from typing import List, Dict, Optional
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ================== НАСТРОЙКИ ==================

BOT_TOKEN = "8329955590:AAGk1Nu1LUHhBWQ7bqeorTctzhxie69Wzf0"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ================== КЛАСС БОТА ==================

class FundingRateBot:
    def __init__(self):
        # Эндпоинты бирж
        self.exchanges = {
            "binance": "https://fapi.binance.com/fapi/v1/premiumIndex",
            "bybit": "https://api.bybit.com/v5/market/tickers?category=linear",
            "mexc": "https://contract.mexc.com/api/v1/contract/detail",
            # OKX: список SWAP-инструментов, funding будем запрашивать отдельно
            "okx": "https://www.okx.com/api/v5/public/instruments?instType=SWAP",
            "htx": "https://api.hbdm.com/swap-api/v1/swap_contract_info",
            "lbank": "https://api.lbank.info/v2/futures/fundingRate.do",
            # Bitget: текущий фандинг по всем USDT-FUTURES
            "bitget": "https://api.bitget.com/api/v2/mix/market/current-fund-rate?productType=USDT-FUTURES",
            "gate": "https://api.gateio.ws/api/v4/futures/usdt/contracts",
            "bingx": "https://api.bingx.com/openApi/swap/v2/quote/fundingRate",
        }

        # Дефолтные интервалы (если биржа не отдаёт свои), в часах
        self.default_interval_hours = {
            "binance": 8.0,
            "bybit": 8.0,
            "mexc": 8.0,
            "okx": 8.0,
            "htx": 4.0,
            "lbank": 6.0,
            "bitget": 8.0,
            "gate": 2.0,
            "bingx": 1.0,
        }

        # Кэш интервалов по символам: { "binance": {"BTCUSDT": 4.0, ...}, "bybit": {...}, ... }
        self.symbol_intervals: Dict[str, Dict[str, float]] = {
            "binance": {},
            "bybit": {},
            "okx": {},
            "bitget": {},
        }

    # ===== ПРЕДЗАГРУЗКА РЕАЛЬНЫХ ИНТЕРВАЛОВ =====

    async def preload_intervals(self):
        """
        Подгрузить реальные интервалы выплат для Binance и Bybit по всем символам.
        Вызывается один раз перед основными запросами фандинга.
        """
        async with aiohttp.ClientSession() as session:
            # Binance: /fapi/v1/fundingInfo — символы с изменёнными интервалами
            try:
                url_binance = "https://fapi.binance.com/fapi/v1/fundingInfo"
                async with session.get(url_binance, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # формат: [ { "symbol": "...", "fundingIntervalHours": 4, ... }, ... ]
                        if isinstance(data, list):
                            for item in data:
                                sym = item.get("symbol")
                                iv = item.get("fundingIntervalHours")
                                try:
                                    iv = float(iv)
                                except (TypeError, ValueError):
                                    continue
                                if sym and iv and iv > 0:
                                    self.symbol_intervals["binance"][sym] = iv
                    else:
                        logger.warning(f"Binance fundingInfo HTTP {resp.status}")
            except Exception as e:
                logger.error(f"Binance preload_intervals error: {e}")

            # Bybit: /v5/market/instruments-info?category=linear
            try:
                url_bybit = "https://api.bybit.com/v5/market/instruments-info?category=linear"
                async with session.get(url_bybit, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # {"result": {"list": [ { "symbol": "...", "fundingInterval": "480", ... }, ...]}}
                        if "result" in data and "list" in data["result"]:
                            for item in data["result"]["list"]:
                                sym = item.get("symbol")
                                iv_min = item.get("fundingInterval")
                                try:
                                    iv_min = float(iv_min)
                                except (TypeError, ValueError):
                                    continue
                                if sym and iv_min and iv_min > 0:
                                    hours = iv_min / 60.0
                                    self.symbol_intervals["bybit"][sym] = hours
                    else:
                        logger.warning(f"Bybit instruments-info HTTP {resp.status}")
            except Exception as e:
                logger.error(f"Bybit preload_intervals error: {e}")

    # ===== ВСПОМОГАТЕЛЬНАЯ ЛОГИКА ДЛЯ ИНТЕРВАЛОВ =====

    def get_interval_hours(
        self,
        exchange: str,
        raw: Optional[Dict] = None,
    ) -> float:
        """
        1) Если есть индивидуальный интервал для конкретного символа – берём его.
        2) Если биржа отдаёт интервал в raw (например Bitget) – читаем оттуда.
        3) Иначе – дефолт из self.default_interval_hours.
        """
        symbol = None
        if raw is not None:
            symbol = raw.get("symbol") or raw.get("instId") or raw.get("contract")

        # --- per-symbol кэш для Binance/Bybit/OKX/Bitget ---
        if symbol:
            ex_cache = self.symbol_intervals.get(exchange)
            if ex_cache:
                iv = ex_cache.get(symbol)
                if iv is not None and iv > 0:
                    return float(iv)

        # --- Bitget: fundingRateInterval в часах ---
        if exchange == "bitget" and raw is not None:
            fri = raw.get("fundingRateInterval")
            if fri is not None:
                try:
                    interval = float(fri)
                    if interval > 0:
                        return interval
                except (TypeError, ValueError):
                    pass

        # TODO: сюда можно добавить логику для других бирж,
        # если они отдают интервал внутри raw.

        interval = self.default_interval_hours.get(exchange, 8.0)
        if interval <= 0:
            interval = 8.0
        return interval

    def enrich_with_yield(
        self,
        exchange: str,
        symbol: str,
        funding_rate_percent: float,
        interval_hours: float,
    ) -> Dict:
        """
        На вход: биржа, символ, фандинг за ОДНУ выплату (%), интервал в часах.
        На выход: словарь, который дальше использует бот.
        """
        payments_per_day = 24.0 / interval_hours
        annual_yield = funding_rate_percent * payments_per_day * 365.0

        return {
            "exchange": exchange,
            "symbol": symbol,
            "funding_rate": funding_rate_percent,
            "interval_hours": interval_hours,
            "daily_payments": payments_per_day,
            "annual_yield": annual_yield,
        }

    # ===== HTTP =====

    async def fetch_exchange_data(
        self,
        session: aiohttp.ClientSession,
        exchange: str,
        url: str,
    ) -> List[Dict]:
        """Получение сырых данных с биржи и парсинг в единый формат"""
        try:
            async with session.get(url, timeout=15) as response:
                if response.status == 200:
                    data = await response.json()
                    return await self.parse_exchange_data(exchange, data)
                else:
                    logger.warning(f"Ошибка {response.status} для {exchange}")
                    return []
        except Exception as e:
            logger.error(f"Ошибка получения данных с {exchange}: {e}")
            return []

    async def parse_exchange_data(self, exchange: str, data: dict) -> List[Dict]:
        """Парсинг данных в единый формат"""
        funding_data: List[Dict] = []

        try:
            # ---------- BINANCE ----------
            if exchange == "binance":
                for item in data:
                    if "lastFundingRate" in item:
                        symbol = item.get("symbol", "")
                        if not symbol.endswith("USDT"):
                            continue

                        fr_raw = item.get("lastFundingRate")
                        try:
                            funding_rate = float(fr_raw) * 100.0
                        except (TypeError, ValueError):
                            continue

                        interval_hours = self.get_interval_hours(exchange, item)
                        funding_data.append(
                            self.enrich_with_yield(
                                exchange, symbol, funding_rate, interval_hours
                            )
                        )

            # ---------- BYBIT ----------
            elif exchange == "bybit":
                if "result" in data and "list" in data["result"]:
                    for item in data["result"]["list"]:
                        symbol = item.get("symbol", "")
                        if not symbol.endswith("USDT"):
                            continue

                        fr_raw = item.get("fundingRate")
                        if fr_raw is None:
                            continue
                        try:
                            funding_rate = float(fr_raw) * 100.0
                        except (TypeError, ValueError):
                            continue

                        interval_hours = self.get_interval_hours(exchange, item)
                        funding_data.append(
                            self.enrich_with_yield(
                                exchange, symbol, funding_rate, interval_hours
                            )
                        )

            # ---------- OKX ----------
            elif exchange == "okx":
                instruments = data.get("data", [])
                if not instruments:
                    logger.warning("OKX: пустой список инструментов")
                    return funding_data

                try:
                    async with aiohttp.ClientSession() as session:
                        for inst in instruments:
                            inst_id = inst.get("instId", "")
                            if not inst_id.endswith("-USDT-SWAP"):
                                continue

                            fr_url = (
                                f"https://www.okx.com/api/v5/public/funding-rate?instId={inst_id}"
                            )
                            try:
                                async with session.get(fr_url, timeout=10) as resp:
                                    if resp.status != 200:
                                        logger.warning(
                                            f"OKX funding-rate {inst_id}: HTTP {resp.status}"
                                        )
                                        continue
                                    fr_json = await resp.json()
                            except Exception as e:
                                logger.error(
                                    f"OKX запрос funding-rate {inst_id} упал: {e}"
                                )
                                continue

                            try:
                                fr_list = fr_json.get("data", [])
                                if not fr_list:
                                    continue
                                fr_raw = fr_list[0].get("fundingRate")
                                if fr_raw is None:
                                    continue
                                funding_rate = float(fr_raw) * 100.0
                            except Exception as e:
                                logger.error(
                                    f"OKX парсинг fundingRate для {inst_id}: {e}"
                                )
                                continue

                            interval_hours = self.get_interval_hours(exchange, inst)
                            symbol = inst_id.replace("-USDT-SWAP", "USDT")

                            funding_data.append(
                                self.enrich_with_yield(
                                    "okx", symbol, funding_rate, interval_hours
                                )
                            )

                except Exception as e:
                    logger.error(f"Ошибка общего парсинга OKX: {e}")

            # ---------- BITGET ----------
            elif exchange == "bitget":
                # data — ответ на /api/v2/mix/market/current-fund-rate?productType=USDT-FUTURES
                if data.get("code") != "00000":
                    logger.warning(f"Bitget: code != 00000: {data.get('code')}")
                    return funding_data

                items = data.get("data", [])
                if not isinstance(items, list):
                    logger.warning("Bitget: неожиданный формат data")
                    return funding_data

                for item in items:
                    symbol = item.get("symbol", "")
                    if not symbol.endswith("USDT"):
                        continue

                    fr_raw = item.get("fundingRate")
                    if fr_raw is None:
                        continue

                    try:
                        funding_rate = float(fr_raw) * 100.0  # доля → %
                    except (TypeError, ValueError):
                        continue

                    interval_hours = self.get_interval_hours(exchange, item)
                    funding_data.append(
                        self.enrich_with_yield(
                            "bitget", symbol, funding_rate, interval_hours
                        )
                    )

            # ---------- ЗАГЛУШКИ ДЛЯ ОСТАЛЬНЫХ ----------
            elif exchange in [
                "mexc",
                "htx",
                "lbank",
                "gate",
                "bingx",
            ]:
                logger.info(f"Парсер для {exchange} пока не реализован")

        except Exception as e:
            logger.error(f"Ошибка парсинга {exchange}: {e}")

        return funding_data

    # ===== ОБЩИЕ ОПЕРАЦИИ =====

    async def get_all_funding_rates(self) -> List[Dict]:
        """Собираем funding rates со всех бирж"""

        # 1. сначала подтянем реальные интервалы выплат где возможно
        try:
            await self.preload_intervals()
        except Exception as e:
            logger.error(f"preload_intervals error: {e}")

        all_data: List[Dict] = []

        async with aiohttp.ClientSession() as session:
            tasks = [
                self.fetch_exchange_data(session, exch, url)
                for exch, url in self.exchanges.items()
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, list):
                    all_data.extend(res)

        return all_data

    def sort_funding_rates(self, data: List[Dict], sort_type: str = "negative") -> List[Dict]:
        """Сортировка funding rates"""
        if sort_type == "negative":
            return sorted(data, key=lambda x: x["funding_rate"])
        elif sort_type == "positive":
            return sorted(data, key=lambda x: x["funding_rate"], reverse=True)
        return data

    def format_funding_message(self, data: List[Dict], limit: int | None = None) -> List[str]:
        """Формируем одно или несколько сообщений (с учётом лимита 4096 символов Telegram)"""
        if not data:
            return ["Данные не найдены"]

        if limit is not None:
            data = data[:limit]

        chunks: List[str] = []
        current = ""

        for item in data:
            funding_sign = "+" if item["funding_rate"] > 0 else ""
            line = (
                f"{item['exchange'].upper()} {item['symbol']}\n"
                f"Фандинг: {funding_sign}{item['funding_rate']:.4f}%\n"
                f"Выплат в сутки: {item['daily_payments']:.0f} раз "
                f"(каждые {item['interval_hours']} ч)\n"
                f"Годовая доходность: {item['annual_yield']:.2f}%\n"
                f"{'-'*30}\n"
            )

            if len(current) + len(line) > 3500:
                chunks.append(current)
                current = line
            else:
                current += line

        if current:
            chunks.append(current)

        return chunks

    async def get_arbitrage_opportunities(self, data: List[Dict]) -> List[str]:
        """Поиск арбитражных возможностей между биржами по одной и той же паре"""
        symbol_groups: Dict[str, List[Dict]] = {}
        for item in data:
            symbol = item["symbol"]
            symbol_groups.setdefault(symbol, []).append(item)

        opportunities = []

        for symbol, rates in symbol_groups.items():
            if len(rates) < 2:
                continue

            rates_sorted = sorted(rates, key=lambda x: x["funding_rate"])
            lowest = rates_sorted[0]
            highest = rates_sorted[-1]

            diff = highest["funding_rate"] - lowest["funding_rate"]
            potential_yield = abs(lowest["annual_yield"]) + abs(highest["annual_yield"])

            if diff > 0.01:
                opportunities.append(
                    {
                        "symbol": symbol,
                        "long_exchange": lowest["exchange"],
                        "short_exchange": highest["exchange"],
                        "funding_diff": diff,
                        "potential_yield": potential_yield,
                    }
                )

        opportunities.sort(key=lambda x: x["potential_yield"], reverse=True)

        if not opportunities:
            return ["Арбитражные возможности не найдены"]

        msg = "🔀 Арбитражные возможности (топ 10):\n\n"
        chunks = []
        current = msg

        for opp in opportunities[:10]:
            line = (
                f"Пара: {opp['symbol']}\n"
                f"🔺 ЛОНГ на {opp['long_exchange'].upper()}\n"
                f"🔻 ШОРТ на {opp['short_exchange'].upper()}\n"
                f"Разница фандинга: {opp['funding_diff']:.4f}%\n"
                f"Потенциальная доходность: {opp['potential_yield']:.2f}%\n"
                f"{'-'*30}\n"
            )
            if len(current) + len(line) > 3500:
                chunks.append(current)
                current = line
            else:
                current += line

        if current:
            chunks.append(current)

        return chunks


# ================== ЭКЗЕМПЛЯР БОТА ==================

bot = FundingRateBot()


# ================== TELEGRAM-HANDLERS ==================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start с кнопками"""
    keyboard = [
        ["📊 Все фандинги (отрицательные)", "📈 Все фандинги (положительные)"],
        ["🏆 Топ 5 лучших фандингов", "🔀 Связки арбитража"],
        ["🔄 Обновить данные"],
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    await update.message.reply_text(
        "🤖 Бот мониторинга Funding Rates\n\n"
        "Выберите действие:",
        reply_markup=reply_markup,
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений (кнопки)"""
    message_text = update.message.text.strip()

    try:
        if message_text == "📊 Все фандинги (отрицательные)":
            await update.message.reply_text("⏳ Загружаю данные...")
            data = await bot.get_all_funding_rates()
            sorted_data = bot.sort_funding_rates(data, "negative")
            chunks = bot.format_funding_message(sorted_data, limit=50)
            for chunk in chunks:
                await update.message.reply_text(chunk)

        elif message_text == "📈 Все фандинги (положительные)":
            await update.message.reply_text("⏳ Загружаю данные...")
            data = await bot.get_all_funding_rates()
            sorted_data = bot.sort_funding_rates(data, "positive")
            chunks = bot.format_funding_message(sorted_data, limit=50)
            for chunk in chunks:
                await update.message.reply_text(chunk)

        elif message_text == "🏆 Топ 5 лучших фандингов":
            await update.message.reply_text("⏳ Загружаю данные...")
            data = await bot.get_all_funding_rates()

            negative_data = [d for d in data if d["funding_rate"] < 0]
            top_negative = bot.sort_funding_rates(negative_data, "negative")[:5]

            positive_data = [d for d in data if d["funding_rate"] > 0]
            top_positive = bot.sort_funding_rates(positive_data, "positive")[:5]

            msg_neg_chunks = bot.format_funding_message(top_negative)
            msg_pos_chunks = bot.format_funding_message(top_positive)

            await update.message.reply_text("🔻 Топ 5 отрицательных фандингов:\n")
            for chunk in msg_neg_chunks:
                await update.message.reply_text(chunk)

            await update.message.reply_text("🔺 Топ 5 положительных фандингов:\n")
            for chunk in msg_pos_chunks:
                await update.message.reply_text(chunk)

        elif message_text == "🔀 Связки арбитража":
            await update.message.reply_text("⏳ Ищу арбитражные возможности...")
            data = await bot.get_all_funding_rates()
            chunks = await bot.get_arbitrage_opportunities(data)
            for chunk in chunks:
                await update.message.reply_text(chunk)

        elif message_text == "🔄 Обновить данные":
            await update.message.reply_text("✅ Данные всегда обновляются при каждом запросе!")

    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text("❌ Произошла ошибка при получении данных")


def main():
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Бот запущен (polling)...")
    application.run_polling()


if __name__ == "__main__":
    main()
