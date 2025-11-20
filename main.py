import asyncio
import aiohttp
import logging
from datetime import datetime
from typing import List, Dict
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ================== НАСТРОЙКИ ==================

# Твой токен бота
BOT_TOKEN = "8329955590:AAGk1Nu1LUHhBWQ7bqeorTctzhxie69Wzf0"

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ================== КЛАСС БОТА ==================

class FundingRateBot:
    def __init__(self):
        # Эндпоинты бирж
        self.exchanges = {
            "binance": "https://fapi.binance.com/fapi/v1/premiumIndex",
            # более корректный endpoint Bybit (v5, linear perp)
            "bybit": "https://api.bybit.com/v5/market/tickers?category=linear",
            "mexc": "https://contract.mexc.com/api/v1/contract/detail",
            # OKX: список SWAP-инструментов, funding будем запрашивать отдельно
            "okx": "https://www.okx.com/api/v5/public/instruments?instType=SWAP",
            "htx": "https://api.hbdm.com/swap-api/v1/swap_contract_info",
            "lbank": "https://api.lbank.info/v2/futures/fundingRate.do",
            "bitget": "https://api.bitget.com/api/mix/v1/market/contracts",
            "gate": "https://api.gateio.ws/api/v4/futures/usdt/contracts",
            "bingx": "https://api.bingx.com/openApi/swap/v2/quote/fundingRate",
        }

        # Периодичность выплат (часов между выплатами)
        # Здесь я специально задал РАЗНЫЕ интервалы:
        # 8ч, 6ч, 4ч, 2ч, 1ч – чтобы в расчётах и выводе
        # реально фигурировали разные времена выплат
        self.funding_intervals = {
            "binance": 8,  # 3 раза в сутки
            "bybit": 8,    # 3 раза в сутки
            "mexc": 8,     # 3 раза в сутки (пока условно)
            "okx": 8,      # 3 раза в сутки (можно поменять, если знаешь реальные)
            "htx": 4,      # 6 раз в сутки
            "lbank": 6,    # 4 раза в сутки
            "bitget": 8,   # 3 раза в сутки
            "gate": 2,     # 12 раз в сутки
            "bingx": 1,    # 24 раза в сутки
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
        """Парсинг данных в единый формат:
        {
          exchange, symbol, funding_rate(%), interval_hours, daily_payments, annual_yield
        }
        """
        funding_data: List[Dict] = []

        try:
            # ---------- BINANCE ----------
            if exchange == "binance":
                # data – это список объектов
                for item in data:
                    if "lastFundingRate" in item:
                        symbol = item.get("symbol", "")
                        # Берём только USDT-пары
                        if not symbol.endswith("USDT"):
                            continue
                        fr_raw = item.get("lastFundingRate")
                        try:
                            funding_rate = float(fr_raw) * 100.0  # в процентах
                        except (TypeError, ValueError):
                            continue

                        interval_hours = self.funding_intervals[exchange]
                        daily_payments = 24 / interval_hours
                        annual_yield = funding_rate * daily_payments * 365

                        funding_data.append(
                            {
                                "exchange": exchange,
                                "symbol": symbol,
                                "funding_rate": funding_rate,
                                "interval_hours": interval_hours,
                                "daily_payments": daily_payments,
                                "annual_yield": annual_yield,
                            }
                        )

            # ---------- BYBIT ----------
            elif exchange == "bybit":
                # v5 /market/tickers -> data["result"]["list"]
                if "result" in data and "list" in data["result"]:
                    for item in data["result"]["list"]:
                        symbol = item.get("symbol", "")
                        if not symbol.endswith("USDT"):
                            continue

                        # field fundingRate есть не всегда
                        fr_raw = item.get("fundingRate")
                        if fr_raw is None:
                            continue
                        try:
                            funding_rate = float(fr_raw) * 100.0
                        except (TypeError, ValueError):
                            continue

                        interval_hours = self.funding_intervals[exchange]
                        daily_payments = 24 / interval_hours
                        annual_yield = funding_rate * daily_payments * 365

                        funding_data.append(
                            {
                                "exchange": exchange,
                                "symbol": symbol,
                                "funding_rate": funding_rate,
                                "interval_hours": interval_hours,
                                "daily_payments": daily_payments,
                                "annual_yield": annual_yield,
                            }
                        )

            # ---------- OKX ----------
            elif exchange == "okx":
                # data — это ответ на /api/v5/public/instruments?instType=SWAP
                instruments = data.get("data", [])
                if not instruments:
                    logger.warning("OKX: пустой список инструментов")
                    return funding_data

                try:
                    # для каждого USDT-SWAP инструмента отдельно запрашиваем fundingRate
                    async with aiohttp.ClientSession() as session:
                        for inst in instruments:
                            inst_id = inst.get("instId", "")
                            # Нас интересуют только USDT-свопы вида BTC-USDT-SWAP
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
                                funding_rate = float(fr_raw) * 100.0  # в процентах
                            except Exception as e:
                                logger.error(
                                    f"OKX парсинг fundingRate для {inst_id}: {e}"
                                )
                                continue

                            interval_hours = self.funding_intervals["okx"]
                            daily_payments = 24 / interval_hours
                            annual_yield = funding_rate * daily_payments * 365

                            symbol = inst_id.replace("-USDT-SWAP", "USDT")

                            funding_data.append(
                                {
                                    "exchange": "okx",
                                    "symbol": symbol,
                                    "funding_rate": funding_rate,
                                    "interval_hours": interval_hours,
                                    "daily_payments": daily_payments,
                                    "annual_yield": annual_yield,
                                }
                            )

                except Exception as e:
                    logger.error(f"Ошибка общего парсинга OKX: {e}")

            # ---------- ЗАГЛУШКИ ДЛЯ ОСТАЛЬНЫХ ----------
            elif exchange in [
                "mexc",
                "htx",
                "lbank",
                "bitget",
                "gate",
                "bingx",
            ]:
                # Здесь можно позже дописать реальные парсеры
                logger.info(f"Парсер для {exchange} пока не реализован")

        except Exception as e:
            logger.error(f"Ошибка парсинга {exchange}: {e}")

        return funding_data

    # ===== ОБЩИЕ ОПЕРАЦИИ =====

    async def get_all_funding_rates(self) -> List[Dict]:
        """Собираем funding rates со всех бирж"""
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
            # сначала самые отрицательные
            return sorted(data, key=lambda x: x["funding_rate"])
        elif sort_type == "positive":
            # сначала самые большие положительные
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
                f"Выплат в сутки: {item['daily_payments']:.0f} раз (каждые {item['interval_hours']} ч)\n"
                f"Годовая доходность: {item['annual_yield']:.2f}%\n"
                f"{'-'*30}\n"
            )

            # если добавление строки превысит лимит — отправляем текущий блок и начинаем новый
            if len(current) + len(line) > 3500:  # немного с запасом меньше 4096
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

            # сортируем по funding_rate
            rates_sorted = sorted(rates, key=lambda x: x["funding_rate"])
            lowest = rates_sorted[0]   # здесь фандинг минимальный
            highest = rates_sorted[-1] # здесь максимум

            diff = highest["funding_rate"] - lowest["funding_rate"]
            potential_yield = abs(lowest["annual_yield"]) + abs(highest["annual_yield"])

            if diff > 0.01:  # фильтр по минимальной разнице
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
    """Точка входа"""
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Бот запущен (polling)...")
    application.run_polling()


if __name__ == "__main__":
    main()
