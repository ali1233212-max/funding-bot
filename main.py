import asyncio
import aiohttp
import logging
from datetime import datetime
from typing import List, Dict, Optional
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    CallbackQueryHandler,
)

# ====================== НАСТРОЙКИ ===========================
BOT_TOKEN = "8329955590:AAGk1Nu1LUHhBWQ7bqeorTctzhxie69Wzf0"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====================== КЛАСС БОТА ===========================
class FundingRateBot:
    def __init__(self):
        # Эндпойнты бирж
        self.exchanges = {
            "binance": "https://api.binance.com/fapi/v1/premiumIndex",
            "bybit": "https://api.bybit.com/v5/market/tickers?category=linear",
            "mexc": "https://contract.mexc.com/api/v1/contract/detail",
            "okx": "https://www.okx.com/api/v5/public/instruments?instType=SWAP",
            "htx": "https://api.hbdm.com/swap-api/v1/swap_batch_funding_rate",
            "lbank": "https://api.lbank.info/v2/futures/fundingRate.do",
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

        # Кэш интервалов по символам
        self.symbol_intervals: Dict[str, Dict[str, float]] = {
            "binance": {},
            "bybit": {},
            "okx": {},
            "bitget": {}
        }

    # ====================== ПРЕДЗАГРУЗКА РЕАЛЬНЫХ ИНТЕРВАЛОВ ======================
    async def preload_intervals(self):
        """Подгрузить реальные интервалы выплат для Binance и Bybit по всем символам"""
        async with aiohttp.ClientSession() as session:
            # Binance
            try:
                url_binance = "https://fapi.binance.com/fapi/v1/fundingInfo"
                async with session.get(url_binance, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
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

            # Bybit
            try:
                url_bybit = "https://api.bybit.com/v5/market/instruments-info?category=linear"
                async with session.get(url_bybit, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
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

    # ====================== ВСПОМОГАТЕЛЬНАЯ ЛОГИКА ДЛЯ ИНТЕРВАЛОВ ======================
    def get_interval_hours(self, exchange: str, raw: Optional[Dict] = None) -> float:
        """Получить интервал в часах с приоритетом: индивидуальный > из raw > дефолтный"""
        symbol = None
        if raw is not None:
            symbol = raw.get("symbol") or raw.get("instId") or raw.get("contract_code")

        # Per-symbol кэш для Binance/Bybit/OKX/Bitget
        if symbol:
            ex_cache = self.symbol_intervals.get(exchange)
            if ex_cache:
                iv = ex_cache.get(symbol)
                if iv is not None and iv > 0:
                    return float(iv)

        # Bitget: fundingRateInterval в часах
        if exchange == "bitget" and raw is not None:
            fri = raw.get("fundingRateInterval")
            if fri is not None:
                try:
                    interval = float(fri)
                    if interval > 0:
                        return interval
                except (TypeError, ValueError):
                    pass

        # HTX: вычисляем интервал из next_funding_time и funding_time
        if exchange == "htx" and raw is not None:
            if 'next_funding_time' in raw and 'funding_time' in raw:
                try:
                    next_ts = int(raw['next_funding_time'])
                    funding_ts = int(raw['funding_time'])
                    interval_hours = (next_ts - funding_ts) / (1000 * 3600)
                    return interval_hours
                except (TypeError, ValueError):
                    pass

        interval = self.default_interval_hours.get(exchange, 8.0)
        if interval <= 0:
            interval = 8.0
        return interval

    def enrich_with_yield(self, exchange: str, symbol: str, funding_rate_percent: float, interval_hours: float) -> Dict:
        """Обогащение данных расчетами доходности"""
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

    # ====================== HTTP ЗАПРОСЫ ======================
    async def fetch_exchange_data(self, session: aiohttp.ClientSession, exchange: str, url: str) -> List[Dict]:
        """Получение сырых данных с биржи"""
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
        allowed_intervals = [1, 2, 3, 4, 6, 8]  # Разрешенные интервалы

        try:
            # --- BINANCE ---
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
                        
                        # Фильтрация по разрешенным интервалам
                        if interval_hours not in allowed_intervals:
                            continue
                            
                        funding_data.append(
                            self.enrich_with_yield(exchange, symbol, funding_rate, interval_hours)
                        )

            # --- BYBIT ---
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
                        
                        # Фильтрация по разрешенным интервалам
                        if interval_hours not in allowed_intervals:
                            continue
                            
                        funding_data.append(
                            self.enrich_with_yield(exchange, symbol, funding_rate, interval_hours)
                        )

            # --- OKX ---
            elif exchange == "okx":
                instruments = data.get("data", [])
                if not instruments:
                    logger.warning("OKX: пустой список инструментов")
                    return funding_data

                async with aiohttp.ClientSession() as session:
                    for inst in instruments:
                        inst_id = inst.get("instId", "")
                        if not inst_id.endswith("-USDT-SWAP"):
                            continue
                            
                        fr_url = f"https://www.okx.com/api/v5/public/funding-rate?instId={inst_id}"
                        try:
                            async with session.get(fr_url, timeout=10) as resp:
                                if resp.status != 200:
                                    logger.warning(f"OKX funding-rate {inst_id}: HTTP {resp.status}")
                                    continue
                                fr_json = await resp.json()
                        except Exception as e:
                            logger.error(f"OKX запрос funding-rate {inst_id} упал: {e}")
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
                            logger.error(f"OKX парсинг fundingRate для {inst_id}: {e}")
                            continue
                            
                        interval_hours = self.get_interval_hours(exchange, inst)
                        
                        # Фильтрация по разрешенным интервалам
                        if interval_hours not in allowed_intervals:
                            continue
                            
                        symbol = inst_id.replace("-USDT-SWAP", "USDT")
                        funding_data.append(
                            self.enrich_with_yield("okx", symbol, funding_rate, interval_hours)
                        )

            # --- BITGET ---
            elif exchange == "bitget":
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
                        funding_rate = float(fr_raw) * 100.0
                    except (TypeError, ValueError):
                        continue

                    interval_hours = self.get_interval_hours(exchange, item)
                    
                    # Фильтрация по разрешенным интервалам
                    if interval_hours not in allowed_intervals:
                        continue
                        
                    funding_data.append(
                        self.enrich_with_yield("bitget", symbol, funding_rate, interval_hours)
                    )

            # --- HTX ---
            elif exchange == "htx":
                if data.get("status") == "ok":
                    for item in data.get("data", []):
                        symbol = item.get("contract_code")
                        if not symbol.endswith("USDT"):
                            continue

                        fr_raw = item.get("funding_rate")
                        try:
                            funding_rate = float(fr_raw) * 100.0
                        except (TypeError, ValueError):
                            continue

                        interval_hours = self.get_interval_hours(exchange, item)
                        
                        # Фильтрация по разрешенным интервалам
                        if interval_hours not in allowed_intervals:
                            continue
                            
                        funding_data.append(
                            self.enrich_with_yield(exchange, symbol, funding_rate, interval_hours)
                        )

            # --- ЗАГЛУШКИ ДЛЯ ОСТАЛЬНЫХ ---
            elif exchange in ["mexc", "lbank", "gate", "bingx"]:
                logger.info(f"Парсер для {exchange} пока не реализован")

        except Exception as e:
            logger.error(f"Ошибка парсинга {exchange}: {e}")

        return funding_data

    # ====================== ОБЩИЕ ОПЕРАЦИИ ======================
    async def get_all_funding_rates(self) -> List[Dict]:
        """Сбор funding rates со всех бирж"""
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

    def format_funding_message(self, data: List[Dict], start_idx: int = 0, limit: int = 20) -> str:
        """Формирование сообщения с пагинацией"""
        if not data:
            return "Данные не найдены"

        end_idx = min(start_idx + limit, len(data))
        page_data = data[start_idx:end_idx]

        message = f"Страница {start_idx//limit + 1}/{(len(data)-1)//limit + 1}\n\n"
        
        for item in page_data:
            funding_sign = "+" if item["funding_rate"] > 0 else ""
            line = (
                f"{item['exchange'].upper()} {item['symbol']}\n"
                f"Фандинг: {funding_sign}{item['funding_rate']:.4f}%\n"
                f"Выплат в сутки: {item['daily_payments']:.1f} раз (каждые {item['interval_hours']} ч)\n"
                f"Годовая доходность: {item['annual_yield']:.2f}%\n"
                f"{'-'*30}\n"
            )
            message += line

        return message

    async def get_arbitrage_opportunities(self, data: List[Dict]) -> List[str]:
        """Поиск арбитражных возможностей с учетом разных интервалов выплат"""
        symbol_groups: Dict[str, List[Dict]] = {}

        for item in data:
            symbol = item["symbol"]
            symbol_groups.setdefault(symbol, []).append(item)

        opportunities = []

        for symbol, rates in symbol_groups.items():
            if len(rates) < 2:
                continue

            rates_sorted = sorted(rates, key=lambda x: x["funding_rate"])
            
            # Рассматриваем все комбинации
            for i in range(len(rates_sorted)):
                for j in range(i + 1, len(rates_sorted)):
                    lowest = rates_sorted[i]
                    highest = rates_sorted[j]
                    
                    # Разница фандинга
                    funding_diff = highest["funding_rate"] - lowest["funding_rate"]
                    
                    # Расчет потенциальной доходности
                    if lowest["interval_hours"] == highest["interval_hours"]:
                        # Одинаковые интервалы
                        daily_diff = funding_diff * lowest["daily_payments"]
                        potential_yield = daily_diff * 365
                    else:
                        # Разные интервалы
                        daily_yield_lowest = lowest["funding_rate"] * lowest["daily_payments"]
                        daily_yield_highest = highest["funding_rate"] * highest["daily_payments"]
                        daily_diff = daily_yield_highest - daily_yield_lowest
                        potential_yield = daily_diff * 365

                    # Фильтр по минимальной доходности
                    if potential_yield < 15:
                        continue

                    opportunities.append({
                        "symbol": symbol,
                        "long_exchange": lowest["exchange"],
                        "short_exchange": highest["exchange"],
                        "funding_diff": funding_diff,
                        "potential_yield": potential_yield,
                        "same_interval": lowest["interval_hours"] == highest["interval_hours"],
                        "long_interval": lowest["interval_hours"],
                        "short_interval": highest["interval_hours"],
                        "long_daily_payments": lowest["daily_payments"],
                        "short_daily_payments": highest["daily_payments"],
                    })

        opportunities.sort(key=lambda x: x["potential_yield"], reverse=True)

        if not opportunities:
            return ["Арбитражные возможности не найдены"]

        # Формируем сообщения
        chunks = []
        current = "📌 Арбитражные возможности:\n\n"

        for opp in opportunities:
            line = (
                f"Пара: {opp['symbol']}\n"
                f"▲ ЛОНГ на {opp['long_exchange'].upper()} "
                f"(интервал: {opp['long_interval']} ч, выплат в сутки: {opp['long_daily_payments']:.1f})\n"
                f"▼ ШОРТ на {opp['short_exchange'].upper()} "
                f"(интервал: {opp['short_interval']} ч, выплат в сутки: {opp['short_daily_payments']:.1f})\n"
                f"Разница фандинга: {opp['funding_diff']:.4f}%\n"
            )
            
            if not opp['same_interval']:
                line += "⚠️ Внимание: интервалы выплат различаются!\n"
                
            line += f"Потенциальная годовая доходность: {opp['potential_yield']:.2f}%\n"
            line += f"{'-'*30}\n"

            if len(current) + len(line) > 3500:
                chunks.append(current)
                current = line
            else:
                current += line

        if current:
            chunks.append(current)

        return chunks

# ====================== ЭКЗЕМПЛЯР БОТА ======================
bot = FundingRateBot()

# ====================== TELEGRAM HANDLERS ======================
user_sessions = {}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start с кнопками"""
    keyboard = [
        ["📉 Все фандинги (отрицательные)", "📈 Все фандинги (положительные)"],
        ["⭐ Топ 5 лучших фандингов", "🔄 Связки арбитража"],
        ["🔄 Обновить данные"],
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    await update.message.reply_text(
        "🤖 Бот мониторинга Funding Rates\n\nВыберите действие:",
        reply_markup=reply_markup,
    )

async def handle_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик пагинации"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    data = query.data.split("_")
    
    if user_id not in user_sessions:
        await query.edit_message_text("Сессия истекла. Начните заново.")
        return
        
    session_data = user_sessions[user_id]
    data_type = session_data["type"]
    all_data = session_data["data"]
    
    if data[0] == "prev":
        new_start = max(0, session_data["start"] - 20)
    else:  # next
        new_start = min(session_data["start"] + 20, len(all_data) - 1)
    
    user_sessions[user_id] = {
        "type": data_type,
        "data": all_data,
        "start": new_start
    }
    
    message_text = bot.format_funding_message(all_data, new_start, 20)
    
    # Создаем кнопки пагинации
    keyboard = []
    if new_start > 0:
        keyboard.append(InlineKeyboardButton("⬅️ Назад", callback_data="prev_"))
    if new_start + 20 < len(all_data):
        keyboard.append(InlineKeyboardButton("Вперед ➡️", callback_data="next_"))
    
    reply_markup = InlineKeyboardMarkup([keyboard]) if keyboard else None
    
    await query.edit_message_text(
        text=message_text,
        reply_markup=reply_markup
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений (кнопки)"""
    message_text = update.message.text.strip()
    user_id = update.message.from_user.id

    try:
        if message_text == "📉 Все фандинги (отрицательные)":
            await update.message.reply_text("📊 Загружаю данные...")
            data = await bot.get_all_funding_rates()
            sorted_data = bot.sort_funding_rates(data, "negative")
            
            user_sessions[user_id] = {
                "type": "negative",
                "data": sorted_data,
                "start": 0
            }
            
            message_text = bot.format_funding_message(sorted_data, 0, 20)
            
            # Кнопки пагинации
            keyboard = []
            if len(sorted_data) > 20:
                keyboard.append(InlineKeyboardButton("Вперед ➡️", callback_data="next_"))
            
            reply_markup = InlineKeyboardMarkup([keyboard]) if keyboard else None
            
            await update.message.reply_text(message_text, reply_markup=reply_markup)

        elif message_text == "📈 Все фандинги (положительные)":
            await update.message.reply_text("📊 Загружаю данные...")
            data = await bot.get_all_funding_rates()
            sorted_data = bot.sort_funding_rates(data, "positive")
            
            user_sessions[user_id] = {
                "type": "positive",
                "data": sorted_data,
                "start": 0
            }
            
            message_text = bot.format_funding_message(sorted_data, 0, 20)
            
            # Кнопки пагинации
            keyboard = []
            if len(sorted_data) > 20:
                keyboard.append(InlineKeyboardButton("Вперед ➡️", callback_data="next_"))
            
            reply_markup = InlineKeyboardMarkup([keyboard]) if keyboard else None
            
            await update.message.reply_text(message_text, reply_markup=reply_markup)

        elif message_text == "⭐ Топ 5 лучших фандингов":
            await update.message.reply_text("⭐ Загружаю данные...")
            data = await bot.get_all_funding_rates()

            negative_data = [d for d in data if d["funding_rate"] < 0]
            top_negative = bot.sort_funding_rates(negative_data, "negative")[:5]

            positive_data = [d for d in data if d["funding_rate"] > 0]
            top_positive = bot.sort_funding_rates(positive_data, "positive")[:5]

            msg_neg_chunks = bot.format_funding_message(top_negative)
            msg_pos_chunks = bot.format_funding_message(top_positive)

            await update.message.reply_text("▼ Топ 5 отрицательных фандингов:\n")
            await update.message.reply_text(msg_neg_chunks)

            await update.message.reply_text("▲ Топ 5 положительных фандингов:\n")
            await update.message.reply_text(msg_pos_chunks)

        elif message_text == "🔄 Связки арбитража":
            await update.message.reply_text("🔄 Ищу арбитражные возможности...")
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
    application.add_handler(CallbackQueryHandler(handle_pagination, pattern="^(prev|next)_"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Бот запущен (polling)...")
    application.run_polling()

if __name__ == "__main__":
    main()
