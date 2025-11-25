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
            "binance": "https://fapi.binance.com/fapi/v1/premiumIndex",
            "bybit": "https://api.bybit.com/v5/market/tickers?category=linear",
            "mexc": "https://contract.mexc.com/api/v1/contract/ticker",
            "okx": "https://www.okx.com/api/v5/public/instruments?instType=SWAP",
            "htx": "https://api.htx.com/linear-swap-api/v1/swap_funding_rate",
            "lbank": "https://api.lbank.info/v2/futures/fundingRate.do",
            "bitget": "https://api.bitget.com/api/v2/mix/market/current-fund-rate?productType=USDT-FUTURES",
            "gate": "https://api.gateio.ws/api/v4/futures/usdt/tickers",
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
        self.symbol_intervals = {
            "binance": {},
            "bybit": {},
            "okx": {},
            "bitget": {}
        }

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

    def get_interval_hours(self, exchange: str, raw: Optional[Dict] = None) -> float:
        """Получить интервал в часах с приоритетом: индивидуальный > из raw > дефолтный"""
        symbol = None
        if raw is not None:
            symbol = raw.get("symbol") or raw.get("instId") or raw.get("contract_code") or raw.get("contract")

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

    async def fetch_exchange_data(self, session: aiohttp.ClientSession, exchange: str, url: str) -> List[Dict]:
        """Получение сырых данных с биржи"""
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            async with session.get(url, timeout=15, headers=headers) as response:
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
        funding_data = []
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
                    if not symbol.endswith("_UMCBL"):
                        continue
                    symbol = symbol.replace("_UMCBL", "USDT")

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
                        symbol = item.get("contract_code", "")
                        if not symbol.endswith("-USDT"):
                            continue
                        symbol = symbol.replace("-USDT", "USDT")

                        fr_raw = item.get("funding_rate")
                        if fr_raw is None:
                            continue
                        try:
                            funding_rate = float(fr_raw) * 100.0
                        except (TypeError, ValueError):
                            continue

                        # Для HTX получаем интервал из времени следующего фандинга
                        next_funding_time = item.get("next_funding_time")
                        funding_time = item.get("funding_time")
                        if next_funding_time and funding_time:
                            try:
                                interval_hours = (next_funding_time - funding_time) / (1000 * 3600)
                            except:
                                interval_hours = self.get_interval_hours(exchange, item)
                        else:
                            interval_hours = self.get_interval_hours(exchange, item)
                        
                        # Фильтрация по разрешенным интервалам
                        if interval_hours not in allowed_intervals:
                            continue
                            
                        funding_data.append(
                            self.enrich_with_yield(exchange, symbol, funding_rate, interval_hours)
                        )

            # --- MEXC ---
            elif exchange == "mexc":
                if data.get("success") is True:
                    for item in data.get("data", []):
                        symbol = item.get("symbol", "")
                        if not symbol.endswith("_USDT"):
                            continue
                        symbol = symbol.replace("_USDT", "USDT")

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

            # --- BINGX ---
            elif exchange == "bingx":
                if data.get("code") == 0:
                    for item in data.get("data", []):
                        symbol = item.get("symbol", "")
                        if not symbol.endswith("-USDT"):
                            continue
                        symbol = symbol.replace("-USDT", "USDT")

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

            # --- GATE ---
            elif exchange == "gate":
                for item in data:
                    symbol = item.get("name", "")
                    if not symbol.endswith("_USDT"):
                        continue
                    symbol = symbol.replace("_USDT", "USDT")

                    fr_raw = item.get("funding_rate")
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

            # --- LBANK ---
            elif exchange == "lbank":
                items = data.get("data", [])
                if not isinstance(items, list):
                    logger.warning("LBank: неожиданный формат data")
                    return funding_data

                for item in items:
                    symbol = item.get("symbol", "")
                    if not symbol.endswith("_USDT"):
                        continue
                    symbol = symbol.replace("_USDT", "USDT")

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

        except Exception as e:
            logger.error(f"Ошибка парсинга {exchange}: {e}")

        return funding_data

    async def get_all_funding_rates(self) -> List[Dict]:
        """Сбор funding rates со всех бирж"""
        try:
            await self.preload_intervals()
        except Exception as e:
            logger.error(f"preload_intervals error: {e}")

        all_data = []

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
        """Сортировка funding rates по годовой доходности"""
        if sort_type == "negative":
            # Для отрицательных: от самой большой отрицательной годовой доходности к меньшей
            return sorted(data, key=lambda x: x["annual_yield"])
        elif sort_type == "positive":
            # Для положительных: от самой большой положительной годовой доходности к меньшей
            return sorted(data, key=lambda x: x["annual_yield"], reverse=True)
        return data

    def format_funding_message(self, data: List[Dict], start_idx: int = 0, limit: int = 20) -> str:
        """Формирование сообщения с пагинацией"""
        if not data:
            return "Данные не найдены"

        end_idx = min(start_idx + limit, len(data))
        page_data = data[start_idx:end_idx]
        total_pages = (len(data) + limit - 1) // limit

        message = f"Страница {start_idx//limit + 1}/{total_pages}\n\n"
        
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

    def create_pagination_keyboard(self, current_page: int, total_pages: int, data_type: str, prefix: str = ""):
        """Создание клавиатуры пагинации с быстрым перемещением"""
        keyboard = []
        
        # Первая страница и предыдущая
        if current_page > 0:
            keyboard.extend([
                InlineKeyboardButton("⏮️ Первая", callback_data=f"{prefix}first_{data_type}"),
                InlineKeyboardButton("◀️ Назад", callback_data=f"{prefix}prev_{data_type}")
            ])
        
        # Номер текущей страницы
        keyboard.append(InlineKeyboardButton(f"{current_page + 1}/{total_pages}", callback_data=f"{prefix}current_{data_type}"))
        
        # Следующая и последняя страница
        if current_page < total_pages - 1:
            keyboard.extend([
                InlineKeyboardButton("Вперед ▶️", callback_data=f"{prefix}next_{data_type}"),
                InlineKeyboardButton("Последняя ⏭️", callback_data=f"{prefix}last_{data_type}")
            ])
        
        # Быстрое перемещение по страницам
        quick_nav = []
        pages_to_show = []
        
        # Показываем первые 3, последние 3 и текущую с соседями
        for i in range(total_pages):
            if i < 3 or i >= total_pages - 3 or abs(i - current_page) <= 1:
                pages_to_show.append(i)
        
        # Убираем дубликаты и сортируем
        pages_to_show = sorted(set(pages_to_show))
        
        for i, page in enumerate(pages_to_show):
            if i > 0 and page - pages_to_show[i-1] > 1:
                quick_nav.append(InlineKeyboardButton("...", callback_data=f"{prefix}dots_{data_type}"))
            
            label = f"•{page+1}•" if page == current_page else str(page+1)
            quick_nav.append(InlineKeyboardButton(label, callback_data=f"{prefix}page_{data_type}_{page}"))
        
        if quick_nav:
            return InlineKeyboardMarkup([keyboard, quick_nav])
        else:
            return InlineKeyboardMarkup([keyboard])

    async def get_arbitrage_opportunities(self, data: List[Dict]) -> List[Dict]:
        """Поиск арбитражных возможностей с учетом разных интервалов выплат"""
        symbol_groups = {}

        for item in data:
            symbol = item["symbol"]
            if symbol not in symbol_groups:
                symbol_groups[symbol] = []
            symbol_groups[symbol].append(item)

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

        # Сортировка арбитражных возможностей по годовой доходности (от большей к меньшей)
        opportunities.sort(key=lambda x: x["potential_yield"], reverse=True)
        return opportunities

    def format_arbitrage_message(self, data: List[Dict], start_idx: int = 0, limit: int = 20) -> str:
        """Формирование сообщения с арбитражными возможностями"""
        if not data:
            return "Арбитражные возможности не найдены"

        end_idx = min(start_idx + limit, len(data))
        page_data = data[start_idx:end_idx]
        total_pages = (len(data) + limit - 1) // limit

        message = f"📌 Арбитражные возможности (страница {start_idx//limit + 1}/{total_pages}):\n\n"

        for opp in page_data:
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
            message += line

        return message

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
    callback_data = query.data
    
    if user_id not in user_sessions:
        await query.edit_message_text("Сессия истекла. Начните заново.")
        return
        
    session_data = user_sessions[user_id]
    data_type = session_data["type"]
    all_data = session_data["data"]
    current_page = session_data.get("page", 0)
    limit = session_data.get("limit", 20)
    total_pages = (len(all_data) + limit - 1) // limit

    # Обработка действий пагинации
    if callback_data.startswith("first_"):
        new_page = 0
    elif callback_data.startswith("prev_"):
        new_page = max(0, current_page - 1)
    elif callback_data.startswith("next_"):
        new_page = min(total_pages - 1, current_page + 1)
    elif callback_data.startswith("last_"):
        new_page = total_pages - 1
    elif callback_data.startswith("page_"):
        try:
            parts = callback_data.split("_")
            new_page = int(parts[3])
        except:
            new_page = current_page
    else:
        new_page = current_page

    user_sessions[user_id]["page"] = new_page
    
    # Форматирование сообщения в зависимости от типа данных
    if data_type in ["negative", "positive"]:
        message_text = bot.format_funding_message(all_data, new_page * limit, limit)
        reply_markup = bot.create_pagination_keyboard(new_page, total_pages, data_type)
    elif data_type == "arbitrage":
        message_text = bot.format_arbitrage_message(all_data, new_page * limit, limit)
        reply_markup = bot.create_pagination_keyboard(new_page, total_pages, data_type, "arb_")
    
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
                "page": 0,
                "limit": 20
            }
            
            message_text = bot.format_funding_message(sorted_data, 0, 20)
            total_pages = (len(sorted_data) + 20 - 1) // 20
            reply_markup = bot.create_pagination_keyboard(0, total_pages, "negative")
            
            await update.message.reply_text(message_text, reply_markup=reply_markup)

        elif message_text == "📈 Все фандинги (положительные)":
            await update.message.reply_text("📊 Загружаю данные...")
            data = await bot.get_all_funding_rates()
            sorted_data = bot.sort_funding_rates(data, "positive")
            
            user_sessions[user_id] = {
                "type": "positive",
                "data": sorted_data,
                "page": 0,
                "limit": 20
            }
            
            message_text = bot.format_funding_message(sorted_data, 0, 20)
            total_pages = (len(sorted_data) + 20 - 1) // 20
            reply_markup = bot.create_pagination_keyboard(0, total_pages, "positive")
            
            await update.message.reply_text(message_text, reply_markup=reply_markup)

        elif message_text == "⭐ Топ 5 лучших фандингов":
            await update.message.reply_text("⭐ Загружаю данные...")
            data = await bot.get_all_funding_rates()

            # Сортируем по годовой доходности
            negative_data = [d for d in data if d["funding_rate"] < 0]
            top_negative = sorted(negative_data, key=lambda x: x["annual_yield"])[:5]  # Самые большие отрицательные

            positive_data = [d for d in data if d["funding_rate"] > 0]
            top_positive = sorted(positive_data, key=lambda x: x["annual_yield"], reverse=True)[:5]  # Самые большие положительные

            msg_neg = bot.format_funding_message(top_negative)
            msg_pos = bot.format_funding_message(top_positive)

            await update.message.reply_text("▼ Топ 5 отрицательных фандингов (по годовой доходности):\n")
            await update.message.reply_text(msg_neg)

            await update.message.reply_text("▲ Топ 5 положительных фандингов (по годовой доходности):\n")
            await update.message.reply_text(msg_pos)

        elif message_text == "🔄 Связки арбитража":
            await update.message.reply_text("🔄 Ищу арбитражные возможности...")
            data = await bot.get_all_funding_rates()
            opportunities = await bot.get_arbitrage_opportunities(data)
            
            user_sessions[user_id] = {
                "type": "arbitrage",
                "data": opportunities,
                "page": 0,
                "limit": 10
            }
            
            message_text = bot.format_arbitrage_message(opportunities, 0, 10)
            total_pages = (len(opportunities) + 10 - 1) // 10
            reply_markup = bot.create_pagination_keyboard(0, total_pages, "arbitrage", "arb_")
            
            await update.message.reply_text(message_text, reply_markup=reply_markup)

        elif message_text == "🔄 Обновить данные":
            await update.message.reply_text("✅ Данные всегда обновляются при каждом запросе!")

    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text("❌ Произошла ошибка при получении данных")

def main():
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(handle_pagination, pattern="^(first|prev|next|last|page|arb_).*"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Бот запущен (polling)...")
    application.run_polling()

if __name__ == "__main__":
    main()
