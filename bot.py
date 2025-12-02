import logging
import asyncio
from datetime import datetime, timezone
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters

# Токены
TELEGRAM_TOKEN = "8329955590:AAGk1Nu1LUHhBWQ7bqeorTctzhxie69Wzf0"
COINGLASS_TOKEN = "2d73a05799f64daab80329868a5264ea"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

class CoinglassAPI:
    """
    Полноценная обёртка над Coinglass API с обработкой ошибок
    """
    def __init__(self):
        self.base_url_v3 = "https://open-api.coinglass.com/api/pro/v1"
        self.base_url_v4 = "https://open-api-v4.coinglass.com/api"
        self.headers_v3 = {
            "accept": "application/json",
            "coinglassSecret": COINGLASS_TOKEN,
        }
        self.headers_v4 = {
            "accept": "application/json",
            "CG-API-KEY": COINGLASS_TOKEN,
        }

    def _normalize_interval(self, val):
        """
        Нормализация интервала фандинга в часы.
        Если приходит None / "" / "?" или некорректное значение — ставим 8ч по умолчанию.
        """
        try:
            if val in (None, "", "?"):
                return 8
            hours = float(val)
            if hours <= 0:
                return 8
            if float(hours).is_integer():
                return int(hours)
            return hours
        except Exception:
            return 8

    def get_funding_rates(self):
        """
        Полный запрос всех ставок фандинга с обработкой ошибок
        """
        url = f"{self.base_url_v4}/futures/funding-rate/exchange-list"
        MAX_RETRIES = 3
        TIMEOUT = 60
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(
                    url,
                    headers=self.headers_v4,
                    timeout=TIMEOUT,
                )
                resp.raise_for_status()
                data = resp.json()
                
                if data.get("code") != "0":
                    logger.warning("Coinglass v4 funding-rate/exchange-list error: %s", data)
                    return None
                
                entries = data.get("data", [])
                result = []
                
                for entry in entries:
                    sym = entry.get("symbol", "")
                    stable_list = entry.get("stablecoin_margin_list") or []
                    token_list = entry.get("token_margin_list") or []
                    
                    # USDT маржа
                    for row in stable_list:
                        try:
                            rate = float(row.get("funding_rate", 0.0))
                        except (TypeError, ValueError):
                            rate = 0.0

                        interval = self._normalize_interval(row.get("funding_rate_interval"))
                            
                        item = {
                            "symbol": sym,
                            "exchangeName": row.get("exchange", ""),
                            # funding_rate уже в процентах за интервал (0.01 = 0.01%)
                            "rate": rate,
                            "marginType": "USDT",
                            "interval": interval,
                            "nextFundingTime": row.get("next_funding_time", ""),
                        }
                        result.append(item)
                    
                    # COIN маржа
                    for row in token_list:
                        try:
                            rate = float(row.get("funding_rate", 0.0))
                        except (TypeError, ValueError):
                            rate = 0.0

                        interval = self._normalize_interval(row.get("funding_rate_interval"))
                            
                        item = {
                            "symbol": sym,
                            "exchangeName": row.get("exchange", ""),
                            "rate": rate,
                            "marginType": "COIN",
                            "interval": interval,
                            "nextFundingTime": row.get("next_funding_time", ""),
                        }
                        result.append(item)
                
                logger.info("Coinglass v4 funding-rate: получили %d записей", len(result))
                return result
                
            except requests.exceptions.ReadTimeout:
                logger.warning("Таймаут при запросе к Coinglass v4 (попытка %d/%d)", attempt, MAX_RETRIES)
                if attempt == MAX_RETRIES:
                    return None
            except requests.exceptions.RequestException as e:
                logger.error("Ошибка сети при запросе к Coinglass: %s", e)
                if attempt == MAX_RETRIES:
                    return None
            except Exception as e:
                logger.exception("Неожиданная ошибка при запросе к Coinglass v4: %s", e)
                return None

    def get_arbitrage_opportunities(self):
        """
        Арбитраж по цене через v3 API (дополнительная функция)
        """
        url = f"{self.base_url_v3}/futures/market"
        params = {"symbol": "BTC"}
        
        try:
            response = requests.get(
                url, headers=self.headers_v3, params=params, timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    return self._calculate_arbitrage(data.get("data", []))
            logger.warning("Coinglass v3 futures/market error: %s", response.text)
            return None
        except Exception as e:
            logger.exception(f"Ошибка при запросе к Coinglass v3 futures/market: {e}")
            return None

    def _calculate_arbitrage(self, market_data):
        """
        Расчет арбитражных возможностей по цене
        """
        opportunities = []
        for coin_data in market_data:
            symbol = coin_data.get("symbol", "")
            exchanges = coin_data.get("exchangeName", [])
            prices = coin_data.get("price", [])
            
            if len(prices) >= 2:
                try:
                    prices_float = [float(p) for p in prices]
                except Exception:
                    continue
                    
                min_price = min(prices_float)
                max_price = max(prices_float)
                
                if min_price > 0:
                    spread_percent = ((max_price - min_price) / min_price) * 100
                    if spread_percent > 0.5:
                        opportunities.append({
                            "symbol": symbol,
                            "min_price": min_price,
                            "max_price": max_price,
                            "spread_percent": round(spread_percent, 2),
                            "exchanges": exchanges,
                        })
        
        return sorted(opportunities, key=lambda x: x["spread_percent"], reverse=True)

    def calculate_funding_arbitrage_from_items(self, funding_items, symbol=None, min_spread=0.0005):
        """
        Расчет арбитража фандинга из загруженных данных
        min_spread тут в тех же единицах, что и rate (проценты за интервал)
        """
        if not funding_items:
            return None
            
        by_symbol = {}
        for item in funding_items:
            sym = item.get("symbol", "")
            if not sym:
                continue
                
            if symbol and sym.upper() != symbol.upper():
                continue
                
            margin_type = item.get("marginType", "USDT")
            if margin_type != "USDT":
                continue
                
            rate = item.get("rate", 0)
            exchange = item.get("exchangeName", "")
            if not exchange:
                continue
                
            try:
                r = float(rate)
            except (TypeError, ValueError):
                continue
                
            by_symbol.setdefault(sym, []).append((exchange, r))
            
        opportunities = []
        for sym, ex_rates in by_symbol.items():
            if len(ex_rates) < 2:
                continue
                
            min_ex, min_rate = min(ex_rates, key=lambda x: x[1])
            max_ex, max_rate = max(ex_rates, key=lambda x: x[1])
            spread = max_rate - min_rate
            
            if abs(spread) < min_spread:
                continue
                
            opportunities.append({
                "symbol": sym,
                "min_exchange": min_ex,
                "max_exchange": max_ex,
                "min_rate": min_rate,
                "max_rate": max_rate,
                "spread": spread,
            })
            
        if not opportunities:
            return None
            
        opportunities.sort(key=lambda x: abs(x["spread"]), reverse=True)
        return opportunities

class CryptoArbBot:
    def __init__(self):
        self.api = CoinglassAPI()
        self.application = Application.builder().token(TELEGRAM_TOKEN).build()
        self.funding_cache = []
        self.funding_cache_updated_at = None
        self.cache_lock = asyncio.Lock()

        # Минимальный модуль ставки, ниже которого считаем её «нулевой» и не используем в арбитраже
        # (в процентах за интервал, т.е. 0.000001% за интервал)
        self.MIN_ABS_RATE = 1e-6

        self.setup_handlers()

    def annualize_rate(self, rate, interval):
        """
        Перевод ставки фандинга за период в годовую ПРОЦЕНТНУЮ ставку (APR).
        rate — в процентах за интервал (0.01 = 0.01%)
        interval — длительность интервала в часах
        """
        try:
            if interval in (None, "", "?"):
                hours = 8.0
            else:
                hours = float(interval)
        except (TypeError, ValueError):
            hours = 8.0

        if hours <= 0:
            hours = 8.0

        periods_per_year = 365.0 * 24.0 / hours
        annual_percent = rate * periods_per_year
        return annual_percent

    def format_annual_rate(self, annual_rate: float) -> str:
        """
        Форматирование годовой ставки, чтобы мелкие значения не превращались в 0.00.
        """
        v = float(annual_rate)
        if abs(v) >= 10:
            return f"{v:+.2f}%"
        elif abs(v) >= 1:
            return f"{v:+.3f}%"
        elif abs(v) >= 0.1:
            return f"{v:+.4f}%"
        else:
            return f"{v:+.5f}%"

    def get_exchange_emoji(self, exchange: str) -> str:
        """
        Один и тот же эмодзи для всех бирж.
        """
        return "🏦"

    async def update_funding_cache(self, context: ContextTypes.DEFAULT_TYPE):
        """
        Безопасное обновление кэша с блокировкой
        """
        async with self.cache_lock:
            try:
                logger.info("Начало обновления кэша фандинга...")
                data = await asyncio.to_thread(self.api.get_funding_rates)
                if data:
                    self.funding_cache = data
                    self.funding_cache_updated_at = datetime.now(timezone.utc)
                    logger.info("Кэш фандинга успешно обновлён: %d записей", len(self.funding_cache))
                else:
                    logger.warning("Не удалось получить данные от Coinglass")
            except Exception as e:
                logger.exception("Критическая ошибка при обновлении кэша: %s", e)

    def get_cached_funding(self, symbol=None):
        """
        Безопасное получение данных из кэша с фильтрацией по символу
        """
        if not self.funding_cache:
            return None
            
        if symbol:
            symbol_upper = symbol.upper()
            return [
                item for item in self.funding_cache
                if item.get("symbol", "").upper() == symbol_upper
            ]
            
        return self.funding_cache

    def get_filtered_funding(self, funding_type="all"):
        """
        Фильтрация и сортировка данных по типу
        rate здесь в процентах за интервал
        """
        data = self.get_cached_funding()
        if not data:
            return None
            
        if funding_type == "negative":
            filtered = [item for item in data if item.get("rate", 0) < 0]
            return sorted(filtered, key=lambda x: x["rate"])
        elif funding_type == "positive":
            filtered = [item for item in data if item.get("rate", 0) > 0]
            return sorted(filtered, key=lambda x: x["rate"], reverse=True)
        else:
            return data

    def get_all_exchanges(self):
        """
        Получить все уникальные биржи из кэша
        """
        if not self.funding_cache:
            return None
            
        exchanges = set()
        for item in self.funding_cache:
            exchange = item.get("exchangeName", "")
            if exchange:
                exchanges.add(exchange)
                
        return sorted(list(exchanges))

    def setup_handlers(self):
        """Настройка всех обработчиков команд"""
        handlers = [
            CommandHandler("start", self.start),
            CommandHandler("negative", self.show_negative),
            CommandHandler("positive", self.show_positive),
            CommandHandler("top10", self.show_top10),
            CommandHandler("arbitrage_bundles", self.show_arbitrage_bundles),
            CommandHandler("price_arbitrage", self.show_price_arbitrage),
            CommandHandler("status", self.show_status),
            CommandHandler("exchanges", self.show_exchanges),
            CallbackQueryHandler(self.button_handler, pattern="^(page_|nav_|funding_)"),
            MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message),
        ]
        
        for handler in handlers:
            self.application.add_handler(handler)

        self.application.add_error_handler(self.error_handler)

    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Глобальный обработчик ошибок"""
        logger.error("Exception while handling an update:", exc_info=context.error)
        try:
            if update and hasattr(update, 'effective_chat'):
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="❌ Произошла ошибка при обработке запроса. Попробуйте еще раз."
                )
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения об ошибке: {e}")

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Главное меню"""
        keyboard = [
            [InlineKeyboardButton("🔴 Все отрицательные", callback_data="nav_negative_1")],
            [InlineKeyboardButton("🟢 Все положительные", callback_data="nav_positive_1")],
            [InlineKeyboardButton("🚀 Топ 10 лучших", callback_data="nav_top10")],
            [InlineKeyboardButton("⚖️ Связки арбитража", callback_data="nav_arbitrage")],
            [InlineKeyboardButton("🏛️ Все биржи", callback_data="nav_exchanges")],
            [InlineKeyboardButton("💰 Ценовой арбитраж", callback_data="nav_price_arb")],
            [InlineKeyboardButton("📊 Статус бота", callback_data="nav_status")],
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        welcome_text = (
            "🤖 <b>Crypto Funding & Arbitrage Bot</b>\n\n"
            "📈 <b>Доступные команды:</b>\n"
            "/negative - все отрицательные фандинги\n"
            "/positive - все положительные фандинги\n"
            "/top10 - топ 10 положительных и отрицательных\n"
            "/arbitrage_bundles - связки арбитража фандинга\n"
            "/exchanges - все доступные биржи\n"
            "/price_arbitrage - ценовой арбитраж\n"
            "/status - статус бота и кэша\n\n"
            "⚡ Особенности:\n"
            "• Пагинация по 20 записей\n"
            "• Сортировка по убыванию процента\n"
            "• Проверка времени выплат в арбитраже\n"
            "• Кеширование каждые 30 секунд\n\n"
            "Все ставки показываются в <b>процентах годовых (APR)</b>, рассчитанных из текущей ставки за интервал."
        )
        
        await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode="HTML")

    async def show_negative(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.show_funding_page(update, context, "negative", 1)

    async def show_positive(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.show_funding_page(update, context, "positive", 1)

    async def show_funding_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE, funding_type: str, page: int):
        """Показать страницу с фандингами (APR)"""
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        if not self.funding_cache:
            error_msg = (
                "⚠️ <b>Данные ещё не загружены</b>\n\n"
                "Кэш фандинга пуст. Возможные причины:\n"
                "• Бот только что запустился\n"
                "• Проблемы с API Coinglass\n"
                "• Превышены лимиты запросов\n\n"
                "Попробуйте через 30 секунд..."
            )
            await send_method(error_msg, parse_mode="HTML")
            return

        filtered_data = self.get_filtered_funding(funding_type)
        if not filtered_data:
            await send_method("🤷‍♂️ <b>Нет данных для отображения</b>\n\nПопробуйте другой раздел.", parse_mode="HTML")
            return

        items_per_page = 20
        total_items = len(filtered_data)
        total_pages = (total_items + items_per_page - 1) // items_per_page
        page = max(1, min(page, total_pages))
        start_idx = (page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        page_data = filtered_data[start_idx:end_idx]

        context.user_data.update({
            'current_page': page,
            'total_pages': total_pages,
            'current_data_type': funding_type,
            'current_data': filtered_data
        })

        title_map = {
            "negative": "🔴 Отрицательные фандинги",
            "positive": "🟢 Положительные фандинги"
        }
        response = f"<b>{title_map[funding_type]} (APR)</b>\n"
        response += f"📄 Страница {page}/{total_pages} | Всего записей: {total_items}\n"
        response += "💡 Показана приблизительная <b>годовая доходность (APR)</b> при линейном пересчёте текущей ставки за интервал.\n\n"

        for i, item in enumerate(page_data, start=start_idx + 1):
            symbol = item.get("symbol", "N/A")
            exchange = item.get("exchangeName", "N/A")
            raw_rate = item.get("rate", 0)
            interval = item.get("interval", 8)
            margin_type = item.get("marginType", "USDT")

            annual_rate = self.annualize_rate(raw_rate, interval)
            annual_str = self.format_annual_rate(annual_rate)
            ex_emoji = self.get_exchange_emoji(exchange)
            emoji = "🔴" if funding_type == "negative" else "🟢"

            response += f"{emoji} <b>{symbol}</b>\n"
            response += f" {ex_emoji} {exchange} ({margin_type})\n"
            response += f" 💰 {annual_str} годовых | ⏰ интервал: {interval}ч | ставка за интервал: {raw_rate:.6f}%\n\n"

        keyboard = []
        if total_pages > 1:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton("◀ Назад", callback_data=f"page_{funding_type}_{page-1}"))
            nav_buttons.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="page_info"))
            if page < total_pages:
                nav_buttons.append(InlineKeyboardButton("Вперед ▶", callback_data=f"page_{funding_type}_{page+1}"))
            keyboard.append(nav_buttons)

        quick_nav = []
        if total_pages > 5:
            quick_pages = set([1, max(1, page-2), page, min(total_pages, page+2), total_pages])
            for quick_page in sorted(quick_pages):
                if quick_page != page:
                    quick_nav.append(InlineKeyboardButton(str(quick_page), callback_data=f"page_{funding_type}_{quick_page}"))
            if quick_nav:
                keyboard.append(quick_nav)

        keyboard.append([InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")])
        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            await send_method(response, reply_markup=reply_markup, parse_mode="HTML")
        except Exception as e:
            logger.error("Ошибка при отправке сообщения: %s", e)
            await send_method("❌ <b>Ошибка при отображении данных</b>\nПопробуйте еще раз.", parse_mode="HTML")

    async def show_top10(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Топ-10 фандингов"""
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        if not self.funding_cache:
            await send_method("⚠️ Данные ещё не загружены. Попробуйте через 30 секунд.")
            return

        positive_data = self.get_filtered_funding("positive")[:10]
        negative_data = self.get_filtered_funding("negative")[:10]

        response = "<b>🚀 Топ 10 лучших фандингов (APR)</b>\n\n"
        response += "<b>🟢 Топ 10 положительных (годовых):</b>\n"
        for i, item in enumerate(positive_data, 1):
            symbol = item.get("symbol", "")
            exchange = item.get("exchangeName", "")
            interval = item.get("interval", 8)
            raw_rate = item.get("rate", 0)
            annual_rate = self.annualize_rate(raw_rate, interval)
            annual_str = self.format_annual_rate(annual_rate)
            ex_emoji = self.get_exchange_emoji(exchange)
            response += (
                f"{i}. <b>{symbol}</b> - {annual_str} годовых "
                f"({ex_emoji} {exchange}, интервал: {interval}ч, ставка за интервал: {raw_rate:.6f}%)\n"
            )

        response += "\n<b>🔴 Топ 10 отрицательных (годовых):</b>\n"
        for i, item in enumerate(negative_data, 1):
            symbol = item.get("symbol", "")
            exchange = item.get("exchangeName", "")
            interval = item.get("interval", 8)
            raw_rate = item.get("rate", 0)
            annual_rate = self.annualize_rate(raw_rate, interval)
            annual_str = self.format_annual_rate(annual_rate)
            ex_emoji = self.get_exchange_emoji(exchange)
            response += (
                f"{i}. <b>{symbol}</b> - {annual_str} годовых "
                f"({ex_emoji} {exchange}, интервал: {interval}ч, ставка за интервал: {raw_rate:.6f}%)\n"
            )

        if self.funding_cache_updated_at:
            cache_time = self.funding_cache_updated_at.strftime("%H:%M:%S")
            response += f"\n🕒 <i>Данные обновлены: {cache_time} UTC</i>"

        keyboard = [[InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")]]
        await send_method(response, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    async def show_arbitrage_bundles(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Арбитражные связки (APR)"""
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        if not self.funding_cache:
            await send_method("⚠️ Данные ещё не загружены. Попробуйте через 30 секунд.")
            return

        symbol_data = {}
        for item in self.funding_cache:
            symbol = item.get("symbol", "")
            if not symbol:
                continue

            rate = item.get("rate", 0)
            # отфильтровываем заведомо «нулевые» ставки
            if abs(rate) < self.MIN_ABS_RATE:
                continue

            if symbol not in symbol_data:
                symbol_data[symbol] = []

            symbol_data[symbol].append({
                'exchange': item.get("exchangeName", ""),
                'rate': rate,
                'interval': item.get("interval", 8),
                'marginType': item.get("marginType", "")
            })

        opportunities = []
        for symbol, exchanges in symbol_data.items():
            if len(exchanges) < 2:
                continue

            # ИСПРАВЛЕНО: раньше брали только marginType == 'USDT', из-за чего Hyperliquid и другие могли пропадать.
            # Теперь сравниваем все биржи по символу, вне зависимости от типа маржи.
            valid_exchanges = exchanges
            if len(valid_exchanges) < 2:
                continue

            min_item = min(valid_exchanges, key=lambda x: x['rate'])
            max_item = max(valid_exchanges, key=lambda x: x['rate'])
            spread = max_item['rate'] - min_item['rate']

            if abs(spread) < 0.0005:
                continue

            time_warning = ""
            if min_item['interval'] != max_item['interval']:
                time_warning = " ⚠️ РАЗНОЕ ВРЕМЯ ВЫПЛАТ!"

            opportunities.append({
                'symbol': symbol,
                'min_exchange': min_item['exchange'],
                'max_exchange': max_item['exchange'],
                'min_rate': min_item['rate'],
                'max_rate': max_item['rate'],
                'min_interval': min_item['interval'],
                'max_interval': max_item['interval'],
                'spread': spread,
                'time_warning': time_warning
            })

        opportunities.sort(key=lambda x: abs(x['spread']), reverse=True)

        response = "<b>⚖️ Связки арбитража фандинга (APR)</b>\n\n"
        if not opportunities:
            response += (
                "🤷‍♂️ <b>Арбитражные возможности не найдены</b>\n\n"
                "Возможные причины:\n"
                "• Слишком маленький спред между биржами\n"
                "• Недостаточно данных по марже\n"
                "• Рынок в состоянии равновесия"
            )
        else:
            response += f"📊 Найдено возможностей: {len(opportunities)}\n"
            response += "💡 Ставки показаны в <b>годовых процентах (APR)</b> с учётом интервала каждой биржи.\n\n"
            for opp in opportunities[:15]:
                min_annual = self.annualize_rate(opp['min_rate'], opp['min_interval'])
                max_annual = self.annualize_rate(opp['max_rate'], opp['max_interval'])
                spread_annual = max_annual - min_annual

                min_emoji = self.get_exchange_emoji(opp['min_exchange'])
                max_emoji = self.get_exchange_emoji(opp['max_exchange'])

                min_annual_str = self.format_annual_rate(min_annual)
                max_annual_str = self.format_annual_rate(max_annual)
                spread_annual_str = self.format_annual_rate(spread_annual)

                response += f"🎯 <b>{opp['symbol']}</b>{opp['time_warning']}\n"
                response += (
                    f" 📉 {min_emoji} {opp['min_exchange']}: {min_annual_str} годовых "
                    f"(интервал: {opp['min_interval']}ч, ставка за интервал: {opp['min_rate']:.6f}%)\n"
                )
                response += (
                    f" 📈 {max_emoji} {opp['max_exchange']}: {max_annual_str} годовых "
                    f"(интервал: {opp['max_interval']}ч, ставка за интервал: {opp['max_rate']:.6f}%)\n"
                )
                response += f" 💰 Спред (APR): {spread_annual_str}\n\n"

        keyboard = [[InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")]]
        await send_method(response, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    async def show_exchanges(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Список бирж"""
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        if not self.funding_cache:
            await send_method("⚠️ Данные ещё не загружены. Попробуйте через 30 секунд.")
            return

        exchanges = self.get_all_exchanges()
        if not exchanges:
            await send_method("🤷‍♂️ Не удалось получить список бирж.")
            return

        response = "<b>🏛️ Все доступные биржи</b>\n\n"
        response += f"📊 Всего бирж: {len(exchanges)}\n\n"

        per_line = 3
        for i in range(0, len(exchanges), per_line):
            line = exchanges[i:i + per_line]
            decorated = [f"{self.get_exchange_emoji(ex)} {ex}" for ex in line]
            response += " • " + " • ".join(decorated) + "\n"

        unique_symbols = len(set(item.get('symbol', '') for item in self.funding_cache))
        total_records = len(self.funding_cache)

        response += f"\n📈 <b>Статистика данных:</b>\n"
        response += f"• Всего записей: {total_records}\n"
        response += f"• Уникальных пар: {unique_symbols}\n"
        response += f"• Бирж: {len(exchanges)}\n"

        if self.funding_cache_updated_at:
            cache_time = self.funding_cache_updated_at.strftime("%H:%M:%S")
            response += f"\n🕒 <i>Данные обновлены: {cache_time} UTC</i>"

        keyboard = [[InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")]]
        await send_method(response, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    async def show_price_arbitrage(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ценовой арбитраж"""
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        await send_method("🔍 Ищу арбитражные возможности по цене...")

        opportunities = self.api.get_arbitrage_opportunities()
        if not opportunities:
            await send_method("🤷‍♂️ Арбитражные возможности по цене не найдены")
            return

        response = "💸 <b>Арбитражные возможности по цене (BTC):</b>\n\n"
        for opp in opportunities[:10]:
            response += f"🎯 <b>{opp['symbol']}</b>\n"
            response += f" 📊 Спред: {opp['spread_percent']}%\n"
            response += f" 💰 Мин: ${opp['min_price']:.2f}\n"
            response += f" 💰 Макс: ${opp['max_price']:.2f}\n\n"

        keyboard = [[InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")]]
        await send_method(response, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    async def show_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Статус бота"""
        if update.callback_query:
            send_method = update.callback_query.edit_message_text
        else:
            send_method = update.message.reply_text

        cache_size = len(self.funding_cache) if self.funding_cache else 0
        last_update = self.funding_cache_updated_at.strftime("%Y-%m-%d %H:%M:%S UTC") if self.funding_cache_updated_at else "Никогда"

        if self.funding_cache:
            positive_count = len([x for x in self.funding_cache if x.get('rate', 0) > 0])
            negative_count = len([x for x in self.funding_cache if x.get('rate', 0) < 0])
            zero_count = len([x for x in self.funding_cache if x.get('rate', 0) == 0])
            unique_symbols = len(set(item.get('symbol', '') for item in self.funding_cache))
            unique_exchanges = len(set(item.get('exchangeName', '') for item in self.funding_cache))
        else:
            positive_count = negative_count = zero_count = unique_symbols = unique_exchanges = 0

        response = (
            "📊 <b>Статус бота</b>\n\n"
            f"• 🗄️ Размер кэша: {cache_size} записей\n"
            f"• 🕒 Последнее обновление: {last_update}\n"
            f"• 📈 Уникальные символы: {unique_symbols}\n"
            f"• 🏛️ Уникальные биржи: {unique_exchanges}\n\n"
            f"<b>Статистика фандингов (по ставке за интервал):</b>\n"
            f"• 🟢 Положительные: {positive_count}\n"
            f"• 🔴 Отрицательные: {negative_count}\n"
            f"• ⚪ Нулевые: {zero_count}\n\n"
            f"<i>Кэш обновляется каждые 30 секунд. Доходность в интерфейсе показана в годовых процентах (APR), "
            f"исходя из последней ставки за интервал.</i>"
        )

        keyboard = [[InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")]]
        await send_method(response, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик инлайн-кнопок"""
        query = update.callback_query
        await query.answer()

        try:
            data = query.data
            if data.startswith("page_"):
                parts = data.split("_")
                if len(parts) == 3:
                    funding_type = parts[1]
                    page = int(parts[2])
                    await self.show_funding_page(update, context, funding_type, page)
            elif data.startswith("nav_"):
                parts = data.split("_")
                nav_type = parts[1]
                if nav_type == "main":
                    await self.show_main_menu(update, context)
                elif nav_type == "negative":
                    await self.show_funding_page(update, context, "negative", 1)
                elif nav_type == "positive":
                    await self.show_funding_page(update, context, "positive", 1)
                elif nav_type == "top10":
                    await self.show_top10(update, context)
                elif nav_type == "arbitrage":
                    await self.show_arbitrage_bundles(update, context)
                elif nav_type == "exchanges":
                    await self.show_exchanges(update, context)
                elif nav_type == "price_arb":
                    await self.show_price_arbitrage(update, context)
                elif nav_type == "status":
                    await self.show_status(update, context)
        except Exception as e:
            logger.error("Ошибка в обработчике кнопок: %s", e)
            try:
                await query.edit_message_text("❌ <b>Произошла ошибка</b>\nПопробуйте еще раз.", parse_mode="HTML")
            except Exception as edit_error:
                logger.error("Не удалось отредактировать сообщение: %s", edit_error)
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="❌ <b>Произошла ошибка</b>\nПопробуйте еще раз.",
                    parse_mode="HTML"
                )

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик текстовых сообщений (быстрый переход по страницам)"""
        text = update.message.text.strip()
        
        if text.isdigit():
            page_num = int(text)
            user_data = context.user_data
            if 'current_data_type' in user_data and 'total_pages' in user_data:
                total_pages = user_data['total_pages']
                funding_type = user_data['current_data_type']
                if 1 <= page_num <= total_pages:
                    await self.show_funding_page(update, context, funding_type, page_num)
                    return
                else:
                    await update.message.reply_text(f"⚠️ Страница должна быть от 1 до {total_pages}")
                    return

        await update.message.reply_text(
            "ℹ️ <b>Быстрая навигация</b>\n\n"
            "Введите номер страницы для быстрого перехода\n"
            "Или используйте команды:\n"
            "/negative - отрицательные фандинги\n"
            "/positive - положительные фандинги\n"
            "/top10 - топ 10 фандингов\n"
            "/arbitrage_bundles - арбитражные связки\n"
            "/exchanges - все доступные биржи",
            parse_mode="HTML"
        )

    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать главное меню (кнопка назад)"""
        keyboard = [
            [InlineKeyboardButton("🔴 Все отрицательные", callback_data="nav_negative_1")],
            [InlineKeyboardButton("🟢 Все положительные", callback_data="nav_positive_1")],
            [InlineKeyboardButton("🚀 Топ 10 лучших", callback_data="nav_top10")],
            [InlineKeyboardButton("⚖️ Связки арбитража", callback_data="nav_arbitrage")],
            [InlineKeyboardButton("🏛️ Все биржи", callback_data="nav_exchanges")],
            [InlineKeyboardButton("💰 Ценовой арбитраж", callback_data="nav_price_arb")],
            [InlineKeyboardButton("📊 Статус бота", callback_data="nav_status")],
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        text = "📋 <b>Главное меню</b>\nВыберите раздел:"
        
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode="HTML")
        else:
            await update.message.reply_text(text, reply_markup=reply_markup, parse_mode="HTML")

    def run(self):
        """Запуск бота"""
        print("🤖 Бот запущен...")
        print("⚡ Кеширование каждые 30 секунд")
        print("📊 Мониторинг фандингов и арбитража")

        self.application.job_queue.run_repeating(
            self.update_funding_cache,
            interval=30,
            first=0,
        )

        try:
            self.application.run_polling(
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES
            )
        except Exception as e:
            logger.error("Ошибка при запуске бота: %s", e)
            print(f"❌ Критическая ошибка: {e}")

if __name__ == "__main__":
    bot = CryptoArbBot()
    bot.run()
