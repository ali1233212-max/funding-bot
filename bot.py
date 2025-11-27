import logging
import asyncio
from datetime import datetime, timezone

import requests
import pandas as pd  # пока не используется, но оставляем
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# Токены (ЗАМЕНИ НА СВОИ)
TELEGRAM_TOKEN = "PUT_TELEGRAM_TOKEN_HERE"
COINGLASS_TOKEN = "PUT_COINGLASS_TOKEN_HERE"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


class CoinglassAPI:
    """
    Класс-обёртка над Coinglass API.
    - v4: фандинг по всем монетам и биржам
    - v3: арбитраж по ЦЕНЕ (futures/market)
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

    def get_funding_rates(self):
        """
        ТЯЖЁЛЫЙ запрос: получить ВСЕ ставки фандинга по всем монетам и биржам.
        Вызывается только в фоне для обновления кэша.
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
                    logger.warning(
                        "Coinglass v4 funding-rate/exchange-list error: %s", data
                    )
                    return None

                entries = data.get("data", [])
                result: list[dict] = []

                for entry in entries:
                    sym = entry.get("symbol", "")
                    stable_list = entry.get("stablecoin_margin_list") or []
                    token_list = entry.get("token_margin_list") or []

                    # USDT / USD маржа
                    for row in stable_list:
                        try:
                            rate = float(row.get("funding_rate", 0.0))
                        except (TypeError, ValueError):
                            rate = 0.0
                        item = {
                            "symbol": sym,
                            "exchangeName": row.get("exchange", ""),
                            "uMarginList": [{"rate": rate}],
                            "marginType": "USDT",
                            "interval": row.get("funding_rate_interval"),
                        }
                        result.append(item)

                    # COIN маржа
                    for row in token_list:
                        try:
                            rate = float(row.get("funding_rate", 0.0))
                        except (TypeError, ValueError):
                            rate = 0.0
                        item = {
                            "symbol": sym,
                            "exchangeName": row.get("exchange", ""),
                            "uMarginList": [{"rate": rate}],
                            "marginType": "COIN",
                            "interval": row.get("funding_rate_interval"),
                        }
                        result.append(item)

                logger.info("Coinglass v4 funding-rate: получили %d записей", len(result))
                return result

            except requests.exceptions.ReadTimeout:
                logger.warning(
                    "Таймаут при запросе к Coinglass v4 funding-rate (попытка %d/%d)",
                    attempt,
                    MAX_RETRIES,
                )
                if attempt == MAX_RETRIES:
                    return None
            except Exception as e:
                logger.exception(
                    "Ошибка при запросе к Coinglass v4 funding-rate/exchange-list: %s",
                    e,
                )
                return None

    def calculate_funding_arbitrage_from_items(
        self, funding_items: list[dict], symbol: str | None = None, min_spread: float = 0.0005
    ):
        """
        Посчитать арбитраж фандинга по уже загруженному списку funding_items.
        """
        if not funding_items:
            return None

        by_symbol: dict[str, list[tuple[str, float]]] = {}

        for item in funding_items:
            sym = item.get("symbol", "")
            if not sym:
                continue

            if symbol and sym.upper() != symbol.upper():
                continue

            margin_type = item.get("marginType", "USDT")
            if margin_type != "USDT":
                continue

            rate_list = item.get("uMarginList", [{}])
            rate = rate_list[0].get("rate", 0) if rate_list else 0
            exchange = item.get("exchangeName", "") or ""

            if not exchange:
                continue

            try:
                r = float(rate)
            except (TypeError, ValueError):
                continue

            by_symbol.setdefault(sym, []).append((exchange, r))

        opportunities: list[dict] = []

        for sym, ex_rates in by_symbol.items():
            if len(ex_rates) < 2:
                continue

            min_ex, min_rate = min(ex_rates, key=lambda x: x[1])
            max_ex, max_rate = max(ex_rates, key=lambda x: x[1])
            spread = max_rate - min_rate

            if abs(spread) < min_spread:
                continue

            opportunities.append(
                {
                    "symbol": sym,
                    "min_exchange": min_ex,
                    "max_exchange": max_ex,
                    "min_rate": min_rate,
                    "max_rate": max_rate,
                    "spread": spread,
                }
            )

        if not opportunities:
            return None

        opportunities.sort(key=lambda x: abs(x["spread"]), reverse=True)
        return opportunities

    def get_arbitrage_opportunities(self):
        """Ценовой арбитраж по BTC через v3 /futures/market."""
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
                        opportunities.append(
                            {
                                "symbol": symbol,
                                "min_price": min_price,
                                "max_price": max_price,
                                "spread_percent": round(spread_percent, 2),
                                "exchanges": exchanges,
                            }
                        )

        return sorted(opportunities, key=lambda x: x["spread_percent"], reverse=True)


class CryptoArbBot:
    def __init__(self):
        self.api = CoinglassAPI()
        self.application = Application.builder().token(TELEGRAM_TOKEN).build()
        self.funding_cache: list[dict] = []
        self.funding_cache_updated_at: datetime | None = None
        self.setup_handlers()

    async def update_funding_cache(self, context: ContextTypes.DEFAULT_TYPE):
        """Фоновое обновление кэша фандинга."""
        try:
            data = await asyncio.to_thread(self.api.get_funding_rates)
            if data:
                self.funding_cache = data
                self.funding_cache_updated_at = datetime.now(timezone.utc)
                logger.info(
                    "Кэш фандинга обновлён: %d записей", len(self.funding_cache)
                )
            else:
                logger.warning("Кэш фандинга: получены пустые данные от Coinglass")
        except Exception as e:
            logger.exception("Не удалось обновить кэш фандинга: %s", e)

    def get_cached_funding(self, symbol: str | None = None):
        """Вернуть данные из кэша, при необходимости отфильтрованные по монете."""
        if not self.funding_cache:
            return None

        if symbol:
            su = symbol.upper()
            return [
                item
                for item in self.funding_cache
                if item.get("symbol", "").upper() == su
            ]

        return self.funding_cache

    def setup_handlers(self):
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("funding", self.funding_rates))
        self.application.add_handler(CommandHandler("arbitrage", self.arbitrage))
        self.application.add_handler(CommandHandler("top_funding", self.top_funding))
        self.application.add_handler(CommandHandler("arb_funding", self.arb_funding))
        self.application.add_handler(CallbackQueryHandler(self.button_handler))

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [
            [
                InlineKeyboardButton("📊 Фандинг ставки", callback_data="funding"),
                InlineKeyboardButton("💸 Арбитраж цены", callback_data="arbitrage"),
            ],
            [
                InlineKeyboardButton("⚖️ Арбитраж фандинга", callback_data="arb_funding"),
                InlineKeyboardButton("🚀 Топ фандинг", callback_data="top_funding"),
            ],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        welcome_text = (
            "🤖 <b>Crypto Funding &amp; Arbitrage Bot</b>\n\n"
            "Доступные команды:\n"
            "/funding - фандинг ставки по всем парам или /funding BTC\n"
            "/arbitrage - ценовой арбитраж между биржами\n"
            "/top_funding - топ высоких фандинг ставок\n"
            "/arb_funding - арбитраж фандинга между биржами\n\n"
            "Бот работает с полным списком монет и бирж через кэш Coinglass.\n"
            "Используйте кнопки ниже для быстрого доступа!"
        )

        await update.message.reply_text(
            welcome_text, reply_markup=reply_markup, parse_mode="HTML"
        )

    async def funding_rates(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать фандинг ставки (из кэша)"""
        await update.message.reply_text("🔄 Получаю данные о фандинг ставках из кэша...")

        symbol = None
        if context.args:
            symbol = context.args[0].upper()

        funding_data = self.get_cached_funding(symbol)

        if not funding_data:
            await update.message.reply_text(
                "⚠️ Данные по фандингу ещё не загружены или Coinglass не ответил.\n"
                "Попробуй ещё раз через 20–30 секунд."
            )
            return

        header = symbol if symbol else "всех монет"
        response = f"📊 <b>Текущие фандинг ставки для {header}:</b>\n\n"

        # Если указан тикер (например /funding BTC) — показываем все биржи по этой монете
        if symbol:
            items_to_show = sorted(
                funding_data,
                key=lambda x: (x.get("marginType", ""), x.get("exchangeName", "")),
            )
        else:
            # Без тикера — топ по абсолютному значению ставки (чтобы не заспамить чат)
            items_to_show = sorted(
                funding_data,
                key=lambda x: abs(
                    float(x.get("uMarginList", [{}])[0].get("rate", 0) or 0)
                ),
                reverse=True,
            )[:15]

        for item in items_to_show:
            symbol_item = item.get("symbol", "")
            rate_list = item.get("uMarginList", [{}])
            rate = rate_list[0].get("rate", 0) if rate_list else 0
            exchange = item.get("exchangeName", "")
            margin_type = item.get("marginType", "USDT")
            interval = item.get("interval", "?")

            try:
                rate_percent = round(float(rate) * 100, 4)
            except Exception:
                rate_percent = 0

            if rate_percent > 0:
                emoji = "🟢"
            elif rate_percent < 0:
                emoji = "🔴"
            else:
                emoji = "⚪️"

            response += f"{emoji} <b>{symbol_item}</b>\n"
            response += f"   Биржа: {exchange} ({margin_type})\n"
            response += f"   Ставка: {rate_percent}% за {interval}ч\n\n"

        await update.message.reply_text(response, parse_mode="HTML")

    async def arbitrage(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать арбитражные возможности по ЦЕНЕ (BTC)"""
        await update.message.reply_text("🔍 Ищу арбитражные возможности по цене...")

        arb_opportunities = self.api.get_arbitrage_opportunities()

        if not arb_opportunities:
            await update.message.reply_text(
                "🤷‍♂️ Арбитражные ценовые возможности не найдены или ошибка API"
            )
            return

        response = "💸 <b>Арбитражные возможности по цене (BTC):</b>\n\n"

        for opp in arb_opportunities[:10]:
            response += f"🎯 <b>{opp['symbol']}</b>\n"
            response += f"   Спред: {opp['spread_percent']}%\n"
            response += f"   Мин: ${opp['min_price']:.2f}\n"
            response += f"   Макс: ${opp['max_price']:.2f}\n\n"

        await update.message.reply_text(response, parse_mode="HTML")

    async def top_funding(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Топ высоких фандинг ставок (из кэша)"""
        await update.message.reply_text("📈 Ищу самые высокие фандинг ставки в кэше...")

        funding_data = self.get_cached_funding()

        if not funding_data:
            await update.message.reply_text(
                "⚠️ Данные по фандингу ещё не загружены или Coinglass не ответил.\n"
                "Попробуй ещё раз через 20–30 секунд."
            )
            return

        filtered_data = []
        for item in funding_data:
            rate_list = item.get("uMarginList", [{}])
            rate = rate_list[0].get("rate", 0) if rate_list else 0
            try:
                r = float(rate)
            except Exception:
                continue
            if r != 0:
                filtered_data.append(item)

        sorted_data = sorted(
            filtered_data,
            key=lambda x: abs(
                float(x.get("uMarginList", [{}])[0].get("rate", 0) or 0)
            ),
            reverse=True,
        )

        response = "🚀 <b>Топ высоких фандинг ставок:</b>\n\n"

        for i, item in enumerate(sorted_data[:10]):
            symbol_item = item.get("symbol", "")
            rate_list = item.get("uMarginList", [{}])
            rate = rate_list[0].get("rate", 0) if rate_list else 0
            exchange = item.get("exchangeName", "")
            margin_type = item.get("marginType", "USDT")
            interval = item.get("interval", "?")

            try:
                rate_percent = round(float(rate) * 100, 4)
            except Exception:
                rate_percent = 0

            emoji = "📈" if rate_percent > 0 else "📉"

            response += f"{i+1}. {emoji} <b>{symbol_item}</b>\n"
            response += f"   Биржа: {exchange} ({margin_type})\n"
            response += f"   Ставка: {rate_percent}% за {interval}ч\n\n"

        await update.message.reply_text(response, parse_mode="HTML")

    async def arb_funding(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Арбитраж фандинга между биржами (из кэша)"""
        await update.message.reply_text("⚖️ Ищу арбитраж фандинга между биржами...")

        symbol = None
        if context.args:
            symbol = context.args[0].upper()

        items = self.get_cached_funding(symbol)

        if not items:
            await update.message.reply_text(
                "⚠️ Данные по фандингу ещё не загружены или Coinglass не ответил.\n"
                "Попробуй ещё раз через 20–30 секунд."
            )
            return

        opportunities = self.api.calculate_funding_arbitrage_from_items(
            items, symbol=symbol, min_spread=0.0005
        )

        if not opportunities:
            await update.message.reply_text(
                "🤷‍♂️ Арбитраж фандинга не найден для выбранных монет."
            )
            return

        header = (
            f"⚖️ <b>Арбитраж фандинга для {symbol}:</b>\n\n"
            if symbol
            else "⚖️ <b>Арбитраж фандинга (USDT-маржа по всем монетам):</b>\n\n"
        )
        response = header

        for opp in opportunities[:10]:
            sym = opp["symbol"]
            min_ex = opp["min_exchange"]
            max_ex = opp["max_exchange"]
            min_rate = opp["min_rate"] * 100
            max_rate = opp["max_rate"] * 100
            spread = opp["spread"] * 100

            response += f"🎯 <b>{sym}</b>\n"
            response += f"   Мин. ставка: {min_ex} → {min_rate:.4f}%\n"
            response += f"   Макс. ставка: {max_ex} → {max_rate:.4f}%\n"
            response += f"   Спред по фандингу: {spread:.4f}%\n\n"

        response += (
            "💡 Идея: шортить там, где ставка выше, и лонговать там, где ниже/отрицательная, "
            "чтобы зарабатывать на разнице funding. Не забывай про комиссии и риск бирж."
        )

        await update.message.reply_text(response, parse_mode="HTML")

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        if query.data == "funding":
            await self.funding_rates_callback(query)
        elif query.data == "arbitrage":
            await self.arbitrage_callback(query)
        elif query.data == "top_funding":
            await self.top_funding_callback(query)
        elif query.data == "arb_funding":
            await self.arb_funding_callback(query)

    async def funding_rates_callback(self, query):
        await query.edit_message_text("🔄 Получаю данные о фандинг ставках из кэша...")

        funding_data = self.get_cached_funding()

        if not funding_data:
            await query.edit_message_text(
                "⚠️ Данные по фандингу ещё не загружены или Coinglass не ответил.\n"
                "Попробуй ещё раз через 20–30 секунд."
            )
            return

        response = "📊 <b>Текущие фандинг ставки (топ по абсолютному значению):</b>\n\n"

        filtered = sorted(
            funding_data,
            key=lambda x: abs(
                float(x.get("uMarginList", [{}])[0].get("rate", 0) or 0)
            ),
            reverse=True,
        )

        for item in filtered[:12]:
            symbol_item = item.get("symbol", "")
            rate_list = item.get("uMarginList", [{}])
            rate = rate_list[0].get("rate", 0) if rate_list else 0
            exchange = item.get("exchangeName", "")
            margin_type = item.get("marginType", "USDT")
            interval = item.get("interval", "?")

            try:
                rate_percent = round(float(rate) * 100, 4)
            except Exception:
                rate_percent = 0

            if rate_percent > 0:
                emoji = "🟢"
            elif rate_percent < 0:
                emoji = "🔴"
            else:
                emoji = "⚪️"

            response += f"{emoji} <b>{symbol_item}</b>\n"
            response += f"   Биржа: {exchange} ({margin_type})\n"
            response += f"   Ставка: {rate_percent}% за {interval}ч\n\n"

        await query.edit_message_text(response, parse_mode="HTML")

    async def arbitrage_callback(self, query):
        await query.edit_message_text("🔍 Ищу арбитражные возможности по цене...")

        arb_opportunities = self.api.get_arbitrage_opportunities()

        if not arb_opportunities:
            await query.edit_message_text(
                "🤷‍♂️ Арбитражные возможности не найдены или ошибка API"
            )
            return

        response = "💸 <b>Арбитражные возможности по цене (BTC):</b>\n\n"

        for opp in arb_opportunities[:8]:
            response += f"🎯 <b>{opp['symbol']}</b>\n"
            response += f"   Спред: {opp['spread_percent']}%\n"
            response += f"   Мин: ${opp['min_price']:.2f}\n"
            response += f"   Макс: ${opp['max_price']:.2f}\n\n"

        await query.edit_message_text(response, parse_mode="HTML")

    async def top_funding_callback(self, query):
        await query.edit_message_text("📈 Ищу самые высокие фандинг ставки в кэше...")

        funding_data = self.get_cached_funding()

        if not funding_data:
            await query.edit_message_text(
                "⚠️ Данные по фандингу ещё не загружены или Coinglass не ответил.\n"
                "Попробуй ещё раз через 20–30 секунд."
            )
            return

        filtered_data = []
        for item in funding_data:
            rate_list = item.get("uMarginList", [{}])
            rate = rate_list[0].get("rate", 0) if rate_list else 0
            try:
                r = float(rate)
            except Exception:
                continue
            if r != 0:
                filtered_data.append(item)

        sorted_data = sorted(
            filtered_data,
            key=lambda x: abs(
                float(x.get("uMarginList", [{}])[0].get("rate", 0) or 0)
            ),
            reverse=True,
        )

        response = "🚀 <b>Топ высоких фандинг ставок:</b>\n\n"

        for i, item in enumerate(sorted_data[:8]):
            symbol_item = item.get("symbol", "")
            rate_list = item.get("uMarginList", [{}])
            rate = rate_list[0].get("rate", 0) if rate_list else 0
            exchange = item.get("exchangeName", "")
            margin_type = item.get("marginType", "USDT")
            interval = item.get("interval", "?")

            try:
                rate_percent = round(float(rate) * 100, 4)
            except Exception:
                rate_percent = 0

            emoji = "📈" if rate_percent > 0 else "📉"

            response += f"{i+1}. {emoji} <b>{symbol_item}</b>\n"
            response += f"   Биржа: {exchange} ({margin_type})\n"
            response += f"   Ставка: {rate_percent}% за {interval}ч\n\n"

        await query.edit_message_text(response, parse_mode="HTML")

    async def arb_funding_callback(self, query):
        await query.edit_message_text("⚖️ Ищу арбитраж фандинга между биржами...")

        items = self.get_cached_funding()

        if not items:
            await query.edit_message_text(
                "⚠️ Данные по фандингу ещё не загружены или Coinglass не ответил.\n"
                "Попробуй ещё раз через 20–30 секунд."
            )
            return

        opportunities = self.api.calculate_funding_arbitrage_from_items(
            items, symbol=None, min_spread=0.0005
        )

        if not opportunities:
            await query.edit_message_text(
                "🤷‍♂️ Арбитраж фандинга не найден для текущих данных."
            )
            return

        response = "⚖️ <b>Арбитраж фандинга (USDT-маржа по всем монетам):</b>\n\n"

        for opp in opportunities[:8]:
            sym = opp["symbol"]
            min_ex = opp["min_exchange"]
            max_ex = opp["max_exchange"]
            min_rate = opp["min_rate"] * 100
            max_rate = opp["max_rate"] * 100
            spread = opp["spread"] * 100

            response += f"🎯 <b>{sym}</b>\n"
            response += f"   Мин. ставка: {min_ex} → {min_rate:.4f}%\n"
            response += f"   Макс. ставка: {max_ex} → {max_rate:.4f}%\n"
            response += f"   Спред по фандингу: {spread:.4f}%\n\n"

        response += (
            "💡 Идея: использовать разницу funding для квази-маркет-нейтральных стратегий.\n"
            "Всегда учитывай комиссии и риски конкретных бирж."
        )

        await query.edit_message_text(response, parse_mode="HTML")

    def run(self):
        print("🤖 Бот запущен...")

        # фоновое обновление кэша раз в 60 секунд
        self.application.job_queue.run_repeating(
            self.update_funding_cache,
            interval=60,
            first=0,
        )

        self.application.run_polling()


if __name__ == "__main__":
    bot = CryptoArbBot()
    bot.run()
