import os
import logging
import requests
import pandas as pd  # оставляю, как просила, на будущее
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# 🔐 СЮДА ВСТАВЬ СВОИ ТОКЕНЫ (СТРОКАМИ БЕЗ КАВЫЧЕК СБОКУ)
# Например: TELEGRAM_TOKEN = "1234567890:AA...."
#           COINGLASS_TOKEN = "2d73a0...."

TELEGRAM_TOKEN = "8329955590:AAGk1Nu1LUHhBWQ7bqeorTctzhxie69Wzf0"      # <-- ВСТАВЬ СВОЙ TELEGRAM ТОКЕН
COINGLASS_TOKEN = "2d73a05799f64daab80329868a5264ea"    # <-- ВСТАВЬ СВОЙ COINGLASS ТОКЕН

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class CoinglassAPI:
    def __init__(self):
        # V3 для старых эндпоинтов
        self.base_url_v3 = "https://open-api.coinglass.com/api/pro/v1"
        # V4 для нового фандинг-арбитража
        self.base_url_v4 = "https://open-api-v4.coinglass.com/api"

        self.headers_v3 = {
            'accept': 'application/json',
            'coinglassSecret': COINGLASS_TOKEN,
        }
        self.headers_v4 = {
            'accept': 'application/json',
            'CG-API-KEY': COINGLASS_TOKEN,
        }

    def get_funding_rates(self, symbol: str | None = None):
        """
        Получить ставки фандинга (старый v3 эндпоинт).
        Возвращает data или None.
        """
        url = f"{self.base_url_v3}/futures/funding_rates"
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()

        try:
            resp = requests.get(url, headers=self.headers_v3, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get("success"):
                return data.get("data", [])
            logger.warning("Coinglass v3 funding_rates вернул неуспех: %s", data)
            return None
        except Exception as e:
            logger.exception("Ошибка при запросе к Coinglass v3 funding_rates: %s", e)
            return None

    def get_arbitrage_opportunities(self):
        """
        Получить арбитражные возможности по ценам между биржами (старый v3 эндпоинт).
        """
        url = f"{self.base_url_v3}/futures/market"
        params = {"symbol": "BTC"}

        try:
            resp = requests.get(url, headers=self.headers_v3, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get("success"):
                return self._calculate_price_arbitrage(data.get("data", []))
            logger.warning("Coinglass v3 futures/market вернул неуспех: %s", data)
            return None
        except Exception as e:
            logger.exception("Ошибка при запросе к Coinglass v3 futures/market: %s", e)
            return None

    def _calculate_price_arbitrage(self, market_data):
        """
        Считает спреды по ценам между биржами.
        """
        opportunities = []

        for coin_data in market_data:
            symbol = coin_data.get("symbol", "")
            exchanges = coin_data.get("exchangeName", [])
            prices = coin_data.get("price", [])

            if not prices or len(prices) < 2:
                continue

            try:
                prices_float = [float(p) for p in prices]
            except Exception:
                continue

            min_price = min(prices_float)
            max_price = max(prices_float)

            if min_price <= 0:
                continue

            spread_percent = (max_price - min_price) / min_price * 100

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

    def get_funding_arbitrage(self, symbols=None, min_spread: float = 0.0005):
        """
        Арбитраж фандинга на v4 эндпоинте:
        /api/futures/funding-rate/exchange-list

        Возвращает список словарей:
        {
          symbol, min_exchange, max_exchange,
          min_rate, max_rate, spread
        }
        spread и rate в долях (0.01 = 1%)
        """
        url = f"{self.base_url_v4}/futures/funding-rate/exchange-list"
        params = {}
        if symbols:
            params["symbol"] = ",".join([s.upper() for s in symbols])

        try:
            resp = requests.get(url, headers=self.headers_v4, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != "0":
                logger.warning("Coinglass v4 funding-rate/exchange-list error: %s", data)
                return None

            opportunities = []

            for entry in data.get("data", []):
                symbol = entry.get("symbol")
                stable_list = entry.get("stablecoin_margin_list") or []
                if len(stable_list) < 2:
                    continue

                try:
                    min_row = min(stable_list, key=lambda r: float(r.get("funding_rate", 0.0)))
                    max_row = max(stable_list, key=lambda r: float(r.get("funding_rate", 0.0)))
                    min_rate = float(min_row.get("funding_rate", 0.0))
                    max_rate = float(max_row.get("funding_rate", 0.0))
                except Exception:
                    continue

                spread = max_rate - min_rate
                if abs(spread) < min_spread:
                    continue

                opportunities.append(
                    {
                        "symbol": symbol,
                        "min_exchange": min_row.get("exchange"),
                        "max_exchange": max_row.get("exchange"),
                        "min_rate": min_rate,
                        "max_rate": max_rate,
                        "spread": spread,
                    }
                )

            opportunities.sort(key=lambda x: abs(x["spread"]), reverse=True)
            return opportunities

        except Exception as e:
            logger.exception("Ошибка при запросе к Coinglass v4 funding-rate/exchange-list: %s", e)
            return None


class CryptoArbBot:
    def __init__(self):
        self.api = CoinglassAPI()
        self.application = Application.builder().token(TELEGRAM_TOKEN).build()
        self.setup_handlers()

    def setup_handlers(self):
        """Настройка обработчиков команд"""
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("funding", self.funding_rates))
        self.application.add_handler(CommandHandler("arbitrage", self.arbitrage))
        self.application.add_handler(CommandHandler("top_funding", self.top_funding))
        self.application.add_handler(CommandHandler("arb_funding", self.arb_funding))
        self.application.add_handler(CallbackQueryHandler(self.button_handler))

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        keyboard = [
            [
                InlineKeyboardButton("📊 Фандинг ставки", callback_data="funding"),
                InlineKeyboardButton("💸 Арбитраж цен", callback_data="arbitrage"),
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
            "/funding – фандинг ставки по всем парам или /funding BTC\n"
            "/arbitrage – ценовой арбитраж между биржами\n"
            "/top_funding – топ высоких фандинг ставок\n"
            "/arb_funding – арбитраж фандинга между биржами\n\n"
            "Используйте кнопки ниже для быстрого доступа!"
        )

        if update.message:
            await update.message.reply_text(
                welcome_text,
                reply_markup=reply_markup,
                parse_mode="HTML",
            )
        elif update.callback_query:
            await update.callback_query.edit_message_text(
                welcome_text,
                reply_markup=reply_markup,
                parse_mode="HTML",
            )

    async def funding_rates(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать фандинг ставки (по v3 API)"""
        if not update.message:
            return

        await update.message.reply_text("🔄 Получаю данные о фандинг ставках...")

        symbol = None
        if context.args:
            symbol = context.args[0]

        funding_data = self.api.get_funding_rates(symbol=symbol)

        if not funding_data:
            await update.message.reply_text("❌ Ошибка получения данных от Coinglass API")
            return

        header_symbol = symbol.upper() if symbol else "всех монет"
        response = f"📊 <b>Текущие фандинг ставки для {header_symbol}:</b>\n\n"

        for i, item in enumerate(funding_data[:15]):  # Ограничиваем вывод
            sym = item.get("symbol", "")
            rate_list = item.get("uMarginList", [{}])
            rate = rate_list[0].get("rate", 0) if rate_list else 0
            exchange = item.get("exchangeName", "")

            try:
                rate_percent = round(float(rate) * 100, 4)
            except Exception:
                rate_percent = 0.0

            emoji = "🟢" if rate_percent > 0 else "🔴"

            response += f"{emoji} <b>{sym}</b>\n"
            if exchange:
                response += f"   Биржа: {exchange}\n"
            response += f"   Ставка: {rate_percent}%\n\n"

        await update.message.reply_text(response, parse_mode="HTML")

    async def arbitrage(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать арбитражные возможности по цене (v3)"""
        if not update.message:
            return

        await update.message.reply_text("🔍 Ищу арбитражные возможности по цене...")

        arb_opportunities = self.api.get_arbitrage_opportunities()

        if not arb_opportunities:
            await update.message.reply_text(
                "🤷‍♂️ Арбитражные ценовые возможности не найдены или ошибка API"
            )
            return

        response = "💸 <b>Арбитражные возможности по цене:</b>\n\n"

        for opp in arb_opportunities[:10]:  # Топ 10 возможностей
            response += f"🎯 <b>{opp['symbol']}</b>\n"
            response += f"   Спред: {opp['spread_percent']}%\n"
            response += f"   Мин: ${opp['min_price']:.2f}\n"
            response += f"   Макс: ${opp['max_price']:.2f}\n\n"

        await update.message.reply_text(response, parse_mode="HTML")

    async def top_funding(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Топ высоких фандинг ставок по v3"""
        if not update.message:
            return

        await update.message.reply_text("📈 Ищу самые высокие фандинг ставки...")

        funding_data = self.api.get_funding_rates()

        if not funding_data:
            await update.message.reply_text("❌ Ошибка получения данных от Coinglass API")
            return

        filtered_data = []
        for item in funding_data:
            rate_list = item.get("uMarginList", [{}])
            rate = rate_list[0].get("rate", 0) if rate_list else 0
            try:
                r = float(rate)
            except Exception:
                continue
            if r != 0.0:
                filtered_data.append(item)

        sorted_data = sorted(
            filtered_data,
            key=lambda x: abs(
                float(x.get("uMarginList", [{}])[0].get("rate", 0) or 0.0)
            ),
            reverse=True,
        )

        response = "🚀 <b>Топ высоких фандинг ставок:</b>\n\n"

        for i, item in enumerate(sorted_data[:10]):
            sym = item.get("symbol", "")
            rate_list = item.get("uMarginList", [{}])
            rate = rate_list[0].get("rate", 0) if rate_list else 0
            exchange = item.get("exchangeName", "")

            try:
                rate_percent = round(float(rate) * 100, 4)
            except Exception:
                rate_percent = 0.0

            emoji = "📈" if rate_percent > 0 else "📉"

            response += f"{i + 1}. {emoji} <b>{sym}</b>\n"
            if exchange:
                response += f"   Биржа: {exchange}\n"
            response += f"   Ставка: {rate_percent}%\n\n"

        await update.message.reply_text(response, parse_mode="HTML")

    async def arb_funding(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Арбитраж фандинга между биржами (v4)"""
        if not update.message:
            return

        await update.message.reply_text("⚖️ Ищу арбитраж фандинга между биржами...")

        symbols = None
        if context.args:
            symbols = [context.args[0]]

        opportunities = self.api.get_funding_arbitrage(symbols=symbols, min_spread=0.0005)

        if not opportunities:
            await update.message.reply_text(
                "🤷‍♂️ Арбитраж фандинга не найден (или недоступен по твоему тарифу API)."
            )
            return

        header = (
            f"⚖️ <b>Арбитраж фандинга для {symbols[0].upper()}:</b>\n\n"
            if symbols
            else "⚖️ <b>Арбитраж фандинга (USDT/USD маржа):</b>\n\n"
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
            response += (
                f"   Мин. ставка: {min_ex} → {min_rate:.4f}%\n"
                f"   Макс. ставка: {max_ex} → {max_rate:.4f}%\n"
                f"   Спред по фандингу: {spread:.4f}%\n\n"
            )

        response += (
            "💡 Логика: можно шортить на бирже с высокой ставкой и лонговать "
            "на бирже с низкой (или отрицательной), чтобы зарабатывать на разнице funding.\n"
            "Обязательно учитывай комиссии и риск бирж."
        )

        await update.message.reply_text(response, parse_mode="HTML")

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик нажатий на кнопки"""
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
        """Обработчик кнопки фандинга"""
        await query.edit_message_text("🔄 Получаю данные о фандинг ставках...")
        funding_data = self.api.get_funding_rates()

        if not funding_data:
            await query.edit_message_text("❌ Ошибка получения данных от Coinglass API")
            return

        response = "📊 <b>Текущие фандинг ставки:</b>\n\n"

        for i, item in enumerate(funding_data[:12]):
            sym = item.get("symbol", "")
            rate_list = item.get("uMarginList", [{}])
            rate = rate_list[0].get("rate", 0) if rate_list else 0
            exchange = item.get("exchangeName", "")

            try:
                rate_percent = round(float(rate) * 100, 4)
            except Exception:
                rate_percent = 0.0

            emoji = "🟢" if rate_percent > 0 else "🔴"

            response += f"{emoji} <b>{sym}</b>\n"
            if exchange:
                response += f"   Биржа: {exchange}\n"
            response += f"   Ставка: {rate_percent}%\n\n"

        await query.edit_message_text(response, parse_mode="HTML")

    async def arbitrage_callback(self, query):
        """Обработчик кнопки арбитража по цене"""
        await query.edit_message_text("🔍 Ищу арбитражные возможности по цене...")
        arb_opportunities = self.api.get_arbitrage_opportunities()

        if not arb_opportunities:
            await query.edit_message_text(
                "🤷‍♂️ Арбитражные ценовые возможности не найдены или ошибка API"
            )
            return

        response = "💸 <b>Арбитражные возможности по цене:</b>\n\n"

        for opp in arb_opportunities[:8]:
            response += f"🎯 <b>{opp['symbol']}</b>\n"
            response += f"   Спред: {opp['spread_percent']}%\n"
            response += f"   Мин: ${opp['min_price']:.2f}\n"
            response += f"   Макс: ${opp['max_price']:.2f}\n\n"

        await query.edit_message_text(response, parse_mode="HTML")

    async def top_funding_callback(self, query):
        """Обработчик кнопки топа фандинга"""
        await query.edit_message_text("📈 Ищу самые высокие фандинг ставки...")
        funding_data = self.api.get_funding_rates()

        if not funding_data:
            await query.edit_message_text("❌ Ошибка получения данных от Coinglass API")
            return

        filtered_data = []
        for item in funding_data:
            rate_list = item.get("uMarginList", [{}])
            rate = rate_list[0].get("rate", 0) if rate_list else 0
            try:
                r = float(rate)
            except Exception:
                continue
            if r != 0.0:
                filtered_data.append(item)

        sorted_data = sorted(
            filtered_data,
            key=lambda x: abs(
                float(x.get("uMarginList", [{}])[0].get("rate", 0) or 0.0)
            ),
            reverse=True,
        )

        response = "🚀 <b>Топ высоких фандинг ставок:</b>\n\n"

        for i, item in enumerate(sorted_data[:8]):
            sym = item.get("symbol", "")
            rate_list = item.get("uMarginList", [{}])
            rate = rate_list[0].get("rate", 0) if rate_list else 0
            exchange = item.get("exchangeName", "")

            try:
                rate_percent = round(float(rate) * 100, 4)
            except Exception:
                rate_percent = 0.0

            emoji = "📈" if rate_percent > 0 else "📉"

            response += f"{i + 1}. {emoji} <b>{sym}</b>\n"
            if exchange:
                response += f"   Биржа: {exchange}\n"
            response += f"   Ставка: {rate_percent}%\n\n"

        await query.edit_message_text(response, parse_mode="HTML")

    async def arb_funding_callback(self, query):
        """Обработчик кнопки арбитража фандинга"""
        await query.edit_message_text("⚖️ Ищу арбитраж фандинга между биржами...")

        opportunities = self.api.get_funding_arbitrage(symbols=None, min_spread=0.0005)

        if not opportunities:
            await query.edit_message_text(
                "🤷‍♂️ Арбитраж фандинга не найден (или недоступен по твоему тарифу API)."
            )
            return

        response = "⚖️ <b>Арбитраж фандинга (USDT/USD маржа):</b>\n\n"

        for opp in opportunities[:8]:
            sym = opp["symbol"]
            min_ex = opp["min_exchange"]
            max_ex = opp["max_exchange"]
            min_rate = opp["min_rate"] * 100
            max_rate = opp["max_rate"] * 100
            spread = opp["spread"] * 100

            response += f"🎯 <b>{sym}</b>\n"
            response += (
                f"   Мин. ставка: {min_ex} → {min_rate:.4f}%\n"
                f"   Макс. ставка: {max_ex} → {max_rate:.4f}%\n"
                f"   Спред по фандингу: {spread:.4f}%\n\n"
            )

        response += (
            "💡 Идея: использовать разницу funding для квази-маркет-нейтральных стратегий.\n"
            "Всегда учитывай комиссии, свопы и риски конкретных бирж."
        )

        await query.edit_message_text(response, parse_mode="HTML")

    def run(self):
        """Запуск бота"""
        print("🤖 Бот запущен...")
        self.application.run_polling()


if __name__ == "__main__":
    bot = CryptoArbBot()
    bot.run()
