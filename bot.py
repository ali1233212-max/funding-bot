import logging
import requests
import pandas as pd  # пока не используется, но оставляем на будущее
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# 🔐 ВСТАВЬ СЮДА СВОИ ТОКЕНЫ
# Пример:
# TELEGRAM_TOKEN = "1234567890:AA...."
# COINGLASS_TOKEN = "2d73a0...."

TELEGRAM_TOKEN = "8329955590:AAGk1Nu1LUHhBWQ7bqeorTctzhxie69Wzf0"    # <-- СЮДА токен бота из BotFather
COINGLASS_TOKEN = "2d73a05799f64daab80329868a5264ea"  # <-- СЮДА API-ключ Coinglass


# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


class CoinglassAPI:
    def __init__(self):
        # v3 оставляем только для ценового арбитража (futures/market)
        self.base_url_v3 = "https://open-api.coinglass.com/api/pro/v1"
        # v4 для всего, что связано с фандингом
        self.base_url_v4 = "https://open-api-v4.coinglass.com/api"

        self.headers_v3 = {
            "accept": "application/json",
            "coinglassSecret": COINGLASS_TOKEN,
        }
        self.headers_v4 = {
            "accept": "application/json",
            "CG-API-KEY": COINGLASS_TOKEN,
        }

    # ========== V4: ФАНДИНГ ==========

    def get_funding_exchange_list_v4(self, symbols=None):
        """
        Обёртка над v4 /api/futures/funding-rate/exchange-list.
        Возвращает список entries из data.
        """
        url = f"{self.base_url_v4}/futures/funding-rate/exchange-list"
        params = {}

        if symbols:
            if isinstance(symbols, str):
                params["symbol"] = symbols.upper()
            else:
                params["symbol"] = ",".join(s.upper() for s in symbols)

        try:
            resp = requests.get(
                url, headers=self.headers_v4, params=params, timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") == "0":
                return data.get("data", [])
            logger.warning(
                "Coinglass v4 funding-rate/exchange-list error: %s", data
            )
            return None
        except Exception as e:
            logger.exception(
                "Ошибка при запросе к Coinglass v4 funding-rate/exchange-list: %s", e
            )
            return None

    def get_flat_funding_list_v4(self, symbols=None, include_token_margin: bool = True):
        """
        Уплощённый список ставок фандинга.

        Возвращает список словарей:
        [
          {
            "symbol": "BTC",
            "exchange": "Binance",
            "rate": 0.0073,        # 0.73%
            "interval": 8,
            "margin_type": "USDT"  # или "COIN"
          },
          ...
        ]
        """
        entries = self.get_funding_exchange_list_v4(symbols)
        if not entries:
            return None

        rows = []

        for entry in entries:
            symbol = entry.get("symbol", "")
            stable_list = entry.get("stablecoin_margin_list") or []
            token_list = entry.get("token_margin_list") or []

            # USDT / USD маржа
            for row in stable_list:
                try:
                    rate = float(row.get("funding_rate", 0.0))
                except Exception:
                    continue
                rows.append(
                    {
                        "symbol": symbol,
                        "exchange": row.get("exchange"),
                        "rate": rate,
                        "interval": row.get("funding_rate_interval"),
                        "margin_type": "USDT",
                    }
                )

            # Coin-маржа
            if include_token_margin:
                for row in token_list:
                    try:
                        rate = float(row.get("funding_rate", 0.0))
                    except Exception:
                        continue
                    rows.append(
                        {
                            "symbol": symbol,
                            "exchange": row.get("exchange"),
                            "rate": rate,
                            "interval": row.get("funding_rate_interval"),
                            "margin_type": "COIN",
                        }
                    )

        return rows

    def get_funding_arbitrage(self, symbols=None, min_spread: float = 0.0005):
        """
        Арбитраж фандинга на базе v4 exchange-list:
        для каждой монеты берём min/max ставку по биржам и считаем спред.
        """
        entries = self.get_funding_exchange_list_v4(symbols)
        if not entries:
            return None

        opportunities = []

        for entry in entries:
            symbol = entry.get("symbol")
            stable_list = entry.get("stablecoin_margin_list") or []
            if len(stable_list) < 2:
                continue

            try:
                min_row = min(
                    stable_list, key=lambda r: float(r.get("funding_rate", 0.0))
                )
                max_row = max(
                    stable_list, key=lambda r: float(r.get("funding_rate", 0.0))
                )
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

    # ========== V3: ЦЕНОВОЙ АРБИТРАЖ (оставляем как было) ==========

    def get_arbitrage_opportunities(self):
        """
        Получить арбитражные возможности по ценам между биржами (старый v3 эндпоинт).
        """
        url = f"{self.base_url_v3}/futures/market"
        params = {"symbol": "BTC"}

        try:
            resp = requests.get(
                url, headers=self.headers_v3, params=params, timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("success"):
                return self._calculate_price_arbitrage(data.get("data", []))
            logger.warning(
                "Coinglass v3 futures/market вернул неуспех: %s", data
            )
            return None
        except Exception as e:
            logger.exception(
                "Ошибка при запросе к Coinglass v3 futures/market: %s", e
            )
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
                welcome_text, reply_markup=reply_markup, parse_mode="HTML"
            )
        elif update.callback_query:
            await update.callback_query.edit_message_text(
                welcome_text, reply_markup=reply_markup, parse_mode="HTML"
            )

    # ---------- /funding ----------

    async def funding_rates(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать фандинг ставки по v4 (exchange-list)"""
        if not update.message:
            return

        await update.message.reply_text("🔄 Получаю данные о фандинг ставках...")

        symbol = None
        if context.args:
            symbol = context.args[0].upper()

        rows = self.api.get_flat_funding_list_v4(symbols=symbol)

        if not rows:
            await update.message.reply_text("❌ Ошибка получения данных от Coinglass API")
            return

        # сортируем по абсолютному значению ставки, самые «жирные» сверху
        rows_sorted = sorted(rows, key=lambda r: abs(r["rate"]), reverse=True)

        header_symbol = symbol if symbol else "всех монет (топ по фандингу)"
        response = f"📊 <b>Текущие фандинг ставки для {header_symbol}:</b>\n\n"

        for row in rows_sorted[:15]:
            rate_percent = row["rate"] * 100
            emoji = "🟢" if rate_percent > 0 else "🔴" if rate_percent < 0 else "⚪️"
            margin_tag = "USDT" if row["margin_type"] == "USDT" else "COIN"
            interval = row["interval"] if row["interval"] is not None else "?"

            response += (
                f"{emoji} <b>{row['symbol']}</b> — {row['exchange']} ({margin_tag})\n"
                f"   Ставка: {rate_percent:.4f}% за {interval}ч\n\n"
            )

        await update.message.reply_text(response, parse_mode="HTML")

    # ---------- /arbitrage ----------

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

        for opp in arb_opportunities[:10]:
            response += f"🎯 <b>{opp['symbol']}</b>\n"
            response += f"   Спред: {opp['spread_percent']}%\n"
            response += f"   Мин: ${opp['min_price']:.2f}\n"
            response += f"   Макс: ${opp['max_price']:.2f}\n\n"

        await update.message.reply_text(response, parse_mode="HTML")

    # ---------- /top_funding ----------

    async def top_funding(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Топ высоких фандинг ставок по v4 (только USDT/USD маржа)"""
        if not update.message:
            return

        await update.message.reply_text("📈 Ищу самые высокие фандинг ставки...")

        rows = self.api.get_flat_funding_list_v4(
            symbols=None, include_token_margin=False
        )

        if not rows:
            await update.message.reply_text("❌ Ошибка получения данных от Coinglass API")
            return

        rows_sorted = sorted(rows, key=lambda r: abs(r["rate"]), reverse=True)

        response = "🚀 <b>Топ высоких фандинг ставок (USDT/USD):</b>\n\n"

        for i, row in enumerate(rows_sorted[:10], start=1):
            rate_percent = row["rate"] * 100
            emoji = "📈" if rate_percent > 0 else "📉" if rate_percent < 0 else "⚪️"
            interval = row["interval"] if row["interval"] is not None else "?"

            response += (
                f"{i}. {emoji} <b>{row['symbol']}</b>\n"
                f"   Биржа: {row['exchange']}\n"
                f"   Ставка: {rate_percent:.4f}% за {interval}ч\n\n"
            )

        await update.message.reply_text(response, parse_mode="HTML")

    # ---------- /arb_funding ----------

    async def arb_funding(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Арбитраж фандинга между биржами (v4)"""
        if not update.message:
            return

        await update.message.reply_text("⚖️ Ищу арбитраж фандинга между биржами...")

        symbols = None
        if context.args:
            symbols = [context.args[0].upper()]

        opportunities = self.api.get_funding_arbitrage(
            symbols=symbols, min_spread=0.0005
        )

        if not opportunities:
            await update.message.reply_text(
                "🤷‍♂️ Арбитраж фандинга не найден (или недоступен по твоему тарифу API)."
            )
            r
