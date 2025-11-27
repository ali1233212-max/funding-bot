mport logging
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
    def __init__(self):
        self.base_url_v4 = "https://open-api-v4.coinglass.com/api"
        self.headers_v4 = {
            "accept": "application/json",
            "CG-API-KEY": COINGLASS_TOKEN,
        }

    def get_funding_rates(self):
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
                            
                        item = {
                            "symbol": sym,
                            "exchangeName": row.get("exchange", ""),
                            "rate": rate,
                            "marginType": "USDT",
                            "interval": row.get("funding_rate_interval", "?"),
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
                            "rate": rate,
                            "marginType": "COIN",
                            "interval": row.get("funding_rate_interval", "?"),
                        }
                        result.append(item)

                logger.info("Coinglass v4 funding-rate: получили %d записей", len(result))
                return result

            except requests.exceptions.ReadTimeout:
                logger.warning("Таймаут при запросе к Coinglass v4 (попытка %d/%d)", attempt, MAX_RETRIES)
                if attempt == MAX_RETRIES:
                    return None
            except Exception as e:
                logger.exception("Ошибка при запросе к Coinglass v4: %s", e)
                return None

class CryptoArbBot:
    def __init__(self):
        self.api = CoinglassAPI()
        self.application = Application.builder().token(TELEGRAM_TOKEN).build()
        self.funding_cache = []
        self.funding_cache_updated_at = None
        self.setup_handlers()

    async def update_funding_cache(self, context: ContextTypes.DEFAULT_TYPE):
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

    def get_filtered_funding(self, funding_type="all"):
        if not self.funding_cache:
            return None

        if funding_type == "negative":
            filtered = [item for item in self.funding_cache if item.get("rate", 0) < 0]
            return sorted(filtered, key=lambda x: x["rate"])
        elif funding_type == "positive":
            filtered = [item for item in self.funding_cache if item.get("rate", 0) > 0]
            return sorted(filtered, key=lambda x: x["rate"], reverse=True)
        else:
            return self.funding_cache

    def setup_handlers(self):
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("negative", self.show_negative))
        self.application.add_handler(CommandHandler("positive", self.show_positive))
        self.application.add_handler(CommandHandler("top10", self.show_top10))
        self.application.add_handler(CommandHandler("arbitrage_bundles", self.show_arbitrage_bundles))
        self.application.add_handler(CallbackQueryHandler(self.button_handler, pattern="^(page_|nav_|funding_)"))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

        # Добавляем обработчик ошибок
        self.application.add_error_handler(self.error_handler)

    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.error("Exception while handling an update:", exc_info=context.error)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [
            [InlineKeyboardButton("🔴 Все отрицательные", callback_data="nav_negative_1")],
            [InlineKeyboardButton("🟢 Все положительные", callback_data="nav_positive_1")],
            [InlineKeyboardButton("🚀 Топ 10 лучших", callback_data="nav_top10")],
            [InlineKeyboardButton("⚖️ Связки арбитража", callback_data="nav_arbitrage")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        welcome_text = (
            "🤖 <b>Crypto Funding & Arbitrage Bot</b>\n\n"
            "Доступные команды:\n"
            "/negative - все отрицательные фандинги\n"
            "/positive - все положительные фандинги\n"
            "/top10 - топ 10 лучших фандингов\n"
            "/arbitrage_bundles - связки арбитража\n\n"
            "Используйте кнопки ниже для быстрого доступа!"
        )

        await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode="HTML")

    async def show_negative(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.show_funding_page(update, context, "negative", 1)

    async def show_positive(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.show_funding_page(update, context, "positive", 1)

    async def show_funding_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE, funding_type: str, page: int):
        if not self.funding_cache:
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text("⚠️ Данные ещё не загружены. Попробуйте через 30 секунд.")
            else:
                await update.message.reply_text("⚠️ Данные ещё не загружены. Попробуйте через 30 секунд.")
            return

        filtered_data = self.get_filtered_funding(funding_type)
        if not filtered_data:
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text("🤷‍♂️ Нет данных для отображения.")
            else:
                await update.message.reply_text("🤷‍♂️ Нет данных для отображения.")
            return

        items_per_page = 20
        total_pages = (len(filtered_data) + items_per_page - 1) // items_per_page
        page = max(1, min(page, total_pages))
        
        start_idx = (page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        page_data = filtered_data[start_idx:end_idx]

        # Сохраняем состояние для пагинации
        context.user_data.update({
            'current_page': page,
            'total_pages': total_pages,
            'current_data_type': funding_type,
            'current_data': filtered_data
        })

        # Создаем сообщение
        title = "🔴 Отрицательные фандинги" if funding_type == "negative" else "🟢 Положительные фандинги"
        response = f"<b>{title}</b>\n"
        response += f"Страница {page}/{total_pages} | Всего: {len(filtered_data)}\n\n"

        for i, item in enumerate(page_data, start=start_idx + 1):
            symbol = item.get("symbol", "")
            exchange = item.get("exchangeName", "")
            rate = item.get("rate", 0) * 100
            interval = item.get("interval", "?")
            
            response += f"{i}. <b>{symbol}</b>\n"
            response += f"   🏛️ {exchange} | {rate:+.4f}% | {interval}ч\n\n"

        # Создаем клавиатуру пагинации
        keyboard = []
        if total_pages > 1:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton("◀ Назад", callback_data=f"page_{funding_type}_{page-1}"))
            
            nav_buttons.append(InlineKeyboardButton(f"[{page}/{total_pages}]", callback_data="page_info"))
            
            if page < total_pages:
                nav_buttons.append(InlineKeyboardButton("Вперед ▶", callback_data=f"page_{funding_type}_{page+1}"))
            
            keyboard.append(nav_buttons)

        keyboard.append([InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")])
        reply_markup = InlineKeyboardMarkup(keyboard)

        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(response, reply_markup=reply_markup, parse_mode="HTML")
        else:
            await update.message.reply_text(response, reply_markup=reply_markup, parse_mode="HTML")

    async def show_top10(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.funding_cache:
            await update.message.reply_text("⚠️ Данные ещё не загружены. Попробуйте через 30 секунд.")
            return

        # Получаем топ 10 положительных и отрицательных
        positive_data = self.get_filtered_funding("positive")[:10]
        negative_data = self.get_filtered_funding("negative")[:10]

        response = "<b>🚀 Топ 10 лучших фандингов</b>\n\n"
        
        response += "<b>🟢 Топ 10 положительных:</b>\n"
        for i, item in enumerate(positive_data, 1):
            symbol = item.get("symbol", "")
            exchange = item.get("exchangeName", "")
            rate = item.get("rate", 0) * 100
            interval = item.get("interval", "?")
            response += f"{i}. <b>{symbol}</b> - {rate:+.4f}% ({exchange}, {interval}ч)\n"

        response += "\n<b>🔴 Топ 10 отрицательных:</b>\n"
        for i, item in enumerate(negative_data, 1):
            symbol = item.get("symbol", "")
            exchange = item.get("exchangeName", "")
            rate = item.get("rate", 0) * 100
            interval = item.get("interval", "?")
            response += f"{i}. <b>{symbol}</b> - {rate:+.4f}% ({exchange}, {interval}ч)\n"

        keyboard = [[InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(response, reply_markup=reply_markup, parse_mode="HTML")

    async def show_arbitrage_bundles(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.funding_cache:
            await update.message.reply_text("⚠️ Данные ещё не загружены. Попробуйте через 30 секунд.")
            return

        # Группируем данные по символам
        symbol_data = {}
        for item in self.funding_cache:
            symbol = item.get("symbol", "")
            if symbol not in symbol_data:
                symbol_data[symbol] = []
            
            symbol_data[symbol].append({
                'exchange': item.get("exchangeName", ""),
                'rate': item.get("rate", 0),
                'interval': item.get("interval", "?")
            })

        # Ищем арбитражные возможности
        opportunities = []
        for symbol, exchanges in symbol_data.items():
            if len(exchanges) < 2:
                continue

            # Находим мин и макс ставки
            min_item = min(exchanges, key=lambda x: x['rate'])
            max_item = max(exchanges, key=lambda x: x['rate'])
            
            spread = max_item['rate'] - min_item['rate']
            if abs(spread) < 0.0005:  # Минимальный спред 0.05%
                continue

            # Проверяем время выплат
            time_warning = ""
            if min_item['interval'] != max_item['interval']:
                time_warning = " ⚠️ РАЗНОЕ ВРЕМЯ ВЫПЛАТ!"

            opportunities.append({
                'symbol': symbol,
                'min_exchange': min_item['exchange'],
                'max_exchange': max_item['exchange'],
                'min_rate': min_item['rate'],
                'max_rate': max_item['rate'],
                'spread': spread,
                'time_warning': time_warning
            })

        # Сортируем по спреду
        opportunities.sort(key=lambda x: abs(x['spread']), reverse=True)

        response = "<b>⚖️ Связки арбитража</b>\n\n"
        
        if not opportunities:
            response += "🤷‍♂️ Арбитражные возможности не найдены"
        else:
            for opp in opportunities[:15]:
                response += f"<b>{opp['symbol']}</b>{opp['time_warning']}\n"
                response += f"📉 {opp['min_exchange']}: {opp['min_rate']*100:+.4f}%\n"
                response += f"📈 {opp['max_exchange']}: {opp['max_rate']*100:+.4f}%\n"
                response += f"💰 Спред: {opp['spread']*100:.4f}%\n\n"

        keyboard = [[InlineKeyboardButton("📋 Главное меню", callback_data="nav_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(response, reply_markup=reply_markup, parse_mode="HTML")

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
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
                    
        except Exception as e:
            logger.error("Ошибка в обработчике кнопок: %s", e)
            try:
                await query.edit_message_text("❌ <b>Произошла ошибка</b>\nПопробуйте еще раз.", parse_mode="HTML")
            except Exception:
                # Если не удалось отредактировать сообщение, отправляем новое
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="❌ <b>Произошла ошибка</b>\nПопробуйте еще раз.",
                    parse_mode="HTML"
                )

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        
        # Проверяем, является ли сообщение номером страницы
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
            "/negative /positive /top10 /arbitrage_bundles",
            parse_mode="HTML"
        )

    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [
            [InlineKeyboardButton("🔴 Все отрицательные", callback_data="nav_negative_1")],
            [InlineKeyboardButton("🟢 Все положительные", callback_data="nav_positive_1")],
            [InlineKeyboardButton("🚀 Топ 10 лучших", callback_data="nav_top10")],
            [InlineKeyboardButton("⚖️ Связки арбитража", callback_data="nav_arbitrage")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = "📋 <b>Главное меню</b>\nВыберите раздел:"
        
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode="HTML")
        else:
            await update.message.reply_text(text, reply_markup=reply_markup, parse_mode="HTML")

    def run(self):
        print("🤖 Бот запущен...")
        print("⚡ Кеширование каждые 30 секунд")
        
        # Фоновое обновление кэша каждые 30 секунд
        self.application.job_queue.run_repeating(
            self.update_funding_cache,
            interval=30,
            first=0,
        )
        
        # Запускаем бота с обработкой ошибок
        try:
            self.application.run_polling(drop_pending_updates=True)
        except Exception as e:
            logger.error("Ошибка при запуске бота: %s", e)
            print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    bot = CryptoArbBot()
    bot.run()
