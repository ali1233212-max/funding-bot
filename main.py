import asyncio
import aiohttp
import logging
from datetime import datetime
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from typing import List, Dict, Tuple
import json

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FundingRateBot:
    def __init__(self):
        self.exchanges = {
            'binance': 'https://fapi.binance.com/fapi/v1/premiumIndex',
            'bybit': 'https://api.bybit.com/v2/public/tickers',
            'mexc': 'https://contract.mexc.com/api/v1/contract/detail',
            'okx': 'https://www.okx.com/api/v5/public/funding-rate',
            'htx': 'https://api.hbdm.com/swap-api/v1/swap_contract_info',
            'lbank': 'https://api.lbank.info/v2/futures/fundingRate.do',
            'bitget': 'https://api.bitget.com/api/mix/v1/market/contracts',
            'gate': 'https://api.gateio.ws/api/v4/futures/usdt/contracts',
            'bingx': 'https://api.bingx.com/openApi/swap/v2/quote/fundingRate'
        }
        
        # Периодичности выплат для разных бирж (в часах)
        self.funding_intervals = {
            'binance': 8,      # 3 раза в сутки
            'bybit': 8,        # 3 раза в сутки  
            'mexc': 8,         # 3 раза в сутки
            'okx': 8,          # 3 раза в сутки
            'htx': 8,          # 3 раза в сутки
            'lbank': 8,        # 3 раза в сутки
            'bitget': 8,       # 3 раза в сутки
            'gate': 8,         # 3 раза в сутки
            'bingx': 8         # 3 раза в сутки
        }

    async def fetch_exchange_data(self, session: aiohttp.ClientSession, exchange: str, url: str) -> List[Dict]:
        """Получение данных с биржи"""
        try:
            async with session.get(url, timeout=10) as response:
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
        """Парсинг данных в зависимости от биржи"""
        funding_data = []
        
        try:
            if exchange == 'binance':
                for item in data:
                    if 'lastFundingRate' in item:
                        symbol = item['symbol']
                        funding_rate = float(item['lastFundingRate']) * 100  # в процентах
                        interval_hours = self.funding_intervals[exchange]
                        daily_payments = 24 / interval_hours
                        annual_yield = funding_rate * daily_payments * 365
                        
                        funding_data.append({
                            'exchange': exchange,
                            'symbol': symbol,
                            'funding_rate': funding_rate,
                            'interval_hours': interval_hours,
                            'daily_payments': daily_payments,
                            'annual_yield': annual_yield
                        })
                        
            elif exchange == 'bybit':
                if 'result' in data:
                    for item in data['result']:
                        if 'funding_rate' in item:
                            symbol = item['symbol']
                            funding_rate = float(item['funding_rate']) * 100
                            interval_hours = self.funding_intervals[exchange]
                            daily_payments = 24 / interval_hours
                            annual_yield = funding_rate * daily_payments * 365
                            
                            funding_data.append({
                                'exchange': exchange,
                                'symbol': symbol,
                                'funding_rate': funding_rate,
                                'interval_hours': interval_hours,
                                'daily_payments': daily_payments,
                                'annual_yield': annual_yield
                            })
            
            # Аналогичные парсеры для других бирж...
            # Для демонстрации добавим заглушки
            elif exchange in ['mexc', 'okx', 'htx', 'lbank', 'bitget', 'gate', 'bingx']:
                # В реальной реализации здесь будут конкретные парсеры для каждой биржи
                logger.info(f"Парсер для {exchange} требует реализации")
                
        except Exception as e:
            logger.error(f"Ошибка парсинга {exchange}: {e}")
            
        return funding_data

    async def get_all_funding_rates(self) -> List[Dict]:
        """Получение всех funding rates со всех бирж"""
        all_data = []
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for exchange, url in self.exchanges.items():
                task = self.fetch_exchange_data(session, exchange, url)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks)
            for result in results:
                all_data.extend(result)
                
        return all_data

    def sort_funding_rates(self, data: List[Dict], sort_type: str = 'negative') -> List[Dict]:
        """Сортировка funding rates"""
        if sort_type == 'negative':
            return sorted(data, key=lambda x: x['funding_rate'])
        elif sort_type == 'positive':
            return sorted(data, key=lambda x: x['funding_rate'], reverse=True)
        else:
            return data

    def format_funding_message(self, data: List[Dict], limit: int = None) -> str:
        """Форматирование сообщения с funding rates"""
        if not data:
            return "Данные не найдены"
            
        if limit:
            data = data[:limit]
            
        message = ""
        for item in data:
            funding_sign = "+" if item['funding_rate'] > 0 else ""
            message += (
                f"{item['exchange'].upper()} {item['symbol']}\n"
                f"Фандинг: {funding_sign}{item['funding_rate']:.4f}%\n"
                f"Выплат в сутки: {item['daily_payments']:.0f} раз\n"
                f"Годовая доходность: {item['annual_yield']:.2f}%\n"
                f"{'-'*30}\n"
            )
            
        return message

    async def get_arbitrage_opportunities(self, data: List[Dict]) -> str:
        """Поиск арбитражных возможностей"""
        # Группируем по символам
        symbol_groups = {}
        for item in data:
            symbol = item['symbol']
            if symbol not in symbol_groups:
                symbol_groups[symbol] = []
            symbol_groups[symbol].append(item)
        
        opportunities = []
        
        for symbol, rates in symbol_groups.items():
            if len(rates) >= 2:
                # Ищем максимальную разницу в funding rates
                rates.sort(key=lambda x: x['funding_rate'])
                lowest = rates[0]   # Для лонга (мы получаем выплаты)
                highest = rates[-1] # Для шорта (мы платим выплаты)
                
                diff = highest['funding_rate'] - lowest['funding_rate']
                potential_yield = abs(lowest['annual_yield']) + abs(highest['annual_yield'])
                
                if diff > 0.01:  # Минимальная разница
                    opportunities.append({
                        'symbol': symbol,
                        'long_exchange': lowest['exchange'],
                        'short_exchange': highest['exchange'],
                        'funding_diff': diff,
                        'potential_yield': potential_yield
                    })
        
        # Сортируем по потенциальной доходности
        opportunities.sort(key=lambda x: x['potential_yield'], reverse=True)
        
        if not opportunities:
            return "Арбитражные возможности не найдены"
            
        message = "🔀 Арбитражные возможности:\n\n"
        for opp in opportunities[:10]:  # Топ 10
            message += (
                f"Пара: {opp['symbol']}\n"
                f"🔺 ЛОНГ на {opp['long_exchange'].upper()}\n"
                f"🔻 ШОРТ на {opp['short_exchange'].upper()}\n"
                f"Разница фандинга: {opp['funding_diff']:.4f}%\n"
                f"Потенциальная доходность: {opp['potential_yield']:.2f}%\n"
                f"{'-'*30}\n"
            )
            
        return message

# Создаем экземпляр бота
bot = FundingRateBot()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда старт с кнопками"""
    keyboard = [
        ["📊 Все фандинги (отрицательные)", "📈 Все фандинги (положительные)"],
        ["🏆 Топ 5 лучших фандингов", "🔀 Связки арбитража"],
        ["🔄 Обновить данные"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    await update.message.reply_text(
        "🤖 Бот мониторинга Funding Rates\n\n"
        "Выберите действие:",
        reply_markup=reply_markup
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    message_text = update.message.text
    
    try:
        if message_text == "📊 Все фандинги (отрицательные)":
            await update.message.reply_text("⏳ Загружаю данные...")
            data = await bot.get_all_funding_rates()
            sorted_data = bot.sort_funding_rates(data, 'negative')
            message = bot.format_funding_message(sorted_data, 50)  # Ограничиваем вывод
            await update.message.reply_text(message)
            
        elif message_text == "📈 Все фандинги (положительные)":
            await update.message.reply_text("⏳ Загружаю данные...")
            data = await bot.get_all_funding_rates()
            sorted_data = bot.sort_funding_rates(data, 'positive')
            message = bot.format_funding_message(sorted_data, 50)
            await update.message.reply_text(message)
            
        elif message_text == "🏆 Топ 5 лучших фандингов":
            await update.message.reply_text("⏳ Загружаю данные...")
            data = await bot.get_all_funding_rates()
            
            # Топ отрицательных
            negative_data = [d for d in data if d['funding_rate'] < 0]
            top_negative = bot.sort_funding_rates(negative_data, 'negative')[:5]
            
            # Топ положительных  
            positive_data = [d for d in data if d['funding_rate'] > 0]
            top_positive = bot.sort_funding_rates(positive_data, 'positive')[:5]
            
            message = "🔻 Топ 5 отрицательных фандингов:\n\n"
            message += bot.format_funding_message(top_negative)
            
            message += "\n🔺 Топ 5 положительных фандингов:\n\n"
            message += bot.format_funding_message(top_positive)
            
            await update.message.reply_text(message)
            
        elif message_text == "🔀 Связки арбитража":
            await update.message.reply_text("⏳ Ищу арбитражные возможности...")
            data = await bot.get_all_funding_rates()
            message = await bot.get_arbitrage_opportunities(data)
            await update.message.reply_text(message)
            
        elif message_text == "🔄 Обновить данные":
            await update.message.reply_text("✅ Данные обновляются при каждом запросе!")
            
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text("❌ Произошла ошибка при получении данных")

def main():
    """Основная функция"""
    # Ваш токен уже вставлен здесь
    application = Application.builder().token("8329955590:AAGk1Nu1LUHhBWQ7bqeorTctzhxie69Wzf0").build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Запускаем бота
    print("Бот запущен...")
    application.run_polling()

if __name__ == "__main__":
    main()
