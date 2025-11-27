import logging
import requests
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# Токены (ЗАМЕНИТЕ НА СВОИ ЕСЛИ НУЖНО)
TELEGRAM_TOKEN = "8329955590:AAGk1Nu1LUHhBWQ7bqeorTctzhxie69Wzf0"
COINGLASS_TOKEN = "2d73a05799f64daab80329868a5264ea"

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

class CoinglassAPI:
    def __init__(self):
        self.base_url = "https://open-api.coinglass.com/api/pro/v1"
        self.headers = {
            'accept': 'application/json',
            'coinglassSecret': COINGLASS_TOKEN
        }
    
    def get_funding_rates(self, symbol=None):
        """Получить ставки фандинга"""
        url = f"{self.base_url}/futures/funding_rates"
        params = {}
        if symbol:
            params['symbol'] = symbol
            
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    return data.get('data', [])
            return None
        except Exception as e:
            print(f"Ошибка при запросе к Coinglass: {e}")
            return None
    
    def get_arbitrage_opportunities(self):
        """Получить арбитражные возможности между биржами"""
        url = f"{self.base_url}/futures/market"
        params = {'symbol': 'BTC'}
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    return self._calculate_arbitrage(data.get('data', []))
            return None
        except Exception as e:
            print(f"Ошибка при запросе к Coinglass: {e}")
            return None
    
    def _calculate_arbitrage(self, market_data):
        """Рассчитать арбитражные возможности"""
        opportunities = []
        
        for coin_data in market_data:
            symbol = coin_data.get('symbol', '')
            exchanges = coin_data.get('exchangeName', [])
            prices = coin_data.get('price', [])
            
            if len(prices) >= 2:
                min_price = min(prices)
                max_price = max(prices)
                
                if min_price > 0:
                    spread_percent = ((max_price - min_price) / min_price) * 100
                    
                    if spread_percent > 0.5:  # Фильтр минимального спреда
                        opportunities.append({
                            'symbol': symbol,
                            'min_price': min_price,
                            'max_price': max_price,
                            'spread_percent': round(spread_percent, 2),
                            'exchanges': exchanges
                        })
        
        return sorted(opportunities, key=lambda x: x['spread_percent'], reverse=True)

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
        self.application.add_handler(CallbackQueryHandler(self.button_handler))
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        keyboard = [
            [
                InlineKeyboardButton("📊 Фандинг ставки", callback_data="funding"),
                InlineKeyboardButton("💸 Арбитраж", callback_data="arbitrage")
            ],
            [
                InlineKeyboardButton("🚀 Топ фандинг", callback_data="top_funding")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        welcome_text = """
🤖 **Crypto Funding & Arbitrage Bot**

Доступные команды:
/funding - Фандинг ставки по всем парам
/arbitrage - Арбитражные возможности
/top_funding - Топ высоких фандинг ставок

Используйте кнопки ниже для быстрого доступа!
        """
        
        await update.message.reply_text(
            welcome_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def funding_rates(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать фандинг ставки"""
        await update.message.reply_text("🔄 Получаю данные о фандинг ставках...")
        
        funding_data = self.api.get_funding_rates()
        
        if not funding_data:
            await update.message.reply_text("❌ Ошибка получения данных от Coinglass API")
            return
        
        response = "📊 **Текущие фандинг ставки:**\n\n"
        
        for i, item in enumerate(funding_data[:15]):  # Ограничиваем вывод
            symbol = item.get('symbol', '')
            rate_list = item.get('uMarginList', [{}])
            rate = rate_list[0].get('rate', 0) if rate_list else 0
            exchange = item.get('exchangeName', '')
            
            try:
                rate_percent = round(float(rate) * 100, 4)
            except:
                rate_percent = 0
                
            emoji = "🟢" if rate_percent > 0 else "🔴"
            
            response += f"{emoji} **{symbol}**\n"
            response += f"   Биржа: {exchange}\n"
            response += f"   Ставка: {rate_percent}%\n\n"
        
        await update.message.reply_text(response, parse_mode='Markdown')
    
    async def arbitrage(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать арбитражные возможности"""
        await update.message.reply_text("🔍 Ищу арбитражные возможности...")
        
        arb_opportunities = self.api.get_arbitrage_opportunities()
        
        if not arb_opportunities:
            await update.message.reply_text("🤷‍♂️ Арбитражные возможности не найдены или ошибка API")
            return
        
        response = "💸 **Арбитражные возможности:**\n\n"
        
        for opp in arb_opportunities[:10]:  # Топ 10 возможностей
            response += f"🎯 **{opp['symbol']}**\n"
            response += f"   Спред: {opp['spread_percent']}%\n"
            response += f"   Мин: ${opp['min_price']:.2f}\n"
            response += f"   Макс: ${opp['max_price']:.2f}\n\n"
        
        await update.message.reply_text(response, parse_mode='Markdown')
    
    async def top_funding(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Топ высоких фандинг ставок"""
        await update.message.reply_text("📈 Ищу самые высокие фандинг ставки...")
        
        funding_data = self.api.get_funding_rates()
        
        if not funding_data:
            await update.message.reply_text("❌ Ошибка получения данных от Coinglass API")
            return
        
        # Фильтруем и сортируем по величине фандинга
        filtered_data = []
        for item in funding_data:
            rate_list = item.get('uMarginList', [{}])
            rate = rate_list[0].get('rate', 0) if rate_list else 0
            if rate and float(rate) != 0:
                filtered_data.append(item)
        
        # Сортируем по абсолютному значению фандинга
        sorted_data = sorted(
            filtered_data, 
            key=lambda x: abs(float(x.get('uMarginList', [{}])[0].get('rate', 0))), 
            reverse=True
        )
        
        response = "🚀 **Топ высоких фандинг ставок:**\n\n"
        
        for i, item in enumerate(sorted_data[:10]):
            symbol = item.get('symbol', '')
            rate_list = item.get('uMarginList', [{}])
            rate = rate_list[0].get('rate', 0) if rate_list else 0
            exchange = item.get('exchangeName', '')
            
            try:
                rate_percent = round(float(rate) * 100, 4)
            except:
                rate_percent = 0
                
            emoji = "📈" if rate_percent > 0 else "📉"
            
            response += f"{i+1}. {emoji} **{symbol}**\n"
            response += f"   Биржа: {exchange}\n"
            response += f"   Ставка: {rate_percent}%\n\n"
        
        await update.message.reply_text(response, parse_mode='Markdown')
    
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
    
    async def funding_rates_callback(self, query):
        """Обработчик кнопки фандинга"""
        await query.edit_message_text("🔄 Получаю данные о фандинг ставках...")
        funding_data = self.api.get_funding_rates()
        
        if not funding_data:
            await query.edit_message_text("❌ Ошибка получения данных от Coinglass API")
            return
        
        response = "📊 **Текущие фандинг ставки:**\n\n"
        
        for i, item in enumerate(funding_data[:12]):
            symbol = item.get('symbol', '')
            rate_list = item.get('uMarginList', [{}])
            rate = rate_list[0].get('rate', 0) if rate_list else 0
            exchange = item.get('exchangeName', '')
            
            try:
                rate_percent = round(float(rate) * 100, 4)
            except:
                rate_percent = 0
                
            emoji = "🟢" if rate_percent > 0 else "🔴"
            
            response += f"{emoji} **{symbol}**\n"
            response += f"   Биржа: {exchange}\n"
            response += f"   Ставка: {rate_percent}%\n\n"
        
        await query.edit_message_text(response, parse_mode='Markdown')
    
    async def arbitrage_callback(self, query):
        """Обработчик кнопки арбитража"""
        await query.edit_message_text("🔍 Ищу арбитражные возможности...")
        arb_opportunities = self.api.get_arbitrage_opportunities()
        
        if not arb_opportunities:
            await query.edit_message_text("🤷‍♂️ Арбитражные возможности не найдены или ошибка API")
            return
        
        response = "💸 **Арбитражные возможности:**\n\n"
        
        for opp in arb_opportunities[:8]:
            response += f"🎯 **{opp['symbol']}**\n"
            response += f"   Спред: {opp['spread_percent']}%\n"
            response += f"   Мин: ${opp['min_price']:.2f}\n"
            response += f"   Макс: ${opp['max_price']:.2f}\n\n"
        
        await query.edit_message_text(response, parse_mode='Markdown')
    
    async def top_funding_callback(self, query):
        """Обработчик кнопки топа фандинга"""
        await query.edit_message_text("📈 Ищу самые высокие фандинг ставки...")
        funding_data = self.api.get_funding_rates()
        
        if not funding_data:
            await query.edit_message_text("❌ Ошибка получения данных от Coinglass API")
            return
        
        filtered_data = []
        for item in funding_data:
            rate_list = item.get('uMarginList', [{}])
            rate = rate_list[0].get('rate', 0) if rate_list else 0
            if rate and float(rate) != 0:
                filtered_data.append(item)
        
        sorted_data = sorted(
            filtered_data, 
            key=lambda x: abs(float(x.get('uMarginList', [{}])[0].get('rate', 0))), 
            reverse=True
        )
        
        response = "🚀 **Топ высоких фандинг ставок:**\n\n"
        
        for i, item in enumerate(sorted_data[:8]):
            symbol = item.get('symbol', '')
            rate_list = item.get('uMarginList', [{}])
            rate = rate_list[0].get('rate', 0) if rate_list else 0
            exchange = item.get('exchangeName', '')
            
            try:
                rate_percent = round(float(rate) * 100, 4)
            except:
                rate_percent = 0
                
            emoji = "📈" if rate_percent > 0 else "📉"
            
            response += f"{i+1}. {emoji} **{symbol}**\n"
            response += f"   Биржа: {exchange}\n"
            response += f"   Ставка: {rate_percent}%\n\n"
        
        await query.edit_message_text(response, parse_mode='Markdown')
    
    def run(self):
        """Запуск бота"""
        print("🤖 Бот запущен...")
        print("📱 Перейдите в Telegram и отправьте /start вашему боту")
        self.application.run_polling()

if __name__ == '__main__':
    bot = CryptoArbBot()
    bot.run()
