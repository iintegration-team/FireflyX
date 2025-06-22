from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import asyncio
import os

BOT_TOKEN = os.getenv('BOT_TOKEN')

storage = MemoryStorage()
dp = Dispatcher(storage=storage)
bot = Bot(token=BOT_TOKEN)

MY_CHAT_ID = 597695657

class PositionSizeState(StatesGroup):
    waiting_for_size = State()
    
class MaxPositionsState(StatesGroup):
    waiting_for_max = State()

main_keyboard = types.ReplyKeyboardMarkup(
    keyboard=[
        [types.KeyboardButton(text="💰 Размер позиции"), 
         types.KeyboardButton(text="🔢 Макс. позиций")]
    ],
    resize_keyboard=True
)

START_MESSAGE = """FireflyX — ваш личный помощник в криптотрейдинге. Бот присылает мгновенные уведомления о совершенных сделках, основанных на анализе новостей, соцсетей и рыночного настроения.
🔥 Реагируй первым. Торгуй умнее.
⚙️ Интеграция с биржами, гибкая настройка сигналов.
🌐 Скоро: ИИ-агент, принимающий решения на основе связки новостей и котировок
"""

async def send_welcome(message: types.Message):
    """This handler will be called when user sends `/start` command"""
    print(f"Chat ID: {message.chat.id}")
    await message.reply(START_MESSAGE, reply_markup=main_keyboard)

_notification_loop = None
_notification_queue = asyncio.Queue()
_notification_task = None

def _get_notification_loop():
    global _notification_loop, _notification_task
    if _notification_loop is None:
        _notification_loop = asyncio.new_event_loop()
        _notification_task = _notification_loop.create_task(_process_notifications())
        
        import threading
        def run_loop():
            asyncio.set_event_loop(_notification_loop)
            _notification_loop.run_forever()
        
        threading.Thread(target=run_loop, daemon=True).start()
    
    return _notification_loop

async def _process_notifications():
    while True:
        text = await _notification_queue.get()
        try:
            await bot.send_message(MY_CHAT_ID, text)
        except Exception as e:
            print(f"Error sending notification: {e}")
        finally:
            _notification_queue.task_done()

def send_notification(text):
    loop = _get_notification_loop()
    loop.call_soon_threadsafe(lambda: _notification_queue.put_nowait(text))

def format_position_close_notification(symbol, position_details):
    """Format notification for position closure with P&L information"""
    if position_details:
        pnl = position_details.get('unrel_pnl', 0)
        avg_price = position_details.get('avg_price', 0)
        qty = position_details.get('qty', 0)
        side = position_details.get('side', '')
        
        position_value = avg_price * qty
        
        emoji = "🟢 ПРИБЫЛЬ" if float(pnl) > 0 else "🔴 УБЫТОК"
        message = f"🔄 ЗАКРЫТИЕ ПОЗИЦИИ | {symbol.upper()}\n\n" \
                f"📊 Детали:\n" \
                f"• Сторона: {side}\n" \
                f"• Количество: {qty}\n" \
                f"• Средняя цена: {avg_price}\n" \
                f"• Сумма позиции: ${position_value:.2f}\n\n" \
                f"{emoji}: ${float(pnl):.2f}"
        
        return message
    else:
        return f"🔄 Закрываем позицию: {symbol.upper()}"

def format_position_open_notification(symbol, position_details, state, approx_amount=None):
    """Format notification for position opening with different states"""
    if position_details:
        avg_price = position_details.get('avg_price', 0)
        qty = position_details.get('qty', 0)
        position_value = avg_price * qty
        
        if state == "STARTED":
            return f"🟡 НАЧАЛО ПАМПА | {symbol.upper()}\n\n" \
                  f"📊 Открыта позиция:\n" \
                  f"• Количество: {qty}\n" \
                  f"• Цена входа: {avg_price}\n" \
                  f"• Сумма: ${position_value:.2f}"
        elif state == "CONFIRMED":
            return f"🟢 ПАМП ПОДТВЕРЖДЕН | {symbol.upper()}\n\n" \
                  f"📊 Увеличена позиция:\n" \
                  f"• Количество: {qty}\n" \
                  f"• Средняя цена: {avg_price}\n" \
                  f"• Общая сумма: ${position_value:.2f}"
        elif state == "STABILIZED":
            return f"🔵 ПАМП СТАБИЛИЗИРОВАЛСЯ | {symbol.upper()}\n\n" \
                  f"📊 Максимальная позиция:\n" \
                  f"• Количество: {qty}\n" \
                  f"• Средняя цена: {avg_price}\n" \
                  f"• Общая сумма: ${position_value:.2f}"
    else:
        emoji = "🟡" if state == "STARTED" else "🟢" if state == "CONFIRMED" else "🔵"
        return f"{emoji} Открываем позицию ({state}): {symbol}, Сумма: ~{approx_amount}$"

dp.message.register(send_welcome, Command(commands=['start']))

async def change_position_size(message: types.Message, state: FSMContext):
    await state.set_state(PositionSizeState.waiting_for_size)
    await message.reply("Введите размер позиции (в USD):")

async def process_position_size(message: types.Message, state: FSMContext):
    try:
        size_value = float(message.text)
        await state.clear()
        await message.reply(f"✅ Размер позиции установлен: ${size_value:.2f}", reply_markup=main_keyboard)
    except ValueError:
        await message.reply("❌ Пожалуйста, введите корректное число.")
        
async def change_max_positions(message: types.Message, state: FSMContext):
    await state.set_state(MaxPositionsState.waiting_for_max)
    await message.reply("Введите максимальное количество позиций:")

async def process_max_positions(message: types.Message, state: FSMContext):
    try:
        max_value = int(message.text)
        if max_value <= 0:
            await message.reply("❌ Значение должно быть положительным числом.")
            return
            
        await state.clear() 
        await message.reply(f"✅ Максимальное количество позиций установлено: {max_value}", reply_markup=main_keyboard)
    except ValueError:
        await message.reply("❌ Пожалуйста, введите корректное целое число.")

dp.message.register(send_welcome, Command(commands=['start']))
dp.message.register(change_position_size, lambda message: message.text == "💰 Размер позиции")
dp.message.register(process_position_size, PositionSizeState.waiting_for_size)
dp.message.register(change_max_positions, lambda message: message.text == "🔢 Макс. позиций")
dp.message.register(process_max_positions, MaxPositionsState.waiting_for_max)

async def main():
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.set_event_loop(asyncio.new_event_loop())
        asyncio.run(main())
    except KeyboardInterrupt:
        pass