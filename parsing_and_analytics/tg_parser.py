from telethon import TelegramClient, errors
import asyncio
import json
from datetime import datetime, timedelta
import pandas as pd
import os
import time
import random
from telethon.errors.rpcerrorlist import AuthKeyUnregisteredError, FloodWaitError, UsernameInvalidError
from shutil import copy2
from telethon.tl.types import Channel, Chat, User

# Читаем креды из JSON-файла
with open("config.json", "r") as file:
    config = json.load(file)

API_ID = config['tg_parser']["API_ID"]
API_HASH = config['tg_parser']["API_HASH"]
CHANNELS = config['tg_parser']["CHANNELS"]

# Имя для промежуточного файла
TEMP_CSV = "/home/koshkidadanet/My Files/test/parser/temp_telegram_messages.csv"
# Имя для финального файла
OUTPUT_CSV = "/home/koshkidadanet/My Files/test/parser/telegram_messages.csv"
# Журнал каналов, к которым не удалось получить доступ
ERROR_LOG = "/home/koshkidadanet/My Files/test/parser/error_channels.txt"

# Используем уникальное имя для парсер-сессии, чтобы не конфликтовать с основной сессией
SESSION_NAME = "parser_session_readonly"

def create_backup(file_path):
    """Создает резервную копию файла, если он существует"""
    if os.path.exists(file_path):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{file_path}.{timestamp}.bak"
        try:
            copy2(file_path, backup_path)
            print(f"Создана резервная копия: {backup_path}")
            return backup_path
        except Exception as e:
            print(f"Ошибка при создании резервной копии: {e}")
    return None

async def save_data(data, filename):
    """Сохраняет данные в CSV-файл"""
    if not data:
        print("Нет данных для сохранения.")
        return False
    
    # Если это финальный файл и он уже существует, создаем резервную копию
    if filename == OUTPUT_CSV and os.path.exists(filename):
        backup_file = create_backup(filename)
        if backup_file:
            print(f"Создана резервная копия перед сохранением новых данных: {backup_file}")
    
    try:
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"Данные сохранены в файл: {filename} (всего {len(data)} записей)")
        return True
    except Exception as e:
        print(f"Ошибка при сохранении данных в {filename}: {e}")
        return False

async def is_channel(client, entity_name):
    """
    Проверяет, является ли сущность каналом.
    Возвращает (bool, entity): (True, entity) если это канал, (False, entity) если нет
    """
    try:
        entity = await client.get_entity(entity_name)
        # Проверяем тип сущности
        if isinstance(entity, Channel):
            # Дополнительно проверяем, что это именно канал, а не супергруппа
            if hasattr(entity, 'broadcast') and entity.broadcast:
                return True, entity
            else:
                return False, entity  # Супергруппа (мегагруппа)
        else:
            return False, entity  # Обычная группа или пользователь
    except Exception as e:
        print(f"Ошибка при проверке типа {entity_name}: {e}")
        return False, None

async def main():
    # Загружаем уже собранные данные, если они есть
    data = []
    processed_messages = set()  # Для отслеживания уже обработанных сообщений
    
    # Сначала проверяем и загружаем данные из финального файла, чтобы не потерять уже собранные данные
    if os.path.exists(OUTPUT_CSV):
        try:
            output_df = pd.read_csv(OUTPUT_CSV)
            print(f"Загружаем существующие данные из {OUTPUT_CSV}...")
            for _, row in output_df.iterrows():
                message_key = f"{row['channel']}_{row['date']}_{row['content'][:20] if isinstance(row['content'], str) else ''}"
                if message_key not in processed_messages:
                    data.append(row.to_dict())
                    processed_messages.add(message_key)
            print(f"Загружено {len(data)} существующих записей из {OUTPUT_CSV}")
        except Exception as e:
            print(f"Ошибка при загрузке существующих данных из {OUTPUT_CSV}: {e}")
    
    # Затем проверяем временный файл для восстановления прерванной работы
    if os.path.exists(TEMP_CSV):
        try:
            temp_df = pd.read_csv(TEMP_CSV)
            print(f"Загружаем данные из временного файла {TEMP_CSV}...")
            # Преобразуем DataFrame обратно в список словарей
            temp_count = 0
            for _, row in temp_df.iterrows():
                message_key = f"{row['channel']}_{row['date']}_{row['content'][:20] if isinstance(row['content'], str) else ''}"
                if message_key not in processed_messages:
                    data.append(row.to_dict())
                    processed_messages.add(message_key)
                    temp_count += 1
            print(f"Добавлено {temp_count} записей из {TEMP_CSV}")
        except Exception as e:
            print(f"Ошибка при загрузке данных из временного файла: {e}")
    
    # Список для хранения каналов, к которым не удалось получить доступ
    failed_channels = []
    
    # Вычисляем дату начала периода (2 месяца назад от текущей даты)
    two_months_ago = datetime.now() - timedelta(days=60)
    
    max_retries = 3
    retry_count = 0
    
    # Создаем клиент с указанием device_model, system_version и app_version
    # Это поможет Telegram идентифицировать этот клиент как отдельное приложение
    client = TelegramClient(
        SESSION_NAME, 
        API_ID, 
        API_HASH,
        device_model="Parser Script",
        system_version="Python 3.x",
        app_version="1.0"
    )
    
    try:
        await client.start()
        
        # Проверка авторизации
        if not await client.is_user_authorized():
            print("Требуется авторизация. Следуйте инструкциям...")
            phone = input("Введите номер телефона: ")
            await client.send_code_request(phone)
            code = input("Введите полученный код: ")
            await client.sign_in(phone, code)
            print("Авторизация успешна. Сессия сохранена.")
        else:
            print("Используем существующую сессию.")
        
        # Запрашиваем каналы в режиме только для чтения
        for channel in CHANNELS:
            print(f"Проверяем {channel}...")
            
            # Пропускаем уже обработанные каналы
            if any(data_item['channel'] == channel for data_item in data):
                print(f"Канал {channel} уже обработан, пропускаем...")
                continue
            
            try:
                # Проверяем, является ли сущность каналом
                is_broadcast_channel, entity = await is_channel(client, channel)
                
                if not is_broadcast_channel:
                    print(f"{channel} не является каналом, это {'супергруппа/чат' if entity else 'неизвестный тип'}. Пропускаем...")
                    failed_channels.append(f"{channel}: Не является каналом")
                    continue
                    
                message_count = 0
                try:
                    async for message in client.iter_messages(entity, offset_date=two_months_ago, reverse=True):
                        # Создаем уникальный ключ для сообщения
                        message_text = message.text if message.text else ""
                        message_key = f"{channel}_{message.date}_{message_text[:20]}"
                        
                        if message_key not in processed_messages and message_text:
                            # Добавляем данные в список
                            data.append({
                                'channel': channel,
                                'date': message.date,
                                'content': message_text
                            })
                            processed_messages.add(message_key)
                            message_count += 1
                            
                            # print(f"Получено сообщение от {message.date}")
                            
                            # Периодически сохраняем промежуточные результаты (каждые 50 сообщений)
                            if message_count % 50 == 0:
                                await save_data(data, TEMP_CSV)
                                # Небольшая пауза, чтобы не превышать лимиты API
                                await asyncio.sleep(0.5)
                    
                    print(f"Выгрузка из канала {channel} завершена, получено {message_count} сообщений")
                    
                    # Добавляем канал в список ошибок, если сообщений не было
                    if message_count == 0:
                        print(f"Канал {channel} не содержит сообщений за последние 60 дней")
                        failed_channels.append(f"{channel}: Нет сообщений за последние 60 дней")
                    
                    # Сохраняем промежуточный результат после обработки канала
                    await save_data(data, TEMP_CSV)
                    
                    # Добавляем случайную задержку между каналами, чтобы избежать блокировки
                    delay = random.uniform(1.0, 3.0)
                    print(f"Пауза {delay:.2f} секунд перед следующим каналом...")
                    await asyncio.sleep(delay)
                    
                except FloodWaitError as e:
                    wait_time = e.seconds
                    print(f"Превышен лимит запросов. Ожидаем {wait_time} секунд...")
                    await save_data(data, TEMP_CSV)  # Сохраняем промежуточные данные
                    await asyncio.sleep(wait_time)  # Используем asyncio.sleep вместо time.sleep
                    continue
            except (errors.RPCError, Exception) as e:
                print(f"Ошибка при обработке канала {channel}: {e}")
                failed_channels.append(f"{channel}: {str(e)}")
                await save_data(data, TEMP_CSV)  # Сохраняем промежуточные данные
        
        # Сохраняем список каналов, к которым не удалось получить доступ
        if failed_channels:
            with open(ERROR_LOG, "w", encoding="utf-8") as f:
                f.write("\n".join(failed_channels))
            print(f"Список недоступных каналов сохранен в {ERROR_LOG}")
        
        # Сохраняем финальный результат - используем режим добавления, а не перезаписи
        success = await save_data(data, OUTPUT_CSV)
        if success:
            # Удаляем временный файл, если финальный файл создан успешно
            if os.path.exists(TEMP_CSV):
                os.remove(TEMP_CSV)
                print(f"Временный файл {TEMP_CSV} удален")
        
        # Выводим статистику
        print(f"Всего обработано {len(data)} сообщений из {len(CHANNELS) - len(failed_channels)} каналов")
        print(f"Не удалось получить доступ к {len(failed_channels)} каналам")
        
    except AuthKeyUnregisteredError:
        print("Ошибка авторизации. Возможно, сессия устарела.")
        # НЕ удаляем файл сессии автоматически, чтобы не нарушить работу других устройств
        print("Вы можете удалить файл сессии вручную, если проблема повторяется:")
        print(f"rm {SESSION_NAME}.session")
        # Сохраняем промежуточные данные
        await save_data(data, TEMP_CSV)
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
        # Сохраняем промежуточные данные
        await save_data(data, TEMP_CSV)
    finally:
        # Корректно завершаем сессию
        await client.disconnect()
        print("Сессия корректно завершена")


if __name__ == "__main__":
    asyncio.run(main())