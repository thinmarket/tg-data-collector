#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Telegram Safe Channel Collector - С АВТОСОХРАНЕНИЕМ
"""

import asyncio
import pandas as pd
from telethon import TelegramClient, errors
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
import os
from datetime import datetime
import logging
import json

# ========== НАСТРОЙКИ ==========
API_ID = 12345678
API_HASH = 'Введите свой API_HASH'
CHANNEL_USERNAME = 'Введите username канала без @'
# ================================

ALPHABET = [
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'а', 'б', 'в', 'г', 'д', 'е', 'ё', 'ж', 'з', 'и', 'й', 'к', 'л', 'м',
    'н', 'о', 'п', 'р', 'с', 'т', 'у', 'ф', 'х', 'ц', 'ч', 'ш', 'щ', 'ъ',
    'ы', 'ь', 'э', 'ю', 'я'
]

DELAY_BETWEEN_REQUESTS = 10
BATCH_SIZE = 3
BATCH_PAUSE = 120
DAILY_LIMIT = 80

OUTPUT_DIR = 'telegram_safe_collector'
PHOTOS_DIR = os.path.join(OUTPUT_DIR, 'photos')
PROGRESS_FILE = os.path.join(OUTPUT_DIR, f'{CHANNEL_USERNAME}_progress.json')
os.makedirs(PHOTOS_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class SafeCollector:
    def __init__(self):
        self.client = TelegramClient('session_qr', API_ID, API_HASH)
        self.channel = None
        self.existing_users = {}
        self.new_users = {}
        self.temp_new_users = {}  # для автосохранения
        self.stats = {
            'total_before': 0,
            'new_found': 0,
            'total_after': 0,
            'requests_today': 0,
            'last_run': None
        }
        self.csv_path = os.path.join(OUTPUT_DIR, f'{CHANNEL_USERNAME}_data.csv')
    
    def load_existing_data(self):
        if os.path.exists(self.csv_path):
            try:
                df = pd.read_csv(self.csv_path)
                for _, row in df.iterrows():
                    self.existing_users[row['user_id']] = dict(row)
                logger.info(f"📥 Загружено существующих записей: {len(self.existing_users)}")
                self.stats['total_before'] = len(self.existing_users)
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки CSV: {e}")
    
    def load_progress(self):
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                    self.processed_letters = progress.get('processed_letters', [])
                    self.stats['requests_today'] = progress.get('requests_today', 0)
                    self.stats['last_run'] = progress.get('last_run')
                    
                    if self.stats['last_run']:
                        last = datetime.fromisoformat(self.stats['last_run'])
                        if (datetime.now() - last).days >= 1:
                            self.stats['requests_today'] = 0
                            logger.info("📅 Новый день - счетчик запросов обнулен")
                    
                    logger.info(f"📥 Загружен прогресс: обработано {len(self.processed_letters)} букв")
                    logger.info(f"📊 Запросов сегодня: {self.stats['requests_today']}/{DAILY_LIMIT}")
            except:
                self.processed_letters = []
        else:
            self.processed_letters = []
    
    def save_progress(self):
        progress = {
            'processed_letters': self.processed_letters,
            'requests_today': self.stats['requests_today'],
            'last_run': datetime.now().isoformat()
        }
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
    
    def save_batch(self):
        """Сохраняет накопленных за батч пользователей"""
        if self.temp_new_users:
            temp_df = pd.DataFrame(list(self.temp_new_users.values()))
            if os.path.exists(self.csv_path):
                existing_df = pd.read_csv(self.csv_path)
                combined_df = pd.concat([existing_df, temp_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['user_id'], keep='last')
                combined_df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
            else:
                temp_df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"💾 АВТОСОХРАНЕНИЕ: +{len(self.temp_new_users)} новых (всего {len(self.existing_users) + len(self.new_users)})")
            
            # Обновляем existing_users для этой сессии
            for uid in self.temp_new_users:
                self.existing_users[uid] = self.temp_new_users[uid]
            
            self.temp_new_users = {}
    
    async def start(self):
        await self.client.connect()
        if not await self.client.is_user_authorized():
            logger.error("❌ Не авторизован. Сначала войди через QR-код.")
            return False
        me = await self.client.get_me()
        logger.info(f"✅ Авторизован: {me.first_name}")
        return True
    
    async def get_channel(self):
        try:
            self.channel = await self.client.get_entity(CHANNEL_USERNAME)
            logger.info(f"📢 Канал: {self.channel.title}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return False
    
    async def search_by_query(self, query):
        try:
            participants = await self.client(GetParticipantsRequest(
                channel=self.channel,
                filter=ChannelParticipantsSearch(query),
                offset=0,
                limit=200,
                hash=0
            ))
            return participants.users
        except errors.FloodWaitError as e:
            wait = e.seconds
            logger.warning(f"⚠️ FloodWait: {wait} сек")
            await asyncio.sleep(wait)
            return await self.search_by_query(query)
        except Exception as e:
            logger.error(f"❌ Ошибка поиска '{query}': {e}")
            return []
    
    async def download_photo(self, user):
        try:
            if user.photo:
                path = await self.client.download_profile_photo(
                    user, 
                    file=os.path.join(PHOTOS_DIR, f'{user.id}.jpg')
                )
                if path:
                    return f'photos/{user.id}.jpg'
        except:
            pass
        return None
    
    async def process_letter(self, letter):
        logger.info(f"🔤 Поиск: '{letter}' (запрос {self.stats['requests_today'] + 1}/{DAILY_LIMIT})")
        
        users = await self.search_by_query(letter)
        batch_new = 0
        
        for user in users:
            if user.id not in self.existing_users and user.id not in self.new_users:
                photo_path = await self.download_photo(user)
                
                user_data = {
                    'user_id': user.id,
                    'username': user.username,
                    'first_name': user.first_name,
                    'last_name': user.last_name,
                    'full_name': f"{user.first_name or ''} {user.last_name or ''}".strip(),
                    'phone': user.phone,
                    'is_bot': user.bot,
                    'is_premium': getattr(user, 'premium', False),
                    'photo': photo_path
                }
                
                self.temp_new_users[user.id] = user_data
                self.new_users[user.id] = user_data
                self.stats['new_found'] += 1
                batch_new += 1
                
                if self.stats['new_found'] % 10 == 0:
                    logger.info(f"✨ Найдено новых: {self.stats['new_found']}")
        
        self.processed_letters.append(letter)
        self.stats['requests_today'] += 1
        
        return batch_new
    
    async def run_collection(self):
        self.load_existing_data()
        self.load_progress()
        
        remaining_letters = [l for l in ALPHABET if l not in self.processed_letters]
        
        if not remaining_letters:
            logger.info("✅ Все буквы уже обработаны!")
            return
        
        logger.info(f"⏳ Начинаем сбор. Осталось букв: {len(remaining_letters)}")
        logger.info(f"⏱ Пауза между запросами: {DELAY_BETWEEN_REQUESTS} сек")
        logger.info(f"📦 Батч: {BATCH_SIZE} запросов, затем пауза {BATCH_PAUSE} сек")
        logger.info(f"📊 Дневной лимит: {DAILY_LIMIT} запросов")
        logger.info(f"📁 CSV файл: {self.csv_path}")
        
        answer = input(f"\nНачать сбор (осталось {len(remaining_letters)} букв)? (да/нет): ")
        if answer.lower() != 'да':
            logger.info("Отменено")
            return
        
        processed_in_session = 0
        
        for i, letter in enumerate(remaining_letters, 1):
            if self.stats['requests_today'] >= DAILY_LIMIT:
                logger.warning(f"⚠️ Достигнут дневной лимит ({DAILY_LIMIT} запросов)")
                logger.info(f"🌙 Продолжим завтра. Обработано сегодня: {processed_in_session} букв")
                
                # Сохраняем последний батч
                self.save_batch()
                self.save_progress()
                return
            
            await self.process_letter(letter)
            processed_in_session += 1
            self.save_progress()
            
            if i < len(remaining_letters):
                if processed_in_session % BATCH_SIZE == 0:
                    logger.info(f"⏸ ДЛИТЕЛЬНАЯ ПАУЗА {BATCH_PAUSE} сек")
                    logger.info("☕ Отдыхаем...")
                    
                    # АВТОСОХРАНЕНИЕ!
                    self.save_batch()
                    
                    await asyncio.sleep(BATCH_PAUSE)
                else:
                    logger.info(f"⏱ Пауза {DELAY_BETWEEN_REQUESTS} сек")
                    await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
        
        # Финальное сохранение
        self.save_batch()
        
        logger.info("=" * 50)
        logger.info("✅ СЕССИЯ ЗАВЕРШЕНА")
        logger.info(f"📊 Было: {self.stats['total_before']}")
        logger.info(f"✨ Найдено новых: {self.stats['new_found']}")
        logger.info(f"📊 Стало: {self.stats['total_before'] + self.stats['new_found']}")
        logger.info(f"📅 Запросов сегодня: {self.stats['requests_today']}")
        logger.info("=" * 50)
    
    async def close(self):
        await self.client.disconnect()

async def main():
    logger.info("=" * 60)
    logger.info("🛡️ SAFE TELEGRAM COLLECTOR (С АВТОСОХРАНЕНИЕМ)")
    logger.info("=" * 60)
    
    collector = SafeCollector()
    
    try:
        if not await collector.start():
            return
        if not await collector.get_channel():
            return
        
        await collector.run_collection()
        
    except KeyboardInterrupt:
        logger.info("\n⏹ Остановлено пользователем")
        collector.save_batch()  # сохраняем при принудительной остановке
        collector.save_progress()
    finally:
        await collector.close()

if __name__ == '__main__':

    asyncio.run(main())
