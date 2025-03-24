import aiohttp
import asyncio
import json
import sqlite3
from typing import Dict, Optional, List
import time
from datetime import datetime, timedelta
import os

class CadastralDatabase:
    def __init__(self, db_name='cadastral.db'):
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self.create_tables()
        
    def create_tables(self):
        with self.conn:
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS parcels (
                    parcel_id INTEGER PRIMARY KEY,
                    municipality_name TEXT,
                    parcel_number TEXT,
                    address TEXT,
                    area REAL,
                    owner_name TEXT,
                    owner_ownership TEXT,
                    owner_address TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_parcel_id ON parcels(parcel_id)')
    
    def insert_many(self, records: List[Dict]):
        with self.conn:
            self.conn.executemany('''
                INSERT OR REPLACE INTO parcels 
                (parcel_id, municipality_name, parcel_number, address, area, 
                owner_name, owner_ownership, owner_address)
                VALUES (:parcel_id, :municipality_name, :parcel_number, :address, :area,
                :owner_name, :owner_ownership, :owner_address)
            ''', records)
    
    def get_last_id(self) -> Optional[int]:
        cursor = self.conn.execute('SELECT MAX(parcel_id) FROM parcels')
        return cursor.fetchone()[0]
    
    def get_stats(self) -> Dict:
        cursor = self.conn.execute('SELECT COUNT(*), MIN(parcel_id), MAX(parcel_id) FROM parcels')
        count, min_id, max_id = cursor.fetchone()
        return {'count': count, 'min_id': min_id, 'max_id': max_id}
    
    def close(self):
        self.conn.close()

class FastCadastralScraper:
    def __init__(self):
        self.base_url = "https://oss.uredjenazemlja.hr/oss/public/cad/parcel-info"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        self.batch_size = 10000
        self.db = CadastralDatabase()
        self.semaphore = asyncio.Semaphore(1000)
        self.start_time = time.time()
        self.successful_requests = 0
        self.failed_requests = 0

    async def get_parcel_info(self, session: aiohttp.ClientSession, parcel_id: int) -> Optional[Dict]:
        async with self.semaphore:
            try:
                params = {'parcelId': parcel_id}
                async with session.get(self.base_url, params=params) as response:
                    if response.status != 200:
                        self.failed_requests += 1
                        return None
                    
                    content = await response.json()
                    self.successful_requests += 1
                    
                    if not content:
                        return None
                    
                    data = {
                        'parcel_id': content.get('parcelId'),
                        'municipality_name': content.get('cadMunicipalityName'),
                        'parcel_number': content.get('parcelNumber'),
                        'address': content.get('address'),
                        'area': content.get('area'),
                        'owner_name': None,
                        'owner_ownership': None,
                        'owner_address': None
                    }
                    
                    if content.get('possessionSheets'):
                        for sheet in content['possessionSheets']:
                            if sheet.get('possessors'):
                                possessor = sheet['possessors'][0]
                                data.update({
                                    'owner_name': possessor.get('name'),
                                    'owner_ownership': possessor.get('ownership'),
                                    'owner_address': possessor.get('address')
                                })
                                break
                    return data
            except Exception:
                self.failed_requests += 1
                return None

    async def process_batch(self, session: aiohttp.ClientSession, start_id: int) -> List[Dict]:
        tasks = [self.get_parcel_info(session, start_id + i) for i in range(self.batch_size)]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    async def scrape_range(self, start_id: int, end_id: int):
        timeout = aiohttp.ClientTimeout(total=300)
        connector = aiohttp.TCPConnector(limit=None, limit_per_host=0, force_close=False, ssl=False)
        
        last_id = self.db.get_last_id()
        current_id = last_id + 1 if last_id else start_id
        print(f"Starting from ID: {current_id:,}")

        batch_durations = []
        
        async with aiohttp.ClientSession(headers=self.headers, timeout=timeout, connector=connector) as session:
            while current_id < end_id:
                batch_start_time = time.time()
                remaining_ids = end_id - current_id
                remaining_batches = remaining_ids / self.batch_size
                
                if batch_durations:
                    avg_duration = sum(batch_durations) / len(batch_durations)
                    eta_seconds = avg_duration * remaining_batches
                    eta = str(timedelta(seconds=int(eta_seconds)))
                    stats = self.db.get_stats()
                    print(f"Progress: {current_id:,}/{end_id:,} ({((current_id - start_id) / (end_id - start_id)) * 100:.2f}%) | Records: {stats['count']:,} | ETA: {eta}")
                
                try:
                    batch_results = await self.process_batch(session, current_id)
                    if batch_results:
                        self.db.insert_many(batch_results)
                    
                    batch_duration = time.time() - batch_start_time
                    batch_durations.append(batch_duration)
                    if len(batch_durations) > 50:
                        batch_durations.pop(0)
                    
                    if self.successful_requests % self.batch_size == 0:
                        success_rate = (self.successful_requests / (self.successful_requests + self.failed_requests)) * 100
                        print(f"Success rate: {success_rate:.2f}% | Successful: {self.successful_requests:,} | Failed: {self.failed_requests:,}")
                    
                except Exception as e:
                    print(f"Error in batch {current_id}: {str(e)}")
                    await asyncio.sleep(60)
                    continue
                
                current_id += self.batch_size

    def close(self):
        self.db.close()

async def main():
    scraper = FastCadastralScraper()
    start_id = 5625555
    end_id = 40150506
    
    try:
        await scraper.scrape_range(start_id, end_id)
    except Exception as e:
        print(f"Fatal error: {str(e)}")
    finally:
        scraper.close()

if __name__ == "__main__":
    asyncio.run(main())