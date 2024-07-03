import asyncio
import asyncmy
import pytz
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from dataclasses import dataclass

from dotenv import load_dotenv
from modules.mongodb_connector import MongoDBConnector
from modules.crypto_utils import decrypt_password
from modules.time_utils import get_kst_time
from modules.load_instance import load_instances_from_mongodb
from config import (
    MONGODB_SLOWLOG_COLLECTION_NAME, EXEC_TIME, POOL_SIZE,
    MAX_RETRIES, RETRY_DELAY, LOG_LEVEL, LOG_FORMAT
)

load_dotenv()

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)


@dataclass
class QueryDetails:
    instance: str
    db: str
    pid: int
    user: str
    host: str
    time: int
    sql_text: str
    start: datetime
    end: Optional[datetime] = None


class SlowQueryMonitor:
    def __init__(self):
        self.pid_time_cache: Dict[tuple, Dict[str, Any]] = {}
        self.ignore_instance_names: List[str] = []
        self.pools: Dict[str, asyncmy.Pool] = {}

    async def query_mysql_instance(self, instance_name: str, pool: asyncmy.Pool, collection: Any) -> None:
        try:
            if instance_name in self.ignore_instance_names:
                logger.info(f"Skipping instance {instance_name} due to ignore list")
                return

            current_pids = set()
            sql_query = """SELECT `ID`, `DB`, `USER`, `HOST`, `TIME`, `INFO`
                            FROM `information_schema`.`PROCESSLIST`
                            WHERE info IS NOT NULL
                            AND DB not in ('information_schema', 'mysql', 'performance_schema')
                            AND USER not in ('monitor', 'rdsadmin', 'system user')
                            ORDER BY `TIME` DESC"""

            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql_query)
                    result = await cur.fetchall()

                    for row in result:
                        await self.process_query_result(instance_name, row, current_pids)

                    await self.handle_finished_queries(instance_name, current_pids, collection)

            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Error querying instance {instance_name}: {e}")

    async def process_query_result(self, instance_name: str, row: tuple, current_pids: set) -> None:
        pid, db, user, host, time, info = row
        current_pids.add(pid)

        if time >= EXEC_TIME:
            cache_data = self.pid_time_cache.setdefault((instance_name, pid), {'max_time': 0})
            cache_data['max_time'] = max(cache_data['max_time'], time)

            if 'start' not in cache_data:
                utc_now = datetime.now(pytz.utc)
                utc_start_timestamp = int((utc_now - timedelta(seconds=EXEC_TIME)).timestamp())
                utc_start_datetime = datetime.fromtimestamp(utc_start_timestamp, pytz.utc)
                cache_data['start'] = utc_start_datetime

            info_cleaned = re.sub(' +', ' ', info).encode('utf-8', 'ignore').decode('utf-8')
            info_cleaned = re.sub(r'[\n\t\r]+', ' ', info_cleaned).strip()

            cache_data['details'] = QueryDetails(
                instance=instance_name,
                db=db,
                pid=pid,
                user=user,
                host=host,
                time=time,
                sql_text=info_cleaned,
                start=cache_data['start']
            )

    async def handle_finished_queries(self, instance_name: str, current_pids: set, collection: Any) -> None:
        for (instance, pid), cache_data in list(self.pid_time_cache.items()):
            if pid not in current_pids and instance == instance_name:
                data_to_insert = vars(cache_data['details'])
                data_to_insert['time'] = cache_data['max_time']
                data_to_insert['end'] = datetime.now(pytz.utc)

                if not await collection.find_one({'_id': data_to_insert.get('_id')}):
                    await collection.insert_one(data_to_insert)
                    logger.info(f"Inserted slow query data for instance {instance_name}, PID {pid}")

                del self.pid_time_cache[(instance, pid)]

    async def create_pool(self, instance_data: Dict[str, Any]) -> Optional[asyncmy.Pool]:
        instance_name = instance_data["instance_name"]
        for attempt in range(MAX_RETRIES):
            try:
                pool = await asyncmy.create_pool(
                    host=instance_data["host"],
                    port=instance_data["port"],
                    user=instance_data["user"],
                    password=decrypt_password(instance_data["password"]),
                    db=instance_data.get("db", ""),
                    maxsize=POOL_SIZE
                )
                logger.info(f"Connection pool created successfully for {instance_name}")
                return pool
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed for {instance_name}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    logger.error(f"Maximum retry attempts reached for {instance_name}. Skipping this instance.")
                    return None

    async def run_mysql_slow_queries(self) -> None:
        try:
            await MongoDBConnector.initialize()
            db = await MongoDBConnector.get_database()
            collection = db[MONGODB_SLOWLOG_COLLECTION_NAME]

            instances = await load_instances_from_mongodb()

            while True:
                tasks = []
                for instance_data in instances:
                    instance_name = instance_data["instance_name"]
                    if instance_name in self.ignore_instance_names:
                        continue

                    if instance_name not in self.pools:
                        self.pools[instance_name] = await self.create_pool(instance_data)

                    if self.pools.get(instance_name):
                        tasks.append(self.query_mysql_instance(instance_name, self.pools[instance_name], collection))

                if tasks:
                    await asyncio.gather(*tasks)

                await asyncio.sleep(1)

        except asyncio.CancelledError:
            logger.info("Async task was cancelled. Cleaning up...")
            await self.cleanup()

        except Exception as e:
            logger.error(f"An error occurred: {e}")

        finally:
            await self.cleanup()

    async def cleanup(self) -> None:
        for pool_name, pool in self.pools.items():
            if pool is not None:
                try:
                    pool.close()
                    await pool.wait_closed()
                    logger.info(f"Closed connection pool for {pool_name}")
                except Exception as e:
                    logger.error(f"An error occurred while closing the pool {pool_name}: {e}")

        await asyncio.sleep(0.1)
        logger.info("Resources have been released and program has been terminated safely.")


if __name__ == '__main__':
    monitor = SlowQueryMonitor()
    asyncio.run(monitor.run_mysql_slow_queries())
