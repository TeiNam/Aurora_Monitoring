import asyncio
import pytz
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from asyncmy.connection import Connection
from asyncmy.pool import Pool

from modules.load_instance import load_instances_from_mongodb
from modules.mongodb_connector import MongoDBConnector
from modules.crypto_utils import decrypt_password
from config import (
    MONGODB_DISK_USAGE_COLLECTION_NAME, MYSQL_METRICS, POOL_SIZE,
    MAX_RETRIES, RETRY_DELAY, LOG_LEVEL, LOG_FORMAT
)

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)


@dataclass
class MySQLMetric:
    name: str
    value: int
    avg_for_hours: float
    avg_for_seconds: float


class MySQLDiskStatusMonitor:
    def __init__(self):
        self.mongodb = None
        self.status_collection = None
        self.mysql_pools: Dict[str, Pool] = {}

    async def initialize(self):
        await MongoDBConnector.initialize()
        self.mongodb = await MongoDBConnector.get_database()
        self.status_collection = self.mongodb[MONGODB_DISK_USAGE_COLLECTION_NAME]

    async def create_mysql_pool(self, instance: Dict[str, Any]) -> Optional[Pool]:
        instance_name = instance['instance_name']
        for attempt in range(MAX_RETRIES):
            try:
                pool = await Pool.create(
                    host=instance['host'],
                    port=instance['port'],
                    user=instance['user'],
                    password=decrypt_password(instance['password']),
                    db=instance.get('db', ''),
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

    async def execute_mysql_query(self, connection: Connection, query: str, single_row: bool = False) -> Optional[Any]:
        try:
            async with connection.cursor() as cur:
                await cur.execute(query)
                if single_row:
                    result = await cur.fetchone()
                    return int(result[1]) if result else 0
                else:
                    return {row[0]: row[1] for row in await cur.fetchall()}
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            return None

    def process_metrics(self, data: Dict[str, str], uptime: int) -> List[MySQLMetric]:
        processed_data = []
        for key, value in data.items():
            if key in MYSQL_METRICS and value != '0':
                value = int(value)
                avg_for_hours = round(value / max(uptime / 3600, 1), 2)
                avg_for_seconds = round(value / max(uptime, 1), 2)
                processed_data.append(MySQLMetric(key, value, avg_for_hours, avg_for_seconds))
        return sorted(processed_data, key=lambda x: x.value, reverse=True)

    async def store_metrics_to_mongodb(self, instance_name: str, metrics: List[MySQLMetric]):
        document = {
            'timestamp': datetime.now(pytz.utc),
            'instance_name': instance_name,
            'metrics': [metric.__dict__ for metric in metrics]
        }
        await self.status_collection.insert_one(document)

    async def fetch_and_save_instance_data(self, instance: Dict[str, Any], pool: Pool):
        async with pool.acquire() as conn:
            uptime = await self.execute_mysql_query(conn, "SHOW GLOBAL STATUS LIKE 'Uptime';", True)
            if uptime is None:
                logger.warning(f"Could not retrieve uptime for {instance['instance_name']}")
                return

            raw_status = {}
            for metric in MYSQL_METRICS:
                query = f"SHOW GLOBAL STATUS LIKE '{metric}';"
                result = await self.execute_mysql_query(conn, query)
                if result:
                    raw_status.update(result)

            if not raw_status:
                logger.warning(f"Could not retrieve global status for {instance['instance_name']}")
                return

            processed_metrics = self.process_metrics(raw_status, uptime)
            await self.store_metrics_to_mongodb(instance["instance_name"], processed_metrics)

    async def run(self):
        try:
            await self.initialize()
            instances = await load_instances_from_mongodb()

            for instance in instances:
                if instance['instance_name'] not in self.mysql_pools:
                    pool = await self.create_mysql_pool(instance)
                    if pool:
                        self.mysql_pools[instance['instance_name']] = pool

            tasks = [
                self.fetch_and_save_instance_data(instance, self.mysql_pools[instance['instance_name']])
                for instance in instances
                if instance['instance_name'] in self.mysql_pools
            ]
            await asyncio.gather(*tasks)

        except Exception as e:
            logger.error(f"An error occurred: {e}")
        finally:
            await self.cleanup()

    async def cleanup(self):
        for pool_name, pool in self.mysql_pools.items():
            try:
                pool.close()
                await pool.wait_closed()
                logger.info(f"Closed connection pool for {pool_name}")
            except Exception as e:
                logger.error(f"An error occurred while closing the pool {pool_name}: {e}")


async def run_selected_metrics_status():
    monitor = MySQLDiskStatusMonitor()
    await monitor.run()


if __name__ == '__main__':
    asyncio.run(run_selected_metrics_status())