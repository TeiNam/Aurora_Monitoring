import re
import asyncio
import asyncmy
import pytz
from datetime import datetime, timedelta
from dotenv import load_dotenv
from modules.crypto_utils import decrypt_password
from modules.time_utils import get_kst_time
from modules.mongodb_connector import MongoDBConnector
from modules.json_loader import load_json
from config import MONGODB_SLOWLOG_COLLECTION_NAME, EXEC_TIME

load_dotenv()

EMOJI_PATTERN = re.compile("["
                           u"\U0001F600-\U0001F64F"
                           u"\U0001F300-\U0001F5FF"
                           u"\U0001F680-\U0001F6FF"
                           u"\U0001F1E0-\U0001F1FF"
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)


def remove_emoji(string):
    return EMOJI_PATTERN.sub(r'', string)


def clean_sql_text(info: str) -> str:
    info_no_emoji = remove_emoji(info)
    info_cleaned = re.sub(r'[\n\t\r]+', ' ', info_no_emoji).strip().encode('utf-8', 'ignore').decode('utf-8')
    return re.sub(' +', ' ', info_cleaned)


class MySqlMonitor:

    def __init__(self, instance_name: str, details: dict, collection):
        self.instance_name = instance_name
        self.details = details
        self.collection = collection
        self.pid_time_cache = {}

    async def query_mysql_instance(self, pool):
        try:
            utc_now = datetime.now(pytz.utc)
            current_pids = set()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """SELECT `ID`, `USER`, `HOST`, `DB`, `TIME`, `INFO`
                           FROM `information_schema`.`PROCESSLIST`
                           WHERE info IS NOT NULL
                           AND DB <> 'information_schema'
                           AND USER <> 'monitor'
                           ORDER BY `TIME` DESC"""
                    )
                    result = await cur.fetchall()

                    for row in result:
                        pid, user, host, db, time, info = row
                        current_pids.add(pid)
                        if time < EXEC_TIME:
                            continue
                        cache_data = self.pid_time_cache.setdefault(pid, {'max_time': 0})
                        cache_data['max_time'] = max(cache_data['max_time'], time)

                        if 'start' not in cache_data:
                            utc_start = utc_now - timedelta(seconds=exec_time)
                            cache_data['start'] = utc_start

                        cache_data['details'] = {
                            'instance': self.instance_name,
                            'pid': pid,
                            'user': user,
                            'host': host,
                            'db': db,
                            'time': time,
                            'sql_text': clean_sql_text(info)
                        }

                    for pid, cache_data in list(self.pid_time_cache.items()):
                        if pid not in current_pids:
                            existing_doc = await self.collection.find_one({'pid': pid, 'instance': self.instance_name})
                            if not existing_doc:
                                data_to_insert = cache_data['details']
                                data_to_insert['time'] = cache_data['max_time']
                                data_to_insert['start'] = cache_data['start']
                                data_to_insert['end'] = utc_now
                                await self.collection.insert_one(data_to_insert)
                            del self.pid_time_cache[pid]

        except Exception as e:
            print(f"{get_kst_time()} - Error querying instance {self.instance_name}: {e}")

        finally:
            for pid, cache_data in self.pid_time_cache.items():
                if 'details' in cache_data:
                    try:
                        await self.collection.insert_one(cache_data['details'])
                    except Exception as e:
                        print(f"{get_kst_time()} - Error saving cached data for PID {pid}: {e}")

            self.pid_time_cache.clear()

    async def close(self):
        for instance_name, pool in self.pools.items():
            if pool is not None:
                try:
                    await pool.close()
                    print(f"{get_kst_time()} - Connection pool closed successfully for {instance_name}")
                except Exception as e:
                    print(f"{get_kst_time()} - Error closing connection pool for {instance_name}: {e}")

        self.pools = {}


class MySqlPoolManager:

    def __init__(self, mongo_collection):
        self.instances_details = load_json("rds_instances.json")
        self.monitors = {instance_detail['instance_name']: MySqlMonitor(instance_detail['instance_name'], instance_detail, mongo_collection) for instance_detail in self.instances_details}
        self.pools = {}

    @staticmethod
    def parse_instances_from_json(json_data):
        instances = []
        for instance_data in json_data:
            instance_detail = {
                "instance_name": instance_data["instance_name"],
                "host": instance_data["host"],
                "port": instance_data["port"],
                "user": instance_data["user"],
                "db": instance_data["db"],
                "password": decrypt_password(instance_data["password"])
            }
            instances.append(instance_detail)
        return instances

    async def create_connection_pool_for_instance(self, instance_name: str, instance_settings: MySqlMonitor, max_retries=3, delay=5):
        retries = 0
        while retries < max_retries:
            try:
                self.pools[instance_name] = await asyncmy.create_pool(
                    host=instance_settings.details['host'],
                    port=instance_settings.details['port'],
                    user=instance_settings.details['user'],
                    password=instance_settings.details['password'],
                    db=instance_settings.details['db']
                )
                print(f"{get_kst_time()} - Connection pool created successfully for {instance_name}")
                return
            except Exception as e:
                retries += 1
                print(f"{get_kst_time()} - Attempt {retries} failed to create connection pool for {instance_name}: {str(e)}")
                if retries < max_retries:
                    print(f"{get_kst_time()} - Retrying to connect to {instance_name} in {delay} seconds...")
                    await asyncio.sleep(delay)
        self.pools[instance_name] = None

    async def create_pools(self):
        for instance_name, instance_settings in self.monitors.items():
            if instance_name not in self.pools or self.pools[instance_name] is None:
                await self.create_connection_pool_for_instance(instance_name, instance_settings, max_retries=3, delay=5)

    async def run_monitoring(self):
        while True:
            tasks = []
            for monitor_name, pool in self.pools.items():
                if pool is not None:
                    task = asyncio.create_task(self.monitors[monitor_name].query_mysql_instance(pool))
                    tasks.append(task)
            if tasks:
                await asyncio.gather(*tasks)
            await asyncio.sleep(1)  # 1초 대기

    async def close(self):
        for instance_name, pool in self.pools.items():
            if pool is not None:
                await pool.close()
                print(f"{get_kst_time()} - Connection pool closed successfully for {instance_name}")
        self.pools = {}


async def run_mysql_slow_queries():
    mongodb = MongoDBConnector.get_database()
    collection = mongodb[MONGODB_SLOWLOG_COLLECTION_NAME]

    manager = MySqlPoolManager(collection)
    try:
        await manager.create_pools()
        await manager.run_monitoring()
    except KeyboardInterrupt:
        print(f"{get_kst_time()} - The program was interrupted by a user.")
    finally:
        await manager.close()
        await MongoDBConnector.client.close()
        print(f"{get_kst_time()} - MySQL Slow Queries Collector's connection to MongoDB has been closed.")


if __name__ == '__main__':
    try:
        asyncio.run(run_mysql_slow_queries())
    except Exception as e:
        print(f"{get_kst_time()} - An error occurred: {e}")
