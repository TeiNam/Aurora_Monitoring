import asyncio
import asyncmy
import pytz
import re
import os
import traceback
from datetime import datetime, timedelta

# Local imports
from dotenv import load_dotenv
from modules.mongodb_connector import MongoDBConnector
from modules.crypto_utils import decrypt_password
from modules.time_utils import get_kst_time
from modules.json_loader import load_json
from modules.slack_noti import send_slack_notification
from config import MONGODB_SLOWLOG_COLLECTION_NAME, EXEC_TIME

load_dotenv()

allowed_users_env = os.getenv("ALLOWED_USERS", "")
allowed_users = [user.strip() for user in allowed_users_env.split(',') if user.strip()]

pid_time_cache = {}


async def query_mysql_instance(instance_name, pool, collection, status_dict):
    try:
        current_pids = set()
        sql_query = """SELECT `ID`, `DB`, `USER`, `HOST`, `TIME`, `INFO`
                        FROM `information_schema`.`PROCESSLIST`
                        WHERE info IS NOT NULL
                        AND DB not in ('information_schema', 'mysql', 'performance_schema')
                        AND USER not in ('monitor', 'rdsadmin')
                        ORDER BY `TIME` DESC"""

        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql_query)
                result = await cur.fetchall()

                for row in result:
                    pid, db, user, host, time, info = row
                    current_pids.add(pid)

                    if time >= EXEC_TIME:
                        cache_data = pid_time_cache.setdefault((instance_name, pid), {'max_time': 0})
                        cache_data['max_time'] = max(cache_data['max_time'], time)
                        # Debug
                        #print(f"{get_kst_time()} - Data cached for instance: {instance_name}, pid: {pid}, max_time: {cache_data['max_time']}")

                        if 'start' not in cache_data:
                            utc_now = datetime.now(pytz.utc)
                            utc_start_timestamp = int((utc_now - timedelta(seconds=EXEC_TIME)).timestamp())
                            utc_start_datetime: datetime = datetime.fromtimestamp(utc_start_timestamp, pytz.utc)
                            cache_data['start'] = utc_start_datetime

                        info_cleaned = re.sub(' +', ' ', info).encode('utf-8', 'ignore').decode('utf-8')
                        info_cleaned = re.sub(r'[\n\t\r]+', ' ', info_cleaned).strip()

                        cache_data['details'] = {
                            'instance': instance_name,
                            'db': db,
                            'pid': pid,
                            'user': user,
                            'host': host,
                            'time': time,
                            'sql_text': info_cleaned
                        }

                for (instance, pid), cache_data in list(pid_time_cache.items()):
                    if pid not in current_pids and instance == instance_name:
                        data_to_insert = cache_data['details']
                        data_to_insert['time'] = cache_data['max_time']
                        data_to_insert['start'] = cache_data['start']

                        utc_now = datetime.now(pytz.utc)
                        data_to_insert['end'] = utc_now

                        # DEBUG
                        # print(f"{get_kst_time()} - {data_to_insert}")
                        if not await collection.find_one({'_id': data_to_insert.get('_id')}):
                            await collection.insert_one(data_to_insert)

                            # 슬랙 노티 모듈을 통한 알림 발송
                            user_email = f"{data_to_insert['user']}@example.com"
                            if data_to_insert['user'].lower() in allowed_users:
                                db_info = data_to_insert.get('db', '알 수 없는 DB')
                                instance_info = data_to_insert.get('instance', '알 수 없는 Instance')
                                pid_info = data_to_insert.get('pid')
                                slack_title = "[SlowQuery Alert]"
                                slack_message = f"님이 실행한 SQL쿼리(PID: {pid_info})가\n *{instance_info}*, *{db_info}* DB에서 *{data_to_insert['time']}* 초 동안 실행 되었습니다.\n 쿼리 검수 및 실행 시 주의가 필요합니다. \n http://172.30.111.48:8000/sql-plan?pid={pid_info}"
                                await send_slack_notification(user_email, slack_title, slack_message)

                        del pid_time_cache[(instance, pid)]

        await asyncio.sleep(1)
        status_dict[instance_name] = "Success"
    except Exception as e:
        if "Cannot use MongoClient after close" in str(e):
            await MongoDBConnector.reconnect()
            status_dict[instance_name] = "Reconnected to MongoDB"
        else:
            status_dict[instance_name] = f"Failed: {e}"


async def run_mysql_slow_queries():
    pools = {}
    instance_status = {}
    max_retries = 3

    try:
        await MongoDBConnector.initialize()

        db = await MongoDBConnector.get_database()
        collection = db[MONGODB_SLOWLOG_COLLECTION_NAME]

        instances = load_json("rds_instances.json")

        while True:
            tasks = []
            for instance_data in instances:
                instance_name = instance_data["instance_name"]
                if instance_name in pools and pools[instance_name] is None:
                    continue
                host = instance_data["host"]
                port = instance_data["port"]
                user = instance_data["user"]
                db = instance_data["db"]
                encrypted_password_base64 = instance_data["password"]
                decrypted_password = decrypt_password(encrypted_password_base64)

                if instance_name not in pools:
                    for attempt in range(max_retries):
                        try:
                            pool = await asyncmy.create_pool(
                                host=host, port=port, user=user, password=decrypted_password, db=db
                            )
                            pools[instance_name] = pool
                            print(f"{get_kst_time()} - Connection pool created successfully for {instance_name}")
                            break
                        except Exception as e:
                            print(f"{get_kst_time()} - Attempt {attempt + 1} failed for {instance_name}: {e}")
                            if attempt == max_retries - 1:
                                pools[instance_name] = None
                                print(
                                    f"{get_kst_time()} - Maximum retry attempts reached for {instance_name}. "
                                    f"Skipping this instance.")

                if pools.get(instance_name):
                    tasks.append(query_mysql_instance(instance_name, pools[instance_name], collection, instance_status))

            if tasks:
                await asyncio.gather(*tasks)
            for instance, status in instance_status.items():
                if "Failed" in status:
                    print(f"{get_kst_time()} - Instance {instance} Status: {status}")

            await asyncio.sleep(1)

    except asyncio.CancelledError:
        print("{get_kst_time()} - Async task was cancelled. Cleaning up...")
        for pool_name, pool in pools.items():
            if pool is not None:
                try:
                    await pool.close()
                except Exception as e1:
                    print(f"{get_kst_time()} - An error occurred while closing the pool {pool_name}: {e1}")

        print(f"{get_kst_time()} - Resources have been released and program has been terminated safely.")
        traceback.print_exc()

    except Exception as e2:
        print(f"An error occurred: {e2}")
        traceback.print_exc()

    finally:
        for pool_name, pool in pools.items():
            if pool is not None:
                try:
                    await pool.close()
                except Exception as ex:
                    print(f"{get_kst_time()} - An error occurred while closing the pool {pool_name}: {ex}")

        await asyncio.sleep(0.1)


if __name__ == '__main__':
    asyncio.run(run_mysql_slow_queries())
