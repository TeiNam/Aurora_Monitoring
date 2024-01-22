import asyncio
import asyncmy
import traceback
from pymongo import UpdateOne

# Local imports
from modules.mongodb_connector import MongoDBConnector
from modules.crypto_utils import decrypt_password
from modules.time_utils import get_kst_time
from modules.json_loader import load_json
from config import MONGODB_DIGEST_COLLECTION_NAME

# Definitions
collection = MongoDBConnector.get_database()[MONGODB_DIGEST_COLLECTION_NAME]
pid_time_cache = {}


async def query_mysql_instance(instance_name, pool, collection, status_dict):
    try:
        batch_size = 1000  # 적절한 배치 크기 설정
        offset = 0

        while True:
            sql_query = f"""SELECT SCHEMA_NAME, DIGEST, DIGEST_TEXT, COUNT_STAR, SUM_TIMER_WAIT, MIN_TIMER_WAIT, 
                            AVG_TIMER_WAIT, MAX_TIMER_WAIT, SUM_LOCK_TIME, SUM_ERRORS, SUM_WARNINGS, SUM_ROWS_AFFECTED, 
                            SUM_ROWS_SENT, SUM_ROWS_EXAMINED, SUM_CREATED_TMP_DISK_TABLES, SUM_CREATED_TMP_TABLES, 
                            SUM_SELECT_FULL_JOIN, SUM_SELECT_FULL_RANGE_JOIN, SUM_SELECT_RANGE, SUM_SELECT_RANGE_CHECK, 
                            SUM_SELECT_SCAN, SUM_SORT_MERGE_PASSES, SUM_SORT_RANGE, SUM_SORT_ROWS, SUM_SORT_SCAN, 
                            SUM_NO_INDEX_USED, SUM_NO_GOOD_INDEX_USED, FIRST_SEEN, LAST_SEEN
                            FROM performance_schema.events_statements_summary_by_digest
                            WHERE SCHEMA_NAME NOT IN ('mysql', 'performance_schema', 'information_schema')
                            AND SCHEMA_NAME IS NOT NULL
                            LIMIT {batch_size} OFFSET {offset}"""

            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql_query)
                    result = await cur.fetchall()

                    if not result:
                        break

                    operations = []
                    for row in result:
                        digest_data = {col: row[idx] for idx, col in enumerate([
                            'schema_name', 'digest', 'digest_text', 'count_star', 'sum_timer_wait', 'min_timer_wait',
                            'avg_timer_wait', 'max_timer_wait', 'sum_lock_time', 'sum_errors', 'sum_warnings',
                            'sum_rows_affected', 'sum_rows_sent', 'sum_rows_examined', 'sum_created_tmp_disk_tables',
                            'sum_created_tmp_tables', 'sum_select_full_join', 'sum_select_full_range_join',
                            'sum_select_range', 'sum_select_range_check', 'sum_select_scan', 'sum_sort_merge_passes',
                            'sum_sort_range', 'sum_sort_rows', 'sum_sort_scan', 'sum_no_index_used',
                            'sum_no_good_index_used', 'first_seen', 'last_seen'])}

                        operations.append(UpdateOne(
                            {'digest': row[1], 'instance': instance_name},
                            {'$set': digest_data},
                            upsert=True))

                    if operations:
                        await collection.bulk_write(operations)

                    offset += batch_size

        status_dict[instance_name] = "Success"
    except Exception as e:
        status_dict[instance_name] = f"Failed: {e}"


async def check_performance_schema(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SHOW VARIABLES LIKE 'performance_schema'")
            result = await cur.fetchone()
            return result is not None and result[1].lower() == 'on'


async def run_gather_digest():
    pools = {}
    instance_status = {}
    max_retries = 3

    try:
        MongoDBConnector.initialize()
        instances = load_json("rds_instances.json")
        tasks = []

        for instance_data in instances:
            instance_name = instance_data["instance_name"]
            host = instance_data["host"]
            port = instance_data["port"]
            user = instance_data["user"]
            db = instance_data["db"]
            encrypted_password_base64 = instance_data["password"]
            decrypted_password = decrypt_password(encrypted_password_base64)

            # 연결 풀 생성
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
                        print(f"{get_kst_time()} - Maximum retry attempts reached for {instance_name}. Skipping this instance.")

            # performance_schema가 활성화되어 있는지 확인
            if pools.get(instance_name):
                performance_schema_enabled = await check_performance_schema(pools[instance_name])
                if performance_schema_enabled:
                    # 코루틴을 태스크로 변환하여 추가
                    task = asyncio.create_task(
                        query_mysql_instance(instance_name, pools[instance_name], collection, instance_status))
                    tasks.append(task)
                else:
                    print(f"{get_kst_time()} - Performance schema is not enabled for {instance_name}. Skipping.")
                    pools[instance_name] = None

            if tasks:
                await asyncio.gather(*tasks)

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

    finally:
        for pool_name, pool in pools.items():
            if pool is not None:
                try:
                    await pool.close()
                except Exception as ex:
                    print(f"{get_kst_time()} - An error occurred while closing the pool {pool_name}: {ex}")

        print(f"{get_kst_time()} - Script completed and resources have been released.")

if __name__ == '__main__':
        asyncio.run(run_gather_digest())
