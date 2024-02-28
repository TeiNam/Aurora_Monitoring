import asyncio
import asyncmy
import traceback
from pymongo import InsertOne, MongoClient

# Local imports
from modules.mongodb_connector import MongoDBConnector
from modules.crypto_utils import decrypt_password
from modules.time_utils import get_kst_time
from modules.json_loader import load_json
from config import MONGODB_HISTORY_COLLECTION_NAME, MONGODB_DIGEST_COLLECTION_NAME


async def fetch_existing_digests(db):
    """MongoDB에서 존재하는 digest 목록을 조회합니다."""
    collection = db[MONGODB_DIGEST_COLLECTION_NAME]
    cursor = collection.find({}, {'digest': 1})
    existing_digests = set(doc['digest'] for doc in await cursor.to_list(length=None))
    return existing_digests


async def check_performance_schema(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SHOW VARIABLES LIKE 'performance_schema'")
            result = await cur.fetchone()
            return result is not None and result[1].lower() == 'on'


async def query_mysql_instance(instance_name, pool, collection, status_dict, existing_digests):
    try:
        batch_size = 1000  # 적절한 배치 크기 설정
        offset = 0

        while True:
            sql_query = f"""SELECT EVENT_NAME, DIGEST, SQL_TEXT, TIMER_START, TIMER_END, TIMER_WAIT
                            FROM performance_schema.events_statements_history
                            WHERE EVENT_NAME in ('statement/sql/select','statement/sql/insert','statement/sql/update','statement/sp/stmt')
                            LIMIT {batch_size} OFFSET {offset}"""

            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql_query)
                    result = await cur.fetchall()

                    if not result:
                        break

                    operations = []
                    for row in result:
                        if row[1] in existing_digests:  # digest가 존재하는 경우에만 추가
                            history_data = {
                                'instance': instance_name,
                                'event_name': row[0],
                                'digest': row[1],
                                'sql_text': row[2],
                                'timer_start': row[3],
                                'timer_end': row[4],
                                'timer_wait': row[5]
                            }
                            operations.append(InsertOne(history_data))

                    if operations:
                        await collection.bulk_write(operations)

                    offset += batch_size

        status_dict[instance_name] = "Success"
    except Exception as e:
        status_dict[instance_name] = f"Failed: {e}"
        traceback.print_exc()


async def run_gather_history():
    pools = {}
    instance_status = {}
    max_retries = 3

    try:
        await MongoDBConnector.initialize()
        db = await MongoDBConnector.get_database()
        collection = db[MONGODB_HISTORY_COLLECTION_NAME]

        existing_digests = await fetch_existing_digests(db)

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
                        query_mysql_instance(instance_name, pools[instance_name], collection, instance_status, existing_digests))
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
                    pool.close()
                except Exception as ex:
                    print(f"{get_kst_time()} - An error occurred while closing the pool {pool_name}: {ex}")

        print(f"{get_kst_time()} - Script completed and resources have been released.")

if __name__ == '__main__':
    asyncio.run(run_gather_history())
