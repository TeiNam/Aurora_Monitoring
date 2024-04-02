import asyncio
import asyncmy
from pymongo import UpdateOne

from modules.mongodb_connector import MongoDBConnector
from modules.crypto_utils import decrypt_password
from modules.load_instance import load_instances_from_mongodb
from config import MONGODB_HISTORY_COLLECTION_NAME, MONGODB_DIGEST_COLLECTION_NAME


async def collect_and_store_digest_data(instance_name, conn, digest_collection):
    digest_query = """SELECT SCHEMA_NAME, DIGEST, DIGEST_TEXT, COUNT_STAR, SUM_TIMER_WAIT, MIN_TIMER_WAIT,
                      AVG_TIMER_WAIT, MAX_TIMER_WAIT, SUM_LOCK_TIME, SUM_ERRORS, SUM_WARNINGS, SUM_ROWS_AFFECTED,
                      SUM_ROWS_SENT, SUM_ROWS_EXAMINED, SUM_CREATED_TMP_DISK_TABLES, SUM_CREATED_TMP_TABLES,
                      SUM_SELECT_FULL_JOIN, SUM_SELECT_FULL_RANGE_JOIN, SUM_SELECT_RANGE, SUM_SELECT_RANGE_CHECK,
                      SUM_SELECT_SCAN, SUM_SORT_MERGE_PASSES, SUM_SORT_RANGE, SUM_SORT_ROWS, SUM_SORT_SCAN,
                      SUM_NO_INDEX_USED, SUM_NO_GOOD_INDEX_USED, FIRST_SEEN, LAST_SEEN
                      FROM performance_schema.events_statements_summary_by_digest
                      WHERE SCHEMA_NAME NOT IN ('mysql', 'performance_schema', 'information_schema')
                      AND SCHEMA_NAME IS NOT NULL"""

    async with conn.cursor() as cur:
        await cur.execute(digest_query)
        digest_results = await cur.fetchall()

    digest_operations = [
        UpdateOne(
            {'digest': row[1], 'instance': instance_name},
            {'$set': {col: row[idx] for idx, col in enumerate([
                'schema_name', 'digest', 'digest_text', 'count_star', 'sum_timer_wait', 'min_timer_wait',
                'avg_timer_wait', 'max_timer_wait', 'sum_lock_time', 'sum_errors', 'sum_warnings',
                'sum_rows_affected', 'sum_rows_sent', 'sum_rows_examined', 'sum_created_tmp_disk_tables',
                'sum_created_tmp_tables', 'sum_select_full_join', 'sum_select_full_range_join',
                'sum_select_range', 'sum_select_range_check', 'sum_select_scan', 'sum_sort_merge_passes',
                'sum_sort_range', 'sum_sort_rows', 'sum_sort_scan', 'sum_no_index_used',
                'sum_no_good_index_used', 'first_seen', 'last_seen'])}},
            upsert=True)
        for row in digest_results
    ]
    if digest_operations:
        await digest_collection.bulk_write(digest_operations)


async def collect_and_store_history_data(instance_name, conn, history_collection):
    history_query = """
    SELECT
        h.DIGEST,
        h.SQL_TEXT,
        d.SCHEMA_NAME,
        (CASE
            WHEN h.EVENT_NAME = 'statement/sql/select' THEN 'select'
            WHEN h.EVENT_NAME = 'statement/sql/insert' THEN 'insert'
            WHEN h.EVENT_NAME = 'statement/sql/update' THEN 'update'
            WHEN h.EVENT_NAME = 'statement/sql/delete' THEN 'delete'
            WHEN h.EVENT_NAME = 'statement/sp/stmt' THEN 'sp'
        END) AS EVENT_NAME,
        h.THREAD_ID,
        h.EVENT_ID,
        h.TIMER_START,
        h.TIMER_END,
        h.TIMER_WAIT
    FROM
        performance_schema.events_statements_history h
    INNER JOIN performance_schema.events_statements_summary_by_digest d ON d.DIGEST = h.DIGEST
    WHERE
        h.EVENT_NAME IN ('statement/sql/select', 'statement/sql/insert', 'statement/sql/update', 'statement/sql/delete', 'statement/sp/stmt')
        AND d.SCHEMA_NAME NOT IN ('mysql', 'performance_schema', 'information_schema')
        AND d.SCHEMA_NAME IS NOT NULL;
    """

    async with conn.cursor() as cur:
        await cur.execute(history_query)
        history_results = await cur.fetchall()

    operations = []
    for row in history_results:
        document = {
            'instance': instance_name,
            'digest': row[0],
            'sql_text': row[1],
            'schema_name': row[2],
            'event_name': row[3],
            'thread_id': row[4],
            'event_id': row[5],
            'timer_start': row[6],
            'timer_end': row[7],
            'timer_wait': row[8]
        }

        operations.append(UpdateOne(
            {'instance': instance_name, 'thread_id': row[4], 'event_id': row[5]},
            {'$set': document},
            upsert=True
        ))

    if operations:
        await history_collection.bulk_write(operations)


async def collect_and_store_data(instance_data, pool, db):
    instance_name = instance_data["instance_name"]
    digest_collection = db[MONGODB_DIGEST_COLLECTION_NAME]
    history_collection = db[MONGODB_HISTORY_COLLECTION_NAME]

    async with pool.acquire() as conn:
        # Run digest and history data collection in parallel
        await asyncio.gather(
            collect_and_store_digest_data(instance_name, conn, digest_collection),
            collect_and_store_history_data(instance_name, conn, history_collection)
        )


async def run():
    try:
        await MongoDBConnector.initialize()
        db = await MongoDBConnector.get_database()
        instances = await load_instances_from_mongodb()

        tasks = []
        for instance_data in instances:
            pool = await asyncmy.create_pool(
                host=instance_data["host"], port=instance_data["port"],
                user=instance_data["user"], password=decrypt_password(instance_data["password"]),
                db=instance_data.get("db", "")
            )
            task = collect_and_store_data(instance_data, pool, db)
            tasks.append(task)

        await asyncio.gather(*tasks)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    asyncio.run(run())
