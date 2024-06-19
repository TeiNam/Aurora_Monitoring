import asyncio
import pytz
from datetime import datetime
from modules.load_instance import load_instances_from_mongodb, handle_instance
from modules.mongodb_connector import MongoDBConnector
from config import MONGODB_DISK_USAGE_COLLECTION_NAME


async def execute_mysql_query(connection, query_string, single_row=False):
    try:
        async with connection.cursor() as cur:
            await cur.execute(query_string)
            if single_row:
                result = await cur.fetchone()
                return int(result[1]) if result else 0
            else:
                return {row[0]: row[1] for row in await cur.fetchall()}
    except Exception as e1:
        print(f"Failed to query MySQL status: {e1}")
        return None


def process_selected_metrics(data, uptime):
    selected_metrics = [
        'Binlog_cache_use', 'Binlog_cache_disk_use',
        'Created_tmp_tables', 'Created_tmp_files', 'Created_tmp_disk_tables'
    ]
    processed_data = {}
    for key, value in data.items():
        if key in selected_metrics and value != '0':
            value = int(value)
            avg_for_hours = round(value / max(uptime / 3600, 1), 2)
            avg_for_seconds = round(value / max(uptime, 1), 2)
            processed_data[key] = {
                'total': value,
                'avgForHours': avg_for_hours,
                'avgForSeconds': avg_for_seconds
            }
    return dict(sorted(processed_data.items(), key=lambda item: item[1]['total'], reverse=True))


async def store_metrics_to_mongodb(collection, instance_name, metrics_status):
    document = {
        'timestamp': datetime.now(pytz.utc),
        'instance_name': instance_name,
        'command_status': metrics_status
    }
    await collection.insert_one(document)


async def fetch_and_save_instance_data(connection, instance, collection):
    uptime = await execute_mysql_query(connection, "SHOW GLOBAL STATUS LIKE 'Uptime';", True)
    if uptime is None:
        print(f"Could not retrieve uptime for {instance['instance_name']}")
        return

    selected_metrics = [
        'Binlog_cache_use', 'Binlog_cache_disk_use',
        'Created_tmp_tables', 'Created_tmp_files', 'Created_tmp_disk_tables'
    ]

    raw_status = {}
    for metric in selected_metrics:
        query = f"SHOW GLOBAL STATUS LIKE '{metric}';"
        result = await execute_mysql_query(connection, query)
        if result:
            raw_status.update(result)

    if not raw_status:
        print(f"Could not retrieve global status for {instance['instance_name']}")
        return

    processed_status = process_selected_metrics(raw_status, uptime)
    await store_metrics_to_mongodb(collection, instance["instance_name"], processed_status)


async def handle_custom_instance(instance, collection):
    await handle_instance(instance, collection, fetch_and_save_instance_data)


async def run_selected_metrics_status():
    await MongoDBConnector.initialize()
    mongodb = await MongoDBConnector.get_database()
    status_collection = mongodb[MONGODB_DISK_USAGE_COLLECTION_NAME]
    instances = await load_instances_from_mongodb()
    tasks = [handle_custom_instance(instance, status_collection) for instance in instances]
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(run_selected_metrics_status())
