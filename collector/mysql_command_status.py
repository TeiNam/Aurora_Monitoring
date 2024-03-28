import asyncio
import pytz
from datetime import datetime
from modules.load_instance import load_instances_from_mongodb, handle_instance
from modules.mongodb_connector import MongoDBConnector
from config import MONGODB_STATUS_COLLECTION_NAME


async def query_mysql_status(connection, query_string, single_row=False):
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


def process_global_status(data, uptime):
    desired_commands = [
        'Com_select', 'Com_delete', 'Com_delete_multi',
        'Com_insert', 'Com_insert_select', 'Com_replace',
        'Com_replace_select', 'Com_update', 'Com_update_multi',
        'Com_flush', 'Com_kill', 'Com_purge', 'Com_admin_commands',
        'Com_commit', 'Com_begin', 'Com_rollback'
    ]
    processed_data = {}
    total_sum = 0
    for key, value in data.items():
        if key in desired_commands and value != '0':
            total_sum += int(value)
    for key, value in data.items():
        if key in desired_commands and value != '0':
            new_key = key[4:]
            value = int(value)
            avg_for_hours = round(value / max(uptime / 3600, 1), 2)
            avg_for_seconds = round(value / max(uptime, 1), 2)
            percentage = round((value / total_sum) * 100, 2) if total_sum > 0 else 0
            processed_data[new_key] = {
                'total': value,
                'avgForHours': avg_for_hours,
                'avgForSeconds': avg_for_seconds,
                'percentage': percentage
            }
    return dict(sorted(processed_data.items(), key=lambda item: item[1]['total'], reverse=True))


async def save_mysql_command_status_to_mongodb(collection, instance_name, command_status):
    document = {
        'timestamp': datetime.now(pytz.utc),
        'instance_name': instance_name,
        'command_status': command_status
    }
    await collection.insert_one(document)


async def query_instance_and_save_to_db(connection, instance, collection):
    uptime = await query_mysql_status(connection, "SHOW GLOBAL STATUS LIKE 'Uptime';", True)
    if uptime is None:
        print(f"Could not retrieve uptime for {instance['instance_name']}")
        return
    raw_status = await query_mysql_status(connection, "SHOW GLOBAL STATUS LIKE 'Com_%';")
    if raw_status is None:
        print(f"Could not retrieve global status for {instance['instance_name']}")
        return
    processed_status = process_global_status(raw_status, uptime)
    await save_mysql_command_status_to_mongodb(collection, instance["instance_name"], processed_status)


async def custom_handle_instance(instance, collection):
    # `handle_instance` 함수에 대한 wrapper로, `query_instance_and_save_to_db` 함수를 인자로 전달합니다.
    await handle_instance(instance, collection, query_instance_and_save_to_db)


async def run_mysql_command_status():
    await MongoDBConnector.initialize()
    mongodb = await MongoDBConnector.get_database()
    status_collection = mongodb[MONGODB_STATUS_COLLECTION_NAME]
    instances = await load_instances_from_mongodb()
    tasks = [custom_handle_instance(instance, status_collection) for instance in instances]
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(run_mysql_command_status())
