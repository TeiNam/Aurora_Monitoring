import asyncio
import asyncmy
import pytz
from datetime import datetime
from modules.crypto_utils import decrypt_password
from modules.mongodb_connector import MongoDBConnector
from modules.json_loader import load_json
from modules.time_utils import get_kst_time
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
        print(f"{get_kst_time()} - Failed to query MySQL status: {e1}")
        return None


def process_global_status(data, uptime):
    desired_commands = [
        'Com_select', 'Com_delete', 'Com_delete_multi',
        'Com_insert', 'Com_insert_select', 'Com_replace',
        'Com_replace_select', 'Com_update', 'Com_update_multi',
        'Com_flush', 'Com_kill', 'Com_purge', 'Com_admin_commands'
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

    processed_data_sorted = dict(sorted(processed_data.items(), key=lambda item: item[1]['total'], reverse=True))

    return processed_data_sorted


async def save_mysql_command_status_to_mongodb(collection, instance_name, command_status):
    update_document = {
        '$set': {
            'timestamp': datetime.now(pytz.utc),
            'command_status': command_status
        }
    }
    await collection.update_one({'instance_name': instance_name}, update_document, upsert=True)


async def query_instance_and_save_to_db(connection, instance, collection):
    uptime = await query_mysql_status(connection, "SHOW GLOBAL STATUS LIKE 'Uptime';", True)
    if uptime is None:
        print(f"{get_kst_time()} - Could not retrieve uptime for {instance['instance_name']}")
        return

    raw_status = await query_mysql_status(connection, "SHOW GLOBAL STATUS LIKE 'Com_%';")
    if raw_status is None:
        print(f"{get_kst_time()} - Could not retrieve global status for {instance['instance_name']}")
        return

    processed_status = process_global_status(raw_status, uptime)
    await save_mysql_command_status_to_mongodb(collection, instance["instance_name"], processed_status)


async def handle_instance(instance, collection):
    try:
        decrypted_password = decrypt_password(instance["password"])
        connection = await asyncmy.connect(
            host=instance["host"], port=instance["port"],
            user=instance["user"], password=decrypted_password, db=instance["db"]
        )
        await query_instance_and_save_to_db(connection, instance, collection)
        await connection.ensure_closed()
    except Exception as e2:
        print(f"{get_kst_time()} - Failed to handle instance: {e2}")


async def run_mysql_command_status():
    await MongoDBConnector.initialize()

    mongodb = await MongoDBConnector.get_database()
    collection = mongodb[MONGODB_STATUS_COLLECTION_NAME]

    instances = load_json('rds_instances.json')
    tasks = [handle_instance(instance, collection) for instance in instances]
    await asyncio.gather(*tasks)

    MongoDBConnector.client.close()


if __name__ == '__main__':
    try:
        asyncio.run(run_mysql_command_status())
    except Exception as ex:
        print(f"{get_kst_time()} - An error occurred: {ex}")
