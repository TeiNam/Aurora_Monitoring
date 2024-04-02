import asyncmy
from modules.crypto_utils import decrypt_password
from modules.mongodb_connector import MongoDBConnector
from config import MONGODB_RDS_INSTANCE_LIST_COLLATION_NAME


async def load_instances_from_mongodb():
    mongodb = await MongoDBConnector.get_database()
    collection = mongodb[MONGODB_RDS_INSTANCE_LIST_COLLATION_NAME]
    instances = await collection.find().to_list(length=None)
    return instances


async def handle_instance(instance, collection, query_instance_and_save_to_db):
    try:
        decrypted_password = decrypt_password(instance["password"])
        connection = await asyncmy.connect(
            host=instance["host"], port=instance.get("port", 3306),
            user=instance["user"], password=decrypted_password, db=instance.get("db", "")
        )
        await query_instance_and_save_to_db(connection, instance, collection)
        await connection.ensure_closed()
    except Exception as e:
        print(f"Failed to handle instance: {e}")
