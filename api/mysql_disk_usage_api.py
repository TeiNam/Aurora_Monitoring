from modules.mongodb_connector import MongoDBConnector
from fastapi import FastAPI, HTTPException, Query
from typing import List
from datetime import timedelta, datetime
from config import MONGODB_DISK_USAGE_COLLECTION_NAME

app = FastAPI()

kst_delta = timedelta(hours=9)


async def get_all_command_status(instance_name, commands):
    db = await MongoDBConnector.get_database()
    collection = db[MONGODB_DISK_USAGE_COLLECTION_NAME]
    query = {'instance_name': instance_name}
    projection = {'_id': 0, 'timestamp': 1, 'command_status': 1}
    cursor = collection.find(query, projection).sort('timestamp', -1)
    documents = await cursor.to_list(length=None)
    if documents:
        return documents
    return None


def transform_data_to_table_format(data, commands: List[str]):
    transformed_data = []
    for entry in data:
        timestamp = entry.get("timestamp")
        if timestamp:
            timestamp = timestamp + kst_delta  # Convert to KST
            timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')  # Format the timestamp
        if "command_status" in entry:
            for command, details in entry["command_status"].items():
                if commands and command not in commands:
                    continue
                row = {
                    "timestamp": timestamp,
                    "command": command,
                    "total": details.get("total", 0),
                    "avgForHours": details.get("avgForHours", 0),
                    "avgForSeconds": details.get("avgForSeconds", 0)
                }
                transformed_data.append(row)
    return transformed_data


@app.get("/status/")
async def read_status(instance_name: str = Query(None, description="The name of the instance to retrieve"),
                      commands: List[str] = Query(None, description="List of commands to retrieve", alias="command")):
    if instance_name:
        data = await get_all_command_status(instance_name, commands)
        if data:
            transformed_data = transform_data_to_table_format(data, commands)
            return transformed_data
        raise HTTPException(status_code=404, detail="Item not found")
    raise HTTPException(status_code=400, detail="Missing instance name")
