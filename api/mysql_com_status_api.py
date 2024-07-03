from modules.mongodb_connector import MongoDBConnector
from fastapi import FastAPI, HTTPException, Query
from config import MONGODB_STATUS_COLLECTION_NAME

app = FastAPI()


async def get_command_status(instance_name):
    db = await MongoDBConnector.get_database()
    collection = db[MONGODB_STATUS_COLLECTION_NAME]
    document = await collection.find({'instance_name': instance_name},
                                     {'_id': 0}).sort('timestamp', -1).limit(1).to_list(length=1)
    if document:
        return document[0]
    return None


def transform_data_to_table_format(data):
    transformed_data = []
    if data and "command_status" in data:
        for command, details in data["command_status"].items():
            row = {
                "command": command,
                "total": details.get("total", 0),
                "avgForHours": details.get("avgForHours", 0),
                "avgForSeconds": details.get("avgForSeconds", 0),
                "percentage": details.get("percentage", 0)
            }
            transformed_data.append(row)
    return transformed_data


@app.get("/")
async def read_status(instance_name: str = Query(None, description="The name of the instance to retrieve")):
    if instance_name:
        data = await get_command_status(instance_name)
        if data:
            transformed_data = transform_data_to_table_format(data)
            return transformed_data
        raise HTTPException(status_code=404, detail="Item not found")
    raise HTTPException(status_code=400, detail="Missing instance name")
