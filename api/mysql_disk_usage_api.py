from modules.mongodb_connector import MongoDBConnector
from fastapi import FastAPI, HTTPException, Query
from typing import List
from datetime import timedelta, datetime
from config import MONGODB_DISK_USAGE_COLLECTION_NAME

app = FastAPI()

kst_delta = timedelta(hours=9)

async def get_all_metrics_status(instance_name: str, metric_names: List[str] = None):
    db = await MongoDBConnector.get_database()
    collection = db[MONGODB_DISK_USAGE_COLLECTION_NAME]
    query = {'instance_name': instance_name}
    projection = {'_id': 0, 'timestamp': 1, 'metrics': 1}
    cursor = collection.find(query, projection).sort('timestamp', -1)
    documents = await cursor.to_list(length=None)
    if documents:
        return documents
    return None

def transform_data_to_table_format(data: List[dict], metric_names: List[str] = None):
    transformed_data = []
    for entry in data:
        timestamp = entry.get("timestamp")
        if timestamp:
            timestamp = timestamp + kst_delta  # Convert to KST
            timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')  # Format the timestamp
        if "metrics" in entry:
            for metric in entry["metrics"]:
                if metric_names and metric["name"] not in metric_names:
                    continue
                row = {
                    "timestamp": timestamp,
                    "name": metric["name"],
                    "value": metric.get("value", 0),
                    "avgForHours": metric.get("avg_for_hours", 0),
                    "avgForSeconds": metric.get("avg_for_seconds", 0)
                }
                transformed_data.append(row)
    return transformed_data

@app.get("/")
async def read_status(
    instance_name: str = Query(None, description="The name of the instance to retrieve"),
    metric_name: List[str] = Query(None, description="List of metric names to retrieve", alias="metric")
):
    if instance_name:
        data = await get_all_metrics_status(instance_name, metric_name)
        if data:
            transformed_data = transform_data_to_table_format(data, metric_name)
            return transformed_data
        raise HTTPException(status_code=404, detail="Item not found")
    raise HTTPException(status_code=400, detail="Missing instance name")