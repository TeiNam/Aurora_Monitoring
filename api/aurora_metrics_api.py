from fastapi import FastAPI, HTTPException, Query, Depends
from datetime import datetime, timedelta
from config import METRICS
from modules.mongodb_connector import MongoDBConnector
from modules.json_loader import load_json
from pytz import timezone

app = FastAPI()

try:
    rds_instances_data = load_json(filename="rds_instances.json")
except HTTPException as e:
    print(e.detail)
    rds_instances_data = []

all_instance_names = [instance['instance_name'] for instance in rds_instances_data]


def convert_utc_to_kst(utc_dt):
    utc_zone = timezone('UTC')
    kst_zone = timezone('Asia/Seoul')
    utc_dt = utc_zone.localize(utc_dt)
    kst_dt = utc_dt.astimezone(kst_zone)
    return kst_dt


def get_metric_collection(metric_name: str):
    db = MongoDBConnector.get_database()
    return db[metric_name]


@app.get("/metrics/")
async def read_metrics(
        instance_name: str = Query(None),
        metric_name: str = Query(None, description="Metric to be queried"),
        collection=Depends(get_metric_collection)
):
    if not metric_name:
        raise HTTPException(status_code=400, detail="Metric name is required")

    if metric_name not in METRICS:
        raise HTTPException(status_code=400, detail=f"Invalid metric name: {metric_name}")

    instance_name_list = instance_name.split(',') if instance_name else all_instance_names

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=7)

    query = {
        "timestamp": {"$gte": start_time, "$lte": end_time},
        "instance_name": {"$in": instance_name_list}
    }

    cursor = collection.find(query, {"_id": 0})
    metrics = await cursor.to_list(None)

    # Convert timestamp from UTC to KST
    for metric in metrics:
        if 'timestamp' in metric:
            metric['timestamp'] = convert_utc_to_kst(metric['timestamp'])

    return metrics
