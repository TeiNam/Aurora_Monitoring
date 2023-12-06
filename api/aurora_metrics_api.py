from fastapi import FastAPI, HTTPException, Query, Depends
from datetime import datetime, timedelta
from modules.mongodb_connector import MongoDBConnector
from modules.json_loader import load_json

app = FastAPI()

METRICS = [
    "AuroraBinlogReplicaLag",
    "ConnectionAttempts",
    "CPUUtilization",
    "DatabaseConnections",
    "Deadlocks",
    "DeleteThroughput",
    "DiskQueueDepth",
    "EngineUptime",
    "FreeableMemory",
    "InsertThroughput",
    "NetworkReceiveThroughput",
    "NetworkTransmitThroughput",
    "ReadIOPS",
    "ReadLatency",
    "ReadThroughput",
    "SelectThroughput",
    "SwapUsage",
    "UpdateThroughput",
    "WriteIOPS",
    "WriteLatency",
    "WriteThroughput",
    "Queries",
]

try:
    rds_instances_data = load_json(filename="rds_instances.json")
except HTTPException as e:
    print(e.detail)
    rds_instances_data = []

all_instance_names = [instance['instance_name'] for instance in rds_instances_data]


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

    return metrics
