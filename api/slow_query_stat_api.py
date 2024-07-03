from fastapi import FastAPI
from modules.mongodb_connector import MongoDBConnector
from config import MONGODB_SLOWLOG_COLLECTION_NAME

app = FastAPI()


@app.get("/")
async def get_statistics():
    db = await MongoDBConnector.get_database()
    aggregation_pipeline = [
        {
            "$group": {
                "_id": {
                    "instance": "$instance",
                    "db": "$db",
                    "user": "$user"
                },
                "total_count": {"$sum": 1},
                "max_time": {"$max": "$time"},
                "total_time": {"$sum": "$time"}
            }
        },
        {
            "$group": {
                "_id": "$_id.instance",
                "dbs": {
                    "$push": {
                        "db": "$_id.db",
                        "user": "$_id.user",
                        "count": "$total_count",
                        "max_time": "$max_time",
                        "total_time": "$total_time"
                    }
                }
            }
        },
        {
            "$unwind": "$dbs"
        },
        {
            "$project": {
                "_id": 0,
                "instance": "$_id",
                "db": "$dbs.db",
                "user": "$dbs.user",
                "count": "$dbs.count",
                "max_time": "$dbs.max_time",
                "total_time": "$dbs.total_time",
                "avg_time": {
                    "$round": [
                        {
                            "$cond": { "if": { "$ne": ["$dbs.count", 0] }, "then": { "$divide": ["$dbs.total_time", "$dbs.count"] }, "else": 0 }
                        },
                        3
                    ]
                }
            }
        }
    ]
    cursor = db[MONGODB_SLOWLOG_COLLECTION_NAME].aggregate(aggregation_pipeline)
    result = await cursor.to_list(length=None)
    return result
