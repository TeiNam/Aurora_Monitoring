from fastapi import FastAPI
from modules.mongodb_connector import MongoDBConnector

app = FastAPI()


@app.get("/statistics")
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
                "total_time": "$dbs.total_time"
            }
        }
    ]
    cursor = db['Aurora4MySlowQuery'].aggregate(aggregation_pipeline)
    result = await cursor.to_list(length=None)
    return result
