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
                        "time": "$total_time"
                    }
                },
                "total_count_per_instance": {"$sum": "$total_count"}
            }
        },
        {
            "$project": {
                "instance": "$_id",
                "total_count_per_instance": 1,
                "dbs": 1
            }
        }
    ]
    cursor = db['Aurora4MySlowQuery'].aggregate(aggregation_pipeline)
    result = await cursor.to_list(length=None)
    return result
