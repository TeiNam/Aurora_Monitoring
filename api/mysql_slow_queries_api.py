import os
from fastapi import FastAPI, Query
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from modules.mongodb_connector import MongoDBConnector
from config import MONGODB_SLOWLOG_COLLECTION_NAME

app = FastAPI()

kst_delta = timedelta(hours=9)


class Item(BaseModel):
    instance: str
    pid: int
    user: str
    host: str
    db: str
    time: int
    sql_text: str
    start: datetime = Field(alias="start")
    end: datetime = Field(alias="end")


@app.get("/items/")
async def get_items(days: int = Query(None, alias="days")):
    db = await MongoDBConnector.get_database()
    collection = db[MONGODB_SLOWLOG_COLLECTION_NAME]
    items = []
    query = {}
    sort = [("_id", -1)]

    if days is not None:
        target_date = datetime.utcnow() - timedelta(days=days)
        query = {"start": {"$gte": target_date}}

    async for item in collection.find(query).sort(sort):
        item['_id'] = str(item['_id'])
        if 'start' in item:
            item['start'] = item['start'] + kst_delta
        if 'end' in item:
            item['end'] = item['end'] + kst_delta
        items.append(Item(**item))

    return items
