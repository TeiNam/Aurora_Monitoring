from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
import logging
from modules.mongodb_connector import MongoDBConnector
from modules.time_utils import convert_utc_to_kst, get_kst_time
from config import MONGODB_SLOWLOG_COLLECTION_NAME

app = FastAPI()

logger = logging.getLogger(__name__)


class SlowQueryItem(BaseModel):
    instance: str
    pid: int
    user: str
    host: str
    db: str
    time: int
    sql_text: str
    start: datetime
    end: datetime


@app.get("/", tags=["Slow Queries"])
async def get_slow_queries(days: int = Query(1, ge=1, le=30, description="Number of days to look back")):
    try:
        db = await MongoDBConnector.get_database()
        collection = db[MONGODB_SLOWLOG_COLLECTION_NAME]

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)

        query = {"start": {"$gte": start_date, "$lte": end_date}}
        sort = [("start", -1)]

        items = []
        async for item in collection.find(query).sort(sort):
            item['_id'] = str(item['_id'])
            item['start'] = convert_utc_to_kst(item['start'])
            item['end'] = convert_utc_to_kst(item['end']) if 'end' in item else None
            items.append(SlowQueryItem(**item))

        logger.info(f"Retrieved {len(items)} slow query items for the last {days} days")

        return {
            "status": "success",
            "data": [item.dict() for item in items]
        }

    except Exception as e:
        logger.error(f"Error retrieving slow query items: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")