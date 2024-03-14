from fastapi import FastAPI, APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from modules.mongodb_connector import MongoDBConnector
from bson import ObjectId

COLLECTION_NAME = "memo"
router = APIRouter()


class Memo(BaseModel):
    id: Optional[str] = None
    content: str
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()


class MemoInDB(Memo):
    _id: ObjectId


class PaginatedResponse(BaseModel):
    data: List[Memo]
    total: int
    page: int
    page_size: int
    total_pages: int


def memo_entity(memo_db) -> Memo:
    return Memo(**memo_db, id=str(memo_db["_id"]))


@router.post("/", response_model=Memo)
async def create_memo(memo: Memo):
    db = await MongoDBConnector.get_database()
    memo_db = memo.dict(exclude_unset=True)
    result = await db[COLLECTION_NAME].insert_one(memo_db)
    return memo_entity({**memo_db, "_id": result.inserted_id})


@router.get("/", response_model=PaginatedResponse)
async def read_memos(page: int = 1, page_size: int = 5):
    db = await MongoDBConnector.get_database()
    total = await db[COLLECTION_NAME].count_documents({})
    skip = (page - 1) * page_size
    memos = await db[COLLECTION_NAME].find().sort("_id", -1).skip(skip).limit(page_size).to_list(None)
    return PaginatedResponse(
        data=[memo_entity(memo) for memo in memos],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size
    )


@router.get("/{memo_id}", response_model=Memo)
async def read_memo(memo_id: str):
    db = await MongoDBConnector.get_database()
    memo = await db[COLLECTION_NAME].find_one({"_id": ObjectId(memo_id)})
    if memo is None:
        raise HTTPException(status_code=404, detail="Memo not found")
    return memo_entity(memo)


@router.delete("/{memo_id}", response_model=Memo)
async def delete_memo(memo_id: str):
    db = await MongoDBConnector.get_database()
    result = await db[COLLECTION_NAME].find_one_and_delete({"_id": ObjectId(memo_id)})
    if result is None:
        raise HTTPException(status_code=404, detail="Memo not found")
    return memo_entity(result)

app = FastAPI()
app.include_router(router)
