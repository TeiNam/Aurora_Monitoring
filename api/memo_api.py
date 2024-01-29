from fastapi import FastAPI, APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from modules.mongodb_connector import MongoDBConnector
from bson import ObjectId

# 컬렉션 이름 선언
COLLECTION_NAME = "memo"

# APIRouter 인스턴스 생성
router = APIRouter()


class Memo(BaseModel):
    id: Optional[str] = None
    content: str
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()


class MemoInDB(Memo):
    _id: ObjectId


def memo_entity(memo_db) -> Memo:
    return Memo(**memo_db, id=str(memo_db["_id"]))


@router.post("/", response_model=Memo)
async def create_memo(memo: Memo):
    db = await MongoDBConnector.get_database()
    memo_db = memo.dict(exclude_unset=True)
    result = await db[COLLECTION_NAME].insert_one(memo_db)
    return memo_entity({**memo_db, "_id": result.inserted_id})


@router.get("/", response_model=List[Memo])
async def read_memos(page: int = 1):
    db = await MongoDBConnector.get_database()
    skip = (page - 1) * 5
    memos = await db[COLLECTION_NAME].find().sort("_id", -1).skip(skip).limit(5).to_list(5)
    return [memo_entity(memo) for memo in memos]


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

# FastAPI 애플리케이션 인스턴스 생성 및 라우터 마운트
app = FastAPI()
app.include_router(router)
