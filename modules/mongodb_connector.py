from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGODB_URI, MONGODB_DB_NAME

class MongoDBConnector:
    _client = None
    _db = None

    @classmethod
    async def initialize(cls):
        if cls._client is None:
            try:
                cls._client = AsyncIOMotorClient(MONGODB_URI)
                cls._db = cls._client[MONGODB_DB_NAME]
            except Exception as e:
                print(f"MongoDB 연결에 실패했습니다: {e}")
                cls._client = None  # 연결 실패 시 클라이언트를 None으로 설정

    @classmethod
    async def get_database(cls):
        await cls.initialize()
        if cls._client is None:
            raise Exception("MongoDB에 연결할 수 없습니다.")
        return cls._db

    @classmethod
    async def reconnect(cls):
        cls._client = None
        await cls.initialize()
