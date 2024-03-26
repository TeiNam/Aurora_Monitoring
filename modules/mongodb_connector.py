from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGODB_URI, MONGODB_DB_NAME


class MongoDBConnector:
    client = None
    db = None

    @classmethod
    async def initialize(cls):
        if cls.client is None:
            try:
                cls.client = AsyncIOMotorClient(MONGODB_URI)
                cls.db = cls.client[MONGODB_DB_NAME]
            except Exception as e:
                print(f"MongoDB 연결에 실패했습니다: {e}")
                cls.client = None  # 연결 실패 시 클라이언트를 None으로 설정

    @classmethod
    async def get_database(cls):
        if cls.client is None:
            cls.client = AsyncIOMotorClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        try:
            # 데이터베이스 연결 테스트를 위해 서버 상태를 체크합니다.
            await cls.client.admin.command('ping')
        except Exception as e:
            print(f"MongoDB 연결 실패: {e}, 연결을 재시도합니다.")
            cls.client = AsyncIOMotorClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        cls.db = cls.client[MONGODB_DB_NAME]
        return cls.db

    @classmethod
    async def reconnect(cls):
        cls.client = None
        await cls.initialize()
