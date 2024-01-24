from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGODB_URI, MONGODB_DB_NAME


class MongoDBConnector:
    client = None
    _db = None

    @classmethod
    async def initialize(cls):
        if cls.client is None:
            cls.client = AsyncIOMotorClient(MONGODB_URI)
            cls._db = cls.client[MONGODB_DB_NAME]

    @classmethod
    async def get_database(cls):
        await cls.initialize()
        return cls._db

    @classmethod
    async def get_client(cls):
        await cls.initialize()
        return cls.client
