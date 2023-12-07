from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGODB_URI, MONGODB_DB_NAME


class MongoDBConnector:
    client = None
    db = None

    @classmethod
    def initialize(cls):
        if not cls.client:
            cls.client = AsyncIOMotorClient(MONGODB_URI)
            cls.db = cls.client[MONGODB_DB_NAME]

    @classmethod
    def get_database(cls):
        if not cls.client:
            cls.initialize()
        return cls.db


MongoDBConnector.initialize()
