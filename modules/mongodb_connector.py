import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()


class MongoDBConnector:
    client = None
    db = None

    @classmethod
    def initialize(cls):
        if not cls.client:
            MONGODB_URI = os.getenv("MONGODB_URI")
            MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME")
            cls.client = AsyncIOMotorClient(MONGODB_URI)
            cls.db = cls.client[MONGODB_DB_NAME]

    @classmethod
    def get_database(cls):
        if not cls.client:
            cls.initialize()
        return cls.db


MongoDBConnector.initialize()
