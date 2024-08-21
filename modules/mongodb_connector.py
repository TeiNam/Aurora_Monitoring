from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGODB_URI, MONGODB_DB_NAME
import logging

class MongoDBConnector:
    client = None
    db = None

    @classmethod
    async def initialize(cls):
        if cls.client is None:
            try:
                cls.client = AsyncIOMotorClient(
                    MONGODB_URI,
                    tls=True,
                    tlsAllowInvalidCertificates=True,
                    tlsAllowInvalidHostnames=True,
                    directConnection=False
                )
                cls.db = cls.client[MONGODB_DB_NAME]
                logging.info("MongoDB에 성공적으로 연결되었습니다.")
            except Exception as e:
                logging.error(f"MongoDB 연결에 실패했습니다: {e}")
                cls.client = None

    @classmethod
    async def get_database(cls):
        if cls.client is None:
            await cls._try_connect()
        try:
            await cls.client.admin.command('ping')
        except Exception as e:
            logging.warning(f"MongoDB 연결 실패: {e}, 연결을 재시도합니다.")
            await cls._try_connect()
        cls.db = cls.client[MONGODB_DB_NAME]
        return cls.db

    @classmethod
    async def _try_connect(cls):
        try:
            cls.client = AsyncIOMotorClient(
                MONGODB_URI,
                tls=True,
                tlsAllowInvalidCertificates=True,
                tlsAllowInvalidHostnames=True,
                directConnection=False,
                serverSelectionTimeoutMS=5000
            )
        except Exception as e:
            cls.client = None
            logging.error(f"MongoDB 재연결 시도 실패: {e}")

    @classmethod
    async def reconnect(cls):
        cls.client = None
        await cls.initialize()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')