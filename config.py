import os
from dotenv import load_dotenv

load_dotenv()

# MongoDB 관련 설정
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME")
MONGODB_SLOWLOG_COLLECTION_NAME = os.getenv("MONGODB_SLOWLOG_COLLECTION_NAME")
MONGODB_DIGEST_COLLECTION_NAME = os.getenv("MONGODB_DIGEST_COLLECTION_NAME")
MONGODB_STATUS_COLLECTION_NAME = os.getenv("MONGODB_STATUS_COLLECTION_NAME")
MONGODB_PLAN_COLLECTION_NAME = os.getenv("MONGODB_PLAN_COLLECTION_NAME")
MONGODB_HISTORY_COLLECTION_NAME = os.getenv("MONGODB_HISTORY_COLLECTION_NAME")
MONGODB_AURORA_INFO_COLLECTION_NAME = os.getenv("MONGODB_AURORA_INFO_COLLECTION_NAME")
MONGODB_RDS_INSTANCE_LIST_COLLATION_NAME = os.getenv("MONGODB_RDS_INSTANCE_LIST_COLLATION_NAME")
MONGODB_DISK_USAGE_COLLECTION_NAME = os.getenv("MONGODB_DISK_USAGE_COLLECTION_NAME")

# MySQL에서 고려하는 슬로우 쿼리의 최소 실행 시간 (단위: 초)
EXEC_TIME = 2

# API 관련 설정
API_MAPPING = {
    "/api/instance_setup": "api.instance_setup_api",
    "/api/rds": "api.aurora_cluster_status_api",
    "/api/mysql_status": "api.mysql_com_status_api",
    "/api/mysql_slow_query": "api.mysql_slow_queries_api",
    "/api/mysql_explain": "api.mysql_slow_query_explain_api",
    "/api/memo": "api.memo_api",
    "/api/slow_query": "api.slow_query_stat_api",
    "/api/mysql_io": "api.mysql_disk_usage_api",
}

ALLOWED_ORIGINS = [
    "http://localhost:8000",
    "https://mgmt.py.devops.torder.tech",  # 프로덕션 도메인
]

# 앱 설정
STATIC_FILES_DIR = "static"
TEMPLATES_DIR = "templates"
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 8000))