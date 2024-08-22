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
RDS_SPECS_COLLECTION_NAME = os.getenv("RDS_SPECS_COLLECTION_NAME")

# MySQL에서 고려하는 슬로우 쿼리의 최소 실행 시간 (단위: 초)
EXEC_TIME = int(os.getenv("SLOW_QUERY_EXEC_TIME", "2"))

# API 관련 설정
API_MAPPING = {
    "/api/v1/instance_setup": "api.instance_setup_api",
    "/api/v1/aurora_cluster": "api.aurora_cluster_status_api",
    "/api/v1/mysql_status": "api.mysql_com_status_api",
    "/api/v1/mysql_slow_query": "api.mysql_slow_queries_api",
    "/api/v1/mysql_explain": "api.mysql_slow_query_explain_api",
    "/api/v1/memo": "api.memo_api",
    "/api/v1/query_statistics": "api.slow_query_stat_api",
    "/api/v1/disk_usage": "api.mysql_disk_usage_api",
}

ALLOWED_ORIGINS = [
    "http://localhost:8000"
]

# MySQL 설정
MYSQL_METRICS = [
    'Binlog_cache_use',
    'Binlog_cache_disk_use',
    'Created_tmp_tables',
    'Created_tmp_files',
    'Created_tmp_disk_tables'
]

DESIRED_COMMANDS = [
    'Com_select', 'Com_delete', 'Com_delete_multi',
    'Com_insert', 'Com_insert_select', 'Com_replace',
    'Com_replace_select', 'Com_update', 'Com_update_multi',
    'Com_flush', 'Com_kill', 'Com_purge', 'Com_admin_commands',
    'Com_commit', 'Com_begin', 'Com_rollback'
]

# 연결 풀 설정
POOL_SIZE = int(os.getenv("MYSQL_POOL_SIZE", "5"))

# 앱 설정
STATIC_FILES_DIR = "static"
TEMPLATES_DIR = "templates"
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 8000))

# 재시도 설정
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))

# 로깅 설정
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

IGNORE_LOGGERS = ['asyncmy', 'aiomysql']  # 무시할 로거 이름 리스트
IGNORE_MESSAGES = ["'INFORMATION_SCHEMA.PROCESSLIST' is deprecated"]  # 무시할 메시지 리스트