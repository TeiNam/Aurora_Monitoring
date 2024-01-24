import re
import sqlparse
import json
import pymysql.cursors
from fastapi import FastAPI, HTTPException, Query, Response, Depends
from datetime import datetime, timezone

from modules.mongodb_connector import MongoDBConnector
from modules.json_loader import load_json
from modules.crypto_utils import decrypt_password
from config import MONGODB_SLOWLOG_COLLECTION_NAME, MONGODB_PLAN_COLLECTION_NAME

app = FastAPI()

rds_instances = load_json("rds_instances.json")


async def get_collection():
    db = await MongoDBConnector.get_database()
    return db[MONGODB_SLOWLOG_COLLECTION_NAME]


async def get_plan_collection():
    db = await MongoDBConnector.get_database()
    return db[MONGODB_PLAN_COLLECTION_NAME]


class SQLQueryExecutor:
    @staticmethod
    def remove_sql_comments(sql_text):
        return re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL)

    @staticmethod
    def validate_sql_query(sql_text):
        query_without_comments = SQLQueryExecutor.remove_sql_comments(sql_text).strip()

        if not query_without_comments.lower().startswith("select"):
            raise ValueError("SELECT 쿼리만 가능합니다.")

        if "into" in query_without_comments.lower().split("from")[0]:
            raise ValueError("SELECT ... INTO ... FROM 형태의 프로시저 쿼리는 실행할 수 없습니다.")

        return query_without_comments

    @staticmethod
    def execute(rds_info, sql_text, db_name):
        decrypted_password = decrypt_password(rds_info["password"])

        try:
            validated_sql = SQLQueryExecutor.validate_sql_query(sql_text)

            connection = pymysql.connect(
                host=rds_info["host"],
                port=rds_info["port"],
                user=rds_info["user"],
                password=decrypted_password,
                database=db_name,
                cursorclass=pymysql.cursors.DictCursor
            )
            with connection.cursor() as cursor:
                explain_query = f"EXPLAIN FORMAT=JSON {validated_sql}"
                cursor.execute(explain_query)
                execution_plan = cursor.fetchall()
            connection.close()
            return execution_plan
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"SQL 실행 중 에러 발생: {str(e)}")


class MarkdownGenerator:
    @staticmethod
    def generate(document):
        formatted_sql = sqlparse.format(document['sql_text'], reindent=True, keyword_case='upper')
        formatted_explain = json.dumps(document['explain_result'], indent=4)
        markdown_content = (
            f"### 인스턴스: {document['instance']}\n\n"
            f"- 데이터베이스: {document['db']}\n"
            f"- PID: {document['pid']}\n"
            f"- 사용자: {document.get('user', 'N/A')}\n"
            f"- 실행시간: {document['time']}\n\n"
            f"- SQL TEXT:\n```sql\n{formatted_sql}\n```\n\n"
            f"- Explain:\n```json\n{formatted_explain}\n```\n\n"
        )
        return markdown_content


class RDSInstanceManager:
    @staticmethod
    def get_info(instance_name):
        return next((item for item in rds_instances if item["instance_name"] == instance_name), None)


@app.post("/explain")
async def execute_sql(pid: int = Query(..., description="The PID to lookup")):
    collection = await get_collection()
    plan_collection = await get_plan_collection()

    if not pid:
        raise HTTPException(status_code=422, detail="PID is required")
    document = await collection.find_one({"pid": pid})
    if document is None:
        raise HTTPException(status_code=404, detail="해당 PID의 문서를 찾을 수 없습니다.")

    rds_info = RDSInstanceManager.get_info(document["instance"])
    if not rds_info:
        raise HTTPException(status_code=400, detail="instance_name에 해당하는 RDS 인스턴스 정보를 찾을 수 없습니다.")

    document_db = document["db"]
    execution_plan_raw = SQLQueryExecutor.execute(rds_info, document["sql_text"], document_db)
    execution_plan = json.loads(execution_plan_raw[0]['EXPLAIN'])

    query_plan_document = {
        "pid": pid,
        "instance": document["instance"],
        "db": document["db"],
        "user": document["user"],
        "time": document["time"],
        "sql_text": SQLQueryExecutor.remove_sql_comments(document["sql_text"]),
        "explain_result": execution_plan,
        "created_at": datetime.now(timezone.utc)
    }
    await plan_collection.update_one({"pid": pid}, {"$set": query_plan_document}, upsert=True)

    return {"message": "SQL 쿼리에 대한 EXPLAIN이 실행 되었으며, 실행 계획이 저장 되었습니다."}


@app.get("/download", response_class=Response)
async def download_markdown(pid: int = Query(...)):
    plan_collection = await get_plan_collection()

    cursor = plan_collection.find({"pid": pid})
    markdown_content = ""
    async for document in cursor:
        markdown_content += MarkdownGenerator.generate(document)

    if not markdown_content:
        raise HTTPException(status_code=404, detail="No records found for the given PID")

    filename = f"slowlog_pid_{pid}.md"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    return Response(content=markdown_content, media_type="text/markdown", headers=headers)
