from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional
from modules.crypto_utils import encrypt_password
from modules.mongodb_connector import MongoDBConnector
from config import MONGODB_RDS_INSTANCE_LIST_COLLATION_NAME

app = FastAPI()


class RDSInstance(BaseModel):
    environment: str = Field(default="DEV")
    db_type: str = Field(default="MySQL")
    cluster_name: Optional[str] = Field(default=None)
    instance_name: str
    host: str
    port: Optional[int] = Field(default=3306)
    region: str = Field(default="ap-northeast-2")
    user: str
    password: str
    db: Optional[str] = Field(default="information_schema")


@app.get("/list_instances/")
async def list_instances():
    collection = MongoDBConnector.db[MONGODB_RDS_INSTANCE_LIST_COLLATION_NAME]
    instances = await collection.find({}, {'_id': 0, 'password': 0}).to_list(length=None)
    return {"instances": instances}


@app.post("/add_instance/", status_code=201)
async def add_instance(rds_instance: RDSInstance):
    encrypted_password_base64 = encrypt_password(rds_instance.password)
    collection = MongoDBConnector.db[MONGODB_RDS_INSTANCE_LIST_COLLATION_NAME]

    instance_data = {
        "environment": rds_instance.environment,
        "db_type": rds_instance.db_type,
        "cluster_name": rds_instance.cluster_name,
        "instance_name": rds_instance.instance_name,
        "host": rds_instance.host,
        "port": rds_instance.port or 3306,
        "region": rds_instance.region or "ap-northeast-2",
        "user": rds_instance.user,
        "password": encrypted_password_base64,
        "db": rds_instance.db
    }

    result = await collection.update_one(
        {"instance_name": rds_instance.instance_name},
        {"$set": instance_data},
        upsert=True
    )

    if result.matched_count:
        return {"message": "Instance updated successfully"}
    else:
        return {"message": "New instance inserted successfully"}


@app.delete("/delete_instance/")
async def delete_instance(instance_name: str):
    collection = MongoDBConnector.db[MONGODB_RDS_INSTANCE_LIST_COLLATION_NAME]
    result = await collection.delete_one({"instance_name": instance_name})

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Instance not found")

    return {"message": "Instance deleted successfully"}
