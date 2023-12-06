from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Union, Optional
import json
from dotenv import load_dotenv
from modules.crypto_utils import encrypt_password

load_dotenv()

app = FastAPI()

FILE_PATH = "rds_instances.json"


async def read_existing_data(file_path: str) -> List[Dict[str, Union[str, str]]]:
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


async def write_data_to_file(file_path: str, data: List[Dict[str, Union[str, str]]]):
    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"Error writing to file: {e}")
        raise


class RDSInstance(BaseModel):
    cluster_name: Optional[str] = "Non-Cluster"
    instance_name: str
    host: str
    port: Optional[int] = 3306
    region: Optional[str] = "ap-northeast-2"
    user: str
    password: str
    db: Optional[str] = "information_schema"


@app.post("/add_instance/", status_code=201)
async def add_instance(rds_instance: RDSInstance, action: str = Query(None, alias='action')):
    encrypted_password_base64 = encrypt_password(rds_instance.password)

    existing_data = await read_existing_data(FILE_PATH)

    instance_data = {
        "cluster_name": rds_instance.cluster_name,
        "instance_name": rds_instance.instance_name,
        "host": rds_instance.host,
        "port": rds_instance.port,
        "region": rds_instance.region,
        "user": rds_instance.user,
        "password": encrypted_password_base64,
        "db": rds_instance.db
    }

    instance_exists = any(
        entry for entry in existing_data
        if entry["instance_name"] == rds_instance.instance_name or entry["host"] == rds_instance.host
    )

    if instance_exists:
        if action == "update":
            for entry in existing_data:
                if entry["instance_name"] == rds_instance.instance_name or entry["host"] == rds_instance.host:
                    entry.update(instance_data)
        elif action == "cancel":
            return {"message": "Instance addition cancelled"}
    else:
        existing_data.append(instance_data)

    await write_data_to_file(FILE_PATH, existing_data)

    return {"message": "Operation completed successfully"}


@app.get("/list_instances/")
async def list_instances():
    data = await read_existing_data(FILE_PATH)
    for entry in data:
        if "password" in entry:
            del entry["password"]
    return {"instances": data}


@app.delete("/delete_instance/")
async def delete_instance(instance_name: str):
    data = await read_existing_data(FILE_PATH)

    updated_data = [entry for entry in data if entry["instance_name"] != instance_name]

    if len(updated_data) == len(data):
        raise HTTPException(status_code=404, detail="Instance not found")

    await write_data_to_file(FILE_PATH, updated_data)

    return {"message": "Instance deleted successfully"}