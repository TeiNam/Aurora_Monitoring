from fastapi import FastAPI
from modules.mongodb_connector import MongoDBConnector
from config import MONGODB_AURORA_INFO_COLLECTION_NAME

app = FastAPI()


@app.get("/aurora_cluster/")
async def get_all_aurora_cluster():
    db = await MongoDBConnector.get_database()
    collection = db[MONGODB_AURORA_INFO_COLLECTION_NAME]

    cursor = collection.find({})
    rds_instances_data = []
    async for document in cursor:
        ordered_document = {
            "region": document.get("region"),
            "DBClusterIdentifier": document.get("DBClusterIdentifier"),
            "DBInstanceIdentifier": document.get("DBInstanceIdentifier"),
            "IsClusterWriter": document.get("IsClusterWriter"),
            "EngineVersion": document.get("EngineVersion"),
            "DBInstanceClass": document.get("DBInstanceClass"),
            "vCPU": document.get("vCPU"),
            "RAM": document.get("RAM"),
            "DBInstanceStatus": document.get("DBInstanceStatus"),
            "AvailabilityZone": document.get("AvailabilityZone"),
            "MultiAZ": document.get("MultiAZ"),
            "DeletionProtection": document.get("DeletionProtection"),
            "Environment": document.get("Environment"),
            "ClusterCreateTime": document.get("ClusterCreateTime"),
            "InstanceCreateTime": document.get("InstanceCreateTime"),
            "created_at": document.get("created_at"),
            "last_updated_at": document.get("last_updated_at")
        }
        rds_instances_data.append(ordered_document)

    return rds_instances_data
