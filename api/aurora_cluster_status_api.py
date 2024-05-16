import logging
from fastapi import FastAPI
from modules.mongodb_connector import MongoDBConnector
from config import MONGODB_AURORA_INFO_COLLECTION_NAME

app = FastAPI()
logging.basicConfig(level=logging.INFO)


@app.get("/aurora_cluster/")
async def get_all_aurora_cluster():
    logging.info("Connecting to MongoDB...")
    db = await MongoDBConnector.get_database()
    collection = db[MONGODB_AURORA_INFO_COLLECTION_NAME]

    logging.info("Counting documents in the collection...")
    document_count = await collection.count_documents({})
    logging.info(f"Document count: {document_count}")

    if document_count == 0:
        logging.warning("No documents found in the collection.")
        return {"message": "No documents found in the collection."}

    logging.info("Fetching documents from the collection...")
    cursor = collection.find({})
    rds_instances_data = []
    async for document in cursor:
        logging.info(f"Document found: {document}")
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

    if not rds_instances_data:
        logging.warning("No data fetched from the collection.")
        return {"message": "No data fetched from the collection."}

    logging.info("Returning fetched data...")
    return rds_instances_data
