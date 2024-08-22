import logging
from fastapi import FastAPI
from modules.mongodb_connector import MongoDBConnector
from config import MONGODB_AURORA_INFO_COLLECTION_NAME

app = FastAPI()
logging.basicConfig(level=logging.INFO)

@app.get("/")
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
            "Region": document.get("Region"),
            "DBClusterIdentifier": document.get("DBClusterIdentifier"),
            "DBInstanceIdentifier": document.get("DBInstanceIdentifier"),
            "IsClusterWriter": document.get("IsClusterWriter"),
            "Engine": document.get("Engine"),
            "EngineVersion": document.get("EngineVersion"),
            "MultiAZ": document.get("MultiAZ"),
            "MasterUsername": document.get("MasterUsername"),
            "Status": document.get("Status"),
            "ClusterCreateTime": document.get("ClusterCreateTime"),
            "last_updated": document.get("last_updated")
        }
        rds_instances_data.append(ordered_document)

    if not rds_instances_data:
        logging.warning("No data fetched from the collection.")
        return {"message": "No data fetched from the collection."}

    logging.info("Returning fetched data...")
    return rds_instances_data