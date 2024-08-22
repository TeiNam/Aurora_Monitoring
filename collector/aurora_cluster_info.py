import asyncio
import boto3
from botocore.exceptions import ClientError
import logging
from modules.mongodb_connector import MongoDBConnector
from pymongo import UpdateOne
from datetime import datetime
from config import (
    MONGODB_RDS_INSTANCE_LIST_COLLATION_NAME,
    MONGODB_AURORA_INFO_COLLECTION_NAME
)

logger = logging.getLogger(__name__)

class AuroraInfoCollector:
    def __init__(self):
        self.ec2_client = boto3.client('ec2')
        self.mongodb_connector = MongoDBConnector()
        self.rds_instance_collection = MONGODB_RDS_INSTANCE_LIST_COLLATION_NAME
        self.aurora_info_collection = MONGODB_AURORA_INFO_COLLECTION_NAME

    async def get_all_regions(self):
        try:
            response = self.ec2_client.describe_regions()
            return [region['RegionName'] for region in response['Regions']]
        except ClientError as e:
            logger.error(f"리전 정보 가져오기 실패: {e}")
            return []

    async def get_aurora_clusters(self, region):
        rds_client = boto3.client('rds', region_name=region)
        try:
            response = rds_client.describe_db_clusters()
            return [(cluster, region) for cluster in response['DBClusters']]
        except ClientError as e:
            logger.error(f"{region} 리전의 Aurora 클러스터 정보 가져오기 실패: {e}")
            return []

    async def get_all_aurora_clusters(self):
        regions = await self.get_all_regions()
        tasks = [self.get_aurora_clusters(region) for region in regions]
        results = await asyncio.gather(*tasks)
        return [item for sublist in results for item in sublist]

    async def get_cluster_info(self, cluster, region):
        return {
            'Region': region,
            'DBClusterIdentifier': cluster['DBClusterIdentifier'],
            'Engine': cluster['Engine'],
            'EngineVersion': cluster['EngineVersion'],
            'MultiAZ': cluster['MultiAZ'],
            'MasterUsername': cluster['MasterUsername'],
            'Status': cluster['Status'],
            'ClusterCreateTime': cluster['ClusterCreateTime'].isoformat(),
        }

    async def get_aurora_info(self):
        db = await self.mongodb_connector.get_database()
        instance_collection = db[self.rds_instance_collection]
        aurora_info_collection = db[self.aurora_info_collection]

        instance_list = await instance_collection.find({}).to_list(length=None)
        all_clusters = await self.get_all_aurora_clusters()

        update_operations = []
        for instance in instance_list:
            cluster_name = instance['cluster_name']
            matching_cluster = next((c for c, r in all_clusters if c['DBClusterIdentifier'] == cluster_name), None)
            matching_region = next((r for c, r in all_clusters if c['DBClusterIdentifier'] == cluster_name), None)

            if matching_cluster and matching_region:
                cluster_info = await self.get_cluster_info(matching_cluster, matching_region)
                members = matching_cluster['DBClusterMembers']

                for member in members:
                    update_operations.append(
                        UpdateOne(
                            {
                                "DBClusterIdentifier": cluster_name,
                                "DBInstanceIdentifier": member['DBInstanceIdentifier']
                            },
                            {"$set": {
                                **cluster_info,
                                "DBInstanceIdentifier": member['DBInstanceIdentifier'],
                                "IsClusterWriter": member['IsClusterWriter'],
                                "last_updated": datetime.utcnow()
                            }},
                            upsert=True
                        )
                    )

                logger.info(f"Prepared update for cluster: {cluster_name} in region: {matching_region} with {len(members)} members")
            else:
                logger.warning(f"No matching Aurora cluster found for: {cluster_name}")

        if update_operations:
            result = await aurora_info_collection.bulk_write(update_operations)
            logger.info(f"Bulk update completed. Modified {result.modified_count} documents.")

async def run_aurora_info_collector():
    collector = AuroraInfoCollector()
    try:
        await collector.get_aurora_info()
        logger.info("Aurora info collection completed successfully.")
    except Exception as e:
        logger.error(f"Error in Aurora info collection: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    asyncio.run(run_aurora_info_collector())