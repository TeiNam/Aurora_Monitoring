import aioboto3
import asyncio
import logging
from typing import Dict, Any, Optional, List
from botocore.exceptions import BotoCoreError, ClientError
from modules.mongodb_connector import MongoDBConnector
from modules.load_instance import load_instances_from_mongodb
from config import (
    MONGODB_AURORA_INFO_COLLECTION_NAME,
    RDS_SPECS_COLLECTION_NAME,
    LOG_LEVEL,
    LOG_FORMAT
)

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class AuroraInfoCollector:
    def __init__(self):
        self.db = None
        self.session = aioboto3.Session()

    async def initialize(self):
        await MongoDBConnector.initialize()
        self.db = await MongoDBConnector.get_database()

    async def fetch_instance_specs_from_mongodb(self, instance_class: str) -> Optional[Dict[str, Any]]:
        collection = self.db[RDS_SPECS_COLLECTION_NAME]
        pipeline = [
            {'$match': {'instance_class': instance_class}},
            {'$project': {'_id': 0, 'vCPU': '$spec.vCPU', 'RAM': '$spec.RAM'}}
        ]
        cursor = collection.aggregate(pipeline)
        spec_data = await cursor.to_list(length=1)
        if not spec_data:
            logger.warning(f"No specs found for {instance_class}")
            return None
        return spec_data[0]

    async def get_rds_instance_info(self, client: Any, instance_name: str) -> Optional[Dict[str, Any]]:
        try:
            response = await client.describe_db_instances(DBInstanceIdentifier=instance_name)
            instances = response.get('DBInstances', [])
            if instances:
                logger.info(f"Found instance: {instance_name}")
                return instances[0]
            logger.warning(f"No instances found with identifier: {instance_name}")
            return None
        except ClientError as e:
            logger.error(f"Error fetching instance {instance_name}: {e}")
            return None

    async def get_rds_cluster_info(self, client: Any, cluster_identifier: str) -> Optional[Dict[str, Any]]:
        try:
            response = await client.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
            clusters = response.get('DBClusters', [])
            if clusters:
                return clusters[0]
            logger.warning(f"No clusters found with identifier: {cluster_identifier}")
            return None
        except ClientError as e:
            logger.error(f"Error fetching cluster {cluster_identifier}: {e}")
            return None

    async def fetch_rds_instance_data(self, client: Any, instance_name: str, region: str) -> Optional[Dict[str, Any]]:
        try:
            instance_data = await self.get_rds_instance_info(client, instance_name)
            if not instance_data:
                return None

            cluster_identifier = instance_data.get('DBClusterIdentifier')
            if not cluster_identifier:
                logger.info(f"Instance {instance_name} is not part of an Aurora cluster.")
                return None

            cluster_data = await self.get_rds_cluster_info(client, cluster_identifier)
            if not cluster_data:
                return None

            is_cluster_writer = any(
                member.get('DBInstanceIdentifier') == instance_name and member.get('IsClusterWriter', False)
                for member in cluster_data.get('DBClusterMembers', [])
            )

            environment_value = next(
                (tag['Value'] for tag in cluster_data.get('TagList', []) if tag['Key'] == 'ENVIRONMENT'),
                None
            )

            instance_class = instance_data.get('DBInstanceClass')
            spec_data = await self.fetch_instance_specs_from_mongodb(instance_class)

            return {
                'region': region,
                'DBClusterIdentifier': cluster_identifier,
                'DBInstanceIdentifier': instance_data.get('DBInstanceIdentifier'),
                'MultiAZ': cluster_data.get('MultiAZ', False),
                'IsClusterWriter': is_cluster_writer,
                'EngineVersion': instance_data.get('EngineVersion'),
                'DBInstanceClass': instance_class,
                'vCPU': spec_data.get('vCPU') if spec_data else None,
                'RAM': spec_data.get('RAM') if spec_data else None,
                'AvailabilityZone': instance_data.get('AvailabilityZone'),
                'DBInstanceStatus': instance_data.get('DBInstanceStatus'),
                'DeletionProtection': cluster_data.get('DeletionProtection', False),
                'ClusterCreateTime': cluster_data.get('ClusterCreateTime'),
                'InstanceCreateTime': instance_data.get('InstanceCreateTime'),
                'Environment': environment_value,
            }
        except Exception as e:
            logger.error(f"Error processing instance {instance_name}: {e}")
            return None

    async def fetch_and_save_rds_instance_data(self, instance_info: Dict[str, Any]):
        region = instance_info.get('region')
        instance_name = instance_info.get('instance_name')

        if not region:
            logger.error(f"No region specified for instance {instance_name}")
            return

        async with self.session.client('rds', region_name=region) as client:
            instance_data = await self.fetch_rds_instance_data(client, instance_name, region)
            if instance_data:
                collection = self.db[MONGODB_AURORA_INFO_COLLECTION_NAME]
                await collection.update_one(
                    {"DBInstanceIdentifier": instance_name},
                    {"$set": instance_data, "$currentDate": {"last_updated_at": True}},
                    upsert=True
                )
                logger.info(f"Updated info for instance {instance_name} in region {region}")
            else:
                logger.info(f"Instance {instance_name} in region {region} is not part of an Aurora cluster or data fetch failed.")

    async def get_aurora_info(self):
        await self.initialize()
        instances_info = await load_instances_from_mongodb()

        tasks = [self.fetch_and_save_rds_instance_data(instance) for instance in instances_info]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        aurora_instances = [r for r in results if r is not None]
        logger.info(f"Processed {len(aurora_instances)} Aurora instances.")

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task resulted in error: {result}")

async def run_aurora_info_collection():
    collector = AuroraInfoCollector()
    await collector.get_aurora_info()

if __name__ == '__main__':
    asyncio.run(run_aurora_info_collection())