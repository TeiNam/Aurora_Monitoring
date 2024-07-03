import aioboto3
import asyncio
import logging
from typing import Dict, Any, List, Optional
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

    async def initialize(self):
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
            return response['DBInstances'][0]
        except client.exceptions.DBInstanceNotFoundFault:
            logger.error(f"Instance {instance_name} not found")
            return None

    async def get_rds_cluster_info(self, client: Any, cluster_identifier: str) -> Optional[Dict[str, Any]]:
        try:
            response = await client.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
            return response['DBClusters'][0]
        except client.exceptions.DBClusterNotFoundFault:
            logger.error(f"Cluster {cluster_identifier} not found")
            return None

    async def fetch_rds_instance_data(self, client: Any, instance_name: str, region: str) -> Optional[Dict[str, Any]]:
        instance_data = await self.get_rds_instance_info(client, instance_name)
        if not instance_data:
            return None

        cluster_identifier = instance_data.get('DBClusterIdentifier')
        cluster_data = {}
        is_cluster_writer = False
        environment_value = None

        if cluster_identifier:
            cluster_data = await self.get_rds_cluster_info(client, cluster_identifier)
            if not cluster_data:
                return None

            for member in cluster_data.get('DBClusterMembers', []):
                if member.get('DBInstanceIdentifier') == instance_name:
                    is_cluster_writer = member.get('IsClusterWriter', False)
                    break

            for tag in cluster_data.get('TagList', []):
                if tag['Key'] == 'ENVIRONMENT':
                    environment_value = tag['Value']
                    break

        if cluster_identifier == "Non-Cluster":
            return None

        instance_class = str(instance_data.get('DBInstanceClass'))
        spec_data = await self.fetch_instance_specs_from_mongodb(instance_class)

        return {
            'region': region,
            'DBClusterIdentifier': cluster_identifier,
            'DBInstanceIdentifier': instance_data.get('DBInstanceIdentifier'),
            'MultiAZ': cluster_data.get('MultiAZ', False),
            'IsClusterWriter': is_cluster_writer,
            'EngineVersion': instance_data.get('EngineVersion'),
            'DBInstanceClass': instance_data.get('DBInstanceClass'),
            'vCPU': spec_data.get('vCPU') if spec_data else None,
            'RAM': spec_data.get('RAM') if spec_data else None,
            'AvailabilityZone': instance_data.get('AvailabilityZone'),
            'DBInstanceStatus': instance_data.get('DBInstanceStatus'),
            'DeletionProtection': cluster_data.get('DeletionProtection', False),
            'ClusterCreateTime': cluster_data.get('ClusterCreateTime'),
            'InstanceCreateTime': instance_data.get('InstanceCreateTime'),
            'Environment': environment_value,
        }

    async def fetch_and_save_rds_instance_data(self, instance_info: Dict[str, Any]):
        region = instance_info['region']
        instance_name = instance_info['instance_name']

        async with aioboto3.Session().client('rds', region_name=region) as client:
            collection = self.db[MONGODB_AURORA_INFO_COLLECTION_NAME]
            instance_data = await self.fetch_rds_instance_data(client, instance_name, region)
            if instance_data:
                await collection.update_one(
                    {"DBInstanceIdentifier": instance_name},
                    {
                        "$set": instance_data,
                        "$currentDate": {"last_updated_at": True}
                    },
                    upsert=True
                )
                logger.info(f"Updated info for instance {instance_name}")
            else:
                logger.info(f"Instance {instance_name} is not part of an Aurora cluster.")

    async def get_aurora_info(self):
        await self.initialize()
        instances_info = await load_instances_from_mongodb()
        tasks = [self.fetch_and_save_rds_instance_data(instance) for instance in instances_info]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        aurora_instances = [r for r in results if r is not None]
        if not aurora_instances:
            logger.info("There are no instances using Aurora clusters.")
        else:
            logger.info(f"Processed {len(aurora_instances)} Aurora instances.")

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task resulted in error: {result}")


async def run_aurora_info_collection():
    collector = AuroraInfoCollector()
    await collector.get_aurora_info()


if __name__ == '__main__':
    asyncio.run(run_aurora_info_collection())