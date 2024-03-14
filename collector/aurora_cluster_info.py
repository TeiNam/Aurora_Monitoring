import aioboto3
import asyncio
import logging
from datetime import datetime
from modules.mongodb_connector import MongoDBConnector
from config import AWS_ACCESS_KEY, AWS_SECRET_KEY, MONGODB_AURORA_INFO_COLLECTION_NAME
from modules.json_loader import load_json

logging.basicConfig(level=logging.ERROR)


async def get_rds_instance_info(client, instance_name):
    try:
        response = await client.describe_db_instances(DBInstanceIdentifier=instance_name)
        return response['DBInstances'][0]
    except client.exceptions.DBInstanceNotFoundFault:
        logging.error(f"Instance {instance_name} not found")
        return None


async def get_rds_cluster_info(client, cluster_identifier):
    try:
        response = await client.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
        return response['DBClusters'][0]
    except client.exceptions.DBClusterNotFoundFault:
        logging.error(f"Cluster {cluster_identifier} not found")
        return None


async def fetch_rds_instance_data(client, collection, instance_name, region):
    instance_data = await get_rds_instance_info(client, instance_name)
    if not instance_data:
        return

    cluster_identifier = instance_data.get('DBClusterIdentifier')
    cluster_data = {}
    is_cluster_writer = False
    environment_value = None

    if cluster_identifier:
        cluster_data = await get_rds_cluster_info(client, cluster_identifier)
        if not cluster_data:
            return

        for member in cluster_data.get('DBClusterMembers', []):
            if member.get('DBInstanceIdentifier') == instance_name:
                is_cluster_writer = member.get('IsClusterWriter', False)
                break

        for tag in cluster_data.get('TagList', []):
            if tag['Key'] == 'ENVIRONMENT':
                environment_value = tag['Value']
                break

    rds_specs = load_json("rds_specs.json")
    instance_class = instance_data.get('DBInstanceClass')
    spec_data = rds_specs.get(instance_class, {})

    return {
        'region': region,
        'DBClusterIdentifier': cluster_identifier,
        'DBInstanceIdentifier': instance_data.get('DBInstanceIdentifier'),
        'MultiAZ': cluster_data.get('MultiAZ', False),
        'IsClusterWriter': is_cluster_writer,
        'EngineVersion': instance_data.get('EngineVersion'),
        'DBInstanceClass': instance_data.get('DBInstanceClass'),
        'vCPU': spec_data.get('vCPU'),
        'RAM': spec_data.get('RAM'),
        'AvailabilityZone': instance_data.get('AvailabilityZone'),
        'DBInstanceStatus': instance_data.get('DBInstanceStatus'),
        'DeletionProtection': cluster_data.get('DeletionProtection', False),
        'ClusterCreateTime': cluster_data.get('ClusterCreateTime', None),
        'InstanceCreateTime': instance_data.get('InstanceCreateTime', None),
        'Environment': environment_value,
    }


async def fetch_and_save_rds_instance_data(client, collection, instance_name, region):
    instance_data = await fetch_rds_instance_data(client, collection, instance_name, region)
    if instance_data:
        cluster_identifier = instance_data.get('DBClusterIdentifier', instance_name)
        current_time = datetime.utcnow()

        await collection.update_one(
            {"DBClusterIdentifier": cluster_identifier},
            {
                "$set": instance_data,
                "$setOnInsert": {"created_at": current_time},
                "$currentDate": {"last_updated_at": True}
            },
            upsert=True
        )


async def get_aurora_info(region_name):
    try:
        rds_instances_info = load_json('rds_instances.json')
    except FileNotFoundError as e:
        logging.error("File not found: rds_instances.json")
        return

    db = await MongoDBConnector.get_database()
    collection = db[MONGODB_AURORA_INFO_COLLECTION_NAME]

    async with aioboto3.Session().client(
        'rds',
        region_name=region_name,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    ) as client:
        tasks = [fetch_and_save_rds_instance_data(client, collection, info['instance_name'], info['region'])
                 for info in rds_instances_info]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Task resulted in error: {result}")


if __name__ == '__main__':
    region = 'ap-northeast-2'
    asyncio.run(get_aurora_info(region), debug=True)
