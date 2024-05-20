import aioboto3
import asyncio
import logging
from modules.mongodb_connector import MongoDBConnector
from modules.load_instance import load_instances_from_mongodb
from config import MONGODB_AURORA_INFO_COLLECTION_NAME

logging.basicConfig(level=logging.ERROR)


async def fetch_instance_specs_from_mongodb(instance_class):
    db = await MongoDBConnector.get_database()
    collection = db['rds_specs']

    pipeline = [
        {'$match': {'instance_class': instance_class}},
        {'$project': {'_id': 0, 'vCPU': '$spec.vCPU', 'RAM': '$spec.RAM'}}
    ]

    cursor = collection.aggregate(pipeline)
    spec_data = await cursor.to_list(length=1)

    if not spec_data:
        print(f"No specs found for {instance_class}")
        return None

    return spec_data[0]


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


async def fetch_rds_instance_data(client, db, instance_name, region):
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

    if not cluster_identifier == "Non-Cluster":
        return None

    instance_class = str(instance_data.get('DBInstanceClass'))
    spec_data = await fetch_instance_specs_from_mongodb(instance_class)

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


async def fetch_and_save_rds_instance_data(instance_info):
    region = instance_info['region']
    instance_name = instance_info['instance_name']

    async with aioboto3.Session().client('rds', region_name=region) as client:
        db = await MongoDBConnector.get_database()
        collection = db[MONGODB_AURORA_INFO_COLLECTION_NAME]
        instance_data = await fetch_rds_instance_data(client, collection, instance_name, region)
        if instance_data:
            await collection.update_one(
                {"DBInstanceIdentifier": instance_name},
                {
                    "$set": instance_data,
                    "$currentDate": {"last_updated_at": True}
                },
                upsert=True
            )
        else:
            print(f"Instance {instance_name} is not the Aurora cluster.")


async def get_aurora_info():
    instances_info = await load_instances_from_mongodb()
    tasks = [fetch_and_save_rds_instance_data(instance) for instance in instances_info]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_none = all(result is None for result in results)
    if all_none:
        print("There are no instances using Aurora clusters.")
    else:
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Task resulted in error: {result}")


if __name__ == '__main__':
    asyncio.run(get_aurora_info(), debug=True)
