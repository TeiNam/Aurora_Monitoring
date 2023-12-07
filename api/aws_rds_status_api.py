import aioboto3
import asyncio
import logging
from fastapi import FastAPI, HTTPException
from config import AWS_ACCESS_KEY, AWS_SECRET_KEY
from modules.json_loader import load_json

app = FastAPI()


async def get_rds_instance_info(client, instance_name):
    try:
        response = await client.describe_db_instances(DBInstanceIdentifier=instance_name)
        return response['DBInstances'][0]
    except client.exceptions.DBInstanceNotFound:
        raise HTTPException(status_code=404, detail=f"Instance {instance_name} not found")


async def get_rds_cluster_info(client, cluster_identifier):
    try:
        response = await client.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
        return response['DBClusters'][0]
    except client.exceptions.DBClusterNotFoundFault:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_identifier} not found")


async def fetch_rds_instance_data(instance_name, region):
    async with aioboto3.Session().client(
            'rds',
            region_name=region,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
    ) as client:
        instance_data = await get_rds_instance_info(client, instance_name)

        cluster_identifier = instance_data.get('DBClusterIdentifier')
        cluster_data = {}
        is_cluster_writer = False
        if cluster_identifier:
            cluster_data = await get_rds_cluster_info(client, cluster_identifier)
            # DBClusterMembers에서 특정 DBInstanceIdentifier와 일치하는 멤버의 IsClusterWriter 속성 검사
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
            'Environment': environment_value,
            'DBClusterIdentifier': cluster_identifier,
            'DBInstanceIdentifier': instance_data.get('DBInstanceIdentifier'),
            'IsClusterWriter': is_cluster_writer,
            'EngineVersion': instance_data.get('EngineVersion'),
            'DBInstanceClass': instance_data.get('DBInstanceClass'),
            'vCPU': spec_data.get('vCPU'),
            'RAM': spec_data.get('RAM'),
            'AvailabilityZone': instance_data.get('AvailabilityZone'),
            'DBInstanceStatus': instance_data.get('DBInstanceStatus'),
            'DeletionProtection': cluster_data.get('DeletionProtection'),
            'ClusterCreateTime': cluster_data.get('ClusterCreateTime'),
            'InstanceCreateTime': instance_data.get('InstanceCreateTime'),
        }


async def fetch_rds_instance_data_safe(instance_name, region):
    try:
        return await fetch_rds_instance_data(instance_name, region)
    except Exception as e:
        logging.error(f"Error fetching data for instance {instance_name}: {e}")
        return {"instance_name": instance_name, "error": str(e)}


@app.get("/instance_status")
async def get_rds_instances():
    try:
        rds_instances_info = load_json('rds_instances.json')
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)

    tasks = []
    for instance_info in rds_instances_info:
        instance_name = instance_info.get('instance_name')
        region = instance_info.get('region')

        if not instance_name or not region:
            continue

        task = fetch_rds_instance_data_safe(instance_name, region)
        tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=False)
    return [result for result in results if result]