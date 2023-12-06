import asyncio
import aioboto3
from datetime import datetime, timedelta
from collections import OrderedDict
from modules.time_utils import get_kst_time
from modules.mongodb_connector import MongoDBConnector
from modules.json_loader import load_json
from config import AWS_ACCESS_KEY, AWS_SECRET_KEY, METRICS

rds_instances_data = load_json('rds_instances.json')


class MetricFetcher:
    def __init__(self, db):
        self.db = db

    async def fetch_and_store_metrics(self):
        while True:
            tasks = [self.fetch_and_store_metric(metric, instance_info['instance_name'], instance_info['region'])
                     for instance_info in rds_instances_data for metric in METRICS]
            await asyncio.gather(*tasks)
            await asyncio.sleep(300)  # 5분에 한번 메트릭 수집

    async def fetch_and_store_metric(self, metric_name, instance_name, region):
        try:
            async with aioboto3.Session(
                    aws_access_key_id=AWS_ACCESS_KEY,
                    aws_secret_access_key=AWS_SECRET_KEY
            ).client('cloudwatch', region_name=region) as cw_client:

                end_time = datetime.utcnow()
                start_time = end_time - timedelta(minutes=5)

                response = await cw_client.get_metric_statistics(
                    Namespace='AWS/RDS',
                    MetricName=metric_name,
                    Dimensions=[
                        {'Name': 'DBInstanceIdentifier', 'Value': instance_name}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=300,
                    Statistics=['Average']
                )

                collection = self.db[metric_name]

                indexes = [
                    ("region", f"{metric_name}_region_1_IDX", None),
                    ("instance_name", f"{metric_name}_instance_name_1_IDX", None),
                    ("timestamp", f"{metric_name}_TTL_90days_IDX", {"expireAfterSeconds": 7776000}),
                ]

                for field, index, options in indexes:
                    await self.ensure_index_exists(collection, field, index, options)

                if response['Datapoints']:
                    document = OrderedDict([
                        ("region", region),
                        ("instance_name", instance_name),
                        ("value", response['Datapoints'][0]['Average']),
                        ("timestamp", end_time)
                    ])
                    await collection.insert_one(document)
        except Exception as e:
            print(f"{get_kst_time()} - Error fetching/storing metric {metric_name} for instance {instance_name}: {e}")

    async def ensure_index_exists(self, collection, index_field, index_name, index_options=None):
        index_options = index_options or {}
        index_info = await collection.index_information()
        if index_name not in index_info:
            await collection.create_index([(index_field, 1)], name=index_name, **index_options)


async def main():
    try:
        mongo_client = MongoDBConnector.get_database()
        metric_fetcher = MetricFetcher(mongo_client)
        await metric_fetcher.fetch_and_store_metrics()
    except Exception as e:
        print(f"{get_kst_time()} - An error occurred: {e}")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"{get_kst_time()} - An error occurred: {e}")
