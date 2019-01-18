import boto3 as boto

import gzip
import json
import datetime
# import multiprocessing
import os
import sys
from time import time
from logging import basicConfig, getLogger, DEBUG


basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=DEBUG)
log = getLogger(__name__)
log.debug('Starting %s', __name__)


def is_multiple_of_seven(n):
    if n % 7 == 0:
        return True
    else:
        return False


def less_than_ten_days_between(first_date, last_date):
    delta = last_date - first_date
    if delta.days < 10:
        return True
    else:
        return False


class Extract():
    def __init__(self):
        pass

    def scan_path(self):
        path = "Data/"
        walk = os.walk(path, followlinks=False)
        for root, dirs, files in walk:
            for name in files:
                yield os.path.join(root, name)


class Transform():
    """docstring for Transform"""

    def __init__(self):
        self.suspicious_users = []
        self.legit_users = []

    def process_file(self, file, load):
        with gzip.open(file, mode='rt', encoding="ISO-8859-1") as f:
            # add to susp users list for plotting later
            file_content = f.readlines()
            last_tweet = json.loads(file_content[0])
            user = {
                "user_id": last_tweet["user"]["id"],
                "tweets": file_content
            }
            if self.is_suspicious(file_content):
                self.suspicious_users.append(user)
                # Build objects that will be inserted into dynamodb
                objs = self.build_objects_to_store(
                    file_content,
                    user["user_id"]
                )
                print(file)
                for obj in objs:
                    load(obj)
            else:
                self.legit_users.append(user)

    def is_suspicious(self, file_content):
        # Read most recent tweet to check if user id is multiple of 7
        last_tweet = json.loads(file_content[0])
        if is_multiple_of_seven(last_tweet['user']['id']):
            return True
        # Read oldest tweet
        first_tweet = json.loads(file_content[-1])
        # Compare dates
        first_date = datetime.datetime.strptime(
            first_tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

        last_date = datetime.datetime.strptime(
            last_tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

        if less_than_ten_days_between(first_date, last_date):
            return True
        return False

    def build_objects_to_store(self, tweets, user_id):
        for tweet in tweets:
            tweet_dict = json.loads(tweet)
            obj = {
                'PutRequest': {
                    'Item': {
                        'Tweet_ID': {'N': str(tweet_dict["id"])},
                        'User_ID': {'N': str(user_id)}
                    }
                }
            }
            yield obj


class Load():
    """The main purpose of this class is to process
       some Twitter data and load it into a
       DynamoDB table"""

    def __init__(self):
        self.access_key_id = os.getenv("MY_ACCESS_KEY_ID")
        self.secret_access_key = os.getenv("MY_SECRET_ACCESS_KEY")
        self.dynamodb = boto.client(
            "dynamodb",
            region_name="us-east-2",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key)
        self.table_name = os.getenv("TABLE_NAME")
        self._buffer = []
        self._batch_size = 24

        self._counter = Counter('Document count', 3)
        self.counter = 0

    def __enter__(self):
        try:
            # Make sure the table is cleared.
            self.dynamodb.delete_table(TableName=self.table_name)
            waiter = self.dynamodb.get_waiter('table_not_exists')
            waiter.wait(TableName=self.table_name)
        except Exception as e:
            print(e)
        try:
            self.table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'Tweet_ID',
                        'KeyType': 'HASH'  # Primary Key
                    },
                    {
                        'AttributeName': 'User_ID',
                        'KeyType': 'RANGE'  # Sort Key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'Tweet_ID',
                        'AttributeType': 'N'
                    },
                    {
                        'AttributeName': 'User_ID',
                        'AttributeType': 'N'
                    }

                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 100
                }
            )
            waiter = self.dynamodb.get_waiter('table_exists')
            waiter.wait(TableName=self.table_name)
            # self.scaling_client = boto.client(
            #     'application-autoscaling',
            #     region_name="us-east-2",
            #     aws_access_key_id=self.access_key_id,
            #     aws_secret_access_key=self.secret_access_key
            # )
            # self.scaling_policy = self.scaling_client.put_scaling_policy(
            #     PolicyName='tweets-etl-cm',
            #     ServiceNamespace='dynamodb',
            #     ResourceId='table/etl_tweets_cm',
            #     ScalableDimension='dynamodb:table:WriteCapacityUnits',
            #     PolicyType='TargetTrackingScaling',
            #     TargetTrackingScalingPolicyConfiguration={
            #         'TargetValue': 70.0,
            #         'PredefinedMetricSpecification': {
            #             'PredefinedMetricType': 'DynamoDBWriteCapacityUtilization',
            #             'ResourceLabel': 'etl-tweets'
            #         },
            #         'ScaleOutCooldown': 70,
            #         'ScaleInCooldown': 70,
            #         'DisableScaleIn': False
            #     })
        except Exception as e:
            print(e)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            self.close()

    def __call__(self, obj):
        self._buffer.append(obj)
        self._counter.count()
        if len(self._buffer) >= self._batch_size:
            self.flush()

    def flush(self):
        if self._buffer:
            # TODO insert batch
            try:
                self.dynamodb.batch_write_item(
                    RequestItems={
                        self.table_name: self._buffer
                    },
                    ReturnConsumedCapacity='INDEXES',
                    ReturnItemCollectionMetrics='SIZE'
                )
            except Exception as e:
                print(e)
            # for tweet in self._buffer:
            #     self.dynamodb.put_item(
            #         TableName=self.table_name,
            #         Item=tweet['PutRequest']['Item']
            #     )
            self.counter += 1
            print(self.counter)
            print("WRITE BATCH")
            self._buffer.clear()

    def close(self):
        self.flush()
        log.debug('Finished inserting %d batches', self._counter.value)


class Counter:
    def __init__(self, message='Count', wait=1):
        self.message, self.wait, self.value, self.next = message, wait, 0, 0

    def count(self):
        self.value += 1
        if self.next < time():
            log.debug('%s: %d', self.message, self.value)
            self.next = time() + self.wait


def main():
    # with multiprocessing.Pool(8) as pool:  # pool of 4 processes
    #     extract = Extract()
    #     transform = Transform()

    #     pool.map(
    #         transform.process_file,
    #         [file for file in extract.scan_path()]
        # )
    with Load() as load:
        extract = Extract()
        transform = Transform()

        for file in extract.scan_path():
            transform.process_file(file, load)


if __name__ == '__main__':
    main()
