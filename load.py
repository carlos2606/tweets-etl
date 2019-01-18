import boto3 as boto
import gzip
import json
import datetime
import os
from collections import deque


def is_multiple_of_seven(n):
    if n % 7 == 0:
        return True
    else:
        return False


def dates_less_than_ten_days(first_date, last_date):
    delta = last_date - first_date
    if delta < 10:
        return True
    else:
        return False


class Extract():
    def __init__(self):
        pass

    def process_files():


        with gzip.open("BFS_SWME_TweetsFromUser_614.txt.gz", 'r') as f:
            last_tweet = json.loads(f.readline())
            print(last_tweet["id"])
            if is_multiple_of_seven(last_tweet['user']['id']):
                return f
                print("Suspicious!!!")
            first_tweet = json.loads(deque(f, 1)[0])
            print(first_tweet["id"])

            first_date = datetime.datetime.strptime(
                first_tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
            print(first_date)

            last_date = datetime.datetime.strptime(
                last_tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
            print(last_date)
            if dates_less_than_ten_days(first_date, last_date):
                print("Suspicious!")
                return f
            return None

class Load():
    """The main purpose of this class is to process
       some Twitter data and load it into a
       DynamoDB table"""

    def __init__(self):
        self.access_key_id = os.getenv("MY_ACCESS_KEY_ID")
        self.secret_access_key = os.getenv("MY_SECRET_ACCESS_KEY")
        self.dynamodb = boto.resource(
            "dynamodb",
            region_name="us-east-2",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key)
        self.table_name = os.getenv("TABLE_NAME")
        self._buffer = []
        self._batch_size = 9

    def __enter__(self):
        # Make sure the table is cleared.
        try:
            table = self.dynamodb.Table(self.table_name)
            table.delete()
        except Exception as e:
            print(e)
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
                    },
                    {
                        'AttributeName': 'Tweet_JSON',
                        'AttributeType': 'S'
                    },

                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
            self.scaling_client = boto.client(
                'application-autoscaling',
                region_name="us-east-2",
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key
            )
            self.scaling_policy = self.scaling_client.put_scaling_policy(
                PolicyName='tweets-etl-cm',
                ServiceNamespace='dynamodb',
                ResourceId='table/etl_tweets_cm',
                ScalableDimension='dynamodb:table:WriteCapacityUnits',
                PolicyType='TargetTrackingScaling',
                TargetTrackingScalingPolicyConfiguration={
                    'TargetValue': 70.0,
                    'PredefinedMetricSpecification': {
                        'PredefinedMetricType': 'DynamoDBWriteCapacityUtilization',
                        'ResourceLabel': 'etl-tweets'
                    },
                    'ScaleOutCooldown': 70,
                    'ScaleInCooldown': 70,
                    'DisableScaleIn': False
                })
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
            self._buffer.clear()

    def close(self):
        self.flush()


def main():
    # with gzip.open("BFS_SWME_TweetsFromUser_614.txt.gz", 'r') as f:
    #     last_tweet = json.loads(f.readline())
    #     print(last_tweet["id"])
    #     if is_multiple_of_seven(last_tweet['user']['id']):
    #         print("Suspicious!!!")
    #     first_tweet = json.loads(deque(f, 1)[0])
    #     print(first_tweet["id"])

    #     first_date = datetime.datetime.strptime(
    #         first_tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    #     print(first_date)

    #     last_date = datetime.datetime.strptime(
    #         last_tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    #     print(last_date)
    #     if dates_less_than_ten_days(first_date, last_date):
    #         print("Suspicious!")
    load = Load()
    print(load.dynamodb)
    with Load() as load:
        print("hi")
        extract = Extract()
        files = extract.process_files()
        if files:
            for obj in files:
                load(obj)

if __name__ == '__main__':
    main()
