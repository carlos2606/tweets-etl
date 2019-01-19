import boto3 as boto

import gzip
import json
import datetime
import os
from time import time
from logging import basicConfig, getLogger, DEBUG


basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=DEBUG)
log = getLogger(__name__)
log.debug('Starting %s', __name__)


class Extract():
    def scan_path(self):
        """
            Scans the directory and returns a list file names to be processed
        """
        path = "Data/"
        walk = os.walk(path, followlinks=False)
        for root, dirs, files in walk:
            for name in files:
                yield os.path.join(root, name)


class Transform():
    def __init__(self):
        self.suspicious_users = []
        self.legit_users = []

    def process_file(self, file, load):
        """
            Opens a compressed file and if user is suspicious tries to load
            all its tweets into dynamodb

            :param file: file name
            :type file: string
            :param load: instance of class Load()
            :type load: Load() object
        """
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
        """
             Method to determine wether an specific user is suspicious of being a bot

            :param file_content: All the tweets from a user
            :type file_content: list
            :returns: True or False
            :rtype: boolean
        """
        # Read most recent tweet to check if user id is multiple of 7
        last_tweet = json.loads(file_content[0])
        if self.is_multiple_of_seven(last_tweet['user']['id']):
            return True
        # Read oldest tweet
        first_tweet = json.loads(file_content[-1])
        # Compare dates
        first_date = datetime.datetime.strptime(
            first_tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

        last_date = datetime.datetime.strptime(
            last_tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
        # Determine wether difference between dates is less than 10 days
        if self.less_than_ten_days_between(first_date, last_date):
            return True
        return False

    def is_multiple_of_seven(self, number):
        """
            Method to determine if a number is multiple of 7

            :param number: Number
            :type number: int
            :returns: True or False
            :rtype: boolean
        """
        if number % 7 == 0:
            return True
        else:
            return False

    def less_than_ten_days_between(self, first_date, last_date):
        """
            Method to determine if the difference between two dates is
            less or equal to 10 days.

            :param first_date: Creation date of the oldest tweet
            :type first_date: datetime.datetime
            :param last_date: Creation date of the most recent tweet
            :type last_date_date: datetime.datetime
            :returns: True or False
            :rtype: boolean
        """
        delta = last_date - first_date
        if delta.days < 10:
            return True
        else:
            return False

    def build_objects_to_store(self, tweets, user_id):
        """
            Creates the object that will be stored in dynamodb

            :param tweets: The list of tweets for this user
            :type tweets: list
            :param user_id: User unique identifier
            :type user_id: string
            :returns: Yields the object created
            :rtype: dict
        """
        for tweet in tweets:
            tweet_dict = json.loads(tweet)
            obj = {
                'PutRequest': {
                    'Item': {
                        'Tweet_ID': tweet_dict["id"],
                        'User_ID': user_id,
                        'Tweet_JSON': tweet
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
        self.dynamodb_resource = boto.resource(
            "dynamodb",
            region_name="us-east-2",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key)
        self.dynamodb_client = boto.client(
            "dynamodb",
            region_name="us-east-2",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key)
        self.table_name = os.getenv("TABLE_NAME")
        self._buffer = []
        self._batch_size = 25  # Max batch size that dynamo can handle

        self._counter = Counter('Records count', 3)
        self.counter = 0

    def __enter__(self):
        """
            When the context manager is created, this method tries first to delete
            the table and then create a new one
        """
        try:
            # Make sure the table is cleared.
            self.dynamodb_client.delete_table(TableName=self.table_name)
            # Wait until the operation above is done
            waiter = self.dynamodb_client.get_waiter('table_not_exists')
            waiter.wait(TableName=self.table_name)
        except Exception as e:
            print(e)
        try:
            self.table = self.dynamodb_resource.create_table(
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
                    'WriteCapacityUnits': 100 #  10 was not enough, increased to speed up the process
                }
            )
            # Wait until the operation above is done
            waiter = self.dynamodb_client.get_waiter('table_exists')
            waiter.wait(TableName=self.table_name)
            # Commenting out the autoscaling policy since user does not have permissions to do so
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
        """
            Executed every time the load object is called.
            Appends objects to the buffer and when it reaches the batch size,
            flushes, which means clearing the buffer and storing those values
            into the table.
        """
        self._buffer.append(obj)
        self._counter.count()
        if len(self._buffer) >= self._batch_size:
            self.flush()

    def flush(self):
        """
            Stores all the values from the buffer when it reached the batch
            size. And finally clears the buffer
        """
        if self._buffer:
            try:
                self.dynamodb_resource.batch_write_item(
                    RequestItems={
                        self.table_name: self._buffer
                    },
                    ReturnConsumedCapacity='INDEXES',
                    ReturnItemCollectionMetrics='SIZE'
                )
            except Exception as e:
                print(e)
            # with self.table.batch_writer() as batch:
            #     for tweet in self._buffer:
            #         batch.put_item(Item=tweet['PutRequest']['Item'])
            self.counter += 1
            self._buffer.clear()

    def close(self):
        """
            Executed when closing the context manager.
            Flushes and stores the rest of objects that remained
            in the buffer.
        """
        self.flush()
        log.debug('Finished inserting %d records', self._counter.value)


class Counter:
    def __init__(self, message='Count', wait=1):
        self.message, self.wait, self.value, self.next = message, wait, 0, 0

    def count(self):
        self.value += 1
        if self.next < time():
            log.debug('%s: %d', self.message, self.value)
            self.next = time() + self.wait


def main():
    # Instantiate Load class
    with Load() as load:
        extract = Extract()
        transform = Transform()
        for file in extract.scan_path():
            # Start the transformation of each file from the generator function scan_path()
            transform.process_file(file, load)


if __name__ == '__main__':
    main()
