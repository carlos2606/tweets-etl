# tweets-etl

ETL process for loading tweets from suspicious users into DynamoDB.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development purposes.

### Prerequisites

Download the data,
```
https://s3.amazonaws.com/astscrmltestdata/RealUsers.tar.gz
```
Decompress it. You will find a single directory which contains 25,000 files, each of the files represents a Twitter user and all its tweets. Each of these files is a single text file that is compressed. Each line in the decompressed files represents a tweet, and is a valid JSON. The contents of the JSON are best described in the Twitter API’s website for the Tweet and User objects.
```
https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object.html
https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object.html
```

Create a "Data" directory in the root of this repository and move the 25,000 .gz files there. The code will decompress them for you, no worries.

### Installing

Intall Docker Engine (https://www.docker.com/products/docker-engine)

Run the following commands from the Docker console

This command will build the Docker image.

```
docker build -t tweets-etl .
```

Run the container from the image created

```
docker run tweets-etl
```

## Built With

* [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) - The AWS SDK for Python


## Authors

* **Carlos Montenegro** - *Initial work* - [carlos2606](https://github.com/carlos2606)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.
