FROM python:3.6

ENV MY_ACCESS_KEY_ID AKIAJIXR6JKRG66GTZRQ
ENV MY_SECRET_ACCESS_KEY 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
ENV TABLE_NAME etl_tweets_cm

COPY . /app

RUN pip install boto3 awscli

WORKDIR /app

CMD [ "python", "load.py" ]
