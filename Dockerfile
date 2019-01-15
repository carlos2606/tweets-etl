FROM python:3.6

ADD load.py /

RUN pip install boto3

CMD [ "python", "load.py" ]
