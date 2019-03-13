FROM python:3.7-slim

MAINTAINER support@cognite.com

WORKDIR /usr/src/app
COPY Pipfile Pipfile.lock ./

RUN pip install pipenv

RUN pipenv install --system --deploy

COPY datapoints-csv-extractor.py ./

CMD ["python", "datapoints-csv-extractor.py"]
