FROM python:3.7

MAINTAINER support@cognite.com

WORKDIR /usr/src/app
COPY Pipfile Pipfile.lock ./

RUN pip install pipenv

RUN pipenv install --system --deploy

COPY csv-extractor/ ./

ENTRYPOINT ["python", "main.py"]
CMD []
