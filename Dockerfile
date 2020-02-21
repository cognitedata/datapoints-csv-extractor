FROM python:3.7

MAINTAINER patrick.mishima@cognite.com

WORKDIR /usr/src/app
COPY Pipfile Pipfile.lock ./

RUN pip install pipenv

RUN pipenv install --system --deploy

COPY csv-extractor/ ./

ENTRYPOINT ["python", "main.py"]

CMD []
