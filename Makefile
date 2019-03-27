IMAGE=eu.gcr.io/cognite-registry/datapoints-csv-extractor:latest

.PHONY: build run push all clean tests


all: clean build

build:
	docker build -t ${IMAGE} .

run:
	docker run -it --rm ${IMAGE}

push:
	docker push ${IMAGE}

tests:
	pipenv run pytest --cov=csv-extractor

dependencies:
	pipenv install --dev

clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -f `find . -type f -name '@*' `
	rm -f `find . -type f -name '#*#' `
	rm -f `find . -type f -name '*.orig' `
	rm -f `find . -type f -name '*.rej' `
	rm -f .coverage
	rm -rf coverage
	rm -rf build
	rm -rf htmlcov
	rm -rf dist
