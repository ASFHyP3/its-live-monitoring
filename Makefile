MAKEFLAGS+=--always-make

export PYTHONPATH = ${PWD}/its_live_monitoring/src:${PWD}/status-messages/src

ECR_REGISTRY ?= 986442313181.dkr.ecr.us-west-2.amazonaws.com
ECR_REPOSITORY ?= asfhyp3/its-live-monitoring
PLATFORM ?= linux/amd64
BUILDX_OUTPUT_TYPE ?= docker

ifndef IMAGE_TAG
SDIST_VERSION != python -m setuptools_scm
IMAGE_TAG = $(subst +,_,$(SDIST_VERSION))
endif

LANDSAT_TOPIC_ARN ?= arn:aws:sns:us-west-2:986442313181:its-live-notify-landsat-test
SENTINEL1_TOPIC_ARN ?= arn:aws:sns:us-west-2:986442313181:its-live-notify-sentinel1-test
SENTINEL2_TOPIC_ARN ?= arn:aws:sns:eu-west-1:986442313181:its-live-notify-sentinel2-test

install:
	python -m pip install --upgrade pip && \
	python -m pip install -r requirements-all.txt

image:
	docker buildx build --platform ${PLATFORM} --provenance=false --output=type=${BUILDX_OUTPUT_TYPE} -t ${ECR_REGISTRY}/${ECR_REPOSITORY}:${IMAGE_TAG} .

test_files ?= 'tests/'
tests:
	export $$(xargs < tests/cfg.env); \
	pytest $(test_files)

landsat-integration:
	export AWS_PAGER='' && \
	$(foreach file, $(wildcard tests/integration/landsat*.json), aws sns publish --profile saml-pub --topic-arn ${LANDSAT_TOPIC_ARN} --message file://${file} --output json;)

sentinel1-integration:
	export AWS_PAGER='' && \
	$(foreach file, $(wildcard tests/integration/sentinel1*.json), aws sns publish --profile saml-pub --topic-arn ${SENTINEL1_TOPIC_ARN} --message file://${file} --output json;)

sentinel2-integration:
	export AWS_PAGER='' && \
	$(foreach file, $(wildcard tests/integration/sentinel2*.json), aws sns publish --region eu-west-1 --profile saml-pub --topic-arn ${SENTINEL2_TOPIC_ARN} --message file://${file} --output json;)

integration: landsat-integration sentinel1-integration sentinel2-integration

static: mypy ruff cfn-lint

mypy_excludes ?= '_test'
mypy:
	mypy --exclude $(mypy_excludes) .

ruff-check:
	ruff check

ruff-format:
	ruff format

ruff: ruff-check ruff-format

cfn-lint:
	cfn-lint --template `find . -name cloudformation.yml` --info --ignore-checks W3002
