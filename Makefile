MAKEFLAGS+=--always-make

export PYTHONPATH = ${PWD}/its_live_monitoring/src:${PWD}/status-messages/src

ECR_REGISTRY ?= 986442313181.dkr.ecr.us-west-2.amazonaws.com
ECR_REPOSITORY ?= asfhyp3/its-live-monitoring
PLATFORM ?= linux/amd64

LANDSAT_TOPIC_ARN ?= arn:aws:sns:us-west-2:986442313181:its-live-notify-landsat-test
SENTINEL2_TOPIC_ARN ?= arn:aws:sns:eu-west-1:986442313181:its-live-notify-sentinel2-test
SENITNEL1_SQS_URL ?= https://sqs.us-west-2.amazonaws.com/986442313181/its-live-monitoring-test-Queue-1UIaYnVv4j5I


install:
	python -m pip install --upgrade pip && \
	python -m pip install -r requirements-all.txt

image:
	export SDIST_VERSION=$$(python -m setuptools_scm) && \
	docker buildx build --platform ${PLATFORM} --provenance=false -t ${ECR_REGISTRY}/${ECR_REPOSITORY}:$(subst +,_,${SDIST_VERSION}) .

test_files ?= 'tests/'
tests:
	export $$(xargs < tests/cfg.env); \
	pytest $(test_files)

landsat-integration:
	export AWS_PAGER='' && \
	$(foreach file, $(wildcard tests/integration/landsat*.json), aws sns publish --profile saml-pub --topic-arn ${LANDSAT_TOPIC_ARN} --message file://${file} --output json;)

sentinel2-integration:
	export AWS_PAGER='' && \
	$(foreach file, $(wildcard tests/integration/sentinel2*.json), aws sns publish --region eu-west-1 --profile saml-pub --topic-arn ${SENTINEL2_TOPIC_ARN} --message file://${file} --output json;)

sentinel1-integration:
	export AWS_PAGER='' && \
	$(foreach file, $(wildcard tests/integration/sentinel1*.json), aws sqs send-message --profile saml-pub --queue-url ${SENITNEL1_SQS_URL} --message-body file://${file} --output json;)

integration: landsat-integration sentinel2-integration sentinel1-integration

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

clean:
	git ls-files -o -- its_live_monitoring/src/ | xargs rm; \
	git ls-files -o -- status-messages/src/ | xargs rm; \
	git ls-files -o -- .pytest_cache | xargs rm; \
	find ./ -empty -type d -delete; \
	rm -f packaged.yml
