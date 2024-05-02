export PYTHONPATH = ${PWD}/its_live_monitoring/src
LANDSAT_TOPIC_ARN ?= arn:aws:sns:us-west-2:986442313181:its-live-notify-landsat-test
SENTINEL2_TOPIC_ARN ?= arn:aws:sns:us-west-2:986442313181:its-live-notify-sentinel2-test

install:
	python -m pip install --upgrade pip && \
	python -m pip install -r requirements-all.txt

install-lambda-deps:
	python -m pip install --upgrade pip && \
	python -m pip install -r requirements-landsat.txt -t landsat/src/ && \
	python -m pip install -r requirements-status-messages.txt -t status-messages/src/

test_file ?= 'tests/'
test:
	pytest $(test_file)

integration:
	export AWS_PAGER='' && \
	$(foreach file, $(wildcard tests/integration/landsat*.json), aws sns publish --profile saml-pub --topic-arn ${LANDSAT_TOPIC_ARN} --message file://${file} --output json;) && \
	$(foreach file, $(wildcard tests/integration/sentinel2*.json), aws sns publish --profile saml-pub --topic-arn ${SENTINEL2_TOPIC_ARN} --message file://${file} --output json;)

static: ruff-check cfn-lint

ruff-check:
	ruff check

ruff-format:
	ruff format

ruff: ruff-check ruff-format

cfn-lint:
	cfn-lint --template `find . -name cloudformation.yml` --info --ignore-checks W3002
