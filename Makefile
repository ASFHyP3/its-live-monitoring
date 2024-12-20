export PYTHONPATH = ${PWD}/its_live_monitoring/src
LANDSAT_TOPIC_ARN ?= arn:aws:sns:us-west-2:986442313181:its-live-notify-landsat-test
SENTINEL2_TOPIC_ARN ?= arn:aws:sns:eu-west-1:986442313181:its-live-notify-sentinel2-test

install:
	python -m pip install --upgrade pip && \
	python -m pip install -r requirements-all.txt

install-lambda-deps:
	python -m pip install --upgrade pip && \
	python -m pip install --no-compile -r requirements-its_live_monitoring.txt -t its_live_monitoring/src/ && \
	python -m pip install --no-compile -r requirements-status-messages.txt -t status-messages/src/

test_file ?= 'tests/'
test:
	export $$(xargs < tests/cfg.env); \
	pytest $(test_file)

landsat-integration:
	export AWS_PAGER='' && \
	$(foreach file, $(wildcard tests/integration/landsat*.json), aws sns publish --profile saml-pub --topic-arn ${LANDSAT_TOPIC_ARN} --message file://${file} --output json;)

sentinel2-integration:
	export AWS_PAGER='' && \
	$(foreach file, $(wildcard tests/integration/sentinel2*.json), aws sns publish --region eu-west-1 --profile saml-pub --topic-arn ${SENTINEL2_TOPIC_ARN} --message file://${file} --output json;)

integration: landsat-integration sentinel2-integration

static: ruff-check cfn-lint

ruff-check:
	ruff check

ruff-format:
	ruff format

ruff: ruff-check ruff-format

cfn-lint:
	cfn-lint --template `find . -name cloudformation.yml` --info --ignore-checks W3002
