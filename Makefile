export PYTHONPATH = ${PWD}/landsat/src

install:
	python -m pip install --upgrade pip && \
	python -m pip install -r requirements-all.txt

install-lambda-deps:
	python -m pip install --upgrade pip && \
	python -m pip install -r requirements-landsat.txt -t landsat/src/

test_file ?= 'tests/'
test:
	pytest $(test_file)

static: ruff-check cfn-lint

ruff-check:
	ruff check

ruff-format:
	ruff format

ruff: ruff-check ruff-format

cfn-lint:
	cfn-lint --template `find . -name cloudformation.yml` --info --ignore-checks W3002
