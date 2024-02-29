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

static: flake8 cfn-lint

flake8:
	flake8 --max-line-length=120

cfn-lint:
	cfn-lint --template `find . -name cloudformation.yml` --info --ignore-checks W3002
