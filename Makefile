.DEFAULT_GOAL := help

.PHONY: help
help:  ## Show this help
	@grep -E '^\S+:.*?## .*$$' $(firstword $(MAKEFILE_LIST)) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-30s %s\n", $$1, $$2}'

.PHONY: all
all: setup lint test docs  ## Execute all the build steps

.PHONY: setup
setup:  ## Setup dev environment
	poetry install
	poetry run vscode

.PHONY: install
install:  ## Install dev environment
	poetry install


.PHONY: lint
lint:  ## Perform linting and formatting
	@echo "Formatting with autopep8"
	@poetry run autopep8 -i -r ./
	@echo "Check for errors with flake8"
	@poetry run flake8 ./
	@echo "Done"

.PHONY: test
test:  ## Run tests
	poetry run pytest --cov=jsonschema2ddl -v test/

.PHONY: docs
docs:   ## Produce documentation
	@poetry run $(MAKE) -s -C docs clean
	@poetry run $(MAKE) -s -C docs html

.PHONY: clean
clean:  ## Remove build artifacts
	@rm -fr build/
	@rm -fr dist/
	@rm -fr .eggs/
	@find . -name '*.egg-info' -exec rm -fr {} +
	@find . -name '*.egg' -exec rm -f {} +
	@find . -name '*.pyc' -exec rm -f {} +
	@find . -name '*.pyo' -exec rm -f {} +
	@find . -name '__pycache__' -exec rm -fr {} +
	@rm -fr .tox/
	@rm -f .coverage
	@rm -fr htmlcov/
	@rm -fr .pytest_cache
	@$(MAKE) -s -C docs clean

.PHONY: precommit
precommit:  clean lint test docs ## Actions befor a commit
