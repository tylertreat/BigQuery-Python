SHELL := /bin/bash
PYTHON := python
PIP := pip

BUILD_DIR := .build

all: deps

clean:
	find . -name "*.py[co]" -delete

distclean: clean
	rm -rf $(BUILD_DIR)
	rm -rf $(LIBS_DIR)

test: clean integrations
deps: py_dev_deps py_deploy_deps

py_deploy_deps: $(BUILD_DIR)/pip-deploy.out

py_dev_deps: $(BUILD_DIR)/pip-dev.out

$(BUILD_DIR)/pip-deploy.out: requirements.txt
	@mkdir -p $(BUILD_DIR)
	$(PIP) install -Ur $< && touch $@

$(BUILD_DIR)/pip-dev.out: requirements_dev.txt
	@mkdir -p $(BUILD_DIR)
	$(PIP) install -Ur $< && touch $@

unit:
	nosetests

integrations:
	nosetests --logging-level=ERROR -a slow --with-coverage --cover-package=bigquery

