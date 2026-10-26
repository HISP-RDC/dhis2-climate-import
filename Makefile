.DEFAULT_GOAL := help

PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PYTHON_VERSION ?= 3.12
UV ?= uv
UV_CACHE_DIR ?= .uv-cache
CLI_BIN ?= .venv/bin/dhis2-climate-import
CLI_FALLBACK := PYTHONPATH=src $(PYTHON) -m dhis2_climate_import
CLI := $(if $(wildcard $(CLI_BIN)),$(CLI_BIN),$(CLI_FALLBACK))

.PHONY: help install lock test compile shell-check check list-jobs check-dhis2 cache-org-units cache-data-elements

help:
	@printf "DHIS2 Climate Import commands\n"
	@printf "  make install              Install locked dependencies with Python 3.12\n"
	@printf "  make lock                 Refresh uv.lock\n"
	@printf "  make test                 Run unit tests\n"
	@printf "  make check                Run compilation, shell checks, and tests\n"
	@printf "  make list-jobs            List configured climate jobs\n"
	@printf "  make check-dhis2          Check DHIS2 connectivity\n"
	@printf "  make cache-org-units      Cache level 3 org units\n"
	@printf "  make cache-data-elements  Cache data elements\n"

install:
	UV_CACHE_DIR=$(UV_CACHE_DIR) $(UV) sync --locked --python $(PYTHON_VERSION)

lock:
	UV_CACHE_DIR=$(UV_CACHE_DIR) $(UV) lock

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests -v

compile:
	PYTHONPATH=src $(PYTHON) -m compileall -q src tests

shell-check:
	bash -n deploy/systemd/dhis2-climate-pipeline.sh deploy/systemd/dhis2-transfer-queue.sh

check: compile shell-check test

list-jobs:
	$(CLI) list-jobs --ids-only

check-dhis2:
	$(CLI) check-dhis2

cache-org-units:
	$(CLI) cache-org-units --level 3

cache-data-elements:
	$(CLI) cache-data-elements
