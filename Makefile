.DEFAULT_GOAL := help

PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
UV ?= python3 -m uv
UV_CACHE_DIR ?= .uv-cache
CLI := PYTHONPATH=src $(PYTHON) -m dhis2_climate_import

.PHONY: help install lock test check-dhis2 cache-org-units cache-data-elements

help:
	@printf "DHIS2 Climate Import commands\n"
	@printf "  make install              Sync the uv environment\n"
	@printf "  make lock                 Refresh uv.lock\n"
	@printf "  make test                 Run unit tests\n"
	@printf "  make check-dhis2          Check DHIS2 connectivity\n"
	@printf "  make cache-org-units      Cache level 3 org units\n"
	@printf "  make cache-data-elements  Cache data elements\n"

install:
	UV_CACHE_DIR=$(UV_CACHE_DIR) $(UV) sync

lock:
	UV_CACHE_DIR=$(UV_CACHE_DIR) $(UV) lock

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests -v

check-dhis2:
	$(CLI) check-dhis2

cache-org-units:
	$(CLI) cache-org-units --level 3

cache-data-elements:
	$(CLI) cache-data-elements
