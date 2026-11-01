.PHONY: setup generate-data cluster-up cluster-down docker-up docker-down test clean

VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip

setup:
	python3.11 -m venv $(VENV) || python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PYTHON) data/generator.py --scale 1

generate-data:
	$(PYTHON) data/generator.py --scale 1

generate-data-large:
	$(PYTHON) data/generator.py --scale 10

docker-up:
	docker compose up -d

docker-down:
	docker compose down -v

cluster-up:
	./scripts/setup_multipass_cluster.sh

cluster-down:
	./scripts/teardown_multipass_cluster.sh

test:
	$(PYTHON) -m pytest -v tests/

clean:
	rm -rf data/warehouse/* metastore_db derby.log .pytest_cache spark-warehouse
