PYTHON := python
DB_PATH := data/ia.db
CONV_JSON := /path/to/conversations.json
FINDINGS := data/findings.json
REPORT := artefacts/report.md

.PHONY: all init ingest build analyze report clean

all: init ingest build analyze report

init:
	$(PYTHON) -m ia.cli init-db --db $(DB_PATH)

ingest:
	$(PYTHON) -m ia.cli ingest --input $(CONV_JSON) --db $(DB_PATH)

build:
	$(PYTHON) -m ia.cli build --db $(DB_PATH)

analyze:
	$(PYTHON) -m ia.cli analyze --db $(DB_PATH) --out $(FINDINGS)

report:
	$(PYTHON) -m ia.cli report --db $(DB_PATH) --findings $(FINDINGS) --out $(REPORT)

clean:
	rm -rf data artefacts
