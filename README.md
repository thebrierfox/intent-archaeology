# Intent Archaeology Repository

This project provides tools to analyse a user's exported ChatGPT conversations in a privacy‑friendly manner.  It is designed to operate *incrementally* – you can ingest a new `conversations.json` file at any time and only new or changed conversation threads will be processed.  The tools reconstruct cross‑thread projects, detect "ghost" problems (unfinished workflows or unresolved errors) and generate human‑readable reports with evidence links.

## Features

* **Incremental ingestion** – each conversation is fingerprinted by its ID, update time and mapping length.  If nothing has changed since the last run it is skipped to avoid unnecessary compute and cost.
* **Streaming JSON parser** – uses `ijson` to avoid loading the entire export into memory.  This makes it feasible to process large exports on resource‑constrained machines.
* **SQLite + FTS5** – ingested conversations are normalised into relational tables (`conversations`, `nodes`, `edges`) and message content is indexed using Full‑Text Search for fast retrieval.
* **Simple heuristics for project grouping and ghost detection** – a baseline implementation is provided in `analyze.py`.  It can be extended with embedding/LLM‑based clustering by wiring in providers via `configs/providers.yaml`.
* **Report generation** – `report.py` renders findings into a Markdown report using Jinja2 templates.  Each claim includes provenance (conversation and node IDs) so that users can locate the original messages.

## Quickstart

To set up and run the pipeline locally:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# initialise a new database
python -m ia.cli init-db --db data/ia.db

# ingest an export (incremental)
python -m ia.cli ingest --input /path/to/conversations.json --db data/ia.db

# build search indices
python -m ia.cli build --db data/ia.db

# run analysis
python -m ia.cli analyze --db data/ia.db --out data/findings.json

# produce a report
python -m ia.cli report --db data/ia.db --findings data/findings.json --out artefacts/report.md
```

See `configs/pipeline.yaml` for tunable pa

rameters such as PII scrubbing and batch sizes.  You may copy `configs/providers.example.yaml` to `configs/providers.yaml` and fill in API keys to enable embedding and LLM‑powered clustering.


## License and ownership

All code and documentation in this repository are the intellectual property of **IntuiTek¹** (William Kyle Million). Unauthorized copying, modification, distribution or use is prohibited without prior written consent. See the `LICENSE` file for details. For licensing inquiries, contact kyle@intuitek.ai.
