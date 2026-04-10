# HZZ distributed analysis with Dask and Docker

This is a minimal implementation of the ATLAS HZZ analysis as a file-parallel distributed workflow.

## Architecture

- `run_dask.py` is the driver.
- `worker.py` contains the file-level processing logic.
- Dask scheduler distributes one ROOT file per task.
- Dask workers run `process_one_file(...)`.
- Docker provides a reproducible environment.

## Files

- `config.py` – analysis settings, dataset definitions, branches, bins
- `worker.py` – cuts, weights, mass calculation, `process_one_file`
- `run_dask.py` – submits tasks, gathers results, merges histograms, saves outputs
- `Dockerfile` – container image
- `docker-compose.yml` – scheduler + workers + runner

## Local test without Docker

Install dependencies:

```bash
pip install -r requirements.txt
```

Run on a small subset first:

```bash
python run_dask.py --limit-files 1
```

This starts a local Dask client automatically if `--scheduler` is not provided.

## Run with Docker Compose

Build and start the distributed system:

```bash
docker compose up --build --scale worker=2
```

The Dask dashboard should be available at:

```text
http://localhost:8787
```

Outputs are written to `results/`.

## Notes

- The implementation follows the notebook structure but accumulates histograms chunk-by-chunk instead of storing all selected events.
- Workers return compact histogram summaries rather than event-level data.
- Start with `--limit-files 1` or `--limit-files 2` while debugging.
