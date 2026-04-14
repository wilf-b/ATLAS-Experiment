# HZZ distributed analysis with Dask and Docker

This is a minimal implementation of the ATLAS HZZ analysis as a file-parallel distributed workflow.

## Architecture

- run_dask.py is the driver.
- worker.py contains the file-level processing logic.
- Dask scheduler distributes a single ROOT file per task.
- Dask workers run process_one_file()

## Files

- config.py – analysis settings, dataset definitions, branches, bins
- worker.py – cuts, weights, mass calculation, process_one_file()
- run_dask.py – submits tasks, gathers results, merges histograms, saves outputs
- Dockerfile – container image
- docker-compose.yml – scheduler + workers + runner

## Local run without Docker

Install dependencies:

```bash
pip install -r requirements.txt
```
then to run

```bash
python run_dask.py
```

## Run with Docker Compose

Build and start the distributed system with more args:

```bash
docker compose up --build --scale worker=[n]
```

The Dask dashboard should be available at:

```
http://localhost:8787
```

Output plot written to folder `results/`.

## Notes

- The implementation follows the [Hzz notebook structure](https://github.com/atlas-outreach-data-tools/notebooks-collection-opendata/blob/master/13-TeV-examples/uproot_python/HZZAnalysis.ipynb) 
- workers accumulates histograms chunk-by-chunk instead of storing all selected events.
- then return compact histogram summaries rather than event-level data.

