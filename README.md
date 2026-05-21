# project-work

Project Work — Università Pegaso, corso di laurea L31 *Informatica per le Aziende Digitali*. Sviluppo di un data warehouse e di una dashboard analitica per una startup digitale di servizi on-demand.

## Stack

- Python 3.11+
- Faker (generatore dati sintetici)
- SQLite (DWH)
- pandas (ETL)
- Streamlit (dashboard)

## Struttura

- `src/generator.py` — generatore Faker per le 4 entità (Customer, Supplier, Operator, Request), esporta in 4 CSV
- `src/etl.py` — pipeline ETL CSV → SQLite DWH, macchina a stati della Request, popolamento dimensionale
- `src/queries.py` — 4 query SQL parametrizzate per i 4 KPI principali
- `src/dashboard.py` — applicazione Streamlit con filtri e drill-down
- `data/csv/` — file CSV sorgente generati dal generator (non versionati)
- `data/` — database SQLite del DWH (non versionato)
- `docs/` — note di sviluppo, changelog, asset
- `tests/` — test

## Setup

Richiede Python 3.10 o successivo (3.12 raccomandato).

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Esecuzione

