# devnotes

Appunti tecnici di sviluppo. Niente di formale.

## TODO immediati

- [x] generatore Faker per le 4 entità (Customer, Supplier, Operator, Request), esportazione 4 CSV
- [x] pipeline ETL idempotente CSV → SQLite DWH (hash SHA-256)
- [x] 10 query SQL su DWH per la dashboard: 7 KPI (4 principali della traccia + 3 secondari) e 3 grafici nominati dalla traccia, parametrizzati sui 3 filtri what-if
- [x] verify_dwh con 8 check strutturali post-load (conteggi, FK, SCD2, monotonia time_dim, hash univoci, 7 indici, integrity_check, distribuzione status)
- [x] verify_queries con 14 check di correttezza KPI (smoke test, schemi dict/DataFrame, coerenza KPI 1↔KPI 6, range [0,1] su ratio, comportamento su set vuoto, dispatcher granularità andamento)
- [ ] view Streamlit con 3 filtri what-if (periodo, zona, categoria)

