# devnotes

Appunti tecnici di sviluppo. Niente di formale.

## TODO immediati

- [x] generatore Faker per le 4 entità (Customer, Supplier, Operator, Request), esportazione 4 CSV
- [x] pipeline ETL idempotente CSV → SQLite DWH (hash SHA-256)
- [x] 10 query SQL su DWH per la dashboard: 7 KPI (4 principali della traccia + 3 secondari) e 3 grafici nominati dalla traccia, parametrizzati sui 3 filtri what-if
- [ ] view Streamlit con 3 filtri what-if (periodo, zona, categoria)

