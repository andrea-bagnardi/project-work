"""Query SQL e funzioni esecutrici per la dashboard "Guida e Vai".

Modulo Model del pattern MVC della dashboard.
Espone 7 KPI (4 principali + 3 secondari) e 3 grafici nominati dalla traccia,
tutti parametrizzati sui 3 filtri what-if: periodo (obbligatorio), zona, categoria.

Forma del modulo: costanti SQL a livello di modulo + funzioni
esecutrici sottili. Nessuna classe, nessuno stato condiviso (come src/etl.py).
"""

import sqlite3
from datetime import date

import pandas as pd

# Soglia di dispatch giornaliero/settimanale del grafico andamento.
ANDAMENTO_WEEKLY_THRESHOLD_DAYS = 90


# --------------------------------------------------------------------------- #
# Costanti SQL
# --------------------------------------------------------------------------- #

# KPI 1 — Numero richieste totali nel periodo.
SQL_KPI1_RICHIESTE = """
SELECT COUNT(*) AS richieste
FROM request_fact rf
JOIN time_dim td ON rf.date_key = td.date_key
JOIN customer_dim cd ON rf.customer_key = cd.customer_key
JOIN supplier_dim sd ON rf.supplier_key = sd.supplier_key
WHERE td.full_date BETWEEN :period_start AND :period_end
  AND (:province IS NULL OR cd.province = :province)
  AND (:category IS NULL OR sd.category = :category)
"""

# KPI 2 — Incassi totali nel periodo.
SQL_KPI2_INCASSI = """
SELECT COALESCE(SUM(rf.price), 0) AS incassi_eur
FROM request_fact rf
JOIN time_dim td ON rf.date_key = td.date_key
JOIN customer_dim cd ON rf.customer_key = cd.customer_key
JOIN supplier_dim sd ON rf.supplier_key = sd.supplier_key
WHERE td.full_date BETWEEN :period_start AND :period_end
  AND (:province IS NULL OR cd.province = :province)
  AND (:category IS NULL OR sd.category = :category)
  AND rf.is_cancelled = 0
  AND rf.is_completed = 1
"""

# KPI 3 — Tempo medio di erogazione (minuti) nel periodo.
SQL_KPI3_TEMPO_MEDIO = """
SELECT AVG(rf.service_duration_minutes) AS tempo_medio_evasione_minuti
FROM request_fact rf
JOIN time_dim td ON rf.date_key = td.date_key
JOIN customer_dim cd ON rf.customer_key = cd.customer_key
JOIN supplier_dim sd ON rf.supplier_key = sd.supplier_key
WHERE td.full_date BETWEEN :period_start AND :period_end
  AND (:province IS NULL OR cd.province = :province)
  AND (:category IS NULL OR sd.category = :category)
  AND rf.is_cancelled = 0
  AND rf.is_completed = 1
"""

# KPI 4 — Clienti attivi nel periodo.
SQL_KPI4_CLIENTI_ATTIVI = """
SELECT COUNT(DISTINCT cd.fiscal_code_hash) AS clienti_attivi
FROM request_fact rf
JOIN time_dim td ON rf.date_key = td.date_key
JOIN customer_dim cd ON rf.customer_key = cd.customer_key
JOIN supplier_dim sd ON rf.supplier_key = sd.supplier_key
WHERE td.full_date BETWEEN :period_start AND :period_end
  AND (:province IS NULL OR cd.province = :province)
  AND (:category IS NULL OR sd.category = :category)
"""

# KPI 5 — Incassi per zona nel periodo (segmentazione di KPI 2 per provincia).
SQL_KPI5_INCASSI_PER_ZONA = """
SELECT
  cd.province AS province,
  SUM(rf.price) AS revenue_in_province,
  COUNT(*) AS n_requests_in_province
FROM request_fact rf
JOIN time_dim td ON rf.date_key = td.date_key
JOIN customer_dim cd ON rf.customer_key = cd.customer_key
JOIN supplier_dim sd ON rf.supplier_key = sd.supplier_key
WHERE td.full_date BETWEEN :period_start AND :period_end
  AND (:province IS NULL OR cd.province = :province)
  AND (:category IS NULL OR sd.category = :category)
  AND rf.is_cancelled = 0
  AND rf.is_completed = 1
GROUP BY cd.province
ORDER BY revenue_in_province DESC
"""

# KPI 6 — Tasso di cancellazione nel periodo.
SQL_KPI6_TASSO_CANCELLAZIONE = """
SELECT
  CAST(SUM(rf.is_cancelled) AS REAL) / NULLIF(COUNT(*), 0) AS tasso_cancellazione
FROM request_fact rf
JOIN time_dim td ON rf.date_key = td.date_key
JOIN customer_dim cd ON rf.customer_key = cd.customer_key
JOIN supplier_dim sd ON rf.supplier_key = sd.supplier_key
WHERE td.full_date BETWEEN :period_start AND :period_end
  AND (:province IS NULL OR cd.province = :province)
  AND (:category IS NULL OR sd.category = :category)
"""

# KPI 7 — Retention nel periodo.
SQL_KPI7_RETENTION = """
WITH per_persona AS (
    SELECT cd.fiscal_code_hash, COUNT(*) AS n_request
    FROM request_fact rf
    JOIN time_dim td ON rf.date_key = td.date_key
    JOIN customer_dim cd ON rf.customer_key = cd.customer_key
    JOIN supplier_dim sd ON rf.supplier_key = sd.supplier_key
    WHERE td.full_date BETWEEN :period_start AND :period_end
      AND (:province IS NULL OR cd.province = :province)
      AND (:category IS NULL OR sd.category = :category)
    GROUP BY cd.fiscal_code_hash
)
SELECT
  CAST(SUM(CASE WHEN n_request >= 2 THEN 1 ELSE 0 END) AS REAL)
  / NULLIF(COUNT(*), 0) AS retention_pct
FROM per_persona
"""

# Grafico G1 — Andamento richieste nel tempo, granularità GIORNALIERA (periodo <= 90 giorni).
SQL_CHART_ANDAMENTO_DAILY = """
SELECT
  td.full_date AS period_label,
  COUNT(*) AS n_requests
FROM request_fact rf
JOIN time_dim td ON rf.date_key = td.date_key
JOIN customer_dim cd ON rf.customer_key = cd.customer_key
JOIN supplier_dim sd ON rf.supplier_key = sd.supplier_key
WHERE td.full_date BETWEEN :period_start AND :period_end
  AND (:province IS NULL OR cd.province = :province)
  AND (:category IS NULL OR sd.category = :category)
GROUP BY period_label
ORDER BY period_label
"""

# Grafico G1 — Andamento richieste nel tempo, granularità SETTIMANALE (periodo > 90 giorni).
SQL_CHART_ANDAMENTO_WEEKLY = """
SELECT
  strftime('%Y-W%W', td.full_date) AS period_label,
  COUNT(*) AS n_requests
FROM request_fact rf
JOIN time_dim td ON rf.date_key = td.date_key
JOIN customer_dim cd ON rf.customer_key = cd.customer_key
JOIN supplier_dim sd ON rf.supplier_key = sd.supplier_key
WHERE td.full_date BETWEEN :period_start AND :period_end
  AND (:province IS NULL OR cd.province = :province)
  AND (:category IS NULL OR sd.category = :category)
GROUP BY period_label
ORDER BY period_label
"""

# Grafico G2 — Top-5 fornitori per incassi (barre orizzontali).
SQL_CHART_TOP_FORNITORI = """
SELECT
  sd.name AS supplier_name,
  sd.category AS category,
  SUM(rf.price) AS revenue_eur,
  COUNT(*) AS n_requests
FROM request_fact rf
JOIN time_dim td ON rf.date_key = td.date_key
JOIN customer_dim cd ON rf.customer_key = cd.customer_key
JOIN supplier_dim sd ON rf.supplier_key = sd.supplier_key
WHERE td.full_date BETWEEN :period_start AND :period_end
  AND (:province IS NULL OR cd.province = :province)
  AND (:category IS NULL OR sd.category = :category)
  AND rf.is_cancelled = 0
  AND rf.is_completed = 1
GROUP BY sd.supplier_key, sd.name, sd.category
ORDER BY revenue_eur DESC
LIMIT 5
"""

# Grafico G3 — Distribuzione richieste per zona x categoria.
SQL_CHART_DISTRIBUZIONE = """
SELECT
  cd.province AS province,
  sd.category AS category,
  COUNT(*) AS n_requests
FROM request_fact rf
JOIN time_dim td ON rf.date_key = td.date_key
JOIN customer_dim cd ON rf.customer_key = cd.customer_key
JOIN supplier_dim sd ON rf.supplier_key = sd.supplier_key
WHERE td.full_date BETWEEN :period_start AND :period_end
  AND (:province IS NULL OR cd.province = :province)
  AND (:category IS NULL OR sd.category = :category)
GROUP BY cd.province, sd.category
ORDER BY cd.province, sd.category
"""


# --------------------------------------------------------------------------- #
# Helper interni
# --------------------------------------------------------------------------- #

def _params(period_start: str, period_end: str,
            province: str | None, category: str | None) -> dict:
    """Confeziona i 4 parametri nominati comuni a tutte le query."""
    return {
        "period_start": period_start,
        "period_end": period_end,
        "province": province,
        "category": category,
    }


def _big_number(value: int | float | None, period_start: str, period_end: str,
                province: str | None, category: str | None,
                reason: str | None = None) -> dict:
    """Confeziona il dict di ritorno dei KPI big number."""
    result: dict = {
        "value": value,
        "period": (period_start, period_end),
        "filters": {"province": province, "category": category},
    }
    if value is None:
        result["reason"] = reason
    return result


# --------------------------------------------------------------------------- #
# KPI big number (1-4, 6, 7) — ritornano dict
# --------------------------------------------------------------------------- #

def run_kpi1_richieste(conn: sqlite3.Connection, period_start: str, period_end: str,
                       province: str | None = None,
                       category: str | None = None) -> dict:
    """KPI 1 — Numero richieste. Count: vuoto -> value=0."""
    df = pd.read_sql(SQL_KPI1_RICHIESTE, conn,
                     params=_params(period_start, period_end, province, category))
    return _big_number(int(df["richieste"].iloc[0]),
                       period_start, period_end, province, category)


def run_kpi2_incassi(conn: sqlite3.Connection, period_start: str, period_end: str,
                     province: str | None = None,
                     category: str | None = None) -> dict:
    """KPI 2 — Incassi totali. Sum: vuoto -> value=0 (COALESCE)."""
    df = pd.read_sql(SQL_KPI2_INCASSI, conn,
                     params=_params(period_start, period_end, province, category))
    return _big_number(float(df["incassi_eur"].iloc[0]),
                       period_start, period_end, province, category)


def run_kpi3_tempo_medio(conn: sqlite3.Connection, period_start: str, period_end: str,
                         province: str | None = None,
                         category: str | None = None) -> dict:
    """KPI 3 — Tempo medio erogazione in minuti.

    Mean: set vuoto (AVG NULL) -> value=None, reason="no_completed_requests".
    """
    df = pd.read_sql(SQL_KPI3_TEMPO_MEDIO, conn,
                     params=_params(period_start, period_end, province, category))
    raw = df["tempo_medio_evasione_minuti"].iloc[0]
    if pd.isna(raw):
        return _big_number(None, period_start, period_end, province, category,
                           reason="no_completed_requests")
    return _big_number(float(raw), period_start, period_end, province, category)


def run_kpi4_clienti_attivi(conn: sqlite3.Connection, period_start: str, period_end: str,
                            province: str | None = None,
                            category: str | None = None) -> dict:
    """KPI 4 — Clienti attivi. Count distinct fiscal_code_hash: vuoto -> value=0."""
    df = pd.read_sql(SQL_KPI4_CLIENTI_ATTIVI, conn,
                     params=_params(period_start, period_end, province, category))
    return _big_number(int(df["clienti_attivi"].iloc[0]),
                       period_start, period_end, province, category)


def run_kpi6_tasso_cancellazione(conn: sqlite3.Connection, period_start: str, period_end: str,
                                 province: str | None = None,
                                 category: str | None = None) -> dict:
    """KPI 6 — Tasso di cancellazione.

    Ratio: denominatore vuoto (NULLIF NULL) -> value=None, reason="no_requests_in_denominator".
    """
    df = pd.read_sql(SQL_KPI6_TASSO_CANCELLAZIONE, conn,
                     params=_params(period_start, period_end, province, category))
    raw = df["tasso_cancellazione"].iloc[0]
    if pd.isna(raw):
        return _big_number(None, period_start, period_end, province, category,
                           reason="no_requests_in_denominator")
    return _big_number(float(raw), period_start, period_end, province, category)


def run_kpi7_retention(conn: sqlite3.Connection, period_start: str, period_end: str,
                       province: str | None = None,
                       category: str | None = None) -> dict:
    """KPI 7 — Retention clienti.

    Ratio: denominatore vuoto (NULLIF NULL) -> value=None, reason="no_requests_in_denominator".
    """
    df = pd.read_sql(SQL_KPI7_RETENTION, conn,
                     params=_params(period_start, period_end, province, category))
    raw = df["retention_pct"].iloc[0]
    if pd.isna(raw):
        return _big_number(None, period_start, period_end, province, category,
                           reason="no_requests_in_denominator")
    return _big_number(float(raw), period_start, period_end, province, category)


# --------------------------------------------------------------------------- #
# KPI 5 (tabella) e grafici G1-G3 — ritornano DataFrame
# --------------------------------------------------------------------------- #

def run_kpi5_incassi_per_zona(conn: sqlite3.Connection, period_start: str, period_end: str,
                              province: str | None = None,
                              category: str | None = None) -> pd.DataFrame:
    """KPI 5 — Incassi per zona. DataFrame: province, revenue_in_province, n_requests_in_province."""
    return pd.read_sql(SQL_KPI5_INCASSI_PER_ZONA, conn,
                       params=_params(period_start, period_end, province, category))


def run_chart_andamento(conn: sqlite3.Connection, period_start: str, period_end: str,
                        province: str | None = None,
                        category: str | None = None) -> pd.DataFrame:
    """Grafico G1 — Andamento richieste nel tempo.

    Dispatch granularità: giornaliera se periodo <= 90 giorni, settimanale altrimenti.
    DataFrame: period_label, n_requests.
    """
    period_days = (date.fromisoformat(period_end) - date.fromisoformat(period_start)).days
    sql = (SQL_CHART_ANDAMENTO_WEEKLY
           if period_days > ANDAMENTO_WEEKLY_THRESHOLD_DAYS
           else SQL_CHART_ANDAMENTO_DAILY)
    return pd.read_sql(sql, conn,
                       params=_params(period_start, period_end, province, category))


def run_chart_top_fornitori(conn: sqlite3.Connection, period_start: str, period_end: str,
                            province: str | None = None,
                            category: str | None = None) -> pd.DataFrame:
    """Grafico G2 — Top-5 fornitori per incassi. DataFrame: supplier_name, category, revenue_eur, n_requests."""
    return pd.read_sql(SQL_CHART_TOP_FORNITORI, conn,
                       params=_params(period_start, period_end, province, category))


def run_chart_distribuzione(conn: sqlite3.Connection, period_start: str, period_end: str,
                            province: str | None = None,
                            category: str | None = None) -> pd.DataFrame:
    """Grafico G3 — Distribuzione richieste per zona x categoria. DataFrame: province, category, n_requests."""
    return pd.read_sql(SQL_CHART_DISTRIBUZIONE, conn,
                       params=_params(period_start, period_end, province, category))
