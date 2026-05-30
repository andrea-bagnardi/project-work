"""Verifica di correttezza delle query della dashboard "Guida e Vai".

Esegue i 14 check C1-C14 contro le funzioni di ``src/queries.py`` eseguite su
``data/dwh.db``. Le condizioni di test sono suddivise in quattro famiglie:

* **C1-C7 — condizioni canoniche** (periodo intero del DWH, nessun filtro):
  smoke test di KPI 1 (=470) e KPI 4 (=200), schema dict big number, schema
  DataFrame KPI 5 + 3 grafici, coerenza KPI 1 / denominatore KPI 6, range [0,1]
  per i ratio KPI 6 e KPI 7.
* **C8-C9 — filtri attivi noti**: restrizione del count su filtri presenti
  nei dati, robustezza dei KPI mean/ratio quando il denominatore non è vuoto.
* **C10-C12 — filtri che svuotano il set**: count/sum -> 0 senza ``reason``,
  mean/ratio -> ``None`` con ``reason`` non vuoto, DataFrame vuoto ma con
  lo schema di colonne canonico intatto.
* **C13-C14 — dispatcher granularità del grafico andamento**: formato di
  ``period_label`` giornaliero (YYYY-MM-DD) per periodi <= 90 giorni,
  settimanale (YYYY-Www) per periodi > 90 giorni.

Uso::

    python src/verify_queries.py

Exit code 0 se tutti i 14 check passano, 1 altrimenti.
"""

from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path
from typing import Callable, List, Tuple

import pandas as pd

# Se eseguito come `python src/verify_queries.py` (no pacchetto), la project root
# non e' su sys.path: aggiungiamola per risolvere `from src.queries import ...`.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.queries import (
    run_chart_andamento,
    run_chart_distribuzione,
    run_chart_top_fornitori,
    run_kpi1_richieste,
    run_kpi2_incassi,
    run_kpi3_tempo_medio,
    run_kpi4_clienti_attivi,
    run_kpi5_incassi_per_zona,
    run_kpi6_tasso_cancellazione,
    run_kpi7_retention,
)

BASE_DIR = Path(__file__).resolve().parent.parent
DWH_PATH = BASE_DIR / "data" / "dwh.db"

# --------------------------------------------------------------------------- #
# Valori attesi e setup di test
# --------------------------------------------------------------------------- #

EXPECTED_KPI1_FULL = 470   # cardinalità di request_fact
EXPECTED_KPI4_FULL = 200   # cardinalità di personas

# Filtri what-if conosciuti: una categoria presente nei dati (3 supplier su 7)
# che restringe ma non svuota il set; uno valore inesistente che lo svuota.
KNOWN_CATEGORY = "driving_school"
EMPTY_PROVINCE = "ZZ"

# Periodi per il dispatcher granularità del grafico andamento:
# soglia ANDAMENTO_WEEKLY_THRESHOLD_DAYS = 90.
DAILY_PERIOD = ("2025-01-01", "2025-03-01")     # 59 giorni
WEEKLY_PERIOD = ("2025-01-01", "2026-12-31")    # 730 giorni

# Schemi colonna canonici.
EXPECTED_COLUMNS = {
    "kpi5_incassi_per_zona": ["province", "revenue_in_province", "n_requests_in_province"],
    "chart_andamento":       ["period_label", "n_requests"],
    "chart_top_fornitori":   ["supplier_name", "category", "revenue_eur", "n_requests"],
    "chart_distribuzione":   ["province", "category", "n_requests"],
}

# Chiavi attese del dict big number.
BIG_NUMBER_KEYS = frozenset({"value", "period", "filters"})

RE_PERIOD_LABEL_DAILY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
RE_PERIOD_LABEL_WEEKLY = re.compile(r"^\d{4}-W\d{2}$")


# --------------------------------------------------------------------------- #
# Helper
# --------------------------------------------------------------------------- #

def _canonical_period(conn: sqlite3.Connection) -> Tuple[str, str]:
    """Periodo canonico = intero range di time_dim."""
    row = conn.execute("SELECT MIN(full_date), MAX(full_date) FROM time_dim").fetchone()
    return row[0], row[1]


def _has_big_number_schema(d: dict) -> bool:
    """True sse d contiene almeno le 3 chiavi obbligatorie e i tipi nidificati attesi."""
    if not BIG_NUMBER_KEYS.issubset(d.keys()):
        return False
    if not (isinstance(d["period"], tuple) and len(d["period"]) == 2):
        return False
    if not (isinstance(d["filters"], dict)
            and {"province", "category"}.issubset(d["filters"].keys())):
        return False
    return True


# --------------------------------------------------------------------------- #
# C1-C7 — Condizioni canoniche (periodo intero DWH, nessun filtro)
# --------------------------------------------------------------------------- #

def check_c1_kpi1_full(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C1 — KPI 1 = 470 sul periodo intero senza filtri."""
    ps, pe = _canonical_period(conn)
    r = run_kpi1_richieste(conn, ps, pe, None, None)
    if r["value"] != EXPECTED_KPI1_FULL:
        return False, f"KPI 1 atteso {EXPECTED_KPI1_FULL}, ottenuto {r['value']}"
    return True, f"KPI 1 = {r['value']}"


def check_c2_kpi4_full(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C2 — KPI 4 = 200 sul periodo intero senza filtri."""
    ps, pe = _canonical_period(conn)
    r = run_kpi4_clienti_attivi(conn, ps, pe, None, None)
    if r["value"] != EXPECTED_KPI4_FULL:
        return False, f"KPI 4 atteso {EXPECTED_KPI4_FULL}, ottenuto {r['value']}"
    return True, f"KPI 4 = {r['value']}"


def check_c3_big_number_schema(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C3 — schema dict big number conforme per i 6 KPI."""
    ps, pe = _canonical_period(conn)
    runners = {
        "kpi1": run_kpi1_richieste,
        "kpi2": run_kpi2_incassi,
        "kpi3": run_kpi3_tempo_medio,
        "kpi4": run_kpi4_clienti_attivi,
        "kpi6": run_kpi6_tasso_cancellazione,
        "kpi7": run_kpi7_retention,
    }
    bad = [name for name, fn in runners.items()
           if not _has_big_number_schema(fn(conn, ps, pe, None, None))]
    if bad:
        return False, f"schema dict non conforme per: {', '.join(bad)}"
    return True, f"6/6 KPI big number conformi a {{value, period, filters}}"


def check_c4_dataframe_schema(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C4 — schema DataFrame conforme per KPI 5 + 3 grafici."""
    ps, pe = _canonical_period(conn)
    dfs = {
        "kpi5_incassi_per_zona": run_kpi5_incassi_per_zona(conn, ps, pe, None, None),
        "chart_andamento":       run_chart_andamento(conn, ps, pe, None, None),
        "chart_top_fornitori":   run_chart_top_fornitori(conn, ps, pe, None, None),
        "chart_distribuzione":   run_chart_distribuzione(conn, ps, pe, None, None),
    }
    mismatches = []
    for name, df in dfs.items():
        cols_got = list(df.columns)
        cols_exp = EXPECTED_COLUMNS[name]
        if cols_got != cols_exp:
            mismatches.append(f"{name}: atteso {cols_exp}, ottenuto {cols_got}")
    if mismatches:
        return False, "schema colonne KO -> " + "; ".join(mismatches)
    return True, "4/4 DataFrame con colonne canoniche conformi"


def check_c5_kpi1_kpi6_coerenza(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C5 — denominatore di KPI 6 coincide con KPI 1 sotto stessi filtri."""
    ps, pe = _canonical_period(conn)
    kpi1 = run_kpi1_richieste(conn, ps, pe, None, None)["value"]
    kpi6 = run_kpi6_tasso_cancellazione(conn, ps, pe, None, None)["value"]
    n_cancelled = conn.execute(
        "SELECT SUM(is_cancelled) FROM request_fact"
    ).fetchone()[0]
    # KPI 6 = n_cancelled / KPI 1  ==>  KPI 6 * KPI 1 == n_cancelled (denominatore = KPI 1).
    product = kpi6 * kpi1
    if abs(product - n_cancelled) > 1e-6:
        return False, (f"denominatore KPI 6 != KPI 1: kpi6*kpi1={product:.6f}, "
                       f"n_cancelled={n_cancelled}")
    return True, f"kpi6*kpi1 = {product:.6f} == n_cancelled = {n_cancelled}"


def check_c6_kpi6_range(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C6 — 0 <= KPI 6 (tasso cancellazione) <= 1 sul periodo canonico."""
    ps, pe = _canonical_period(conn)
    v = run_kpi6_tasso_cancellazione(conn, ps, pe, None, None)["value"]
    if not (0.0 <= v <= 1.0):
        return False, f"KPI 6 fuori range: {v}"
    return True, f"KPI 6 = {v:.4f} in [0,1]"


def check_c7_kpi7_range(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C7 — 0 <= KPI 7 (retention) <= 1 sul periodo canonico."""
    ps, pe = _canonical_period(conn)
    v = run_kpi7_retention(conn, ps, pe, None, None)["value"]
    if not (0.0 <= v <= 1.0):
        return False, f"KPI 7 fuori range: {v}"
    return True, f"KPI 7 = {v:.4f} in [0,1]"


# --------------------------------------------------------------------------- #
# C8-C9 — Filtri attivi noti
# --------------------------------------------------------------------------- #

def check_c8_filtro_restringe(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C8 — KPI 1 con filtro categoria noto restringe il count rispetto a C1."""
    ps, pe = _canonical_period(conn)
    kpi1_full = run_kpi1_richieste(conn, ps, pe, None, None)["value"]
    kpi1_filtered = run_kpi1_richieste(conn, ps, pe, None, KNOWN_CATEGORY)["value"]
    if not (0 < kpi1_filtered < kpi1_full):
        return False, (f"filtro non restringe: full={kpi1_full}, "
                       f"filtered(category={KNOWN_CATEGORY})={kpi1_filtered}")
    return True, f"category={KNOWN_CATEGORY}: {kpi1_filtered} < {kpi1_full} (full)"


def check_c9_mean_ratio_non_none(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C9 — KPI 3/6/7 ritornano value non None con filtro che include denominatore valido."""
    ps, pe = _canonical_period(conn)
    runners = {
        "kpi3": run_kpi3_tempo_medio,
        "kpi6": run_kpi6_tasso_cancellazione,
        "kpi7": run_kpi7_retention,
    }
    bad = [name for name, fn in runners.items()
           if fn(conn, ps, pe, None, KNOWN_CATEGORY)["value"] is None]
    if bad:
        return False, f"value=None inatteso con filtro popolato: {', '.join(bad)}"
    return True, f"3/3 mean/ratio con value non None su category={KNOWN_CATEGORY}"


# --------------------------------------------------------------------------- #
# C10-C12 — Filtri che svuotano il set
# --------------------------------------------------------------------------- #

def check_c10_count_sum_empty(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C10 — KPI count/sum su set vuoto ritornano value=0 senza 'reason'."""
    ps, pe = _canonical_period(conn)
    results = {
        "kpi1": run_kpi1_richieste(conn, ps, pe, EMPTY_PROVINCE, None),
        "kpi2": run_kpi2_incassi(conn, ps, pe, EMPTY_PROVINCE, None),
        "kpi4": run_kpi4_clienti_attivi(conn, ps, pe, EMPTY_PROVINCE, None),
    }
    bad = []
    for name, r in results.items():
        if r["value"] != 0:
            bad.append(f"{name} value={r['value']}")
        elif "reason" in r:
            bad.append(f"{name} ha 'reason' (non atteso)")
    if bad:
        return False, "; ".join(bad)
    return True, "3/3 count/sum -> value=0 senza chiave 'reason'"


def check_c11_mean_ratio_empty(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C11 — KPI mean/ratio su set vuoto ritornano value=None con reason valida."""
    ps, pe = _canonical_period(conn)
    results = {
        "kpi3": (run_kpi3_tempo_medio(conn, ps, pe, EMPTY_PROVINCE, None),
                 "no_completed_requests"),
        "kpi6": (run_kpi6_tasso_cancellazione(conn, ps, pe, EMPTY_PROVINCE, None),
                 "no_requests_in_denominator"),
        "kpi7": (run_kpi7_retention(conn, ps, pe, EMPTY_PROVINCE, None),
                 "no_requests_in_denominator"),
    }
    bad = []
    for name, (r, expected_reason) in results.items():
        if r["value"] is not None:
            bad.append(f"{name} value={r['value']} (atteso None)")
        elif r.get("reason") != expected_reason:
            bad.append(f"{name} reason='{r.get('reason')}' (atteso '{expected_reason}')")
    if bad:
        return False, "; ".join(bad)
    return True, "3/3 mean/ratio -> value=None con reason canonica"


def check_c12_dataframe_empty_schema(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C12 — KPI 5 e 3 grafici su set vuoto: DataFrame vuoto ma con schema canonico intatto."""
    ps, pe = _canonical_period(conn)
    dfs = {
        "kpi5_incassi_per_zona": run_kpi5_incassi_per_zona(conn, ps, pe, EMPTY_PROVINCE, None),
        "chart_andamento":       run_chart_andamento(conn, ps, pe, EMPTY_PROVINCE, None),
        "chart_top_fornitori":   run_chart_top_fornitori(conn, ps, pe, EMPTY_PROVINCE, None),
        "chart_distribuzione":   run_chart_distribuzione(conn, ps, pe, EMPTY_PROVINCE, None),
    }
    bad = []
    for name, df in dfs.items():
        if len(df) != 0:
            bad.append(f"{name}: atteso 0 righe, ottenute {len(df)}")
        elif list(df.columns) != EXPECTED_COLUMNS[name]:
            bad.append(f"{name}: colonne {list(df.columns)} != {EXPECTED_COLUMNS[name]}")
    if bad:
        return False, "; ".join(bad)
    return True, "4/4 DataFrame vuoti con colonne canoniche intatte"


# --------------------------------------------------------------------------- #
# C13-C14 — Dispatcher granularità grafico andamento
# --------------------------------------------------------------------------- #

def check_c13_andamento_daily(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C13 — periodo <= 90 giorni: period_label in formato giornaliero YYYY-MM-DD."""
    ps, pe = DAILY_PERIOD
    df = run_chart_andamento(conn, ps, pe, None, None)
    if df.empty:
        return False, f"DataFrame vuoto su periodo {ps}->{pe} (atteso almeno 1 riga)"
    bad = [label for label in df["period_label"]
           if not RE_PERIOD_LABEL_DAILY.match(str(label))]
    if bad:
        return False, f"period_label non daily: {bad[:3]}"
    return True, f"{len(df)} righe, period_label formato daily (es. {df['period_label'].iloc[0]})"


def check_c14_andamento_weekly(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """C14 — periodo > 90 giorni: period_label in formato settimanale YYYY-Www."""
    ps, pe = WEEKLY_PERIOD
    df = run_chart_andamento(conn, ps, pe, None, None)
    if df.empty:
        return False, f"DataFrame vuoto su periodo {ps}->{pe} (atteso almeno 1 riga)"
    bad = [label for label in df["period_label"]
           if not RE_PERIOD_LABEL_WEEKLY.match(str(label))]
    if bad:
        return False, f"period_label non weekly: {bad[:3]}"
    return True, f"{len(df)} righe, period_label formato weekly (es. {df['period_label'].iloc[0]})"


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #

CHECKS: List[Tuple[str, Callable[[sqlite3.Connection], Tuple[bool, str]]]] = [
    ("C1 — KPI 1 = 470 (periodo intero, no filtri)",     check_c1_kpi1_full),
    ("C2 — KPI 4 = 200 (periodo intero, no filtri)",     check_c2_kpi4_full),
    ("C3 — schema dict big number conforme §4.1",        check_c3_big_number_schema),
    ("C4 — schema DataFrame KPI 5 + grafici §5",         check_c4_dataframe_schema),
    ("C5 — coerenza KPI 1 / denominatore KPI 6",         check_c5_kpi1_kpi6_coerenza),
    ("C6 — KPI 6 in [0,1]",                              check_c6_kpi6_range),
    ("C7 — KPI 7 in [0,1]",                              check_c7_kpi7_range),
    ("C8 — filtro categoria restringe KPI 1",            check_c8_filtro_restringe),
    ("C9 — mean/ratio con filtro popolato != None",      check_c9_mean_ratio_non_none),
    ("C10 — count/sum su set vuoto -> value=0",          check_c10_count_sum_empty),
    ("C11 — mean/ratio su set vuoto -> value=None+reason", check_c11_mean_ratio_empty),
    ("C12 — KPI 5 + grafici su set vuoto: schema intatto", check_c12_dataframe_empty_schema),
    ("C13 — andamento <=90gg: period_label daily",       check_c13_andamento_daily),
    ("C14 — andamento >90gg: period_label weekly",       check_c14_andamento_weekly),
]


def main() -> int:
    print(f"============ VERIFY QUERIES START ({DWH_PATH.name}) ============")
    if not DWH_PATH.exists():
        print(f"ABORT: DWH non trovato a {DWH_PATH}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(DWH_PATH)
    try:
        n_passed = 0
        for label, fn in CHECKS:
            ok, message = fn(conn)
            status = "PASS" if ok else "FAIL"
            print(f"[{status}] {label}: {message}")
            if ok:
                n_passed += 1
    finally:
        conn.close()

    total = len(CHECKS)
    print(f"============ VERIFY QUERIES END ({n_passed}/{total} check passati) ============")
    return 0 if n_passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
