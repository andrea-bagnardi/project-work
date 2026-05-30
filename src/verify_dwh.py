"""Verifica strutturale del Data Warehouse "Guida e Vai".

Esegue 8 check post-load contro ``data/dwh.db``:
    1. Conteggi riga delle 5 tabelle (time_dim=730, operator_dim=18,
       supplier_dim=7, customer_dim=220, request_fact=470).
    2. Integrità FK di request_fact verso le 4 dimensioni (nessun orfano).
    3. SCD2 self-check: customer_dim ha esattamente 200 righe is_current=1 su 220
       e ogni fiscal_code_hash ha una sola versione corrente.
    4. Monotonia di time_dim: 730 date contigue, no gap, no duplicati.
    5. hash_record univoco in request_fact (470/470).
    6. Presenza dei 7 indici idx_* attesi.
    7. PRAGMA integrity_check restituisce 'ok'.
    8. Distribuzione status entro le tolleranze attese.

Uso::

    python src/verify_dwh.py

Exit code 0 se tutti gli 8 check passano, 1 altrimenti.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from typing import Callable, List, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
DWH_PATH = BASE_DIR / "data" / "dwh.db"

# --------------------------------------------------------------------------- #
# Valori attesi
# --------------------------------------------------------------------------- #

EXPECTED_ROW_COUNTS = {
    "time_dim": 730,
    "operator_dim": 18,
    "supplier_dim": 7,
    "customer_dim": 220,
    "request_fact": 470,
}

EXPECTED_INDEXES = frozenset({
    "idx_request_fact_customer",
    "idx_request_fact_date",
    "idx_request_fact_status",
    "idx_customer_dim_fiscal",
    "idx_customer_dim_validity",
    "idx_customer_dim_province",   # filtro what-if zona
    "idx_supplier_dim_category",   # filtro what-if categoria
})

# Tolleranze sui ratio di stato: ±5pp su invoiced/cancelled,
# ±2pp sugli altri tre. Centri attesi (sui 470 totali): ~80% invoiced, ~5%
# completed, ~2% confirmed, ~1% booked, ~11% cancelled.
EXPECTED_STATUS_DISTRIBUTION = {
    "invoiced":  (0.80, 0.05),
    "completed": (0.05, 0.02),
    "confirmed": (0.02, 0.02),
    "booked":    (0.01, 0.02),
    "cancelled": (0.11, 0.05),
}


# --------------------------------------------------------------------------- #
# Check singoli (ognuno ritorna (ok, messaggio))
# --------------------------------------------------------------------------- #

def check_row_counts(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """Check 1 — conteggi delle 5 tabelle."""
    actual = {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in EXPECTED_ROW_COUNTS
    }
    mismatches = [
        f"{t}: atteso {EXPECTED_ROW_COUNTS[t]}, ottenuto {actual[t]}"
        for t in EXPECTED_ROW_COUNTS if actual[t] != EXPECTED_ROW_COUNTS[t]
    ]
    if mismatches:
        return False, "righe inattese -> " + "; ".join(mismatches)
    summary = ", ".join(f"{t}={actual[t]}" for t in EXPECTED_ROW_COUNTS)
    return True, summary


def check_fk_integrity(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """Check 2 — nessuna riga di request_fact con FK orfana sulle 4 dimensioni."""
    orphan_checks = {
        "customer_key": "customer_dim",
        "supplier_key": "supplier_dim",
        "operator_key": "operator_dim",
        "date_key":     "time_dim",
    }
    orphans = {}
    for fk_col, dim_table in orphan_checks.items():
        n = conn.execute(
            f"SELECT COUNT(*) FROM request_fact rf "
            f"WHERE rf.{fk_col} NOT IN (SELECT {fk_col} FROM {dim_table})"
        ).fetchone()[0]
        if n:
            orphans[fk_col] = n
    if orphans:
        return False, "FK orfane -> " + "; ".join(f"{k}: {v}" for k, v in orphans.items())
    return True, "tutte le FK di request_fact risolvono nelle 4 dimensioni"


def check_scd2(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """Check 3 — customer_dim: 200 versioni correnti, una sola per fiscal_code_hash."""
    n_current = conn.execute(
        "SELECT COUNT(*) FROM customer_dim WHERE is_current = 1"
    ).fetchone()[0]
    n_personas = conn.execute(
        "SELECT COUNT(DISTINCT fiscal_code_hash) FROM customer_dim"
    ).fetchone()[0]
    multi_current = conn.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT fiscal_code_hash FROM customer_dim "
        "  WHERE is_current = 1 GROUP BY fiscal_code_hash HAVING COUNT(*) > 1"
        ")"
    ).fetchone()[0]
    if n_current != 200:
        return False, f"is_current=1 atteso 200, ottenuto {n_current}"
    if n_personas != 200:
        return False, f"fiscal_code_hash distinti attesi 200, ottenuti {n_personas}"
    if multi_current:
        return False, f"{multi_current} fiscal_code_hash con piu' di una versione corrente"
    return True, "200 persone, 200 versioni correnti (1:1), no duplicati su is_current"


def check_time_dim_monotonia(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """Check 4 — time_dim: 730 date contigue, no gap, no duplicati."""
    n_rows, n_distinct = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT full_date) FROM time_dim"
    ).fetchone()
    if n_rows != n_distinct:
        return False, f"duplicati su full_date ({n_rows} righe, {n_distinct} distinti)"
    span_days = conn.execute(
        "SELECT julianday(MAX(full_date)) - julianday(MIN(full_date)) + 1 FROM time_dim"
    ).fetchone()[0]
    if int(span_days) != n_rows:
        return False, f"gap nel calendario: {n_rows} righe, span {int(span_days)} giorni"
    bounds = conn.execute(
        "SELECT MIN(full_date), MAX(full_date) FROM time_dim"
    ).fetchone()
    return True, f"{n_rows} date contigue da {bounds[0]} a {bounds[1]}"


def check_hash_univoci(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """Check 5 — request_fact: hash_record univoco su tutte le 470 righe."""
    n_total, n_distinct = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT hash_record) FROM request_fact"
    ).fetchone()
    if n_total != n_distinct:
        return False, f"hash non univoci: {n_distinct}/{n_total} distinti"
    return True, f"hash_record univoco su {n_total}/{n_total} righe"


def check_indexes(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """Check 6 — i 7 indici idx_* attesi sono presenti."""
    actual = frozenset(
        row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='index' AND name LIKE 'idx_%'"
        )
    )
    missing = EXPECTED_INDEXES - actual
    extra = actual - EXPECTED_INDEXES
    if missing or extra:
        parts = []
        if missing:
            parts.append("mancanti: " + ", ".join(sorted(missing)))
        if extra:
            parts.append("inattesi: " + ", ".join(sorted(extra)))
        return False, "; ".join(parts)
    return True, f"7/7 indici attesi presenti"


def check_integrity_pragma(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """Check 7 — PRAGMA integrity_check restituisce 'ok'."""
    result = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if result != "ok":
        return False, f"PRAGMA integrity_check -> {result}"
    return True, "PRAGMA integrity_check -> ok"


def check_status_distribution(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """Check 8 — distribuzione status entro tolleranze."""
    total = conn.execute("SELECT COUNT(*) FROM request_fact").fetchone()[0]
    if total == 0:
        return False, "request_fact vuoto"
    actual = {
        row[0]: row[1] / total
        for row in conn.execute(
            "SELECT status, COUNT(*) FROM request_fact GROUP BY status"
        )
    }
    out_of_range = []
    summary_parts = []
    for status, (target, tol) in EXPECTED_STATUS_DISTRIBUTION.items():
        observed = actual.get(status, 0.0)
        summary_parts.append(f"{status}={observed * 100:.1f}%")
        if abs(observed - target) > tol:
            out_of_range.append(
                f"{status}: {observed * 100:.1f}% (atteso {target * 100:.0f}% +- {tol * 100:.0f}pp)"
            )
    if out_of_range:
        return False, "fuori tolleranza -> " + "; ".join(out_of_range)
    return True, ", ".join(summary_parts)


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #

CHECKS: List[Tuple[str, Callable[[sqlite3.Connection], Tuple[bool, str]]]] = [
    ("check_row_counts",          check_row_counts),
    ("check_fk_integrity",        check_fk_integrity),
    ("check_scd2",                check_scd2),
    ("check_time_dim_monotonia",  check_time_dim_monotonia),
    ("check_hash_univoci",        check_hash_univoci),
    ("check_indexes",             check_indexes),
    ("check_integrity_pragma",    check_integrity_pragma),
    ("check_status_distribution", check_status_distribution),
]


def main() -> int:
    print(f"============ VERIFY DWH START ({DWH_PATH.name}) ============")
    if not DWH_PATH.exists():
        print(f"ABORT: DWH non trovato a {DWH_PATH}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(DWH_PATH)
    try:
        n_passed = 0
        for name, fn in CHECKS:
            ok, message = fn(conn)
            status = "PASS" if ok else "FAIL"
            print(f"[{status}] {name}: {message}")
            if ok:
                n_passed += 1
    finally:
        conn.close()

    total = len(CHECKS)
    print(f"============ VERIFY DWH END ({n_passed}/{total} check passati) ============")
    return 0 if n_passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
