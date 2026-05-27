"""Pipeline ETL di "Guida e Vai": dai 4 CSV sorgente al Data Warehouse SQLite.

Trasforma i CSV in ``data/csv/`` (customers, suppliers, operators, requests)
nel modello dimensionale a stella: 1 fact (``request_fact``) + 4 dimensioni
(``customer_dim`` SCD2, ``supplier_dim`` e ``operator_dim`` SCD1, ``time_dim``
da calendario). Output: il file ``data/dwh.db``; il report di esecuzione è
stampato a stdout.

Architettura e pattern principali:
  - ``ETLPipeline``     -> pattern Template Method: run() orchestra
                          _extract -> _transform -> _load in sequenza fissa.
  - ``RequestStateMachine`` -> pattern State table-driven:
                          usata in modalità validazione delle traiettorie.
  - connessione SQLite unica creata in __init__ e chiusa in run().

Modalità di caricamento: REBUILD (DROP + CREATE + INSERT), idempotente per
costruzione a partire dai CSV. L'hash SHA-256 (``hash_record``) è conservato
come attributo della fact per tracciabilità/deduplica.
"""

from __future__ import annotations

import hashlib
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# --------------------------------------------------------------------------- #
# Costanti di configurazione
# --------------------------------------------------------------------------- #

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_DIR = BASE_DIR / "data" / "csv"
DWH_PATH = BASE_DIR / "data" / "dwh.db"

# "Presente" del progetto: nessuna Request può essere successiva.
CUTOFF = pd.Timestamp("2026-05-22 23:59:59")

# Orizzonte di time_dim: copre tutte le Request (2025-2026).
TIME_DIM_START = "2025-01-01"
TIME_DIM_END = "2026-12-31"

# Soglia di sicurezza: oltre il 5% di righe scartate su una tabella, abortire.
DISCARD_THRESHOLD = 0.05

# Enum ammessi.
CUSTOMER_STATUSES = frozenset({"active", "historicized"})
SUPPLIER_CATEGORIES = frozenset(
    {"driving_school", "motorization", "renewal_agency", "medical_center"}
)
OPERATOR_ACTIVITY_TYPES = frozenset(
    {"front_office", "back_office", "instructor", "examiner", "administrative"}
)
OPERATOR_STATUSES = frozenset({"active", "terminated"})
REQUEST_STATUSES = frozenset(
    {"booked", "confirmed", "completed", "invoiced", "cancelled"}
)

# Mappature italiane esplicite (indipendenti dal locale di sistema).
MONTH_NAMES_IT = {
    1: "gennaio", 2: "febbraio", 3: "marzo", 4: "aprile", 5: "maggio",
    6: "giugno", 7: "luglio", 8: "agosto", 9: "settembre", 10: "ottobre",
    11: "novembre", 12: "dicembre",
}
# Chiave 0=lunedi ... 6=domenica (convenzione di pandas .dayofweek).
DAY_NAMES_IT = {
    0: "lunedì", 1: "martedì", 2: "mercoledì", 3: "giovedì",
    4: "venerdì", 5: "sabato", 6: "domenica",
}

# Festività civili italiane a data fissa, chiave 'MM-DD'.
FIXED_HOLIDAYS_IT = {
    "01-01": "Capodanno",
    "04-25": "Festa della Liberazione",
    "05-01": "Festa del Lavoro",
    "06-02": "Festa della Repubblica",
    "08-15": "Ferragosto",
    "11-01": "Ognissanti",
    "12-08": "Immacolata Concezione",
    "12-25": "Natale",
    "12-26": "Santo Stefano",
}
# Pasqua (variabile) per il range 2025-2026, valorizzata a mano.
EASTER_DATES_IT = {
    "2025-04-20": "Pasqua",
    "2026-04-05": "Pasqua",
}


# --------------------------------------------------------------------------- #
# Report di esecuzione
# --------------------------------------------------------------------------- #

@dataclass
class ETLReport:
    """Sintesi numerica dell'esecuzione, restituita da ``ETLPipeline.run``."""

    rows_read: Dict[str, int] = field(default_factory=dict)
    rows_discarded: Dict[str, int] = field(default_factory=dict)
    rows_inserted: Dict[str, int] = field(default_factory=dict)
    duration_seconds: float = 0.0


# --------------------------------------------------------------------------- #
# Pattern State: macchina a stati della Request
# --------------------------------------------------------------------------- #

State = str  # 'booked' | 'confirmed' | 'completed' | 'invoiced' | 'cancelled'
Event = str  # 'create' | 'confirm' | 'complete' | 'invoice' | 'cancel'


class RequestStateMachine:
    """Macchina a stati table-driven della Request (Pattern State).

    Le 6 transizioni sono codificate in una tabella statica ``TRANSITIONS``.
    Nella pipeline è usata in "modalità validazione": per ogni Request del CSV
    si verifica che lo stato dichiarato sia raggiungibile dallo stato iniziale.
    """

    TRANSITIONS: Dict[Tuple[State, Event], State] = {
        ("initial", "create"): "booked",
        ("booked", "confirm"): "confirmed",
        ("confirmed", "complete"): "completed",
        ("completed", "invoice"): "invoiced",
        ("booked", "cancel"): "cancelled",
        ("confirmed", "cancel"): "cancelled",
    }

    TERMINAL_STATES = frozenset({"invoiced", "cancelled"})

    def __init__(self, initial_state: State = "initial") -> None:
        self._state = initial_state

    @property
    def state(self) -> State:
        return self._state

    def transition(self, event: Event) -> bool:
        """Esegue la transizione; ritorna True se valida, False altrimenti."""
        key = (self._state, event)
        if key not in self.TRANSITIONS:
            return False
        self._state = self.TRANSITIONS[key]
        return True

    def is_terminal(self) -> bool:
        return self._state in self.TERMINAL_STATES

    @classmethod
    def validate_trajectory(
        cls, target_state: State, cancel_from: Optional[State] = None
    ) -> bool:
        """Verifica che ``target_state`` sia raggiungibile da 'initial'.

        Applica la sequenza minima di transizioni del percorso completo
        (booked -> confirmed -> completed -> invoiced) oppure di cancellazione.
        Per 'cancelled', ``cancel_from`` indica lo stato di partenza
        (default 'booked', il caso modale).
        """
        sm = cls()

        # Ogni Request nasce con create -> booked.
        if not sm.transition("create"):
            return False

        if target_state == "booked":
            return sm.state == "booked"

        if target_state == "cancelled":
            from_state = cancel_from or "booked"
            if from_state == "confirmed" and not sm.transition("confirm"):
                return False
            return sm.transition("cancel") and sm.state == "cancelled"

        # Percorso completo: confirmed -> completed -> invoiced.
        if not sm.transition("confirm"):
            return False
        if target_state == "confirmed":
            return sm.state == "confirmed"

        if not sm.transition("complete"):
            return False
        if target_state == "completed":
            return sm.state == "completed"

        if not sm.transition("invoice"):
            return False
        return sm.state == "invoiced" and target_state == "invoiced"


# --------------------------------------------------------------------------- #
# Helper: hashing
# --------------------------------------------------------------------------- #

def compute_hash_record(*fields: object) -> str:
    """SHA-256 (hex lowercase) dei campi naturali concatenati con '|'.

    Ogni campo viene convertito a stringa; i valori NULL/NaN/NaT diventano
    stringa vuota, così l'hash è stabile e riproducibile.
    """
    parts: List[str] = []
    for value in fields:
        try:
            is_null = pd.isna(value)
        except (TypeError, ValueError):
            is_null = False
        parts.append("" if is_null else str(value))
    payload = "|".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Helper: validazione "alla porta"
# --------------------------------------------------------------------------- #

def _split_valid(
    df: pd.DataFrame, reasons: pd.Series, table: str, key_col: str
) -> Tuple[pd.DataFrame, int]:
    """Separa righe valide/scartate in base a ``reasons`` (stringa vuota = OK).

    Stampa ogni scarto con chiave naturale e motivo, e ritorna
    (df_valide, n_scartate).
    """
    ko_mask = reasons != ""
    for idx in df.index[ko_mask]:
        print(f"  [scarto] {table}: {key_col}={df.at[idx, key_col]}: {reasons.at[idx]}")
    return df[~ko_mask].copy(), int(ko_mask.sum())


def validate_customers(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """Check sui Customer."""
    reasons = pd.Series("", index=df.index, dtype="object")

    def mark(mask: pd.Series, reason: str) -> None:
        applicable = mask.fillna(False) & (reasons == "")
        reasons[applicable] = reason

    mark(df["id"].duplicated(keep=False), "id duplicato")
    mark(~df["fiscal_code_hash"].str.match(r"^[0-9a-f]{64}$", na=False),
         "fiscal_code_hash non valido")
    mark(~df["status"].isin(CUSTOMER_STATUSES), "status non ammesso")
    mark(~df["has_marketing_consent"].isin([0, 1]),
         "has_marketing_consent non in {0,1}")
    mark(df["birth_date"] > df["registration_datetime"],
         "birth_date > registration_datetime")

    # Esattamente 1 riga active per fiscal_code_hash.
    active_per_fch = (
        df[df["status"] == "active"].groupby("fiscal_code_hash").size()
    )
    bad_fchs = {
        fch for fch in df["fiscal_code_hash"].unique()
        if active_per_fch.get(fch, 0) != 1
    }
    mark(df["fiscal_code_hash"].isin(bad_fchs),
         "righe status=active != 1 per fiscal_code_hash")

    return _split_valid(df, reasons, "customers", "id")


def validate_suppliers(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """Check sui Supplier."""
    reasons = pd.Series("", index=df.index, dtype="object")

    def mark(mask: pd.Series, reason: str) -> None:
        applicable = mask.fillna(False) & (reasons == "")
        reasons[applicable] = reason

    mark(df["id"].duplicated(keep=False), "id duplicato")
    mark(~df["category"].isin(SUPPLIER_CATEGORIES), "category non ammessa")
    mark(~df["is_active"].isin([0, 1]), "is_active non in {0,1}")

    return _split_valid(df, reasons, "suppliers", "id")


def validate_operators(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """Check sugli Operator."""
    reasons = pd.Series("", index=df.index, dtype="object")

    def mark(mask: pd.Series, reason: str) -> None:
        applicable = mask.fillna(False) & (reasons == "")
        reasons[applicable] = reason

    mark(df["id"].duplicated(keep=False), "id duplicato")
    mark(~df["activity_type"].isin(OPERATOR_ACTIVITY_TYPES),
         "activity_type non ammesso")
    mark(~df["status"].isin(OPERATOR_STATUSES), "status non ammesso")
    mark(df["hire_date"].isna(), "hire_date mancante")

    return _split_valid(df, reasons, "operators", "id")


def validate_requests(
    df: pd.DataFrame,
    customers_ok: pd.DataFrame,
    suppliers_ok: pd.DataFrame,
    operators_ok: pd.DataFrame,
) -> Tuple[pd.DataFrame, int]:
    """Check sulle Request."""
    reasons = pd.Series("", index=df.index, dtype="object")

    def mark(mask: pd.Series, reason: str) -> None:
        applicable = mask.fillna(False) & (reasons == "")
        reasons[applicable] = reason

    valid_customer_ids = set(customers_ok["id"].dropna())
    valid_supplier_ids = set(suppliers_ok["id"].dropna())
    valid_operator_ids = set(operators_ok["id"].dropna())

    mark(df["id"].duplicated(keep=False), "id duplicato")
    mark(~df["customer_id"].isin(valid_customer_ids), "FK customer_id non trovata")
    mark(~df["supplier_id"].isin(valid_supplier_ids), "FK supplier_id non trovata")
    mark(~df["operator_id"].isin(valid_operator_ids), "FK operator_id non trovata")
    mark(~df["status"].isin(REQUEST_STATUSES), "status non ammesso")
    mark(~(df["price"] > 0), "price non positivo")

    # Coerenza stati <-> campi.
    status = df["status"]
    completed_null = df["completed_datetime"].isna()
    duration_null = df["service_duration_minutes"].isna()
    notes_null = df["notes"].isna()
    done = status.isin(["completed", "invoiced"])
    pending = status.isin(["booked", "confirmed"])
    mark(done & (completed_null | duration_null),
         "stato completato ma completed_datetime/durata mancante")
    mark(pending & (~completed_null | ~duration_null),
         "stato pendente ma completed_datetime/durata valorizzati")
    mark((status == "cancelled") & (~completed_null | ~duration_null | notes_null),
         "cancellata con campi incoerenti o senza motivo")

    # Coerenza temporale.
    mark((~completed_null) & (df["completed_datetime"] < df["request_datetime"]),
         "completed_datetime < request_datetime")
    mark(df["request_datetime"] > CUTOFF, "request_datetime oltre il cutoff")

    # Request successiva alla prima registrazione della persona.
    id_to_fch = customers_ok.set_index("id")["fiscal_code_hash"]
    fch_min_reg = customers_ok.groupby("fiscal_code_hash")["registration_datetime"].min()
    req_fch = df["customer_id"].map(id_to_fch)
    req_min_reg = req_fch.map(fch_min_reg)
    mark(req_min_reg.notna() & (df["request_datetime"] <= req_min_reg),
         "request_datetime <= registration_datetime del Customer")

    return _split_valid(df, reasons, "requests", "id")


# --------------------------------------------------------------------------- #
# Helper: costruzione delle dimensioni
# --------------------------------------------------------------------------- #

def build_customer_dim_scd2(customers_df: pd.DataFrame) -> pd.DataFrame:
    """Costruisce customer_dim con SCD Type 2, generalizzata per N versioni.

    Per ogni gruppo (fiscal_code_hash), ordinato per registration_datetime:
      - valid_from = registration_datetime della versione i-esima;
      - valid_to   = registration_datetime della versione i+1 (NULL se ultima);
      - is_current = 1 solo per l'ultima versione.
    La PK surrogata customer_key è assegnata da SQLite (AUTOINCREMENT) all'INSERT.
    """
    rows: List[dict] = []
    for fch, group in customers_df.groupby("fiscal_code_hash", sort=False):
        versions = group.sort_values("registration_datetime").to_dict("records")
        n = len(versions)
        for i, v in enumerate(versions):
            is_current = 1 if i == n - 1 else 0
            valid_to = versions[i + 1]["registration_datetime"] if i < n - 1 else None
            # Coerenza CSV<->SCD2: la versione corrente deve essere quella 'active'.
            if (is_current == 1) != (v["status"] == "active"):
                print(f"  [avviso] customer_dim: incoerenza SCD2 per {fch} "
                      f"(is_current={is_current}, status={v['status']})")
            rows.append({
                "customer_id": v["id"],
                "last_name": v["last_name"],
                "first_name": v["first_name"],
                "fiscal_code_hash": fch,
                "birth_date": v["birth_date"],
                "email": v["email"],
                "city": v["city"],
                "province": v["province"],
                "zip_code": v["zip_code"],
                "has_marketing_consent": v["has_marketing_consent"],
                "registration_datetime": v["registration_datetime"],
                "valid_from": v["registration_datetime"],
                "valid_to": valid_to,
                "is_current": is_current,
            })
    return pd.DataFrame(rows)


def build_supplier_dim(suppliers_df: pd.DataFrame) -> pd.DataFrame:
    """SCD1: rimappatura 1:1 dal CSV.
    La PK surrogata supplier_key è assegnata da SQLite (AUTOINCREMENT) all'INSERT."""
    return suppliers_df.rename(columns={"id": "supplier_id"})[
        ["supplier_id", "name", "category", "vat_number",
         "city", "province", "zip_code", "is_active"]
    ].copy()


def build_operator_dim(operators_df: pd.DataFrame) -> pd.DataFrame:
    """SCD1: rimappatura 1:1 dal CSV.
    La PK surrogata operator_key è assegnata da SQLite (AUTOINCREMENT) all'INSERT."""
    out = operators_df.rename(columns={"id": "operator_id"}).copy()
    out["hire_date"] = out["hire_date"].dt.strftime("%Y-%m-%d")
    return out[
        ["operator_id", "last_name", "first_name", "email", "activity_type",
         "city", "province", "hire_date", "status"]
    ]


def build_time_dim(start: str = TIME_DIM_START, end: str = TIME_DIM_END) -> pd.DataFrame:
    """Genera time_dim da calendario.

    PK ``date_key`` in formato intero YYYYMMDD; ``full_date`` come TEXT ISO.
    """
    dates = pd.date_range(start=start, end=end, freq="D")
    full_date = dates.strftime("%Y-%m-%d")

    holiday_name: List[Optional[str]] = []
    is_holiday: List[int] = []
    for fd in full_date:
        name = EASTER_DATES_IT.get(fd) or FIXED_HOLIDAYS_IT.get(fd[5:])
        holiday_name.append(name)
        is_holiday.append(1 if name else 0)

    return pd.DataFrame({
        "date_key": [int(d.strftime("%Y%m%d")) for d in dates],
        "full_date": full_date,
        "day": dates.day,
        "month": dates.month,
        "month_name_it": [MONTH_NAMES_IT[m] for m in dates.month],
        "quarter": dates.quarter,
        "year": dates.year,
        "day_of_week": dates.dayofweek + 1,  # 1=lunedi ... 7=domenica
        "day_name_it": [DAY_NAMES_IT[w] for w in dates.dayofweek],
        "is_weekend": (dates.dayofweek >= 5).astype(int),
        "is_holiday_it": is_holiday,
        "holiday_name_it": holiday_name,
        "is_summer": dates.month.isin([7, 8, 9]).astype(int),  # luglio-settembre
    })


# --------------------------------------------------------------------------- #
# Helper: lookup per la fact
# --------------------------------------------------------------------------- #

def lookup_customer_key(
    customer_dim: pd.DataFrame, fiscal_code_hash: str, at_date: pd.Timestamp
) -> int:
    """Lookup SCD2 by-date: customer_key della versione valida a ``at_date``.

    Regola: valid_from <= at_date AND (valid_to IS NULL OR at_date < valid_to).
    """
    candidates = customer_dim[
        (customer_dim["fiscal_code_hash"] == fiscal_code_hash)
        & (customer_dim["valid_from"] <= at_date)
        & (customer_dim["valid_to"].isna() | (at_date < customer_dim["valid_to"]))
    ]
    if len(candidates) != 1:
        raise ValueError(
            f"lookup SCD2 inconsistente per {fiscal_code_hash} a {at_date}: "
            f"trovate {len(candidates)} versioni, attesa 1."
        )
    return int(candidates.iloc[0]["customer_key"])


# --------------------------------------------------------------------------- #
# DDL del DWH
# --------------------------------------------------------------------------- #

DDL_STATEMENTS = [
    """
    CREATE TABLE time_dim (
        date_key        INTEGER PRIMARY KEY,
        full_date       TEXT    NOT NULL UNIQUE,
        day             INTEGER NOT NULL,
        month           INTEGER NOT NULL,
        month_name_it   TEXT    NOT NULL,
        quarter         INTEGER NOT NULL,
        year            INTEGER NOT NULL,
        day_of_week     INTEGER NOT NULL,
        day_name_it     TEXT    NOT NULL,
        is_weekend      INTEGER NOT NULL,
        is_holiday_it   INTEGER NOT NULL,
        holiday_name_it TEXT,
        is_summer       INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE customer_dim (
        customer_key          INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id           INTEGER NOT NULL,
        last_name             TEXT    NOT NULL,
        first_name            TEXT    NOT NULL,
        fiscal_code_hash      TEXT    NOT NULL,
        birth_date            TEXT    NOT NULL,
        email                 TEXT,
        city                  TEXT    NOT NULL,
        province              TEXT    NOT NULL,
        zip_code              TEXT    NOT NULL,
        has_marketing_consent INTEGER NOT NULL,
        registration_datetime TEXT    NOT NULL,
        valid_from            TEXT    NOT NULL,
        valid_to              TEXT,
        is_current            INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE supplier_dim (
        supplier_key INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier_id  INTEGER NOT NULL,
        name         TEXT    NOT NULL,
        category     TEXT    NOT NULL,
        vat_number   TEXT    NOT NULL,
        city         TEXT    NOT NULL,
        province     TEXT    NOT NULL,
        zip_code     TEXT    NOT NULL,
        is_active    INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE operator_dim (
        operator_key  INTEGER PRIMARY KEY AUTOINCREMENT,
        operator_id   INTEGER NOT NULL,
        last_name     TEXT    NOT NULL,
        first_name    TEXT    NOT NULL,
        email         TEXT,
        activity_type TEXT    NOT NULL,
        city          TEXT    NOT NULL,
        province      TEXT    NOT NULL,
        hire_date     TEXT    NOT NULL,
        status        TEXT    NOT NULL
    )
    """,
    """
    CREATE TABLE request_fact (
        request_id               INTEGER PRIMARY KEY,
        customer_key             INTEGER NOT NULL,
        supplier_key             INTEGER NOT NULL,
        operator_key             INTEGER NOT NULL,
        date_key                 INTEGER NOT NULL,
        price                    REAL    NOT NULL,
        service_duration_minutes INTEGER,
        is_completed             INTEGER NOT NULL,
        is_cancelled             INTEGER NOT NULL,
        status                   TEXT    NOT NULL,
        hash_record              TEXT    NOT NULL,
        request_datetime         TEXT    NOT NULL,
        completed_datetime       TEXT,
        notes                    TEXT,
        FOREIGN KEY (customer_key) REFERENCES customer_dim (customer_key),
        FOREIGN KEY (supplier_key) REFERENCES supplier_dim (supplier_key),
        FOREIGN KEY (operator_key) REFERENCES operator_dim (operator_key),
        FOREIGN KEY (date_key)     REFERENCES time_dim (date_key)
    )
    """,
]

INDEX_STATEMENTS = [
    "CREATE INDEX idx_request_fact_customer ON request_fact (customer_key)",
    "CREATE INDEX idx_request_fact_date ON request_fact (date_key)",
    "CREATE INDEX idx_request_fact_status ON request_fact (status)",
    "CREATE INDEX idx_customer_dim_fiscal ON customer_dim (fiscal_code_hash, is_current)",
    "CREATE INDEX idx_customer_dim_validity "
    "ON customer_dim (fiscal_code_hash, valid_from, valid_to)",
    # idx_customer_dim_province — accelera filtro what-if zona (queries_specs §9.2, figura5 §8)
    "CREATE INDEX IF NOT EXISTS idx_customer_dim_province "
    "ON customer_dim (province, is_current)",
    # idx_supplier_dim_category — accelera filtro what-if categoria (queries_specs §9.2, figura5 §8)
    "CREATE INDEX IF NOT EXISTS idx_supplier_dim_category ON supplier_dim (category)",
]


def execute_ddl(conn: sqlite3.Connection) -> None:
    """Crea le 5 tabelle e i 7 indici del DWH."""
    cursor = conn.cursor()
    for statement in DDL_STATEMENTS:
        cursor.execute(statement)
    for statement in INDEX_STATEMENTS:
        cursor.execute(statement)
    conn.commit()


# --------------------------------------------------------------------------- #
# Pipeline ETL (pattern Template Method)
# --------------------------------------------------------------------------- #

class ETLPipeline:
    """Pipeline ETL (Template Method).

    ``run`` è il metodo template che esegue in sequenza fissa
    _extract -> _transform -> _load. La connessione SQLite è unica (creata
    qui, chiusa in run con try/finally).
    """

    def __init__(self, csv_dir: Path, dwh_path: Path) -> None:
        self.csv_dir = csv_dir
        self.dwh_path = dwh_path
        self.report = ETLReport()
        dwh_path.parent.mkdir(parents=True, exist_ok=True)
        # REBUILD da zero: rimuovo un eventuale DWH precedente.
        dwh_path.unlink(missing_ok=True)
        self.conn = sqlite3.connect(str(dwh_path))

    # --- entry point -------------------------------------------------------- #

    def run(self) -> ETLReport:
        start = time.perf_counter()
        print("============ ETL PIPELINE START ============")
        print(f"CSV dir: {self.csv_dir}")
        print(f"DWH path: {self.dwh_path}")
        try:
            extracted = self._extract()
            transformed = self._transform(extracted)
            self.report.rows_inserted = self._load(transformed)
            self._print_load_summary()
        finally:
            self.conn.close()

        self.report.duration_seconds = time.perf_counter() - start
        print(f"============ ETL PIPELINE END (durata "
              f"{self.report.duration_seconds:.2f}s) ============")
        return self.report

    # --- Extract ------------------------------------------------------------ #

    def _extract(self) -> Dict[str, pd.DataFrame]:
        print("--- EXTRACT ---")
        # keep_default_na=False + na_values=[""]: solo le stringhe vuote sono NULL.
        # Evita che la sigla provincia "NA" (Napoli) sia letta come valore mancante.
        na_opts = {"keep_default_na": False, "na_values": [""]}
        extracted = {
            "customers": pd.read_csv(
                self.csv_dir / "customers.csv",
                encoding="utf-8-sig",
                parse_dates=["birth_date", "registration_datetime"],
                dtype={"has_marketing_consent": "Int8", "id": "Int64"},
                **na_opts,
            ),
            "suppliers": pd.read_csv(
                self.csv_dir / "suppliers.csv",
                encoding="utf-8-sig",
                dtype={"is_active": "Int8", "id": "Int64"},
                **na_opts,
            ),
            "operators": pd.read_csv(
                self.csv_dir / "operators.csv",
                encoding="utf-8-sig",
                parse_dates=["hire_date"],
                dtype={"id": "Int64"},
                **na_opts,
            ),
            "requests": pd.read_csv(
                self.csv_dir / "requests.csv",
                encoding="utf-8-sig",
                parse_dates=["request_datetime", "completed_datetime"],
                dtype={
                    "id": "Int64",
                    "customer_id": "Int64",
                    "supplier_id": "Int64",
                    "operator_id": "Int64",
                    "price": "float64",
                    "service_duration_minutes": "Int64",
                },
                **na_opts,
            ),
        }
        for name, df in extracted.items():
            self.report.rows_read[name] = len(df)
            print(f"{name}.csv: {len(df)} righe lette")

        req = extracted["requests"]
        print(f"Range temporale Request: {req['request_datetime'].min()} -> "
              f"{req['request_datetime'].max()}")
        return extracted

    # --- Transform ---------------------------------------------------------- #

    def _transform(self, extracted: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        print("--- TRANSFORM ---")

        # 1. Validazione "alla porta".
        customers_ok, c_ko = validate_customers(extracted["customers"])
        self._print_validation("customers", customers_ok, c_ko)
        suppliers_ok, s_ko = validate_suppliers(extracted["suppliers"])
        self._print_validation("suppliers", suppliers_ok, s_ko)
        operators_ok, o_ko = validate_operators(extracted["operators"])
        self._print_validation("operators", operators_ok, o_ko)
        requests_ok, r_ko = validate_requests(
            extracted["requests"], customers_ok, suppliers_ok, operators_ok
        )
        self._print_validation("requests", requests_ok, r_ko)

        # 2-4. Costruzione delle dimensioni.
        customer_dim = build_customer_dim_scd2(customers_ok)
        supplier_dim = build_supplier_dim(suppliers_ok)
        operator_dim = build_operator_dim(operators_ok)
        time_dim = build_time_dim()

        n_personas = customer_dim["fiscal_code_hash"].nunique()
        n_multi = int((customer_dim.groupby("fiscal_code_hash").size() > 1).sum())
        n_current = int(customer_dim["is_current"].sum())
        print(f"customer_dim SCD2: {n_personas} customer-persona, {n_multi} con più "
              f"versioni, {len(customer_dim)} righe totali, {n_current} is_current=1")
        print(f"supplier_dim: {len(supplier_dim)} righe")
        print(f"operator_dim: {len(operator_dim)} righe")
        print(f"time_dim: {len(time_dim)} righe "
              f"({time_dim['full_date'].iloc[0]} -> {time_dim['full_date'].iloc[-1]})")

        # 5. Validazione delle traiettorie (macchina a stati).
        bad_trajectory = requests_ok[
            ~requests_ok["status"].map(RequestStateMachine.validate_trajectory)
        ]
        for idx in bad_trajectory.index:
            print(f"  [scarto] requests: id={requests_ok.at[idx, 'id']}: "
                  f"traiettoria stato '{requests_ok.at[idx, 'status']}' non raggiungibile")
        if len(bad_trajectory):
            requests_ok = requests_ok.drop(index=bad_trajectory.index)
            r_ko += len(bad_trajectory)
            self._check_threshold("requests", self.report.rows_read["requests"], r_ko)
        print(f"Macchina a stati: {len(requests_ok)} traiettorie validate, "
              f"{len(bad_trajectory)} scartate")

        return {
            "customer_dim": customer_dim,
            "supplier_dim": supplier_dim,
            "operator_dim": operator_dim,
            "time_dim": time_dim,
            "requests": requests_ok,
        }

    # --- Load (REBUILD, 2 fasi) -------------------------------------------- #

    def _load(self, transformed: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        print("--- LOAD ---")
        conn = self.conn
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")  # essenziale: integrità FK

        # DROP in ordine inverso alle FK, poi CREATE.
        for table in ["request_fact", "customer_dim", "supplier_dim",
                      "operator_dim", "time_dim"]:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        execute_ddl(conn)

        # Fase 1: caricamento dimensioni (date/datetime serializzati a TEXT ISO).
        customer_dim = self._serialize_customer_dim(transformed["customer_dim"])
        inserted: Dict[str, int] = {}
        for name, df in [
            ("time_dim", transformed["time_dim"]),
            ("operator_dim", transformed["operator_dim"]),
            ("supplier_dim", transformed["supplier_dim"]),
            ("customer_dim", customer_dim),
        ]:
            df.to_sql(name, conn, if_exists="append", index=False,
                      method="multi", chunksize=400)
            inserted[name] = len(df)
        conn.commit()

        # Rilettura per ottenere le PK surrogate AUTOINCREMENT.
        dims = {
            n: pd.read_sql_query(f"SELECT * FROM {n}", conn)
            for n in ["customer_dim", "supplier_dim", "operator_dim"]
        }

        # Fase 2: costruzione e caricamento della fact.
        request_fact = self._build_request_fact(transformed["requests"], dims)
        request_fact.to_sql("request_fact", conn, if_exists="append", index=False,
                            method="multi", chunksize=400)
        inserted["request_fact"] = len(request_fact)
        self._unique_hash_in_fact = (
            int(request_fact["hash_record"].nunique()), len(request_fact)
        )
        conn.commit()

        print("DROP+CREATE+INSERT completato")
        return inserted

    def _build_request_fact(
        self, requests: pd.DataFrame, dims: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """Assembla request_fact risolvendo le 4 FK (lookup SCD2 per il Customer)."""
        customer_dim = dims["customer_dim"].copy()
        customer_dim["valid_from"] = pd.to_datetime(customer_dim["valid_from"])
        customer_dim["valid_to"] = pd.to_datetime(customer_dim["valid_to"])

        id_to_fch = dict(zip(customer_dim["customer_id"], customer_dim["fiscal_code_hash"]))
        supplier_map = dict(zip(dims["supplier_dim"]["supplier_id"],
                                dims["supplier_dim"]["supplier_key"]))
        operator_map = dict(zip(dims["operator_dim"]["operator_id"],
                                dims["operator_dim"]["operator_key"]))

        rows: List[dict] = []
        for r in requests.itertuples(index=False):
            request_dt = r.request_datetime
            request_dt_iso = request_dt.strftime("%Y-%m-%d %H:%M:%S")
            completed_iso = (
                None if pd.isna(r.completed_datetime)
                else r.completed_datetime.strftime("%Y-%m-%d %H:%M:%S")
            )
            duration = (
                None if pd.isna(r.service_duration_minutes)
                else int(r.service_duration_minutes)
            )
            status = r.status
            rows.append({
                "request_id": int(r.id),
                "customer_key": lookup_customer_key(
                    customer_dim, id_to_fch[r.customer_id], request_dt
                ),
                "supplier_key": int(supplier_map[r.supplier_id]),
                "operator_key": int(operator_map[r.operator_id]),
                "date_key": int(request_dt.strftime("%Y%m%d")),
                "price": float(r.price),
                "service_duration_minutes": duration,
                "is_completed": 1 if status in ("completed", "invoiced") else 0,
                "is_cancelled": 1 if status == "cancelled" else 0,
                "status": status,
                "hash_record": compute_hash_record(
                    r.customer_id, r.supplier_id, r.operator_id,
                    request_dt_iso, f"{float(r.price):.2f}",
                ),
                "request_datetime": request_dt_iso,
                "completed_datetime": completed_iso,
                "notes": None if pd.isna(r.notes) else str(r.notes),
            })

        columns = [
            "request_id", "customer_key", "supplier_key", "operator_key", "date_key",
            "price", "service_duration_minutes", "is_completed", "is_cancelled",
            "status", "hash_record", "request_datetime", "completed_datetime", "notes",
        ]
        return pd.DataFrame(rows, columns=columns)

    @staticmethod
    def _serialize_customer_dim(customer_dim: pd.DataFrame) -> pd.DataFrame:
        """Serializza i campi data/datetime di customer_dim a TEXT ISO per SQLite."""
        out = customer_dim.copy()
        out["birth_date"] = pd.to_datetime(out["birth_date"]).dt.strftime("%Y-%m-%d")
        for col in ("registration_datetime", "valid_from", "valid_to"):
            series = pd.to_datetime(out[col])
            out[col] = series.dt.strftime("%Y-%m-%d %H:%M:%S").where(series.notna(), None)
        return out

    # --- utility di stampa/sicurezza --------------------------------------- #

    def _print_validation(self, table: str, df_ok: pd.DataFrame, n_ko: int) -> None:
        total = self.report.rows_read[table]
        self.report.rows_discarded[table] = n_ko
        print(f"Validazione {table}: {len(df_ok)} OK / {n_ko} KO")
        self._check_threshold(table, total, n_ko)

    def _check_threshold(self, table: str, total: int, n_ko: int) -> None:
        if total > 0 and n_ko / total > DISCARD_THRESHOLD:
            print(f"ABORT: {table} ha {n_ko}/{total} righe scartate "
                  f"(> {DISCARD_THRESHOLD * 100:.0f}%), DWH non costruito", file=sys.stderr)
            self.conn.close()
            sys.exit(1)

    def _print_load_summary(self) -> None:
        ins = self.report.rows_inserted
        print(f"Inserite: time_dim={ins.get('time_dim', 0)}, "
              f"operator_dim={ins.get('operator_dim', 0)}, "
              f"supplier_dim={ins.get('supplier_dim', 0)}, "
              f"customer_dim={ins.get('customer_dim', 0)}, "
              f"request_fact={ins.get('request_fact', 0)}")
        distinct_h, total_h = self._unique_hash_in_fact
        print(f"request_fact: hash univoci {distinct_h}/{total_h}")


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> None:
    ETLPipeline(CSV_DIR, DWH_PATH).run()


if __name__ == "__main__":
    main()
