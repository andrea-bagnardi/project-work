"""Generazione dei dati sintetici di "Guida e Vai".

Produce 4 file CSV in ``data/csv/`` (customers, suppliers, operators, requests).
La generazione è completamente deterministica:
a parità di SEED i 4 file sono identici byte-per-byte.

Ordine di generazione: prima i Customer (le Request hanno FK verso di loro),
poi Supplier e Operator, infine le Request.
"""

from __future__ import annotations

import csv
import hashlib
import random
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

# --------------------------------------------------------------------------- #
# Costanti di configurazione
# --------------------------------------------------------------------------- #

SEED = 20260525  # seed condiviso per tutti i RNG (random, numpy, Faker) per generazione deterministica

# Orizzonte temporale: da gennaio 2025 (prima Request possibile) a maggio 2026
CUTOFF = datetime(2026, 5, 22, 23, 59, 59)   # "presente" del progetto
REQUEST_START = date(2025, 1, 1)             # prima Request possibile
REGISTRATION_START = date(2024, 6, 1)        # prima registrazione Customer
HIRE_START = date(2020, 1, 1)                # prima assunzione Operator

# Output
CSV_DIR = Path(__file__).resolve().parent.parent / "data" / "csv"

# Numerosità per entità: 200 Customer-persona (di cui ~20 con versione storicizzata), 7 Supplier, 18 Operator, ~450 Request.
NUM_CUSTOMERS = 200

# Città italiane con provincia e CAP coerenti.
# I pesi danno bias verso le metropoli (Roma+Milano+Napoli ~30% combinato).
CITIES = [
    ("Roma", "RM", "00185"),
    ("Milano", "MI", "20121"),
    ("Napoli", "NA", "80133"),
    ("Torino", "TO", "10121"),
    ("Bologna", "BO", "40121"),
    ("Firenze", "FI", "50122"),
    ("Genova", "GE", "16121"),
    ("Palermo", "PA", "90133"),
    ("Bari", "BA", "70121"),
    ("Catania", "CT", "95124"),
    ("Verona", "VR", "37121"),
    ("Venezia", "VE", "30121"),
    ("Padova", "PD", "35121"),
    ("Trieste", "TS", "34121"),
    ("Brescia", "BS", "25121"),
]
CITY_WEIGHTS = np.array([9, 8, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 4], dtype=float)
CITY_P = CITY_WEIGHTS / CITY_WEIGHTS.sum()

# Moltiplicatori di stagionalità per mese e giorno della settimana.
MONTH_MULT = {1: 0.7, 2: 0.8, 3: 1.0, 4: 1.0, 5: 1.2, 6: 1.3,
              7: 1.5, 8: 1.4, 9: 1.3, 10: 1.0, 11: 0.9, 12: 0.6}
WEEKDAY_MULT = {0: 1.0, 1: 1.1, 2: 1.1, 3: 1.0, 4: 0.9, 5: 1.3, 6: 0.1}

# Festività italiane
HOLIDAYS = {
    date(2025, 1, 1), date(2025, 4, 20), date(2025, 4, 25), date(2025, 5, 1),
    date(2025, 6, 2), date(2025, 8, 15), date(2025, 11, 1), date(2025, 12, 8),
    date(2025, 12, 25), date(2025, 12, 26),
    date(2026, 1, 1), date(2026, 4, 5), date(2026, 4, 25), date(2026, 5, 1),
    date(2026, 6, 2), date(2026, 8, 15), date(2026, 11, 1), date(2026, 12, 8),
    date(2026, 12, 25), date(2026, 12, 26),
}

# Fasce orarie e relative probabilità
HOUR_BANDS = [(9, 12), (12, 14), (14, 18), (18, 20)]
HOUR_BAND_P = [0.35, 0.05, 0.50, 0.10]

# Configurazione delle 4 categorie di Supplier.
# count = numero di supplier, price = range €, duration = range minuti.
CATEGORY_CONFIG = {
    "driving_school":     {"count": 3, "price": (35.0, 50.0), "duration": (45, 60)},
    "motorization":  {"count": 1, "price": (110.0, 130.0), "duration": (20, 45)},
    "renewal_agency":       {"count": 1, "price": (80.0, 120.0), "duration": (15, 30)},
    "medical_center": {"count": 2, "price": (50.0, 80.0), "duration": (15, 30)},
}
# Modello di nome commerciale per categoria
CATEGORY_NAME = {
    "driving_school": "Autoscuola",
    "motorization": "Motorizzazione Civile",
    "renewal_agency": "Agenzia Pratiche Auto",
    "medical_center": "Centro Medico",
}

# Distribuzione Operator per activity_type
ACTIVITY_COUNTS = {
    "front_office": 5, "instructor": 4, "back_office": 3,
    "examiner": 3, "administrative": 3,
}

# Motivi di cancellazione
CANCEL_REASONS = [
    "cliente impossibilitato",
    "richiesta riprogrammata da cliente",
    "indisponibilita fornitore",
    "errore di prenotazione",
    "documentazione incompleta",
]
GENERIC_NOTE = "richiesta gestita regolarmente"

# Distribuzione a coda lunga delle Request per Customer.
# (numero di customer, (min, max)). Somma = 200 customer.
# 80 customer su 200 hanno >=2 Request, distribuzione utile a dare forma realistica
# al volume di Request e ad alimentare il KPI secondario retention (vedi queries_specs).
RETENTION_BUCKETS = [
    (120, (1, 1)),
    (32, (2, 2)),
    (24, (3, 4)),
    (16, (5, 8)),
    (8, (9, 15)),
]


# --------------------------------------------------------------------------- #
# Funzioni di supporto
# --------------------------------------------------------------------------- #

def _setup_seed() -> Faker:
    """Inizializza i 3 RNG con lo stesso seed e restituisce il generatore Faker."""
    random.seed(SEED)
    np.random.seed(SEED)
    Faker.seed(SEED)
    return Faker("it_IT")


def _slugify(text: str) -> str:
    """Normalizza una stringa per uso in email/dominio (ascii minuscolo)."""
    norm = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return norm.lower().replace(" ", "").replace("'", "").replace(".", "")


def _round_half_euro(value: float) -> float:
    """Arrotonda un prezzo al mezzo euro."""
    return round(value * 2) / 2


def _round_5_minutes(value: float) -> int:
    """Arrotonda una durata a 5 minuti."""
    return int(round(value / 5.0) * 5)


def _fmt_dt(dt: datetime | None) -> str:
    """Formatta un datetime in ISO 8601 'YYYY-MM-DD HH:MM:SS' (vuoto se None)."""
    return "" if dt is None else dt.strftime("%Y-%m-%d %H:%M:%S")


def _fmt_date(d: date) -> str:
    """Formatta una data in ISO 8601 'YYYY-MM-DD'."""
    return d.strftime("%Y-%m-%d")


def _sample_city() -> tuple[str, str, str]:
    """Campiona una città (con provincia e CAP) secondo i pesi metropoli."""
    idx = int(np.random.choice(len(CITIES), p=CITY_P))
    return CITIES[idx]


def _build_day_table() -> tuple[list[date], np.ndarray, np.ndarray]:
    """Precostruisce i giorni candidati [REQUEST_START, cutoff] con i pesi di
    stagionalità (mese × giorno_settimana × festività)."""
    days: list[date] = []
    weights: list[float] = []
    cur = REQUEST_START
    end = CUTOFF.date()
    while cur <= end:
        if cur in HOLIDAYS:
            weight = 0.0  # festività: zero Request
        else:
            weight = MONTH_MULT[cur.month] * WEEKDAY_MULT[cur.weekday()]
        days.append(cur)
        weights.append(weight)
        cur += timedelta(days=1)
    ordinals = np.array([day.toordinal() for day in days])
    return days, np.array(weights, dtype=float), ordinals


def _sample_request_datetime(days, weights, ordinals, not_before: date) -> datetime:
    """Campiona un request_datetime nei giorni >= not_before (la registrazione del
    Customer deve precedere la prima Request), pesato per la
    stagionalità, con ora dalla distribuzione."""
    mask = ordinals >= not_before.toordinal()
    weight = weights * mask
    total = weight.sum()
    if total == 0:
        # Registrazione troppo a ridosso del cutoff: ripiega sull'ultimo giorno utile.
        idx = len(days) - 1
    else:
        idx = int(np.random.choice(len(days), p=weight / total))
    day = days[idx]
    band = HOUR_BANDS[int(np.random.choice(len(HOUR_BANDS), p=HOUR_BAND_P))]
    hour = int(np.random.randint(band[0], band[1]))
    minute = int(np.random.randint(0, 60))
    return datetime(day.year, day.month, day.day, hour, minute, 0)


# --------------------------------------------------------------------------- #
# Generazione delle 4 entità: Customer, Supplier, Operator, Request
# --------------------------------------------------------------------------- #

def generate_customers(fake: Faker) -> pd.DataFrame:
    """Genera i Customer.

    Restituisce ~220 righe: 200 Customer-persona, di cui ~20 (10%) presenti in 2
    versioni (una 'historicized' + una 'active') che condividono lo stesso
    fiscal_code_hash ma differiscono in città e registration_datetime.
    """
    # Personas con storicizzazione (10% = 20 customer)
    n_historicized = int(round(NUM_CUSTOMERS * 0.10))
    historicized_personas = set(
        int(x) for x in np.random.choice(
            np.arange(1, NUM_CUSTOMERS + 1), size=n_historicized, replace=False)
    )

    reg_span_days = (CUTOFF.date() - REGISTRATION_START).days
    rows = []
    next_id = 1

    for persona in range(1, NUM_CUSTOMERS + 1):
        last_name = fake.last_name()
        first_name = fake.first_name()
        fiscal_code_hash = hashlib.sha256(fake.unique.ssn().encode("utf-8")).hexdigest()
        birth = fake.date_of_birth(minimum_age=18, maximum_age=70)
        phone = "" if np.random.random() < 0.03 else f"+393{np.random.randint(0, 10**9):09d}"
        consent = 1 if np.random.random() < 0.60 else 0
        email = f"{_slugify(first_name)}.{_slugify(last_name)}@example.it"
        city, province, zip_code = _sample_city()

        # registration biased verso le date più vecchie (startup in crescita)
        offset = int(np.random.triangular(0, reg_span_days * 0.25, reg_span_days))
        reg_dt = datetime.combine(
            REGISTRATION_START + timedelta(days=offset), datetime.min.time(),
        ) + timedelta(hours=int(np.random.randint(8, 20)),
                      minutes=int(np.random.randint(0, 60)))

        base = {
            "last_name": last_name, "first_name": first_name,
            "fiscal_code_hash": fiscal_code_hash, "birth_date": _fmt_date(birth),
            "email": email, "phone": phone, "city": city, "province": province,
            "zip_code": zip_code, "has_marketing_consent": str(consent),
        }

        if persona in historicized_personas:
            # Versione storicizzata (vecchia): città e registrazione originali.
            rows.append({"id": next_id, "registration_datetime": _fmt_dt(reg_dt),
                         "status": "historicized", **base})
            next_id += 1
            # Versione corrente (active): cambio città e registrazione successiva.
            new_city, new_province, new_zip = _sample_city()
            while new_city == city:
                new_city, new_province, new_zip = _sample_city()
            change_offset = int(np.random.randint(30, max(31, reg_span_days - offset)))
            new_reg = min(reg_dt + timedelta(days=change_offset), CUTOFF)
            active = dict(base)
            active.update(city=new_city, province=new_province, zip_code=new_zip)
            rows.append({"id": next_id, "registration_datetime": _fmt_dt(new_reg),
                         "status": "active", **active})
            next_id += 1
        else:
            rows.append({"id": next_id, "registration_datetime": _fmt_dt(reg_dt),
                         "status": "active", **base})
            next_id += 1

    columns = ["id", "last_name", "first_name", "fiscal_code_hash", "birth_date",
               "email", "phone", "city", "province", "zip_code",
               "registration_datetime", "has_marketing_consent", "status"]
    df = pd.DataFrame(rows)
    df["id"] = df["id"].astype(str)
    return df[columns]


def generate_suppliers(fake: Faker) -> pd.DataFrame:
    """Genera i 7 Supplier per categoria.

    Esattamente 1 supplier ha is_active = 0; ~10% dei contatti (email/phone) sono
    mancanti.
    """
    rows = []
    next_id = 1
    for category, cfg in CATEGORY_CONFIG.items():
        for _ in range(cfg["count"]):
            city, province, zip_code = _sample_city()
            name = f"{CATEGORY_NAME[category]} {city}"
            slug = _slugify(name)
            email = "" if np.random.random() < 0.10 else f"info@{slug}.it"
            phone = "" if np.random.random() < 0.10 else f"+390{np.random.randint(0, 10**9):09d}"
            rows.append({
                "id": next_id, "name": name, "category": category,
                "vat_number": f"{np.random.randint(0, 10**11):011d}",
                "city": city, "province": province, "zip_code": zip_code,
                "email": email, "phone": phone, "is_active": "1",
            })
            next_id += 1

    # Esattamente 1 supplier inattivo
    inactive_idx = int(np.random.choice(len(rows)))
    rows[inactive_idx]["is_active"] = "0"

    columns = ["id", "name", "category", "vat_number", "city", "province",
               "zip_code", "email", "phone", "is_active"]
    df = pd.DataFrame(rows)
    df["id"] = df["id"].astype(str)
    return df[columns]


def generate_operators(fake: Faker) -> pd.DataFrame:
    """Genera i 18 Operator per activity_type.

    ~70% assunti dopo gen 2023; 1 operator ha status 'terminated'.
    """
    rows = []
    next_id = 1
    hire_recent_span = (CUTOFF.date() - date(2023, 1, 1)).days
    hire_old_span = (date(2023, 1, 1) - HIRE_START).days

    for activity_type, count in ACTIVITY_COUNTS.items():
        for _ in range(count):
            last_name = fake.last_name()
            first_name = fake.first_name()
            email = f"{_slugify(first_name)}.{_slugify(last_name)}@guidaevai.it"
            city, province, _zip = _sample_city()
            # 70% assunti dopo gen 2023, 30% nel periodo 2020-2022
            if np.random.random() < 0.70:
                hire = date(2023, 1, 1) + timedelta(days=int(np.random.randint(0, hire_recent_span)))
            else:
                hire = HIRE_START + timedelta(days=int(np.random.randint(0, hire_old_span)))
            rows.append({
                "id": next_id, "last_name": last_name, "first_name": first_name,
                "email": email, "hire_date": _fmt_date(hire),
                "activity_type": activity_type, "city": city, "province": province,
                "status": "active",
            })
            next_id += 1

    # 1 operator terminato (data_specs §7.3): turnover con Request storiche.
    terminated_idx = int(np.random.choice(len(rows)))
    rows[terminated_idx]["status"] = "terminated"

    columns = ["id", "last_name", "first_name", "email", "hire_date",
               "activity_type", "city", "province", "status"]
    df = pd.DataFrame(rows)
    df["id"] = df["id"].astype(str)
    return df[columns]


def _assign_request_counts() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Assegna a ciascuna delle 200 personas il numero di Request secondo la coda
    lunga definita da RETENTION_BUCKETS, con un processo di aggiustamento per
    rientrare nella numerosità attesa di ~450 Request (tolleranza ±20).
    Restituisce array di counts, min e max per ciascuna persona."""
    counts = np.zeros(NUM_CUSTOMERS, dtype=int)
    lo = np.zeros(NUM_CUSTOMERS, dtype=int)
    hi = np.zeros(NUM_CUSTOMERS, dtype=int)
    order = np.random.permutation(NUM_CUSTOMERS)

    pos = 0
    for n_customers, (rmin, rmax) in RETENTION_BUCKETS:
        for _ in range(n_customers):
            p = order[pos]
            counts[p] = int(np.random.randint(rmin, rmax + 1))
            lo[p], hi[p] = rmin, rmax
            pos += 1

    total = int(counts.sum())
    # Riduci verso 470 partendo dalle personas con più Request (resta nei bucket).
    desc = np.argsort(counts)[::-1]
    i = 0
    while total > 470 and i < NUM_CUSTOMERS * 30:
        p = desc[i % NUM_CUSTOMERS]
        if counts[p] > lo[p]:
            counts[p] -= 1
            total -= 1
        i += 1
    # Aumenta verso 430 partendo dalle personas con meno Request.
    asc = np.argsort(counts)
    i = 0
    while total < 430 and i < NUM_CUSTOMERS * 30:
        p = asc[i % NUM_CUSTOMERS]
        if counts[p] < hi[p]:
            counts[p] += 1
            total += 1
        i += 1
    return counts, lo, hi


def generate_requests(customers: pd.DataFrame, suppliers: pd.DataFrame,
                      operators: pd.DataFrame) -> pd.DataFrame:
    """Genera ~450 Request.

    Il customer_id punta sempre alla riga 'active' del Customer-persona; il
    vincolo registration < prima Request usa la registrazione più vecchia della
    persona (recuperata dal CSV via fiscal_code_hash).
    """
    days, weights, ordinals = _build_day_table()

    # Personas: riga active (per la FK) + registrazione più vecchia.
    active = customers[customers["status"] == "active"][
        ["id", "fiscal_code_hash", "registration_datetime"]
    ].reset_index(drop=True)
    reg_min = customers.groupby("fiscal_code_hash")["registration_datetime"].min()
    personas = [
        (int(row["id"]),
         datetime.strptime(reg_min[row["fiscal_code_hash"]], "%Y-%m-%d %H:%M:%S").date())
        for _, row in active.iterrows()
    ]

    # Lookup su Supplier/Operator e pool ammessi per stato.
    sup_category = {int(r["id"]): r["category"] for _, r in suppliers.iterrows()}
    all_supplier_ids = [int(x) for x in suppliers["id"]]
    active_supplier_ids = [int(r["id"]) for _, r in suppliers.iterrows() if r["is_active"] == "1"]
    all_operator_ids = [int(x) for x in operators["id"]]
    active_operator_ids = [int(r["id"]) for _, r in operators.iterrows() if r["status"] == "active"]

    counts, _lo, _hi = _assign_request_counts()

    rows = []
    next_id = 1
    cutoff_ord = CUTOFF.date().toordinal()

    for persona_idx, (customer_id, reg_date) in enumerate(personas):
        # +1 giorno: la prima Request deve seguire STRETTAMENTE la registrazione
        # anche a livello di orario sullo stesso giorno.
        not_before = max(reg_date + timedelta(days=1), REQUEST_START)
        for _ in range(int(counts[persona_idx])):
            dt = _sample_request_datetime(days, weights, ordinals, not_before)
            age_days = cutoff_ord - dt.date().toordinal()

            completed_dt = None
            # Regola A: 10% cancellate, indipendentemente dall'eta'.
            if np.random.random() < 0.10:
                status = "cancelled"
            else:
                # Regola B: avanzamento sulle non-cancellate in base all'eta'.
                if age_days > 30:
                    status = "invoiced"
                elif age_days >= 7:
                    status = "completed"
                elif age_days >= 2:
                    status = "confirmed"
                else:
                    status = "booked"

                if status in ("invoiced", "completed"):
                    # Ritardo di erogazione: 85% entro 24h, 15% fino a 14 giorni.
                    if np.random.random() < 0.85:
                        delay_min = np.random.uniform(0, 24 * 60)
                    else:
                        delay_min = np.random.uniform(24 * 60, 14 * 24 * 60)
                    completed_dt = dt + timedelta(minutes=int(round(delay_min)))
                    if completed_dt > CUTOFF:
                        # Erogazione oltre il cutoff: la Request non è ancora completata.
                        completed_dt = None
                        status = "confirmed" if age_days >= 2 else "booked"

            # Selezione Supplier/Operator: i record inattivi/terminati gestiscono
            # solo Request storiche, mai booked/confirmed.
            if status in ("booked", "confirmed"):
                supplier_id = int(np.random.choice(active_supplier_ids))
                operator_id = int(np.random.choice(active_operator_ids))
            else:
                supplier_id = int(np.random.choice(all_supplier_ids))
                operator_id = int(np.random.choice(all_operator_ids))

            category = sup_category[supplier_id]
            cfg = CATEGORY_CONFIG[category]
            price = _round_half_euro(np.random.uniform(*cfg["price"]))

            if status in ("invoiced", "completed"):
                duration = str(_round_5_minutes(np.random.uniform(*cfg["duration"])))
            else:
                duration = ""

            if status == "cancelled":
                notes = CANCEL_REASONS[int(np.random.choice(len(CANCEL_REASONS)))]
            else:
                notes = GENERIC_NOTE if np.random.random() < 0.02 else ""

            rows.append({
                "id": next_id, "customer_id": customer_id, "supplier_id": supplier_id,
                "operator_id": operator_id, "request_datetime": _fmt_dt(dt),
                "completed_datetime": _fmt_dt(completed_dt), "status": status,
                "price": f"{price:.2f}", "service_duration_minutes": duration,
                "notes": notes,
            })
            next_id += 1

    columns = ["id", "customer_id", "supplier_id", "operator_id", "request_datetime",
               "completed_datetime", "status", "price", "service_duration_minutes", "notes"]
    df = pd.DataFrame(rows)
    for col in ("id", "customer_id", "supplier_id", "operator_id"):
        df[col] = df[col].astype(str)
    return df[columns]


# --------------------------------------------------------------------------- #
# Esportazione
# --------------------------------------------------------------------------- #

def _write_csv(df: pd.DataFrame, filename: str) -> None:
    """Scrive un DataFrame in CSV con le specifiche di formattazione
    (UTF-8 con BOM, separatore virgola, LF, QUOTE_MINIMAL, NULL = stringa vuota)."""
    path = CSV_DIR / filename
    df.to_csv(path, index=False, encoding="utf-8-sig", sep=",",
              lineterminator="\n", quoting=csv.QUOTE_MINIMAL, na_rep="")


def main() -> None:
    """seed, generazione delle 4 entità, export CSV."""
    fake = _setup_seed()
    CSV_DIR.mkdir(parents=True, exist_ok=True)

    # Ordine vincolato: Customer prima (le Request hanno FK verso i Customer).
    customers = generate_customers(fake)
    suppliers = generate_suppliers(fake)
    operators = generate_operators(fake)
    requests = generate_requests(customers, suppliers, operators)

    _write_csv(customers, "customers.csv")
    _write_csv(suppliers, "suppliers.csv")
    _write_csv(operators, "operators.csv")
    _write_csv(requests, "requests.csv")

    print(f"Generati 4 CSV in {CSV_DIR}")


if __name__ == "__main__":
    main()
