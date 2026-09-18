"""
Générateur de données - Projet "Plateforme data fintech" (phase 1)

Simule le système source d'une fintech de paiement. À chaque lot ("batch"),
il dépose des fichiers JSON Lines dans une zone d'atterrissage :

    <output_dir>/raw/transactions/     flux d'événements de paiement
    <output_dir>/raw/customers_cdc/    flux CDC des clients (INSERT/UPDATE/DELETE)
    <output_dir>/raw/merchants/        référentiel des marchands
    <output_dir>/generator_state/      état interne + vérité terrain fraude
                                       (NE PAS ingérer avec Auto Loader)

Défauts injectés volontairement (tous réglables dans Config) :
    - doublons de transactions
    - événements en retard (1 à 3 jours)
    - enregistrements invalides (montant nul, négatif, mal typé, client absent,
      marchand inconnu, date dans le futur)
    - lignes JSON tronquées (illisibles)
    - événements CDC périmés, arrivant après une version plus récente
    - évolution de schéma : le champ `device_os` apparaît à partir d'un lot donné
    - scénarios de fraude : rafale de transactions, montant anormal

Bibliothèque standard uniquement : rien à installer.

Usage local :
    python generator/generate_fintech_data.py --output-dir ./landing --batches 10

Usage dans un notebook Databricks :
    from generator.generate_fintech_data import run
    run("/Volumes/fintech/landing/files", batches=10)
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
@dataclass
class Config:
    n_initial_customers: int = 500
    n_merchants: int = 80
    tx_per_batch: int = 1000
    new_customers_per_batch: int = 5
    customer_updates_per_batch: int = 10
    p_customer_delete: float = 0.1      # proba qu'un lot contienne une clôture de compte
    p_new_merchant: float = 0.2         # proba qu'un lot ajoute un marchand

    p_duplicate: float = 0.02           # part de transactions réémises à l'identique
    p_late: float = 0.03                # part de transactions en retard de 1 à 3 jours
    p_bad_record: float = 0.02          # part d'enregistrements invalides
    p_malformed_line: float = 0.002     # part de lignes JSON tronquées
    p_cdc_stale: float = 0.05           # part d'updates CDC suivis d'une version périmée
    p_fraud_burst: float = 0.3          # proba qu'un lot contienne une rafale frauduleuse
    p_fraud_big_amount: float = 0.3     # proba qu'un lot contienne un montant anormal

    schema_evolution_batch: int = 5     # `device_os` apparaît à partir de ce lot
    batch_interval_minutes: int = 5
    currency: str = "EUR"


CITIES = ["Paris", "Lyon", "Marseille", "Lille", "Bordeaux", "Nantes", "Toulouse"]
FIRST_NAMES = ["Awa", "Moussa", "Fatou", "Ibrahima", "Marie", "Jean", "Aïcha",
               "Omar", "Sophie", "Cheikh", "Nadia", "Paul", "Khady", "Lucas"]
LAST_NAMES = ["Diop", "Ndiaye", "Martin", "Fall", "Bernard", "Sow", "Ba",
              "Dubois", "Gueye", "Petit", "Sarr", "Moreau", "Kane", "Laurent"]
MERCHANT_CATEGORIES = ["grocery", "restaurant", "transport", "telecom",
                       "fuel", "pharmacy", "electronics", "utilities"]
CHANNELS = ["APP", "USSD", "CARD", "QR"]
KYC_LEVELS = ["BASIC", "STANDARD", "PREMIUM"]
DEVICE_OS = ["android", "ios", "feature_phone"]


# --------------------------------------------------------------------------
# Utilitaires
# --------------------------------------------------------------------------
def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def new_uuid(rng: random.Random) -> str:
    """UUID dérivé du générateur aléatoire : un même seed redonne les mêmes ids."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def write_lines_atomic(lines: list[str], dest: Path) -> None:
    """Écrit d'abord en local puis copie le fichier fini.

    Auto Loader ne doit jamais voir un fichier à moitié écrit.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as tmp:
        tmp.write("\n".join(lines) + "\n")
        tmp_path = tmp.name
    shutil.copyfile(tmp_path, dest)
    os.remove(tmp_path)


def load_state(state_file: Path) -> dict:
    if state_file.exists():
        return json.loads(state_file.read_text(encoding="utf-8"))
    return {"batch": 0, "cdc_seq": 0, "next_customer": 1, "next_merchant": 1,
            "customers": {}, "merchants": []}


def save_state(state: dict, state_file: Path) -> None:
    write_lines_atomic([json.dumps(state, ensure_ascii=False)], state_file)


# --------------------------------------------------------------------------
# Clients (flux CDC) et marchands
# --------------------------------------------------------------------------
def cdc_event(state: dict, op: str, customer: dict, now: datetime) -> dict:
    """Chaque événement CDC porte un numéro de séquence strictement croissant.

    C'est `seq` (et non l'ordre d'arrivée) qui fait foi : il servira de
    `sequence_by` dans AUTO CDC pour ignorer les versions périmées.
    """
    state["cdc_seq"] += 1
    return {"op": op, "seq": state["cdc_seq"], "changed_at": iso(now), **customer}


def make_customer(state: dict, rng: random.Random, now: datetime) -> dict:
    cid = f"C-{state['next_customer']:06d}"
    state["next_customer"] += 1
    return {
        "customer_id": cid,
        "full_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
        "phone": f"+33{rng.randint(600000000, 799999999)}",
        "city": rng.choice(CITIES),
        "kyc_level": rng.choices(KYC_LEVELS, weights=[6, 3, 1])[0],
        "status": "ACTIVE",
        "created_at": iso(now),
    }


def make_merchant(state: dict, rng: random.Random, now: datetime) -> dict:
    mid = f"M-{state['next_merchant']:05d}"
    state["next_merchant"] += 1
    category = rng.choice(MERCHANT_CATEGORIES)
    return {
        "merchant_id": mid,
        "merchant_name": f"{category.title()} {rng.choice(LAST_NAMES)} {rng.randint(1, 99)}",
        "category": category,
        "city": rng.choice(CITIES),
        "onboarded_at": iso(now),
    }


def gen_customer_changes(state: dict, rng: random.Random, now: datetime,
                         cfg: Config, n_new: int) -> list[dict]:
    events: list[dict] = []

    for _ in range(n_new):
        customer = make_customer(state, rng, now)
        state["customers"][customer["customer_id"]] = customer
        events.append(cdc_event(state, "INSERT", customer, now))

    active_ids = list(state["customers"])
    n_updates = min(cfg.customer_updates_per_batch, len(active_ids))
    for cid in rng.sample(active_ids, k=n_updates):
        before = dict(state["customers"][cid])
        after = dict(before)
        if rng.random() < 0.6:                       # déménagement
            after["city"] = rng.choice([c for c in CITIES if c != before["city"]])
        else:                                        # montée de niveau KYC
            idx = KYC_LEVELS.index(before["kyc_level"])
            after["kyc_level"] = KYC_LEVELS[min(idx + 1, len(KYC_LEVELS) - 1)]
        if after == before:
            continue
        state["customers"][cid] = after
        new_event = cdc_event(state, "UPDATE", after, now)
        events.append(new_event)
        if rng.random() < cfg.p_cdc_stale:
            # Version périmée (seq plus petit) qui arrive APRÈS la version récente.
            events.append({"op": "UPDATE", "seq": new_event["seq"] - 1,
                           "changed_at": iso(now - timedelta(minutes=30)), **before})

    if active_ids and rng.random() < cfg.p_customer_delete:
        cid = rng.choice(list(state["customers"]))
        closed = state["customers"].pop(cid)
        closed["status"] = "CLOSED"
        events.append(cdc_event(state, "DELETE", closed, now))

    return events


# --------------------------------------------------------------------------
# Transactions
# --------------------------------------------------------------------------
def base_transaction(state: dict, rng: random.Random, ts: datetime, cfg: Config,
                     customer_id: str | None = None) -> dict:
    tx = {
        "transaction_id": new_uuid(rng),
        "customer_id": customer_id or rng.choice(list(state["customers"])),
        "merchant_id": rng.choice(state["merchants"]),
        "amount": round(rng.lognormvariate(3.2, 1.0), 2),
        "currency": cfg.currency,
        "channel": rng.choice(CHANNELS),
        "status": rng.choices(["SUCCESS", "FAILED", "PENDING"], weights=[90, 7, 3])[0],
        "event_ts": iso(ts),
    }
    if state["batch"] >= cfg.schema_evolution_batch:
        tx["device_os"] = rng.choice(DEVICE_OS)      # évolution de schéma
    return tx


def corrupt(tx: dict, rng: random.Random) -> dict:
    """Rend un enregistrement invalide d'une des six façons possibles."""
    kind = rng.randint(1, 6)
    if kind == 1:
        tx["amount"] = None
    elif kind == 2:
        tx["amount"] = -abs(tx["amount"])
    elif kind == 3:
        tx["amount"] = f"{tx['amount']:.2f}".replace(".", ",")   # "12,50" en texte
    elif kind == 4:
        tx["customer_id"] = None
    elif kind == 5:
        tx["merchant_id"] = "M-UNKNOWN"
    else:
        tx["event_ts"] = "2099-01-01T00:00:00.000Z"
    return tx


def gen_transactions(state: dict, rng: random.Random, now: datetime,
                     cfg: Config) -> tuple[list[str], list[dict]]:
    """Retourne (lignes JSON à écrire, vérité terrain des fraudes injectées)."""
    txs: list[dict] = []
    truth: list[dict] = []
    window = cfg.batch_interval_minutes * 60

    for _ in range(cfg.tx_per_batch):
        ts = now - timedelta(seconds=rng.uniform(0, window))
        roll = rng.random()
        if roll < cfg.p_late:
            ts -= timedelta(days=rng.randint(1, 3), hours=rng.randint(0, 23))
        tx = base_transaction(state, rng, ts, cfg)
        if cfg.p_late <= roll < cfg.p_late + cfg.p_bad_record:
            tx = corrupt(tx, rng)
        txs.append(tx)

    if rng.random() < cfg.p_fraud_burst:             # rafale : 15 à 25 paiements en 2 min
        cid = rng.choice(list(state["customers"]))
        start = now - timedelta(seconds=rng.uniform(120, window))
        for _ in range(rng.randint(15, 25)):
            ts = start + timedelta(seconds=rng.uniform(0, 120))
            tx = base_transaction(state, rng, ts, cfg, customer_id=cid)
            tx["status"] = "SUCCESS"
            txs.append(tx)
            truth.append({"transaction_id": tx["transaction_id"], "scenario": "burst"})

    if rng.random() < cfg.p_fraud_big_amount:        # montant anormal
        ts = now - timedelta(seconds=rng.uniform(0, window))
        tx = base_transaction(state, rng, ts, cfg)
        tx["amount"] = round(tx["amount"] * rng.uniform(50, 120) + 2000, 2)
        tx["status"] = "SUCCESS"
        txs.append(tx)
        truth.append({"transaction_id": tx["transaction_id"], "scenario": "big_amount"})

    lines = [json.dumps(tx, ensure_ascii=False) for tx in txs]
    n_dupes = int(len(lines) * cfg.p_duplicate)
    lines.extend(rng.sample(lines, k=n_dupes))       # doublons exacts
    rng.shuffle(lines)                               # l'ordre d'arrivée n'est pas garanti
    lines = [ln[: len(ln) // 2] if rng.random() < cfg.p_malformed_line else ln
             for ln in lines]                        # lignes tronquées
    return lines, truth


# --------------------------------------------------------------------------
# Orchestration d'un lot
# --------------------------------------------------------------------------
def run_batch(output_dir: Path, state: dict, now: datetime, cfg: Config, seed: int) -> dict:
    state["batch"] += 1
    batch = state["batch"]
    rng = random.Random(seed + batch)                # un lot donné est reproductible
    raw = output_dir / "raw"
    stamp = now.strftime("%Y%m%dT%H%M%S")
    tag = f"{batch:05d}_{stamp}"

    merchants: list[dict] = []
    if not state["merchants"]:                       # tout premier lot : amorçage
        merchants = [make_merchant(state, rng, now) for _ in range(cfg.n_merchants)]
        n_new_customers = cfg.n_initial_customers
    else:
        if rng.random() < cfg.p_new_merchant:
            merchants = [make_merchant(state, rng, now)]
        n_new_customers = cfg.new_customers_per_batch
    state["merchants"].extend(m["merchant_id"] for m in merchants)

    cdc = gen_customer_changes(state, rng, now, cfg, n_new_customers)
    tx_lines, truth = gen_transactions(state, rng, now, cfg)

    if merchants:
        write_lines_atomic([json.dumps(m, ensure_ascii=False) for m in merchants],
                           raw / "merchants" / f"merchants_{tag}.json")
    if cdc:
        write_lines_atomic([json.dumps(e, ensure_ascii=False) for e in cdc],
                           raw / "customers_cdc" / f"customers_cdc_{tag}.json")
    write_lines_atomic(tx_lines, raw / "transactions" / f"transactions_{tag}.json")
    if truth:
        write_lines_atomic([json.dumps({**t, "batch": batch}) for t in truth],
                           output_dir / "generator_state" / "fraud_truth" / f"truth_{tag}.json")

    return {"batch": batch, "transactions": len(tx_lines), "cdc_events": len(cdc),
            "new_merchants": len(merchants), "fraud_tx": len(truth)}


def run(output_dir: str, batches: int = 1, seed: int = 42, cfg: Config | None = None) -> None:
    cfg = cfg or Config()
    out = Path(output_dir)
    state_file = out / "generator_state" / "state.json"
    state = load_state(state_file)
    end = datetime.now(timezone.utc)
    for i in range(batches):
        # Plusieurs lots d'un coup : on les étale dans le passé, un par intervalle.
        now = end - timedelta(minutes=cfg.batch_interval_minutes * (batches - 1 - i))
        print(run_batch(out, state, now, cfg, seed))
        save_state(state, state_file)                # état sauvé après chaque lot


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Générateur de données fintech")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--batches", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    run(args.output_dir, args.batches, args.seed)
