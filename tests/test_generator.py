"""Tests du générateur de données.

Ils vérifient que le système source simulé se comporte comme annoncé dans
docs/data_contract.md : si un défaut injecté disparaît, les tests des couches
Silver et Gold ne prouvent plus rien.
"""
import json
from collections import Counter
from pathlib import Path

import pytest

from generator.generate_fintech_data import Config, run


def read_lines(folder: Path) -> list[str]:
    lines = []
    for file in sorted(folder.glob("*.json")):
        lines += [ln for ln in file.read_text(encoding="utf-8").splitlines() if ln]
    return lines


def parse_valid(lines: list[str]) -> list[dict]:
    records = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return records


@pytest.fixture(scope="module")
def landing(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("landing")
    # Taux de lignes tronquées relevé pour que le test soit stable sur peu de lots.
    run(str(out), batches=8, seed=42, cfg=Config(p_malformed_line=0.01))
    return out


def test_three_feeds_are_written(landing):
    for feed in ("transactions", "customers_cdc", "merchants"):
        assert list((landing / "raw" / feed).glob("*.json")), f"aucun fichier pour {feed}"


def test_state_is_kept_out_of_raw(landing):
    assert (landing / "generator_state" / "state.json").exists()
    assert not list((landing / "raw").rglob("state.json"))


def test_duplicates_are_injected(landing):
    txs = parse_valid(read_lines(landing / "raw" / "transactions"))
    counts = Counter(tx["transaction_id"] for tx in txs)
    assert sum(1 for n in counts.values() if n > 1) > 0


def test_malformed_lines_are_injected(landing):
    lines = read_lines(landing / "raw" / "transactions")
    assert len(parse_valid(lines)) < len(lines)


def test_invalid_amounts_are_injected(landing):
    txs = parse_valid(read_lines(landing / "raw" / "transactions"))
    assert any(tx["amount"] is None for tx in txs)
    assert any(isinstance(tx["amount"], str) for tx in txs)
    assert any(isinstance(tx["amount"], float) and tx["amount"] < 0 for tx in txs)


def test_schema_evolves_at_configured_batch(landing):
    files = sorted((landing / "raw" / "transactions").glob("*.json"))
    first = parse_valid(files[0].read_text(encoding="utf-8").splitlines())
    last = parse_valid(files[-1].read_text(encoding="utf-8").splitlines())
    assert all("device_os" not in tx for tx in first)
    assert all("device_os" in tx for tx in last)


def test_cdc_sequence_identifies_latest_version(landing):
    events = parse_valid(read_lines(landing / "raw" / "customers_cdc"))
    assert {e["op"] for e in events} >= {"INSERT", "UPDATE"}
    # Pour un client donné, le plus grand `seq` correspond à l'état final connu.
    state = json.loads((landing / "generator_state" / "state.json").read_text(encoding="utf-8"))
    latest: dict[str, dict] = {}
    for event in events:
        cid = event["customer_id"]
        if cid not in latest or event["seq"] > latest[cid]["seq"]:
            latest[cid] = event
    for cid, customer in state["customers"].items():
        assert latest[cid]["city"] == customer["city"]
        assert latest[cid]["kyc_level"] == customer["kyc_level"]


def test_fraud_truth_matches_transactions(landing):
    tx_ids = {tx["transaction_id"]
              for tx in parse_valid(read_lines(landing / "raw" / "transactions"))}
    truth_dir = landing / "generator_state" / "fraud_truth"
    truth = parse_valid(read_lines(truth_dir)) if truth_dir.exists() else []
    assert truth, "aucun scénario de fraude généré sur 8 lots"
    assert {t["transaction_id"] for t in truth} <= tx_ids


def test_run_resumes_from_saved_state(tmp_path):
    run(str(tmp_path), batches=2, seed=1)
    run(str(tmp_path), batches=1, seed=1)
    state = json.loads((tmp_path / "generator_state" / "state.json").read_text(encoding="utf-8"))
    assert state["batch"] == 3
    assert len(list((tmp_path / "raw" / "transactions").glob("*.json"))) == 3


def test_same_seed_gives_same_transaction_ids(tmp_path):
    ids = []
    for name in ("a", "b"):
        out = tmp_path / name
        run(str(out), batches=1, seed=7)
        txs = parse_valid(read_lines(out / "raw" / "transactions"))
        ids.append(sorted(tx["transaction_id"] for tx in txs))
    assert ids[0] == ids[1]
