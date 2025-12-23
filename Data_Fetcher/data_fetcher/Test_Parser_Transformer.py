# Example orchestration snippet
from parsers.json_parser import parse_json_accounts, parse_json_transactions
from parsers.csv_parser import parse_csv_transactions
from parsers.xml_ofx_parser import parse_camt_accounts, parse_custom_bankB_statement, parse_ofx_transactions
from parsers.html_parser import parse_html_accounts, parse_html_transactions
from transformers.finance import to_canonical_accounts, to_canonical_transactions
import json

import json
from pathlib import Path

def load_text(path: str) -> str:
    """Read a text file (HTML, OFX, CSV as raw string)."""
    p = Path(path)
    with p.open('r', encoding='utf-8') as f:
        data = f.read()
    if not data.strip():
        raise ValueError(f"{p} is empty")
    return data

def load_json(path: str):
    """Read a JSON file and return a Python object."""
    p = Path(path)
    with p.open('r', encoding='utf-8') as f:
        obj = json.load(f)
    return obj

# 1) parse
raw_accts = parse_json_accounts(load_json("sample_data/bankA/accounts.json"))
raw_accts_html = parse_html_accounts(load_text("sample_data/bankC/accounts.html"))

raw_tx_html = parse_html_transactions(load_text("sample_data/bankA/transactions.html"))
raw_tx_csv = parse_csv_transactions(load_text("sample_data/bankA/transactions.csv"))
raw_tx_ofx = parse_ofx_transactions(load_text("sample_data/bankC/statement.ofx"))


# 2) transform - transactions test
canon_tx_html = to_canonical_transactions(raw_tx_html)
print(type(canon_tx_html))
print("canon_tx_html: ",json.dumps(canon_tx_html, indent=4, sort_keys=True))

canon_tx_ofx = to_canonical_transactions(raw_tx_ofx)
print(type(canon_tx_ofx))
print("canon_tx_ofx: ",json.dumps(canon_tx_ofx, indent=4, sort_keys=True))

canon_tx_csv = to_canonical_transactions(raw_tx_csv)
print(type(canon_tx_csv))
print("canon_tx_csv: ",json.dumps(canon_tx_csv, indent=4, sort_keys=True))




