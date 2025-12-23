import pandas as pd
from typing import Dict, Any, List
from data_transformer import parse_mapping,dataframe_to_docs
# assume you already imported: parse_mapping, dataframe_to_docs
import json
from rich.console import Console
from rich.table import Table
from rich.json import JSON
console = Console()

from colorama import Fore, Style


def print_colored_json(data:dict):
    """Pretty print JSON with colors in terminal."""
    console.print(JSON(json.dumps(data, indent = 2)))

def print_sql_table(df: pd.DataFrame, title = "NoSQL_to_SQL Records"):
    '''Print dict as SQL-Style table in terminal.'''
    table = Table(title=title)
    # Add columns
    for col in df.columns:
        table.add_column(str(col), style="cyan", no_wrap=True)
    # Add rows
    for _, row in df.iterrows():
        table.add_row(*[str(val) for val in row.tolist()])
    console.print(table)

print(Fore.LIGHTMAGENTA_EX+"-- Input: SQL --"+ Style.RESET_ALL)







# --- Sample inputs (3 tables) ---
df_clients = pd.DataFrame([
    {"client_id": 825, "name": "Alice"},
    {"client_id": 1746, "name": "Bob"},
])
print_sql_table(df_clients, title = "Client Info Table")

df_cards = pd.DataFrame([
    {"client_id": 825,  "card_brand": "Visa", "card_type": "Debit",  "card_number": "****5119", "expires": "12/2022"},
    {"client_id": 825,  "card_brand": "Visa", "card_type": "Credit", "card_number": "****0690", "expires": "08/2024"},
    {"client_id": 1746, "card_brand": "Visa", "card_type": "Debit",  "card_number": "****8463", "expires": "07/2022"},
])
print_sql_table(df_cards, title = "Card Info Table")

df_tx = pd.DataFrame([
    {"client_id": 825, "date": "2017-08-16", "details": "INTERNAL FUND TRANSFER", "deposit_amt": None, "withdrawal_amt": 133900},
    {"client_id": 825, "date": "2017-08-16", "details": "Indo GIBL",               "deposit_amt": 5000,  "withdrawal_amt": None},
    {"client_id": 1746,"date": "2017-08-16", "details": "Indo GIBL",               "deposit_amt": 331650,"withdrawal_amt": None},
])
print_sql_table(df_tx, title = "Transaction Table")






# --- Table-level mappings (no YAML, pure dicts) ---
map_cards = parse_mapping({
    "source_kind": "sql", "target_kind": "nosql",
    "fields": {
        "brand":       {"candidates": ["card_brand"]},
        "type":        {"candidates": ["card_type"]},
        "last4":       {"candidates": ["card_number"]},   # keep masked in input; or slice last4 deterministically here
        "expires":     {"candidates": ["expires"]},
        "client_id":   {"candidates": ["client_id"]},
    },
    "rules": {"trim_strings": True}
})

map_tx = parse_mapping({
    "source_kind": "sql", "target_kind": "nosql",
    "fields": {
        "date":        {"candidates": ["date"]},
        "description": {"candidates": ["details"]},
        "deposit":     {"candidates": ["deposit_amt"], "cast": "float"},
        "withdrawal":  {"candidates": ["withdrawal_amt"], "cast": "float"},
        "client_id":   {"candidates": ["client_id"]},
    },
    "rules": {"trim_strings": True, "drop_null_target_fields": True}
})

# --- Normalize each table deterministically ---
cards_norm: List[Dict[str, Any]] = dataframe_to_docs(df_cards, map_cards)
tx_norm:    List[Dict[str, Any]] = dataframe_to_docs(df_tx,    map_tx)

# build dicts without groupby.apply (no warnings)
cards_df = pd.DataFrame(cards_norm)
cards_by_client = {
    cid: grp.drop(columns=["client_id"]).to_dict("records")
    for cid, grp in cards_df.groupby("client_id", dropna=False)
}

tx_df = pd.DataFrame(tx_norm)
tx_by_client = {
    cid: grp.drop(columns=["client_id"]).to_dict("records")
    for cid, grp in tx_df.groupby("client_id", dropna=False)
}

# --- Build one document per client (deterministic composition) ---
clients = df_clients.drop_duplicates(subset=["client_id"])[["client_id", "name"]].to_dict("records")
docs: List[Dict[str, Any]] = []
for c in clients:
    cid = c["client_id"]
    docs.append({
        "client_id": cid,
        "name": c["name"],
        "cards": cards_by_client.get(cid, []),
        "transactions": tx_by_client.get(cid, []),
    })

print(Fore.LIGHTMAGENTA_EX+"-- Output: NoSQL Docs --"+ Style.RESET_ALL)
print_colored_json(docs)


