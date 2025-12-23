from data_transformer import parse_mapping,dataframe_to_docs, select_and_rename_columns
import pandas as pd

import json
from rich.console import Console
from rich.table import Table
from rich.json import JSON

from colorama import Fore, Style

console = Console()

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


# --- Quick test (run this file directly) ---
if __name__ == "__main__":
    # Simulate SQL->NoSQL
    '''
    Two main inputs:

    1. Original data – e.g. a Pandas DataFrame from SQL, CSV, Mongo, etc.
    2. Mapping dict – tells the transformer how to map fields (candidates, defaults, casts, rules).

    which can reuse the same core functions for cards, transactions, patients, universities, etc.
    '''

    df_sql = pd.DataFrame([
        {"amt": "12.345 ", "ccy": None, "descr": " Coffee ", "account_id": "A1", "transaction_id": "T1"},
        {"amt": "7.8",     "ccy": "EUR", "descr": "Sandwich", "account_id": "A2", "transaction_id": "T2"},
    ])

    '''
    May need LLM to write the mapping dict for different domain data
    '''
    map_sql_to_nosql = {
        "source_kind": "sql",
        "target_kind": "nosql",
        "fields": {
            "amount":        {"candidates": ["amount", "amt", "debit"], "cast": "decimal(18,2)"},
            "currency":      {"candidates": ["currency", "ccy"], "default": "USD"},
            "description":   {"candidates": ["description", "descr", "narr"]},
            "account_id":    {"candidates": ["account_id", "acct_id"]},
            "transaction_id":{"candidates": ["transaction_id", "txn_id", "id"]},
        },
        "rules": {"trim_strings": True, "drop_null_target_fields": True}
    }
    cfg1 = parse_mapping(map_sql_to_nosql)
    print(cfg1)

    docs = dataframe_to_docs(df_sql, cfg1)
    print(Fore.LIGHTMAGENTA_EX+"-- SQL -> NoSQL docs --"+ Style.RESET_ALL)
    print_colored_json(docs)

    # Simulate NoSQL->SQL using the produced docs
    df_nosql = pd.DataFrame(docs)
    map_nosql_to_sql = {
        "source_kind": "nosql",
        "target_kind": "sql",
        "fields": {
            "amount":        {"candidates": ["amount"], "cast": "decimal(18,2)"},
            "currency":      {"candidates": ["currency"]},
            "description":   {"candidates": ["description"]},
            "account_id":    {"candidates": ["account_id"]},
            "transaction_id":{"candidates": ["transaction_id"]},
        },
        "rules": {"trim_strings": True}
    }
    cfg2 = parse_mapping(map_nosql_to_sql)
    df_for_sql = select_and_rename_columns(df_nosql, cfg2)
    print(Fore.LIGHTMAGENTA_EX+"-- NoSQL -> SQL Table --"+ Style.RESET_ALL)
    print_sql_table(df_for_sql)
