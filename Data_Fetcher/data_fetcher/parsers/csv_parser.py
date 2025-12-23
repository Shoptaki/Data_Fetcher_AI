from __future__ import annotations
import csv
from io import StringIO
from typing import Dict, Any, List

def parse_csv_transactions(csv_text: str) -> Dict[str, Any]:
    """
    Supports bankA (standard headers) and bankB (vendor/txn_category, debit_amount positive).
    Returns {"transactions":[...], "meta": {...}}
    """
    rdr = csv.DictReader(StringIO(csv_text))
    out: List[Dict[str, Any]] = []
    meta = {"source": "csv", "columns": rdr.fieldnames}

    for row in rdr:
        # bankA style
        if {"date","amount","currency","merchant","category","account_id"}.issubset(row.keys()):
            amt = float(row["amount"])
            out.append({
                "txn_id": row.get("txn_id"),
                "account_id": row["account_id"],
                "date": row["date"],
                "amount": amt,
                "currency": row["currency"],
                "merchant_raw": row["merchant"],
                "mcc": None,
                "category": row.get("category"),
            })
        # bankB style
        elif {"txn_date","debit_amount","ccy","vendor","txn_category","acct_ref"}.issubset(row.keys()):
            amt = -abs(float(row["debit_amount"]))  # outflow to negative
            date = row["txn_date"].replace("/", "-")
            out.append({
                "txn_id": row.get("id"),
                "account_id": row["acct_ref"],
                "date": date,
                "amount": amt,
                "currency": row["ccy"],
                "merchant_raw": row["vendor"],
                "mcc": None,
                "category": row.get("txn_category"),
            })
        else:
            # pass through unknown schema minimally
            out.append({
                "txn_id": row.get("id"),
                "account_id": row.get("account_id") or row.get("acct_ref"),
                "date": row.get("date") or row.get("txn_date"),
                "amount": float(row.get("amount") or row.get("debit_amount") or 0),
                "currency": row.get("currency") or row.get("ccy"),
                "merchant_raw": row.get("merchant") or row.get("vendor"),
                "mcc": row.get("mcc"),
                "category": row.get("category") or row.get("txn_category"),
            })
    return {"transactions": out, "meta": meta}
