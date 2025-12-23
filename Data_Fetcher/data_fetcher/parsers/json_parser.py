# data_fetcher/parsers/json_parser.py
from __future__ import annotations
from typing import Any, Dict

def parse_json_accounts(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accepts Bank A (Plaid-like) or Bank B variant JSON accounts.
    Returns internal raw dict: {"accounts": [ ... mapped raw ... ], "meta": {...}}
    """
    out, meta = [], {"source": "json"}
    if "accounts" in obj:  # bankA style
        for a in obj["accounts"]:
            out.append({
                "account_id": a.get("account_id"),
                "type": a.get("type"),
                "subtype": a.get("subtype"),
                "mask": a.get("mask"),
                "currency": (a.get("balances") or {}).get("iso_currency_code"),
                "current": (a.get("balances") or {}).get("current"),
                "available": (a.get("balances") or {}).get("available"),
                "name": a.get("name"),
            })
    elif "accounts_list" in obj:  # bankB style
        for a in obj["accounts_list"]:
            bal = a.get("balance") or {}
            out.append({
                "account_id": a.get("id"),
                "type": a.get("kind"),
                "subtype": a.get("subkind"),
                "mask": a.get("last4"),
                "currency": bal.get("ccy"),
                "current": bal.get("cur"),
                "available": bal.get("avail"),
                "name": a.get("official_name"),
            })
    elif "accounts" in obj and "institution" in obj:  # bankC tx json carries accounts+txns
        # In case accounts are embedded
        for a in obj.get("accounts", []):
            out.append({
                "account_id": a.get("acct"),
                "type": "depository",
                "subtype": "savings",
                "mask": None,
                "currency": a.get("currency"),
                "current": None,
                "available": None,
                "name": f"{obj.get('institution','bank')}-{a.get('acct')}",
            })
    return {"accounts": out, "meta": meta}

def parse_json_transactions(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accepts Bank C nested transactions JSON or Plaid-like arrays.
    Returns {"transactions":[...], "meta": {...}}
    """
    items, meta = [], {"source": "json"}
    # bankC nested
    if "accounts" in obj and any("txns" in a for a in obj.get("accounts", [])):
        for a in obj["accounts"]:
            acct_id = a.get("acct")
            ccy = a.get("currency")
            for t in a.get("txns", []):
                items.append({
                    "txn_id": t.get("id"),
                    "account_id": acct_id,
                    "date": (t.get("when") or "")[:10],  # ISO -> YYYY-MM-DD
                    "amount": t.get("amt"),
                    "currency": ccy,
                    "merchant_raw": t.get("who"),
                    "mcc": t.get("mcc"),
                    "category": None,
                })
    # generic fallback
    elif "transactions" in obj:
        for t in obj["transactions"]:
            items.append({
                "txn_id": t.get("id") or t.get("txn_id"),
                "account_id": t.get("account_id"),
                "date": t.get("date"),
                "amount": t.get("amount"),
                "currency": t.get("currency"),
                "merchant_raw": t.get("merchant"),
                "mcc": t.get("mcc"),
                "category": t.get("category"),
            })
    return {"transactions": items, "meta": meta}
