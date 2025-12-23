#transformer_finance

from __future__ import annotations
from typing import Dict, Any, List, Optional

# simple in-memory dictionaries (replace with TTL cache/lookups)
MERCHANT_ALIASES = {"AMZN Mkt": "Amazon", "Starbcks": "Starbucks", "CoffeeShop": "Starbucks"}
CATEGORY_RULES = {"Amazon": "Shopping", "Starbucks": "Food & Beverage", "Payroll": "Income"}

def _norm_merchant(name: Optional[str]) -> Optional[str]:
    if not name:
        return name
    return MERCHANT_ALIASES.get(name, name)

def _categorize(name: Optional[str], fallback: Optional[str] = None) -> Optional[str]:
    if name and name in CATEGORY_RULES:
        return CATEGORY_RULES[name]
    return fallback

def to_canonical_accounts(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Raw {"accounts":[...]} -> {"accounts":[{account_id,type,subtype,mask,currency,current,available,name}]}
    """
    out: List[Dict[str, Any]] = []
    for a in raw.get("accounts", []):
        out.append({
            "account_id": a.get("account_id"),
            "type": a.get("type"),
            "subtype": a.get("subtype"),
            "mask": a.get("mask"),
            "currency": a.get("currency") or "USD",
            "current": a.get("current"),
            "available": a.get("available"),
            "name": a.get("name"),
        })
    return {"accounts": out}

def to_canonical_transactions(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Raw {"transactions":[...]} -> canonical list with normalized merchants & categories.
    Adds simple paging stub.
    """
    items: List[Dict[str, Any]] = []
    for t in raw.get("transactions", []):
        merchant_norm = _norm_merchant(t.get("merchant_raw"))
        items.append({
            "txn_id": t.get("txn_id"),
            "account_id": t.get("account_id"),
            "date": t.get("date"),
            "amount": float(t.get("amount") or 0),
            "currency": t.get("currency") or "USD",
            "merchant_raw": t.get("merchant_raw"),
            "merchant_norm": merchant_norm,
            "mcc": t.get("mcc"),
            "category": _categorize(merchant_norm, t.get("category")),
            "meta": {},
        })
    return {"transactions": items, "paging": {"cursor": None, "has_more": False}}
