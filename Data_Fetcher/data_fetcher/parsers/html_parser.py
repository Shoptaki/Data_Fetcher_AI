from __future__ import annotations
from bs4 import BeautifulSoup  # pip install beautifulsoup4
from typing import Dict, Any, List

def parse_html_accounts(html_text: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    rows = soup.select("table tr")
    headers = [th.get_text(strip=True).lower() for th in rows[0].find_all("th")]
    out: List[Dict[str, Any]] = []
    for r in rows[1:]:
        cells = [td.get_text(strip=True) for td in r.find_all("td")]
        rec = dict(zip(headers, cells))
        out.append({
            "account_id": rec.get("acct no") or rec.get("acct") or rec.get("account_id"),
            "type": rec.get("type"),
            "subtype": rec.get("subtype"),
            "mask": rec.get("mask"),
            "currency": rec.get("currency"),
            "current": float(rec.get("current") or 0) if rec.get("current") else None,
            "available": None,
            "name": f"{rec.get('type','acct')} {rec.get('mask','')}".strip(),
        })
    return {"accounts": out, "meta": {"source": "html"}}

def parse_html_transactions(html_text: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    rows = soup.select("table#tx tr")
    headers = [th.get_text(strip=True).lower() for th in rows[0].find_all("th")]
    out: List[Dict[str, Any]] = []
    for r in rows[1:]:
        cells = [td.get_text(strip=True) for td in r.find_all("td")]
        rec = dict(zip(headers, cells))
        out.append({
            "txn_id": None,
            "account_id": rec.get("acct") or rec.get("account_id"),
            "date": rec.get("date"),
            "amount": float(rec.get("amount") or 0),
            "currency": rec.get("cur") or rec.get("currency"),
            "merchant_raw": rec.get("merchant") or rec.get("name"),
            "mcc": None,
            "category": None,
        })
    return {"transactions": out, "meta": {"source": "html"}}
