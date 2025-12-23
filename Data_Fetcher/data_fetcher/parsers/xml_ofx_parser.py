from __future__ import annotations
from typing import Dict, Any, List, Optional
import xml.etree.ElementTree as ET

NS_CAMT = {"c": "urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"}

def parse_camt_accounts(xml_text: str) -> Dict[str, Any]:
    """
    Minimal extraction from camt.053 snippet for balances.
    """
    out: List[Dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_text)
        # Find balance
        amt_el = root.find(".//c:Bal/c:Amt", NS_CAMT)
        if amt_el is not None:
            out.append({
                "account_id": None,
                "type": "depository",
                "subtype": "checking",
                "mask": None,
                "currency": amt_el.attrib.get("Ccy"),
                "current": float(amt_el.text or 0),
                "available": None,
                "name": "Statement Balance",
            })
    except ET.ParseError:
        pass
    return {"accounts": out, "meta": {"source": "xml", "format": "camt.053"}}

def parse_custom_bankB_statement(xml_text: str) -> Dict[str, Any]:
    """
    Parses BankB custom XML with Accounts/Transactions.
    """
    out_accounts, out_tx = [], []
    root = ET.fromstring(xml_text)
    acct = root.find(".//Account")
    if acct is not None:
        acct_id = acct.attrib.get("id")
        ccy = acct.attrib.get("currency")
        bal = acct.find("./Balance")
        out_accounts.append({
            "account_id": acct_id,
            "type": "depository",
            "subtype": "checking",
            "mask": None,
            "currency": ccy,
            "current": float(bal.attrib.get("current")) if bal is not None else None,
            "available": float(bal.attrib.get("available")) if bal is not None else None,
            "name": "Account",
        })
        for tx in acct.findall(".//Transactions/Tx"):
            out_tx.append({
                "txn_id": None,
                "account_id": acct_id,
                "date": (tx.attrib.get("d") or "")[:10],
                "amount": float(tx.attrib.get("amt") or 0),
                "currency": ccy,
                "merchant_raw": tx.attrib.get("m"),
                "mcc": None,
                "category": None,
            })
    return {"accounts": out_accounts, "transactions": out_tx, "meta": {"source": "xml", "format": "custom"}}

def parse_ofx_transactions(xml_text: str) -> Dict[str, Any]:
    """
    Simple OFX/QFX transaction parser (non-namespace).
    """
    root = ET.fromstring(xml_text)
    items: List[Dict[str, Any]] = []
    ccy = (root.find(".//CURDEF").text if root.find(".//CURDEF") is not None else "USD")
    for st in root.findall(".//STMTTRN"):
        date = st.findtext("DTPOSTED", default="")
        if len(date) >= 8:
            date = f"{date[0:4]}-{date[4:6]}-{date[6:8]}"
        amt = float(st.findtext("TRNAMT", default="0"))
        name = st.findtext("NAME", default=None)
        items.append({
            "txn_id": None,
            "account_id": None,
            "date": date,
            "amount": amt,
            "currency": ccy,
            "merchant_raw": name,
            "mcc": None,
            "category": None,
        })
    return {"transactions": items, "meta": {"source": "ofx"}}
