# Multi‑Institution Sample Set (Finance)

This pack simulates 3 different banks and multiple formats (JSON, CSV, XML, HTML/OFX).
Use it to test your Data Fetcher: connectors → parsers → transformers → canonical schema.

## Banks & Files

- bankA/
  - accounts.json         (Plaid-like JSON with balances)
  - transactions.csv      (CSV with standard headers)
  - statement.xml         (ISO20022 camt-like snippet)
  - transactions.html     (HTML table)
- bankB/
  - accounts_v2.json      (JSON with different field names)
  - tx_v2.csv             (CSV with different headers; debit_amount positive)
  - statement_v2.xml      (custom XML structure)
- bankC/
  - accounts.html         (HTML table for accounts)
  - transactions.json     (nested JSON with different keys)
  - statement.ofx         (OFX/QFX-like)

## Canonical Schema Targets (v1)

- Account
  { "account_id": "str", "type": "depository|credit|loan", "subtype": "checking|savings|...",
    "mask": "str?", "currency": "str", "current": 0.0, "available": 0.0? }

- Transaction
  { "txn_id":"str","account_id":"str","date":"YYYY-MM-DD","amount":-23.45,"currency":"USD",
    "merchant_raw":"str","merchant_norm":"str?","mcc":"str?","category":"str?" }

## Known Variations to Handle

- bankB amounts: `debit_amount` positive for outflow -> transform to negative.
- bankB JSON fields: `accounts_list[*].balance.cur|avail|ccy` map to current|available|currency.
- bankC nested JSON keys: `txns[*].when|amt|who` -> date|amount|merchant.
- HTML tables require selector-based parsing.
- XML/OFX use different paths and namespaces.

## Notes
- All data is synthetic for parser development. No PII, no real tokens.
