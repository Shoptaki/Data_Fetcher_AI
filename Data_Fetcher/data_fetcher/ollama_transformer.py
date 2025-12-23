#!/usr/bin/env python3
"""
Ollama Data-Format Transformer — Local Test Harness
- Detects input type (CSV/HTML/XML/JSON)
- Calls Ollama /api/chat with JSON Schema ("format")
- Parses + minimally validates output
- Runs demo tests
Usage:
  python ollama_transformer_test.py
Requirements:
  - Ollama running locally (http://localhost:11434), with model pulled:
      ollama pull llama3.2:1b
"""

import json
import re
from typing import Any, Dict, List
import urllib.request
from urllib.error import URLError, HTTPError
from colorama import Fore, Style, init
init(autoreset=True)  # so colors reset automatically
from rich import print_json


OLLAMA_URL = ""
MODEL = "llama3.2:3b"

# ---- Define your canonical JSON schema here ----
SCHEMA = {
  "type": "object",
  "properties": {
    "account_id": {"type": ["string","null"]},
    "transactions": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "tx_id":       {"type": ["string","null"]},
          "date":        {"type": ["string","null"]},
          "amount":      {"type": ["number","null"]},
          "currency":    {"type": ["string","null"]},
          "merchant":    {"type": ["string","null"]},
          "description": {"type": ["string","null"]},
          "category":    {"type": ["string","null"]}
        },
        "required": ["date","amount","currency","merchant","description"]
      }
    }
  },
  "required": ["account_id","transactions"]
}

SYSTEM_PROMPT = (
    "You are a strict data transformer. You will receive messy CSV/HTML/XML/JSON snippets. "
    "Follow the rules below strictly when generating the result:\n"
    
    "1. Output only normalized JSON that exactly matches the given schema, with no explanatory text.\n"
    "2. Do not add fields not defined by the schema; field names must match the schema.\n"
    "3. Do not drop any source rows: output one transaction per row and preserve the original order.\n"
    "4. When a field is missing or unknown, return null (do not use 0 or empty string; if amount is missing, do not write 0).\n"
    "5. Infer header meaning from the values: natural-language narrative text maps to 'description'; three-letter codes (e.g., USD/CAD/EUR) map to 'currency'.\n"
    "6. Do not omit non-blank cells: if a cell has a value, map it to the corresponding field; if the narrative text contains a merchant name (e.g., 'Starbucks', 'Shell'), copy it to 'merchant'.\n"
    "7. Never fabricate or guess; if uncertain, use null.\n"
    "8. The result must be strict JSON and all structures and types must satisfy the schema (fields that allow null may return null).\n"
    "9. If a row has a numeric value that looks like money, always map it to 'amount' even if the currency cell is blank. "
    "Missing currency must be null, but amount must still keep the numeric value.\n"
)

def detect_input_type(text: str) -> str:
    t = text.strip()
    if t.startswith("{") or t.startswith("["):
        return "json"
    if t.startswith("<"):
        # naive: treat as HTML/XML
        return "xml_or_html"
    # simple CSV heuristic: presence of commas across multiple lines
    lines = [ln for ln in t.splitlines() if ln.strip()]
    comma_lines = sum(1 for ln in lines if ("," in ln or ";" in ln))
    if comma_lines >= max(1, len(lines)//2):
        return "csv"
    return "plain"

def build_user_prompt(raw: str, account_hint: str = "") -> str:
    dtype = detect_input_type(raw)
    header = f"Source type: {dtype}\n"
    if account_hint:
        header += f"Account hint: {account_hint}\n"
    header += "-----\nSOURCE:\n"
    return header + raw

def call_ollama(messages: List[Dict[str, str]], schema: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        "model": MODEL,
        "stream": False,
        "format": schema,
        "messages": messages
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        OLLAMA_URL,
        data=data,
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            body = resp.read().decode("utf-8")
            obj = json.loads(body)
            # Ollama wraps the model output inside obj["message"]["content"]
            content = obj.get("message", {}).get("content", "")
            # content should itself be JSON (string). If it's already a dict, handle gracefully.
            if isinstance(content, str):
                try:
                    return json.loads(content)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Model content is not valid JSON: {content[:200]}...") from e
            elif isinstance(content, dict):
                return content
            else:
                raise ValueError(f"Unexpected content type: {type(content)}")
    except HTTPError as e:
        raise RuntimeError(f"HTTP error from Ollama: {e.code} {e.reason}") from e
    except URLError as e:
        raise RuntimeError(f"Cannot reach Ollama at {OLLAMA_URL}. Is it running?") from e

def _type_ok(value, expected):
    def single(v, t):
        if t == "null":   return v is None
        if t == "string": return isinstance(v, str)
        if t == "number": return isinstance(v, (int, float)) and not isinstance(v, bool)
        if t == "boolean":return isinstance(v, bool)
        if t == "array":  return isinstance(v, list)
        if t == "object": return isinstance(v, dict)
        return True
    if isinstance(expected, list):
        return any(single(value, t) for t in expected)
    return single(value, expected)

def validate_minimal(obj: Any, schema: Dict[str, Any]) -> List[str]:
    """Very small validator for common cases; replace with 'jsonschema' for full checks."""
    errors: List[str] = []

    def check(o: Any, s: Dict[str, Any], path: str):
        stype = s.get("type")
        if stype and not _type_ok(o, stype):
            errors.append(f"{path}: expected {stype}, got {type(o).__name__}")
            return
        if stype == "object":
            props = s.get("properties", {})
            required = s.get("required", [])
            for req in required:
                if not isinstance(o, dict) or req not in o:
                    errors.append(f"{path}.{req}: missing required")
            if isinstance(o, dict):
                for k, v in o.items():
                    if k in props:
                        check(v, props[k], f"{path}.{k}")
        if stype == "array":
            item_schema = s.get("items")
            if isinstance(o, list) and item_schema:
                for i, item in enumerate(o):
                    check(item, item_schema, f"{path}[{i}]")

    check(obj, schema, "$")
    return errors

def transform(raw_source: str) -> Dict[str, Any]:
    user_prompt = build_user_prompt(raw_source)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt}
    ]
    result = call_ollama(messages, SCHEMA)
    errs = validate_minimal(result, SCHEMA)

    if errs:
        raise ValueError("Schema validation failed: \n  - " + "\n  - ".join(errs))
    return result

