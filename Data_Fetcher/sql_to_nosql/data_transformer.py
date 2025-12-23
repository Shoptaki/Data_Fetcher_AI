import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import pandas as pd

# --- Core config structures (from dict, not YAML) ---
@dataclass
class FieldRule:
    candidates: List[str]
    cast: Optional[str] = None
    default: Optional[Any] = None

@dataclass
class MappingConfig:
    source_kind: str        # "sql" or "nosql"
    target_kind: str        # "nosql" or "sql"
    fields: Dict[str, FieldRule]
    rules: Dict[str, Any]   # e.g., {"trim_strings": True, "drop_null_target_fields": True}

def parse_mapping(raw: Dict[str, Any]) -> MappingConfig:
    fields_cfg = {
        tgt: FieldRule(
            candidates=list(spec.get("candidates", [])),
            cast=spec.get("cast"),
            default=spec.get("default"),
        )
        for tgt, spec in (raw.get("fields") or {}).items()
    }
    return MappingConfig(
        source_kind=raw.get("source_kind", ""),
        target_kind=raw.get("target_kind", ""),
        fields=fields_cfg,
        rules=raw.get("rules", {}),
    )

# --- Transform helpers ---
CAST_NUMERIC_RE = re.compile(r"decimal\((\d+),(\d+)\)", re.IGNORECASE)

def apply_cast(value: Any, cast_spec: Optional[str]) -> Any:
    if value is None or not cast_spec:
        return value
    cs = cast_spec.lower()
    try:
        if cs in {"int", "integer"}: return int(value)
        if cs in {"float", "double"}: return float(value)
        m = CAST_NUMERIC_RE.match(cs)
        if m: return round(float(value), int(m.group(2)))  # simplify; use Decimal in prod
    except Exception:
        pass
    return value

def first_present(row: Dict[str, Any], candidates: List[str]) -> Any:
    for name in candidates:
        if name in row and row[name] is not None:
            return row[name]
    return None

def transform_record(row: Dict[str, Any], cfg: MappingConfig) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for target_field, fr in cfg.fields.items():
        raw_val = first_present(row, fr.candidates)
        if raw_val is None and fr.default is not None:
            raw_val = fr.default
        val = apply_cast(raw_val, fr.cast)
        if isinstance(val, str) and cfg.rules.get("trim_strings"):
            val = val.strip()
        if not (val is None and cfg.rules.get("drop_null_target_fields")):
            out[target_field] = val
    return out

# --- Public core APIs you can call ---
def dataframe_to_docs(df: pd.DataFrame, cfg: MappingConfig) -> List[Dict[str, Any]]:
    return [transform_record(r, cfg) for r in df.to_dict(orient="records")]

def select_and_rename_columns(df: pd.DataFrame, cfg: MappingConfig) -> pd.DataFrame:
    # For NoSQL->SQL direction (pick/rename columns deterministically)
    if df.empty: return df
    out = pd.DataFrame()
    for tgt, fr in cfg.fields.items():
        for c in fr.candidates:
            if c in df.columns:
                col = df[c]
                if cfg.rules.get("trim_strings") and col.dtype == "object":
                    col = col.apply(lambda x: x.strip() if isinstance(x, str) else x)
                out[tgt] = col
                break
        else:
            out[tgt] = fr.default
        if fr.cast:
            out[tgt] = out[tgt].apply(lambda v: apply_cast(v, fr.cast))
    if cfg.rules.get("drop_null_target_fields"):
        out = out.dropna(axis=1, how="all")
    return out

