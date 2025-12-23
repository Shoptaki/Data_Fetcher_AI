# data_fetcher/utils/bank_router.py
import yaml
from pathlib import Path

_PROFILE = yaml.safe_load((Path(__file__).parents[2] / "config" / "bank_profiles.yaml").read_text())

def resolve(institution_id: str) -> dict:
    inst = _PROFILE["institutions"].get(institution_id)
    if not inst:
        raise ValueError(f"Unknown institution_id: {institution_id}")
    return inst


