from typing import Any, Dict, List
from pydantic import BaseModel, Field, ValidationError

class AccountBalance(BaseModel):
    account_id: str
    current: float
    available: float | None = None
    currency: str = Field(default="USD")

class BalanceResponse(BaseModel):
    accounts: List[AccountBalance]

def validate_balance(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        validated = BalanceResponse.model_validate(payload)
        return validated.model_dump()
    except ValidationError as e:
        # Map to your platform's error format or raise upstream
        raise ValueError(f"Schema validation failed: {e}") from e
