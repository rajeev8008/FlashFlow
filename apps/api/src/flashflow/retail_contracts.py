from datetime import datetime
from typing import Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, model_validator


class ForecastOutput(BaseModel):
    model_config = ConfigDict(extra="allow")
    expected_sales: float = Field(ge=0, allow_inf_nan=False)
    lower_bound: float = Field(ge=0, allow_inf_nan=False)
    upper_bound: float = Field(ge=0, allow_inf_nan=False)
    model_version: str = Field(min_length=1)
    fallback: bool
    interval_method: str
    horizon_minutes: int = Field(60, ge=60, le=60)
    risk_level: Literal["CRITICAL", "HIGH", "MEDIUM", "HEALTHY"] | None = None
    estimated_stockout_minutes: float | None = Field(None, ge=0, allow_inf_nan=False)
    recommended_quantity: int = Field(0, ge=0)

    @model_validator(mode="after")
    def interval_order(self):
        if not self.lower_bound <= self.expected_sales <= self.upper_bound:
            raise ValueError("Forecast interval must contain point prediction")
        return self


class ScenarioRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["NORMAL", "FLASH_SALE", "DEMAND_SPIKE", "RESTOCK"]
    product_ids: list[UUID] = Field(min_length=1, max_length=3)
    seed: int = Field(42, ge=0, le=2**31 - 1)
    bins: int = Field(24, ge=1, le=120)
    strength: float = Field(6, ge=1, le=10)
    restock_quantity: int = Field(50, ge=1, le=500)
    demo_start_stock: int | None = Field(None, ge=1, le=500)


class DecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decision: Literal["APPROVE", "REJECT"]
    operator: str = Field("Local demo operator", min_length=1, max_length=80)
    note: str = Field("", max_length=500)


class AnalystRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    question: str = Field(min_length=1, max_length=1000)
    product_id: UUID | None = None


class ToolArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    product_id: UUID | None = None
    limit: int = Field(10, ge=1, le=20)


class ToolResult(BaseModel):
    tool: str
    retrieved_at: datetime
    data: dict | list | None = None
    error: str | None = None
