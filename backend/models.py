from pydantic import BaseModel, Field


class AskRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=500)
    user_id: str = Field(..., min_length=1)
    role: str = Field(default="operations_manager")


class AskResponse(BaseModel):
    status: str
    sql: str | None = None
    summary: str | None = None      # Phase 4
    rows: list[dict] | None = None  # Phase 2 ✓
    row_count: int | None = None
    chart: dict | None = None
    error: str | None = None
    error_type: str | None = None
