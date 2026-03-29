from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from backend.llm import generate_sql, LLMError


app = FastAPI(
    title="Ops Q&A API",
    description="Natural language → SQL → Snowflake for the operations team.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React dev server
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


# ---------- Request / Response models ----------

class AskRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=500)
    user_id: str = Field(..., min_length=1)
    role: str = Field(default="operations_manager")


class AskResponse(BaseModel):
    status: str
    sql: str | None = None
    summary: str | None = None   # populated in Phase 4
    rows: list[dict] | None = None  # populated in Phase 2
    chart: dict | None = None
    error: str | None = None
    error_type: str | None = None


# ---------- Routes ----------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest):
    # Phase 1: generate SQL and return it — no execution yet
    try:
        sql = generate_sql(req.question)
    except LLMError as e:
        raise HTTPException(status_code=502, detail=str(e))

    # In Phase 1 we just print/return the SQL for inspection
    print(f"\n[Phase 1] Question : {req.question}")
    print(f"[Phase 1] Generated SQL:\n{sql}\n")

    return AskResponse(
        status="success",
        sql=sql,
        summary="(summarization not yet implemented — Phase 4)",
        rows=None,
    )
