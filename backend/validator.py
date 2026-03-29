"""
SQL Validator — Phase 3

Implements all rules from design doc §7 in strict order:
  7.1 Syntax validation
  7.2 Safety checks (block DDL / DML)
  7.3 Structural rules (LIMIT, SELECT *, joins)
  7.4 Column validation (no PII, only allowlisted columns)

Every check is deterministic and runs before any Snowflake call.
The validator never retries — a failure is a hard stop (design doc §9).
"""

import re
import sqlglot
import sqlglot.expressions as exp

from backend.prompt import ALLOWED_TABLES, BLOCKED_COLUMNS


# ---------------------------------------------------------------------------
# Validation result
# ---------------------------------------------------------------------------

class ValidationResult:
    def __init__(self, ok: bool, error_type: str = "", message: str = ""):
        self.ok = ok
        self.error_type = error_type
        self.message = message

    def __bool__(self):
        return self.ok

    @classmethod
    def pass_(cls) -> "ValidationResult":
        return cls(ok=True)

    @classmethod
    def fail(cls, error_type: str, message: str) -> "ValidationResult":
        return cls(ok=False, error_type=error_type, message=message)


# ---------------------------------------------------------------------------
# 7.1  Syntax validation
# ---------------------------------------------------------------------------

def _check_syntax(sql: str) -> ValidationResult:
    try:
        statements = sqlglot.parse(sql, dialect="snowflake")
    except sqlglot.errors.ParseError as e:
        return ValidationResult.fail(
            "SQL_SYNTAX_ERROR",
            f"SQL failed to parse: {e}",
        )

    if not statements or statements[0] is None:
        return ValidationResult.fail("SQL_SYNTAX_ERROR", "Empty or unparseable SQL")

    if len(statements) > 1:
        return ValidationResult.fail(
            "SQL_MULTIPLE_STATEMENTS",
            "Only a single SQL statement is allowed",
        )

    return ValidationResult.pass_()


# ---------------------------------------------------------------------------
# 7.2  Safety checks — block any DDL / DML keyword
# ---------------------------------------------------------------------------

_FORBIDDEN_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|MERGE|REPLACE"
    r"|EXEC|EXECUTE|CALL|GRANT|REVOKE|COPY)\b",
    re.IGNORECASE,
)

def _check_safety(sql: str) -> ValidationResult:
    match = _FORBIDDEN_KEYWORDS.search(sql)
    if match:
        return ValidationResult.fail(
            "SQL_UNSAFE_OPERATION",
            f"Forbidden operation '{match.group().upper()}' is not allowed",
        )
    return ValidationResult.pass_()


# ---------------------------------------------------------------------------
# 7.3  Structural rules
# ---------------------------------------------------------------------------

def _check_structure(sql: str) -> ValidationResult:
    tree = sqlglot.parse_one(sql, dialect="snowflake")

    # Must be a SELECT
    if not isinstance(tree, exp.Select):
        return ValidationResult.fail(
            "SQL_NOT_SELECT",
            "Only SELECT statements are allowed",
        )

    # No SELECT *
    for col in tree.find_all(exp.Star):
        return ValidationResult.fail(
            "SQL_SELECT_STAR",
            "SELECT * is not allowed — specify column names explicitly",
        )

    # Must have LIMIT ≤ 1000
    limit_node = tree.find(exp.Limit)
    if limit_node is None:
        return ValidationResult.fail(
            "SQL_MISSING_LIMIT",
            "Query must include a LIMIT clause (max 1000)",
        )

    try:
        limit_val = int(limit_node.expression.this)
    except (AttributeError, ValueError, TypeError):
        return ValidationResult.fail(
            "SQL_INVALID_LIMIT",
            "LIMIT value could not be parsed",
        )

    if limit_val > 1000:
        return ValidationResult.fail(
            "SQL_LIMIT_EXCEEDED",
            f"LIMIT {limit_val} exceeds the maximum of 1000",
        )

    # No subqueries
    for subquery in tree.find_all(exp.Subquery):
        return ValidationResult.fail(
            "SQL_SUBQUERY_NOT_ALLOWED",
            "Subqueries are not allowed in v1 — use JOINs instead",
        )

    # No CROSS JOIN
    for join in tree.find_all(exp.Join):
        if join.args.get("kind") and join.args["kind"].upper() == "CROSS":
            return ValidationResult.fail(
                "SQL_CROSS_JOIN_NOT_ALLOWED",
                "CROSS JOINs are not allowed",
            )

    # Max 3 joins
    joins = list(tree.find_all(exp.Join))
    if len(joins) > 3:
        return ValidationResult.fail(
            "SQL_TOO_MANY_JOINS",
            f"Query has {len(joins)} JOINs; maximum is 3",
        )

    # No window functions
    for window in tree.find_all(exp.Window):
        return ValidationResult.fail(
            "SQL_WINDOW_FUNCTION_NOT_ALLOWED",
            "Window functions (ROW_NUMBER, RANK, LAG, etc.) are not allowed in v1",
        )

    return ValidationResult.pass_()


# ---------------------------------------------------------------------------
# 7.4  Column validation — allowlist + PII blocklist
# ---------------------------------------------------------------------------

_ALLOWED_TABLES_UPPER = {
    t.upper(): {c.upper() for c in cols}
    for t, cols in ALLOWED_TABLES.items()
}
_BLOCKED_COLUMNS_UPPER = {c.upper() for c in BLOCKED_COLUMNS}
_ALL_ALLOWED_COLUMNS = {c for cols in _ALLOWED_TABLES_UPPER.values() for c in cols}


def _check_columns(sql: str) -> ValidationResult:
    tree = sqlglot.parse_one(sql, dialect="snowflake")

    # Collect all table references used in the query
    used_tables = set()
    for table in tree.find_all(exp.Table):
        used_tables.add(table.name.upper())

    # All tables must be allowlisted
    for table in used_tables:
        if table not in _ALLOWED_TABLES_UPPER:
            return ValidationResult.fail(
                "SQL_TABLE_NOT_ALLOWED",
                f"Table '{table}' is not in the approved table list",
            )

    # Build set of columns allowed across all referenced tables
    allowed_for_query = set()
    for table in used_tables:
        allowed_for_query.update(_ALLOWED_TABLES_UPPER[table])

    # Check every column reference
    for col in tree.find_all(exp.Column):
        col_name = col.name.upper()

        # Block PII columns regardless of table
        if col_name in _BLOCKED_COLUMNS_UPPER:
            return ValidationResult.fail(
                "SQL_PII_COLUMN_BLOCKED",
                f"Column '{col_name}' is blocked — it contains PII",
            )

        # Column must belong to at least one of the referenced tables
        if col_name not in allowed_for_query:
            return ValidationResult.fail(
                "SQL_COLUMN_NOT_ALLOWED",
                f"Column '{col_name}' is not in the approved column list "
                f"for the tables used in this query",
            )

    return ValidationResult.pass_()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def validate_sql(sql: str) -> ValidationResult:
    """
    Run all validation checks in order. Returns on first failure.
    A passing result means the SQL is safe to send to Snowflake.
    """
    for check in [_check_syntax, _check_safety, _check_structure, _check_columns]:
        result = check(sql)
        if not result:
            return result

    return ValidationResult.pass_()
