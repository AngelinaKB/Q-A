"""
Tests for the SQL validator — covers all rules from design doc §7.

Run with:
    pip install pytest
    pytest tests/test_validator.py -v
"""

import pytest
from backend.validator import validate_sql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def passes(sql: str) -> bool:
    return validate_sql(sql).ok

def fails_with(sql: str, error_type: str) -> bool:
    result = validate_sql(sql)
    assert not result.ok, f"Expected failure but got pass for: {sql}"
    assert result.error_type == error_type, (
        f"Expected error_type={error_type!r} but got {result.error_type!r}\n"
        f"Message: {result.message}"
    )
    return True


# ---------------------------------------------------------------------------
# 7.1  Syntax
# ---------------------------------------------------------------------------

class TestSyntax:
    def test_valid_simple_query(self):
        assert passes(
            "SELECT EMPLOYEEID, HIREDATE FROM GETEMPLOYEES LIMIT 10"
        )

    def test_invalid_syntax(self):
        assert fails_with("SELECT FROM WHERE", "SQL_SYNTAX_ERROR")

    def test_empty_string(self):
        assert fails_with("", "SQL_SYNTAX_ERROR")

    def test_multiple_statements_blocked(self):
        assert fails_with(
            "SELECT EMPLOYEEID FROM GETEMPLOYEES LIMIT 10; SELECT COUNTRYCODE FROM GETCOUNTRIES LIMIT 10",
            "SQL_MULTIPLE_STATEMENTS",
        )


# ---------------------------------------------------------------------------
# 7.2  Safety — DDL / DML blocked
# ---------------------------------------------------------------------------

class TestSafety:
    @pytest.mark.parametrize("keyword", [
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
        "CREATE", "TRUNCATE", "MERGE", "EXEC", "GRANT",
    ])
    def test_forbidden_keywords_blocked(self, keyword):
        sql = f"{keyword} INTO GETEMPLOYEES VALUES (1)"
        result = validate_sql(sql)
        assert not result.ok
        assert result.error_type == "SQL_UNSAFE_OPERATION"

    def test_delete_in_comment_still_blocked(self):
        # Regex catches keyword even in comments — intentionally conservative
        sql = "SELECT EMPLOYEEID FROM GETEMPLOYEES -- DELETE this later\nLIMIT 10"
        result = validate_sql(sql)
        assert not result.ok
        assert result.error_type == "SQL_UNSAFE_OPERATION"


# ---------------------------------------------------------------------------
# 7.3  Structural rules
# ---------------------------------------------------------------------------

class TestStructure:
    def test_missing_limit_blocked(self):
        assert fails_with(
            "SELECT EMPLOYEEID FROM GETEMPLOYEES",
            "SQL_MISSING_LIMIT",
        )

    def test_limit_over_1000_blocked(self):
        assert fails_with(
            "SELECT EMPLOYEEID FROM GETEMPLOYEES LIMIT 1001",
            "SQL_LIMIT_EXCEEDED",
        )

    def test_limit_exactly_1000_passes(self):
        assert passes("SELECT EMPLOYEEID FROM GETEMPLOYEES LIMIT 1000")

    def test_select_star_blocked(self):
        assert fails_with(
            "SELECT * FROM GETEMPLOYEES LIMIT 10",
            "SQL_SELECT_STAR",
        )

    def test_subquery_blocked(self):
        assert fails_with(
            "SELECT EMPLOYEEID FROM (SELECT EMPLOYEEID FROM GETEMPLOYEES) LIMIT 10",
            "SQL_SUBQUERY_NOT_ALLOWED",
        )

    def test_cross_join_blocked(self):
        assert fails_with(
            "SELECT e.EMPLOYEEID FROM GETEMPLOYEES e CROSS JOIN GETCOUNTRIES c LIMIT 10",
            "SQL_CROSS_JOIN_NOT_ALLOWED",
        )

    def test_three_joins_passes(self):
        sql = """
        SELECT e.EMPLOYEEID, d.DEPARTMENTNAME, c.COUNTRYNAME, f.FUNCTIONALAREANAME
        FROM GETEMPLOYEES e
        JOIN GETDEPARTMENTS d ON e.DEPARTMENTCODE = d.DEPARTMENTCODE
        JOIN GETCOUNTRIES c ON e.COUNTRYCODE = c.COUNTRYCODE
        JOIN GETFUNCTIONALAREAS f ON e.FUNCTIONALAREACODE = f.FUNCTIONALAREACODE
        LIMIT 100
        """
        assert passes(sql)

    def test_four_joins_blocked(self):
        sql = """
        SELECT e.EMPLOYEEID
        FROM GETEMPLOYEES e
        JOIN GETDEPARTMENTS d ON e.DEPARTMENTCODE = d.DEPARTMENTCODE
        JOIN GETCOUNTRIES c ON e.COUNTRYCODE = c.COUNTRYCODE
        JOIN GETFUNCTIONALAREAS f ON e.FUNCTIONALAREACODE = f.FUNCTIONALAREACODE
        JOIN GETVERTICALS v ON e.VERTICALCODE = v.VERTICALCODE
        LIMIT 10
        """
        assert fails_with(sql, "SQL_TOO_MANY_JOINS")

    def test_window_function_blocked(self):
        assert fails_with(
            "SELECT EMPLOYEEID, ROW_NUMBER() OVER (PARTITION BY DIVISION ORDER BY HIREDATE) AS rn FROM GETEMPLOYEES LIMIT 10",
            "SQL_WINDOW_FUNCTION_NOT_ALLOWED",
        )


# ---------------------------------------------------------------------------
# 7.4  Column validation
# ---------------------------------------------------------------------------

class TestColumns:
    def test_allowed_columns_pass(self):
        assert passes(
            "SELECT EMPLOYEEID, HIREDATE, DIVISION FROM GETEMPLOYEES LIMIT 10"
        )

    def test_pii_firstname_blocked(self):
        assert fails_with(
            "SELECT FIRSTNAME FROM GETEMPLOYEES LIMIT 10",
            "SQL_PII_COLUMN_BLOCKED",
        )

    def test_pii_lastname_blocked(self):
        assert fails_with(
            "SELECT LASTNAME FROM GETEMPLOYEES LIMIT 10",
            "SQL_PII_COLUMN_BLOCKED",
        )

    def test_pii_email_blocked(self):
        assert fails_with(
            "SELECT EMAIL FROM GETEMPLOYEES LIMIT 10",
            "SQL_PII_COLUMN_BLOCKED",
        )

    def test_unknown_column_blocked(self):
        assert fails_with(
            "SELECT SALARY FROM GETEMPLOYEES LIMIT 10",
            "SQL_COLUMN_NOT_ALLOWED",
        )

    def test_unknown_table_blocked(self):
        assert fails_with(
            "SELECT ID FROM USERS LIMIT 10",
            "SQL_TABLE_NOT_ALLOWED",
        )

    def test_lookup_table_allowed(self):
        assert passes(
            "SELECT DEPARTMENTCODE, DEPARTMENTNAME FROM GETDEPARTMENTS LIMIT 50"
        )

    def test_join_with_valid_columns_passes(self):
        sql = """
        SELECT e.EMPLOYEEID, e.HIREDATE, d.DEPARTMENTNAME
        FROM GETEMPLOYEES e
        JOIN GETDEPARTMENTS d ON e.DEPARTMENTCODE = d.DEPARTMENTCODE
        LIMIT 100
        """
        assert passes(sql)


# ---------------------------------------------------------------------------
# Real-world question examples (end-to-end validator checks)
# ---------------------------------------------------------------------------

class TestRealWorldQueries:
    def test_headcount_by_division(self):
        assert passes("""
            SELECT DIVISION, COUNT(EMPLOYEEID) AS headcount
            FROM GETEMPLOYEES
            WHERE EMPLOYMENTSTATUS = 'Active'
            GROUP BY DIVISION
            ORDER BY headcount DESC
            LIMIT 1000
        """)

    def test_terminations_this_year(self):
        assert passes("""
            SELECT TERMREASON, COUNT(EMPLOYEEID) AS count
            FROM GETEMPLOYEES
            WHERE TERMEFFECTIVEDATE >= DATE_TRUNC('year', CURRENT_DATE())
            GROUP BY TERMREASON
            ORDER BY count DESC
            LIMIT 1000
        """)

    def test_hires_last_month(self):
        assert passes("""
            SELECT COUNT(EMPLOYEEID) AS hire_count
            FROM GETEMPLOYEES
            WHERE HIREDATE >= DATEADD('month', -1, CURRENT_DATE())
            LIMIT 1000
        """)
