ALLOWED_TABLES = {
    "GETEMPLOYEES": [
        "EMPLOYEEID", "HIREDATE", "EMPLOYMENTSTATUS", "EMPLOYEECLASS",
        "WORKFROMHOME", "LEGALENTITYCODE", "LEGALENTITYNAME",
        "WORKLOCATIONCODE", "WORKLOCATIONNAME", "COUNTRYCODE",
        "DIVISION", "FUNCTIONALAREACODE", "FUNCTIONALAREANAME",
        "VERTICALCODE", "VERTICALNAME", "PROFITCENTERCODE", "PROFITCENTERNAME",
        "CLIENTCODE", "CLIENTNAME", "COSTCENTERCODE", "COSTCENTERNAME",
        "JOBCODE", "JOBTITLE", "DEPARTMENTCODE",
        "TRAININGSTARTDATE", "TRAININGENDDATE",
        "NESTINGSTARTDATE", "NESTINGENDDATE",
        "HYBRID", "TERMINATIONDATE", "TERMEFFECTIVEDATE",
        "TERMCATEGORY", "TERMREASON",
    ],
    "GETCOSTCENTERS": [
        "COSTCENTERCODE", "COSTCENTERNAME",
        "PROFITCENTERCODE", "PROFITCENTERNAME",
        "VERTICALCODE", "VERTICALNAME",
    ],
    "GETDEPARTMENTS": [
        "DEPARTMENTCODE", "DEPARTMENTNAME",
        "PARENTDEPARTMENTCODE", "PARENTDEPARTMENTNAME",
    ],
    "GETCOUNTRIES": [
        "COUNTRYCODE", "COUNTRYNAME",
        "CURRENCYCODE", "CURRENCYNAME",
    ],
    "GETFUNCTIONALAREAS": [
        "FUNCTIONALAREACODE", "FUNCTIONALAREANAME",
    ],
    "GETPROFITCENTERS": [
        "PROFITCENTERCODE", "PROFITCENTERNAME",
        "CLIENTCODE", "CLIENTNAME",
    ],
    "GETVERTICALS": [
        "VERTICALCODE", "VERTICALNAME",
    ],
    "GETWORKLOCATIONS": [
        "WORKLOCATIONCODE", "WORKLOCATIONNAME",
        "COUNTRYCODE", "COUNTRYNAME",
        "STATECODE", "STATENAME",
    ],
}

BLOCKED_COLUMNS = [
    "FIRSTNAME", "LASTNAME", "EMAIL", "PHONE",
    "ADDRESS", "PERSONALPHONE", "WORKEMAIL", "PERSONALEMAIL",
]

def _example_queries(max_rows: int) -> str:
    return f"""
Q: How many employees were hired last month?
SQL: SELECT COUNT(EMPLOYEEID) AS hire_count FROM GETEMPLOYEES WHERE HIREDATE >= DATEADD('month', -1, CURRENT_DATE()) LIMIT {max_rows};

Q: How many active employees are in each division?
SQL: SELECT DIVISION, COUNT(EMPLOYEEID) AS headcount FROM GETEMPLOYEES WHERE EMPLOYMENTSTATUS = 'Active' GROUP BY DIVISION ORDER BY headcount DESC LIMIT {max_rows};

Q: Which countries have the most employees?
SQL: SELECT COUNTRYCODE, COUNT(EMPLOYEEID) AS headcount FROM GETEMPLOYEES WHERE EMPLOYMENTSTATUS = 'Active' GROUP BY COUNTRYCODE ORDER BY headcount DESC LIMIT {max_rows};

Q: How many employees left the company this year and what were the termination reasons?
SQL: SELECT TERMREASON, COUNT(EMPLOYEEID) AS count FROM GETEMPLOYEES WHERE TERMEFFECTIVEDATE >= DATE_TRUNC('year', CURRENT_DATE()) GROUP BY TERMREASON ORDER BY count DESC LIMIT {max_rows};
"""


def build_sql_prompt(question: str) -> str:
    from backend.config import settings

    tables_block = "\n".join(
        f"- {table}: {', '.join(cols)}"
        for table, cols in ALLOWED_TABLES.items()
    )

    return f"""You are a SQL generation assistant for a Snowflake data warehouse.
Your job is to convert natural language questions into valid Snowflake SQL queries.

## STRICT RULES — you must follow every one of these:
1. Only generate SELECT statements. Never INSERT, UPDATE, DELETE, DROP, ALTER, or CREATE.
2. Every query MUST include LIMIT {settings.max_rows} (or less).
3. Never use SELECT *. Always name specific columns.
4. Only use tables and columns from the approved list below.
5. Maximum 3 JOINs per query.
6. No subqueries in v1 — use JOINs instead.
7. No CROSS JOINs.
8. No window functions (ROW_NUMBER, RANK, LAG, etc.) in v1.
9. Do not reference blocked columns (PII): {', '.join(BLOCKED_COLUMNS)}.
10. If the question cannot be answered with the available tables/columns, say so clearly.

## APPROVED TABLES AND COLUMNS:
{tables_block}

## EXAMPLE QUERIES:
{_example_queries(settings.max_rows)}

## OUTPUT FORMAT:
Respond with ONLY the SQL query. No explanation, no markdown, no backticks.
If you cannot generate a valid query, respond with: CANNOT_GENERATE: <reason>

## QUESTION:
{question}

SQL:"""
