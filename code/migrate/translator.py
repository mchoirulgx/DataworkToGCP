"""ODPS (MaxCompute) SQL -> BigQuery/Dataform translation.

The repo knowledge base (docs/03 step 5, docs/07 appendix B) is the contract:
MaxCompute SQL is Hive-derived, so translation is a conservative, rule-based
rewrite. Anything uncertain is *flagged*, not silently rewritten -- the compile
and verify gates then decide. This keeps human review to a minimum without
silently shipping wrong SQL.
"""

from __future__ import annotations

import os
import re

# MaxCompute -> BigQuery type map (docs/07 appendix B).
# DATETIME can map to either DATETIME or TIMESTAMP depending on the target
# semantics (guide appendix B: "DATETIME or TIMESTAMP -- choose per timezone
# handling"). Default is DATETIME; set DATETIME_MAPS_TO=TIMESTAMP to change.
_DATETIME_TARGET = os.getenv("DATETIME_MAPS_TO", "DATETIME").upper()
_TYPE_MAP: dict[str, str] = {
    "TINYINT": "INT64",
    "SMALLINT": "INT64",
    "INT": "INT64",
    "BIGINT": "INT64",
    "FLOAT": "FLOAT64",
    "DOUBLE": "FLOAT64",
    "STRING": "STRING",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "DATE": "DATE",
    "DATETIME": _DATETIME_TARGET,
    "TIMESTAMP": "TIMESTAMP",
    "BOOLEAN": "BOOL",
    "BINARY": "BYTES",
}

# Expose as module-level constant for callers.
TYPE_MAP = _TYPE_MAP

# BigQuery reserved words commonly used as bare identifiers in ODPS code.
BIGQUERY_RESERVED = {
    "default", "rank", "row", "rows", "group", "order", "over",
    "range", "current", "partition", "window", "values", "source",
}

# Safe ODPS built-in -> BigQuery rewrites. Deliberately small.
FUNCTION_MAP: dict[str, str] = {
    "NVL": "IFNULL",
    "IF": "IF",
    "COALESCE": "COALESCE",
    "CONCAT": "CONCAT",
    "LENGTH": "LENGTH",
    "UPPER": "UPPER",
    "LOWER": "LOWER",
    "TRIM": "TRIM",
    "ABS": "ABS",
    "ROUND": "ROUND",
    "GREATEST": "GREATEST",
    "LEAST": "LEAST",
    "REGEXP_REPLACE": "REGEXP_REPLACE",
    "REGEXP_EXTRACT": "REGEXP_EXTRACT",
    "TO_DATE": "DATE",
}

# Built-ins whose semantics differ (units, ordering, nullability) -> flag.
FLAG_FUNCTIONS = {
    "DATEADD", "DATEDIFF", "DATEPART", "UNIX_TIMESTAMP", "FROM_UNIXTIME",
    "SPLIT_PART", "WM_CONCAT", "MAPJOIN", "LATERAL", "EXPLODE", "STACK",
}


def translate_type(odps_type: str) -> str:
    """Map a MaxCompute column type to its BigQuery equivalent."""
    t = odps_type.strip().upper()
    if t in TYPE_MAP:
        return TYPE_MAP[t]
    m = re.match(r"(DECIMAL|NUMERIC)\(\s*(\d+)\s*,\s*(\d+)\s*\)", t)
    if m:
        return "BIGNUMERIC" if int(m.group(2)) > 38 else "NUMERIC"
    m = re.match(r"(VARCHAR|CHAR)\(\s*\d+\s*\)", t)
    if m:
        return "STRING"
    m = re.match(r"MAP<(.+),\s*(.+)>", t)
    if m:
        return f"ARRAY<STRUCT<key {translate_type(m.group(1))}, value {translate_type(m.group(2))}>>"
    m = re.match(r"ARRAY<(.+)>", t)
    if m:
        return f"ARRAY<{translate_type(m.group(1))}>"
    m = re.match(r"STRUCT<(.+)>", t)
    if m:
        return f"STRUCT<{m.group(1)}>"
    return t  # unknown: keep verbatim so the compile gate can flag it


def _split_top_level(text: str, sep: str = ",") -> list[str]:
    """Split on sep, ignoring occurrences inside ()/[]/''/\"\"."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    quote: str | None = None
    for ch in text:
        if quote:
            current.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in ("'", '"'):
            quote = ch
            current.append(ch)
            continue
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth = max(0, depth - 1)
        if ch == sep and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current or parts:
        parts.append("".join(current).strip())
    return [p for p in parts if p]


# ---------------------------------------------------------------------------
# Statement parsing
# ---------------------------------------------------------------------------
STATEMENT_KINDS = ("CREATE TABLE", "INSERT OVERWRITE TABLE", "INSERT INTO",
                   "CREATE EXTERNAL TABLE", "DROP TABLE", "TRUNCATE TABLE",
                   "ALTER TABLE")


def parse_statements(content: str) -> list[dict]:
    """Classify the statements in an ODPS node body."""
    statements = _split_top_level(content, ";")
    out = []
    for stmt in statements:
        s = stmt.strip()
        if not s:
            continue
        upper = s.upper()
        kind = next((k for k in STATEMENT_KINDS if upper.startswith(k)), None)
        if kind in ("DROP TABLE", "TRUNCATE TABLE"):
            out.append({"kind": kind.lower(), "raw": s})
            continue
        if kind == "CREATE TABLE" or upper.startswith("CREATE TABLE IF NOT EXISTS"):
            out.append({"kind": "create", "raw": s})
            continue
        if kind in ("INSERT OVERWRITE TABLE", "INSERT INTO"):
            out.append({"kind": "insert", "raw": s})
            continue
        if kind is None:
            out.append({"kind": "query", "raw": s})
            continue
        out.append({"kind": "other", "raw": s})
    return out


CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.$]+)\s*\((.*?)\)"
    r"(?:\s+PARTITIONED\s*\([^)]*\))?\s*(?:\s+COMMENT\s+['\"][^'\"]*['\"])?\s*$",
    re.DOTALL | re.IGNORECASE,
)
COLUMN_RE = re.compile(r"^\s*([`\w]+)\s+([^'\"]+?)\s*(?:COMMENT\s+['\"](.*?)['\"])?\s*$",
                       re.DOTALL | re.IGNORECASE)

INSERT_RE = re.compile(
    r"INSERT\s+(?:OVERWRITE|INTO)\s+TABLE\s+([\w.$]+)"
    r"(?:\s+PARTITION\s*\([^)]*\))?\s*(.+)$",
    re.DOTALL | re.IGNORECASE,
)


def parse_create(content: str) -> dict | None:
    """Return {name, columns: [(name, bq_type, comment)], flags} or None."""
    m = CREATE_TABLE_RE.match(content.strip())
    if not m:
        return None
    name = m.group(1).split(".")[-1]
    columns = []
    flags = []
    for col in _split_top_level(m.group(2)):
        cm = COLUMN_RE.match(col)
        if not cm:
            flags.append(f"Unparsed column def: {col!r}")
            continue
        colname, coltype, comment = cm.group(1), cm.group(2), cm.group(3)
        bqtype = translate_type(coltype)
        if bqtype.upper() == coltype.strip().upper() and coltype.strip().upper() not in TYPE_MAP:
            flags.append(f"Unknown type {coltype!r} on {colname} kept verbatim")
        columns.append((colname.lstrip("`").rstrip("`"), bqtype, comment or ""))
    return {"name": name, "columns": columns, "flags": flags}


def parse_insert(content: str) -> dict | None:
    """Return {table, query, flags} for INSERT OVERWRITE/INTO TABLE ... SELECT."""
    m = INSERT_RE.match(content.strip())
    if not m:
        return None
    return {"table": m.group(1).split(".")[-1], "query": m.group(2).strip(), "flags": []}


# ---------------------------------------------------------------------------
# Query translation
# ---------------------------------------------------------------------------
VAR_RE = re.compile(r"\$\{(\w+)\}")
# Bracket-notation variables: $[yyyyymmdd] — supports day offsets, e.g.
# $[yyyymmdd-1] (guide Appendix A: "supports date/time arithmetic").
BRACKET_VAR_RE = re.compile(r"\$\[([A-Za-z_]\w*)(\s*[+-]\d+)?\]")
TABLE_REF_RE = re.compile(
    r"(?i)\b(from|join|into)\s+([`a-zA-Z_][\w.$]*)"
)

# DataWorks var (docs/07 appendix A) -> Dataform project vars reference.
KNOWN_VARS = {"bizdate", "yyyymmdd", "cyctime"}
# Day-offset arithmetic only makes sense for YYYYMMDD-shaped date vars.
DATE_VARS = {"bizdate", "yyyymmdd"}


def _bracket_offset_js(var_name: str, days: int) -> str:
    """Dataform SQLX JS expression: date var + days offset, formatted YYYYMMDD.

    Emits ``${...}`` — Dataform evaluates the JS at compile time and inlines the
    returned string, so it works both bare and inside single quotes.
    """
    op = "+" if days >= 0 else "-"
    js = (
        "(() => {"
        f'const b=dataform.projectConfig.vars["{var_name}"]'
        ".replace(/(\\d{4})(\\d{2})(\\d{2})/,'$1-$2-$3');"
        "const d=new Date(b+'T00:00:00Z');"
        f"d.setUTCDate(d.getUTCDate(){op}{abs(days)});"
        "const p=n=>String(n).padStart(2,'0');"
        "return d.getUTCFullYear()+p(d.getUTCMonth()+1)+p(d.getUTCDate());"
        "})()"
    )
    return f"${{{js}}}"


def translate_query(query: str, vars_available: bool = False,
                    available_vars: set[str] | None = None) -> tuple[str, list[str]]:
    """Translate an ODPS SELECT body to GoogleSQL. Returns (sql, flags).

    When *available_vars* is provided (a set of variable names declared in the
    workflow), those are treated as resolved and rewritten to
    ``${{dataform.projectConfig.vars["name"]}}`` — no review flag is emitted.
    Otherwise the legacy *vars_available* + *KNOWN_VARS* path is used.
    """
    flags: list[str] = []
    sql = query.strip().rstrip(";").strip()

    lines = []
    for line in sql.splitlines():
        stripped = line.strip().upper()
        if stripped.startswith(("SET ", "USE ")):
            flags.append(f"Dropped directive line: {line.strip()!r}")
            continue
        if stripped.startswith(("--#", "#")):
            continue
        lines.append(line)
    sql = "\n".join(lines)

    # mapjoin hints are no-ops in BigQuery (it chooses the plan).
    sql = re.sub(r"(?i)/\*\+.*?mapjoin.*?\*/", "", sql, flags=re.DOTALL)

    # Table references -> ${ref(...)} so Dataform builds the dependency graph.
    def _ref(m: re.Match) -> str:
        verb, table = m.group(1).lower(), m.group(2)
        bare = table.lstrip("`").rstrip("`").split(".")[-1]
        return f"{verb} ${{ref(\"{bare}\")}}"

    sql = TABLE_REF_RE.sub(_ref, sql)

    # ODPS variables -> Dataform vars (resolved when declared in workflow_settings).
    def _var(m: re.Match) -> str:
        name = m.group(1)
        resolved = (
            (available_vars is not None and name in available_vars)
            or (vars_available and name in KNOWN_VARS)
        )
        if resolved:
            return f"${{dataform.projectConfig.vars[\"{name}\"]}}"
        flags.append(f"Unresolved variable ${{{name}}} -- add it to workflow_settings vars or a DAG macro")
        return m.group(0)

    sql = VAR_RE.sub(_var, sql)

    # Bracket-notation variables: $[yyyyommdd] -> Dataform project vars,
    # $[yyyymmdd±N] -> date-arithmetic JS (guide Appendix A / §14).
    def _bracket_var(m: re.Match) -> str:
        name = m.group(1)
        offset = m.group(2)
        resolved = (
            (available_vars is not None and name in available_vars)
            or (vars_available and name in KNOWN_VARS)
        )
        if not resolved:
            flags.append(f"Unresolved bracket variable $[{name}] -- add it to workflow_settings vars or a DAG macro")
            return m.group(0)
        if offset:
            if name not in DATE_VARS:
                flags.append(
                    f"Offset arithmetic on $[{name}] not supported "
                    f"(only YYYYMMDD date vars {sorted(DATE_VARS)})"
                )
                return m.group(0)
            days = int(offset)
            return _bracket_offset_js(name, days)
        return f"${{dataform.projectConfig.vars[\"{name}\"]}}"

    sql = BRACKET_VAR_RE.sub(_bracket_var, sql)

    # Function rewrites.
    for src, dst in FUNCTION_MAP.items():
        if re.search(rf"(?<![\w.]){src}\s*\(", sql):
            sql = re.sub(rf"(?<![\w.]){src}(?=\s*\()", dst, sql)
    for fn in FLAG_FUNCTIONS:
        if re.search(rf"(?<![\w.]){fn}\s*\(", sql):
            flags.append(f"ODPS built-in {fn}(...) needs manual review (semantics differ in BigQuery)")

    # Reserved identifiers: flag, don't rewrite.
    idents = set(re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", sql))
    for ident in idents & BIGQUERY_RESERVED:
        flags.append(f"Identifier {ident!r} is a BigQuery reserved word; backtick it if it breaks compile")

    return sql, flags


def build_model_sql(dml_content: str, vars_available: bool = False,
                    available_vars: set[str] | None = None) -> tuple[str | None, list[str]]:
    """Extract the SELECT body of an INSERT OVERWRITE node."""
    stmts = parse_statements(dml_content)
    for stmt in stmts:
        if stmt["kind"] == "insert":
            parsed = parse_insert(stmt["raw"])
            if parsed:
                return translate_query(parsed["query"], vars_available,
                                       available_vars=available_vars)
    return None, ["No INSERT ... SELECT found; node content is not a simple DML model"]
