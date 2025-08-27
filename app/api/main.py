import traceback
import duckdb
import re
import os
import subprocess

from typing import Union
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pathlib import Path


SRC_ROOT = Path(os.getenv("DELTA_ROOT", "/datalake"))
GOLD_PATH = f"{SRC_ROOT}/gold"

app = FastAPI(title="API BI")

conn = duckdb.connect(database=":memory:")

try:
    conn.execute("INSTALL delta;")
    conn.execute("LOAD delta;")
    HAS_DELTA_EXT = True
except Exception:
    HAS_DELTA_EXT = False


class SQLBody(BaseModel):
    sql: str


def get_available_tables():
    from pathlib import Path
    tables = []
    gold_dir = Path(GOLD_PATH)
    if gold_dir.exists():
        for item in gold_dir.iterdir():
            if item.is_dir():
                tables.append(item.name)
    return tables


def convert_sql_to_delta_scan(sql: str):

    available_tables = get_available_tables()

    for table in available_tables:
        pattern = r'\b(FROM|JOIN)\s+' + re.escape(table) + r'\b'
        
        def replace_func(match):
            keyword = match.group(1)  # FROM ou JOIN
            delta_path = f"{GOLD_PATH}/{table}"
            return f"{keyword} delta_scan('{delta_path}')"
        
        sql = re.sub(pattern, replace_func, sql, flags=re.IGNORECASE)
    
    return sql

def run_sql(sql: str, auto_convert: bool = True):
    try:

        if auto_convert:
            original_sql = sql
            sql = convert_sql_to_delta_scan(original_sql)
        
        cur = conn.execute(sql)
        cols = [c[0] for c in cur.description] if cur.description else []
        rows = cur.fetchall()
        results = [dict(zip(cols, r)) for r in rows]
        return results
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SQL execution error: {e}\n{traceback.format_exc()}")

@app.post("/query")
def query_sql(body: SQLBody):
    return run_sql(body.sql)

@app.get("/query/{table_name}")
def query_table(table_name: str, limit: int = 10):
    sql = f"SELECT * FROM delta_scan('{GOLD_PATH}/{table_name}') LIMIT {limit}"
    return run_sql(sql)

@app.get("/tables")
def list_tables():
    return {"tables": get_available_tables()}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/run-etl")
def run_etl():
    result = subprocess.run(
        ["python", "-m", "pipelines.etl"],
        capture_output=True,
        text=True
    )
    return {"returncode": result.returncode}