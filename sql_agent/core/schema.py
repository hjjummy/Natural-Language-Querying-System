# core/schema.py
from __future__ import annotations
import duckdb
import json
from pathlib import Path
from typing import Dict, Any

def introspect_table(db_path: str, table_name: str, out_path: str | None = None) -> Dict[str, Any]:
    """
    DuckDB의 PRAGMA table_info를 이용해 스키마 정보를 JSON으로 추출.
    - db_path: manufacturing.db 경로
    - table_name: 테이블명 (예: fact_manufacturing)
    - out_path: JSON 저장 경로 (없으면 None)
    """
    con = duckdb.connect(db_path)
    info_df = con.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
    con.close()

    schema = {
        "table": table_name,
        "columns": [
            {"name": r["name"], "type": r["type"], "notnull": bool(r["notnull"])}
            for _, r in info_df.iterrows()
        ]
    }

    if out_path:
        Path(out_path).write_text(json.dumps(schema, ensure_ascii=False, indent=2), encoding="utf-8")
    return schema


def load_schema(json_path: str) -> Dict[str, Any]:
    """저장된 schema.json 로드"""
    return json.loads(Path(json_path).read_text(encoding="utf-8"))
