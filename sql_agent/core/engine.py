# core/engine.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Any
import time
import duckdb
import pandas as pd

from .config import DUCKDB_PATH, MODEL_SQL, MODEL_REWRITE  # ← DB_PATH → DUCKDB_PATH, MODEL_REWRITE 추가
from .llm import rewrite, to_sql
from .io import to_md_table                              # ← 공용 Markdown 변환 사용
                   # ← SELECT/LIMIT 가드 공용 사용

# =====================================================================
# 히스토리 매니저 (동일)
# =====================================================================
class HistoryManager:
    def __init__(self, max_tokens: int = 3000):
        self.turns = []
        self.max_tokens = max_tokens

    def _is_dup(self, rec: dict) -> bool:
        if not self.turns:
            return False
        last = self.turns[-1]
        keys = ["orig_q", "rewritten_q", "a", "executed_sql"]
        return all((last.get(k, "") or "") == (rec.get(k, "") or "") for k in keys)

    def add(
        self,
        orig_q: str,
        rewritten_q: str,
        answer_md: str,
        used: list[str] | None = None,
        executed_sql: str | None = None,
        meta: dict | None = None,
    ):
        """원본질문, 리라이팅된 질의, 결과표, 사용컬럼, SQL까지 저장"""
        rec = {
            "orig_q": (orig_q or "").strip(),
            "rewritten_q": (rewritten_q or "").strip(),
            "a": (answer_md or "").strip(),
            "used": used or [],
            "executed_sql": executed_sql or "",
            "meta": meta or {},
        }
        if not self._is_dup(rec):
            self.turns.append(rec)

    def _format(self, t):
        import json as _json
        return (
            "<turn>\n"
            f"<Q_original>{t.get('orig_q','')}</Q_original>\n"
            f"<Q_rewritten>{t.get('rewritten_q','')}</Q_rewritten>\n"
            f"<A>{t.get('a','')}</A>\n"
            f"<used_columns>{_json.dumps(t.get('used', []), ensure_ascii=False)}</used_columns>\n"
            f"<executed_sql>{t.get('executed_sql','')}</executed_sql>\n"
            "</turn>"
        )

    def build(self) -> str:
        if not self.turns:
            return ""
        try:
            import tiktoken
            enc = tiktoken.get_encoding("cl100k_base")
            def tokens(x: str) -> int: return len(enc.encode(x))
        except Exception:
            def tokens(x: str) -> int: return max(1, (len(x) + 3) // 4)
        pieces, total = [], 0
        for t in reversed(self.turns):
            blk = self._format(t)
            cost = tokens(blk)
            if total + cost > self.max_tokens:
                break
            pieces.append(blk); total += cost
        return "\n".join(reversed(pieces))


# =====================================================================
# DuckDB 유틸
# =====================================================================
def _connect_db(db_path: str | Path = DUCKDB_PATH) -> duckdb.DuckDBPyConnection:  # ← DUCKDB_PATH 사용
    return duckdb.connect(str(db_path))

def _introspect_table(con: duckdb.DuckDBPyConnection, table: str) -> dict:
    info = con.execute(f"PRAGMA table_info('{table}')").df()
    cols = [{"name": str(r["name"]), "type": str(r["type"]), "description": ""} for _, r in info.iterrows()]
    return {"table": table, "columns": cols}

# =====================================================================
# 재시도 옵션
# =====================================================================
@dataclass
class RetryOptions:
    max_retries: int = 2
    backoff_sec: float = 0.6
    retry_on_empty: bool = True
    retry_on_error: bool = True

# =====================================================================
# 단발 실행
# =====================================================================
def ask_one_sql(
    question: str,
    history: HistoryManager,
    table: str = "fact_manufacturing",
    model_for_sql: str = MODEL_SQL,
    retry: RetryOptions = RetryOptions(),
) -> dict:
    hist = history.build()
    con = _connect_db()

    schema_partial = _introspect_table(con, table)

    last_error = None
    attempt = 0
    seed_q = question

    while attempt <= retry.max_retries:
        try:
            # 1) 리라이팅 → 반드시 MODEL_REWRITE
            rw = rewrite(hist, seed_q if attempt == 0 else f"{seed_q} (조건을 더 구체화하여 SQL이 비지 않게 해줘)", model=MODEL_REWRITE)
            q_eff = rw.get("rewritten", seed_q)

            # 2) LLM → SQL(JSON)
            gen = to_sql(
                question=q_eff,
                schema_partial=schema_partial,
                shots=None,
                model=model_for_sql
            )
            sql_raw = gen.get("sql", "") or gen.get("clean_sql", "")
            sql_clean = gen.get("clean_sql", sql_raw).strip()

            # 3) 가드 (SELECT-only + LIMIT≤500)
            executed_sql = guard_sql(sql_clean, max_limit=500)

            # 4) 실행
            df = con.execute(executed_sql).df()

            # 5) 빈 결과 재시도
            if retry.retry_on_empty and (df is None or df.empty):
                attempt += 1
                if attempt > retry.max_retries:
                    md = to_md_table(df)
                    history.add(q=f"(orig) {question} || (rewritten) {q_eff}", a="(empty)", used=[])
                    return {
                        "success": True,
                        "rewritten": q_eff,
                        "rewrite_reason": rw.get("reason", ""),
                        "sql": sql_raw,
                        "clean_sql": sql_clean,
                        "executed_sql": executed_sql,
                        "df": df,
                        "markdown": md,
                        "error": None,
                        "retry_info": {"attempts": attempt, "status": "empty_final"},
                    }
                time.sleep(retry.backoff_sec * (2 ** (attempt - 1)))
                continue

            # 6) 성공
            md = to_md_table(df)
            history.add(
                q=f"(orig) {question} || (rewritten) {q_eff}",
                a=md,
                used=[c["name"] for c in schema_partial["columns"]],
            )
            return {
                "success": True,
                "rewritten": q_eff,
                "rewrite_reason": rw.get("reason", ""),
                "sql": sql_raw,
                "clean_sql": sql_clean,
                "executed_sql": executed_sql,
                "df": df,
                "markdown": md,
                "error": None,
                "retry_info": {"attempts": attempt + 1, "status": "ok"},
            }

        except Exception as e:
            last_error = str(e)
            if not retry.retry_on_error:
                raise
            attempt += 1
            if attempt > retry.max_retries:
                return {
                    "success": False,
                    "rewritten": seed_q,
                    "rewrite_reason": "",
                    "sql": "",
                    "clean_sql": "",
                    "executed_sql": "",
                    "df": pd.DataFrame(),
                    "markdown": "| error |\n|---|\n| " + (last_error or "unknown") + " |",
                    "error": last_error,
                    "retry_info": {"attempts": attempt, "status": "error_final", "error": last_error},
                }
            time.sleep(retry.backoff_sec * (2 ** (attempt - 1)))

    return {
        "success": False,
        "rewritten": question,
        "rewrite_reason": "",
        "sql": "",
        "clean_sql": "",
        "executed_sql": "",
        "df": pd.DataFrame(),
        "markdown": "| error |\n|---|\n| unknown |",
        "error": "unknown",
        "retry_info": {"attempts": attempt, "status": "unknown"},
    }

# 호환 래퍼
def ask_one(*, question: str, history: HistoryManager, table: str = "fact_manufacturing") -> dict:
    return ask_one_sql(question=question, history=history, table=table)

def ask_one_with_retry(*, question: str, history: HistoryManager, table: str = "fact_manufacturing",
                       retry: RetryOptions = RetryOptions()) -> dict:
    return ask_one_sql(question=question, history=history, table=table, retry=retry)
