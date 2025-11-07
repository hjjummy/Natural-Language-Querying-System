# core/sql_executor.py
from __future__ import annotations
import re
from typing import Any, Dict, Optional, List

import duckdb
import pandas as pd

from .config import DUCKDB_PATH, SQL_SCHEMA, MODEL_SQL  # ✅ DUCKDB_PATH/SQL_SCHEMA 사용
from .llm import rewrite, to_sql

# (선택) 히스토리 기록에 사용
try:
    from .engine import HistoryManager  # 없으면 무시
except Exception:
    HistoryManager = None  # type: ignore


# ─────────────────────────────────────────────
# 🔎 컬럼 설명(도메인 사전) — 필요 시 자유롭게 보강
# ─────────────────────────────────────────────
# COLUMN_DESCRIPTIONS: Dict[str, str] = {
#     "factory_code": "공장 코드 (예: AA24, AC25 등)",
#     "line_code": "라인 식별자 (예: AAA157)",
#     "product_code": "제품 코드 (예: CCCCCCC-DD092)",
#     "line_grade": "제품 등급 (예: U 1st, S 3rd 등)",
#     "edition_type": "에디션 유형 (예: M=Main, W=Wholesale, J=Joint, T=Trial)",
#     "efficiency_index": "공정 효율 지수 (무단위, K 열)",
#     "output_qty": "생산량 (pcs)",
#     "cycle_time_s": "사이클 타임 (초)",
#     "mold_temp_c": "금형 온도 (°C)",
#     "inj_pressure_bar": "사출 압력 (bar)",
#     "conv_speed_mps": "컨베이어 속도 (m/s)",
#     "inproc_pass_flag": "공정 내 합격 여부 (0/1)",
#     "rebound_coeff_pct": "반발탄성 (%)",
#     "final_perf_score": "최종 성능 지수",
# }
COLUMN_DESCRIPTIONS: Dict[str, str] = {
    # ────────────────────────────────
    # 기본 식별 코드 영역
    # ────────────────────────────────
    "factory_code": (
        "공장 코드 / 생산 지역 식별자.\n"
        "- AC25, AB25, AA24 등으로 표기되며, 창신INC의 주요 생산 거점을 구분함.\n"
        "- 예시:\n"
        "  • AA24: 중국 푸젠 공장 (신소재 테스트 중심)\n"
        "  • AA25: 중국 광둥 공장 (표준형 제품 중심)\n"
        "  • AB25: 인도네시아 자카르타 공장 (도매형 대량 생산 중심)\n"
        "  • AC25: 베트남 동나이 공장 (주력 생산 거점, 품질·물량 핵심 역할)"
    ),

    "line_code": (
        "라인 식별자.\n"
        "- 특정 공장 내 개별 생산 라인을 구분하는 코드.\n"
        "- 예시: AAA157, AAA130 등.\n"
        "- 보통 한 라인은 동일 제품군 또는 동일 등급의 생산을 담당함."
    ),

    "product_code": (
        "제품 코드 (SKU / 품번).\n"
        "- 각 완성품을 구분하는 고유 코드로, 품번 또는 바코드 역할.\n"
        "- 예시: CCCCCCC-DD084, CCCCCCC-DD142 등."
    ),

    # ────────────────────────────────
    # 라인·제품 등급 및 유형
    # ────────────────────────────────
    "line_grade": (
        "생산 라인의 품질 등급 코드.\n"
        "- 자동화율, 숙련도, 품질 기준에 따라 6단계로 분류됨.\n"
        "- 주요 코드 의미:\n"
        "  • U 1st: 상위 1등급 \n"
        "  • U 2nd: 상위 2등급 \n"
        "  • U 3rd: 상위 3등급 \n"
        "  • S 1st: 표준 1등급 \n"
        "  • S 2nd: 표준 2등급 \n"
        "  • S 3rd: 표준 3등급 "
    ),

    "edition_type": (
        "제품의 생산 유형(에디션 코드).\n"
        "- 유통 성격 또는 한정판 여부를 구분하는 코드.\n"
        "- 주요 코드 의미:\n"
        "  • M: Main Edition — 표준형 메인 생산품\n"
        "  • W: Wholesale Edition — 도매/대량 유통용 버전\n"
        "  • J: Joint Edition — 합작 또는 브랜드 공동 생산품\n"
        "  • T: Trial Edition — 시험생산·한정판 버전\n"
        "  • (공백): Standard Edition — 일반 정규 양산품"
    ),

    # ────────────────────────────────
    # 생산/공정 데이터
    # ────────────────────────────────
    "efficiency_index": (
        "공정 효율 지수 (K 열).\n"
        "- 라인별 주요 KPI로서, 주기·수율·품질을 종합 평가한 무단위 지수.\n"
        "- 값이 높을수록 공정 효율이 우수함.\n"
        "- 일반적으로 30~200 사이 분포."
    ),

    "output_qty": (
        "생산량 (pcs 단위).\n"
        "- 주어진 기간 또는 배치 단위의 총 생산 수량.\n"
        "- 예시 범위: 0 ~ 375,000.\n"
        "- 생산량 0은 테스트 또는 비가동 상태를 의미할 수 있음."
    ),

    "cycle_time_s": (
        "사이클 타임 (Cycle Time, 초 단위).\n"
        "- 제품 1개가 생산 완료되는 데 걸리는 평균 공정 시간.\n"
        "- 예시 범위: 2.0 ~ 180초.\n"
        "- 값이 작을수록 라인 효율이 높음."
    ),

    "mold_temp_c": (
        "금형 온도 (Mold Temperature, °C).\n"
        "- 성형 또는 프레스 시 금형의 설정 온도.\n"
        "- 예시 범위: 80 ~ 140°C.\n"
        "- 온도가 낮으면 경화 불량, 높으면 변형 가능성 있음."
    ),

    "inj_pressure_bar": (
        "사출 압력 (Injection Pressure, bar).\n"
        "- 성형 시 금형 내부로 재료를 주입하는 압력.\n"
        "- 예시 범위: 50 ~ 180 bar.\n"
        "- 재료 점도·제품 두께에 따라 최적 압력이 달라짐."
    ),

    "conv_speed_mps": (
        "컨베이어 속도 (Conveyor Speed, m/s).\n"
        "- 생산 라인의 이송 속도.\n"
        "- 예시 범위: 0.2 ~ 2.0 m/s.\n"
        "- 지나치게 빠르면 품질 저하, 느리면 효율 저하 가능."
    ),

    "inproc_pass_flag": (
        "공정 내 합격 여부 플래그 (0/1 혹은 0/100).\n"
        "- 1 또는 100: 해당 공정 단계에서 합격 처리됨.\n"
        "- 0: 불합격 또는 재작업 필요 상태.\n"
        "- 일부 설비에서는 100을 True로 사용하는 방식도 있음."
    ),

    "rebound_coeff_pct": (
        "완성품 반발탄성 계수 (Rebound Coefficient, %).\n"
        "- 후공정 품질 검사 항목으로, 제품의 반발탄성률을 측정.\n"
        "- 예시 범위: 5 ~ 150%.\n"
        "- 소재 특성 및 경도에 따라 달라지며, Y 컬럼은 항상 후공정 품질 지표로 고정."
    ),

    "final_perf_score": (
        "최종 성능 지수 (Final Performance Score).\n"
        "- 모든 공정 및 검사 데이터를 종합한 품질 평가 점수.\n"
        "- 정규화된 지수로 100이 기준선이며, 우수한 경우 100 초과 가능.\n"
        "- 예시 범위: 0 ~ 120+."
    ),
}


# ─────────────────────────────────────────────
# 🔐 SQL Guard: SELECT-only / LIMIT 보장
# ─────────────────────────────────────────────
_FORBIDDEN = ("delete", "update", "insert", "drop", "alter", "truncate")

def _strip_sql_comments(sql: str) -> str:
    s = re.sub(r"--.*?$", "", sql, flags=re.M)     # -- line
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.S)    # /* block */
    return s.strip()

def ensure_select_only(sql: str) -> bool:
    """
    - 단일 문장만 허용(중간 세미콜론 금지)
    - WITH ... SELECT 또는 SELECT 로 시작
    - DDL/DML 금지
    """
    s = _strip_sql_comments(sql)
    if ";" in s[:-1]:
        return False
    tok = s.lstrip().lower()
    if not (tok.startswith("select") or tok.startswith("with")):
        return False
    if any(f in tok for f in _FORBIDDEN):
        return False
    return True

def ensure_limit(sql: str, max_rows: int = 500) -> str:
    """
    LIMIT 미존재 시 LIMIT max_rows 추가.
    존재하더라도 max_rows 초과면 max_rows 로 교체.
    """
    s = _strip_sql_comments(sql).rstrip(";")
    m = re.search(r"\blimit\s+(\d+)\b", s, flags=re.I)
    if not m:
        return f"{s}\nLIMIT {max_rows}"
    try:
        n = int(m.group(1))
        if n <= max_rows:
            return s
        return re.sub(r"(?i)\blimit\s+\d+\b", f"LIMIT {max_rows}", s)
    except Exception:
        return f"{s}\nLIMIT {max_rows}"


# ─────────────────────────────────────────────
# 🗄️ DuckDB 실행기 + 유틸
# ─────────────────────────────────────────────
def connect(db_path: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    """DuckDB 연결 생성"""
    return duckdb.connect(db_path or str(DUCKDB_PATH))

def _df_to_markdown(df: pd.DataFrame, max_rows: int = 50) -> str:
    if df is None or df.empty:
        return "| (empty) |\n|---|\n| (no rows) |"
    df = df.head(max_rows)
    header = "| " + " | ".join(map(str, df.columns)) + " |"
    sep    = "|" + "|".join(["---"] * len(df.columns)) + "|"
    body   = []
    for _, row in df.iterrows():
        cells = ["" if pd.isna(v) else str(v) for v in row.tolist()]
        body.append("| " + " | ".join(cells) + " |")
    return "\n".join([header, sep, *body])

def execute_sql(sql: str, db_path: Optional[str] = None, limit: int = 500) -> Dict[str, Any]:
    """가드 적용 후 실행"""
    if not ensure_select_only(sql):
        return {
            "success": False,
            "executed_sql": sql,
            "df": pd.DataFrame(),
            "markdown": "| error |\n|---|\n| SELECT-only 쿼리만 허용됩니다. |",
            "row_count": 0,
            "error": "SELECT-only guard violation",
        }

    sql_limited = ensure_limit(sql, limit)
    con = connect(db_path)
    try:
        df = con.execute(sql_limited).df()
        return {
            "success": True,
            "executed_sql": sql_limited,
            "df": df,
            "markdown": _df_to_markdown(df),
            "row_count": len(df),
            "error": None,
        }
    except Exception as e:
        return {
            "success": False,
            "executed_sql": sql_limited,
            "df": pd.DataFrame(),
            "markdown": f"| error |\n|---|\n| {str(e)} |",
            "row_count": 0,
            "error": str(e),
        }
    finally:
        try:
            con.close()
        except Exception:
            pass

def list_tables(schema: Optional[str] = None, db_path: Optional[str] = None) -> List[str]:
    """
    현재 스키마의 테이블 목록 반환.
    DuckDB 문법: SHOW TABLES  또는 SHOW TABLES FROM <schema>
    """
    con = connect(db_path)
    try:
        if schema:
            rows = con.execute(f"SHOW TABLES FROM {schema}").fetchall()
        else:
            rows = con.execute("SHOW TABLES").fetchall()
        return [r[0] for r in rows]
    finally:
        try:
            con.close()
        except Exception:
            pass


# ─────────────────────────────────────────────
# 🧭 스키마 인트로스펙션 (PRAGMA + 샘플값 + 설명)
# ─────────────────────────────────────────────
def introspect_table(
    table: str,
    db_path: Optional[str] = None,
    sample_per_col: int = 5
) -> Dict[str, Any]:
    """
    PRAGMA table_info 로 컬럼(type) 목록 추출 +
    각 컬럼별 DISTINCT 샘플값(sample_values) + 컬럼 설명(description)을 포함한 요약 JSON 생성.
    """
    con = connect(db_path)
    try:
        # 타입 정보
        info = con.execute(f"PRAGMA table_info('{table}')").df()
        if info.empty:
            return {"table": table, "columns": []}

        fqtn = f"{SQL_SCHEMA}.{table}" if SQL_SCHEMA else table
        cols: List[Dict[str, Any]] = []

        for _, row in info.iterrows():
            col_name = str(row["name"])
            col_type = str(row["type"])

            # DISTINCT 샘플값
            try:
                sample_df = con.execute(
                    f"SELECT DISTINCT {col_name} AS v FROM {fqtn} WHERE {col_name} IS NOT NULL LIMIT {sample_per_col}"
                ).df()
                samples = [] if sample_df.empty else sample_df["v"].dropna().astype(str).tolist()
            except Exception:
                samples = []

            cols.append({
                "name": col_name,
                "type": col_type,
                "description": COLUMN_DESCRIPTIONS.get(col_name, ""),
                "sample_values": samples,
            })

        return {"table": table, "columns": cols}
    finally:
        try:
            con.close()
        except Exception:
            pass


# ─────────────────────────────────────────────
# 🔁 One-shot 통합 (Rewrite → ToSQL → Execute)
# ─────────────────────────────────────────────
def _build_history_text(history: Optional["HistoryManager"]) -> str:
    return history.build() if (history and hasattr(history, "build")) else ""

def search(
    question: str,
    table: str = "fact_manufacturing",
    history: Optional["HistoryManager"] = None,
    model_for_rewrite: Optional[str] = None,
    model_for_sql: Optional[str] = None,
    db_path: Optional[str] = None
) -> Dict[str, Any]:
    # 1) 스키마 요약
    schema = introspect_table(table, db_path=db_path, sample_per_col=5)

    # ✅ 2) 히스토리 문자열 한 번만 생성
    hist_text = _build_history_text(history)

    # 3) 리라이팅(대화 이력 반영)
    rw = rewrite(history_text=hist_text, question=question, model=model_for_rewrite or MODEL_SQL)
    q_eff = rw.get("rewritten", question)

    # 4) SQL 생성(JSON 표준 출력) — ✅ hist_text 전달
    gen = to_sql(
        question=q_eff,
        schema_partial=schema,
        shots=None,
        model=model_for_sql or MODEL_SQL,
        history_text=hist_text,   # << 여기!
    )
    sql = gen.get("clean_sql") or gen.get("sql") or ""
    if not sql.strip():
        return {
            "success": False,
            "rewritten": q_eff,
            "executed_sql": "",
            "df": pd.DataFrame(),
            "markdown": "| error |\n|---|\n| SQL 생성 실패 |",
            "row_count": 0,
            "error": "Empty SQL from LLM",
            "generation": gen,
            "schema_used": schema,
        }

    # 5) 실행
    out = execute_sql(sql, db_path=db_path, limit=500)

    # 6) 히스토리 적재 (확장형/구형 모두 지원)
    if history is not None:
        try:
            # 확장형 시그니처 (orig_q, rewritten_q, answer_md, used, executed_sql, meta)
            history.add(
                orig_q=question,
                rewritten_q=q_eff,
                answer_md=out.get("markdown", ""),
                used=[c["name"] for c in schema.get("columns", [])],
                executed_sql=out.get("executed_sql", ""),
                meta={"reasoning": gen.get("reasoning", "")},
            )
        except TypeError:
            # 구형 시그니처 (q, a, used)
            history.add(
                q=f"(orig) {question} || (rewritten) {q_eff}",
                a=out.get("markdown", ""),
                used=[c["name"] for c in schema.get("columns", [])],
            )

    return {
        **out,
        "rewritten": q_eff,
        "generation": gen,
        "schema_used": schema,
    }



# ─────────────────────────────────────────────
# 공개 심볼
# ─────────────────────────────────────────────
__all__ = [
    "ensure_select_only", "ensure_limit",
    "connect", "execute_sql",
    "list_tables", "introspect_table",
    "search",
]
