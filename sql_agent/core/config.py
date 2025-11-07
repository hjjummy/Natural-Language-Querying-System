from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# 🔹 환경 변수 로드
# ─────────────────────────────────────────────
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or ""
if not OPENAI_API_KEY:
    raise RuntimeError("❌ .env에 OPENAI_API_KEY가 필요함.")

# (선택) 외부 LLM 엔드포인트를 쓰는 경우
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL") or None

# ─────────────────────────────────────────────
# 🔹 기본 경로 및 설정
# ─────────────────────────────────────────────
ROOT_DIR        = Path(".").resolve()
DATA_DIR        = ROOT_DIR / "data"
DB_DIR          = ROOT_DIR / "db"
SCHEMA_DIR      = ROOT_DIR / "schema"          # 스키마/DDL/시맨틱 사전 위치
WORKSPACE_ROOT  = ROOT_DIR / "workspace"
CACHE_DIR       = WORKSPACE_ROOT / "cache"
SESSIONS_DIR    = WORKSPACE_ROOT / "sessions"
LOGS_DIR        = ROOT_DIR / "logs"

# DuckDB 파일 경로 (이미 구축된 DB 가정)
DUCKDB_PATH     = DB_DIR / "manufacturing.db"

# 시맨틱 스키마/용어집(JSON) 경로(LLM 프롬프트용, 있으면 사용)
SEMANTIC_SCHEMA_JSON = SCHEMA_DIR / "schema.json"

# (선택) 초기 DDL 파일 경로 — 일반 운영에서는 사용 안 함(이미 구축 가정)
DDL_SQL_PATH    = SCHEMA_DIR / "ddl.sql"

# ─────────────────────────────────────────────
# 🔹 LLM 모델 설정
# ─────────────────────────────────────────────
# 질문 정규화/리라이팅
MODEL_REWRITE   = os.getenv("MODEL_REWRITE", "gpt-4o")
# NL→SQL 생성
MODEL_SQL       = os.getenv("MODEL_SQL", "gpt-4o")

# ─────────────────────────────────────────────
# 🔹 RAG(few-shot) 검색 설정 (DuckDB 내장 벡터)
# ─────────────────────────────────────────────
USE_RAG                 = True                  # 벡터 예시가 없으면 자동으로 건너뜀
RAG_TOPK                = 3
EMBEDDING_DIM           = int(os.getenv("EMBEDDING_DIM", "1536"))
EMBEDDING_MODEL         = os.getenv("EMBEDDING_MODEL", "text-embedding-3-large")  # 필요시 변경

# ─────────────────────────────────────────────
# 🔹 SQL 가드레일/화이트리스트
# ─────────────────────────────────────────────
SQL_DEFAULT_LIMIT       = 500                   # LIMIT 미지정 시 강제 주입
SQL_FORBIDDEN_KEYWORDS  = {
    "ATTACH", "DETACH", "CREATE", "ALTER", "DROP",
    "INSERT", "UPDATE", "DELETE", "REPLACE", "COPY",
    "EXPORT", "IMPORT", "PRAGMA", "TRANSACTION", "GRANT", "REVOKE"
}
# 실제 사용할 테이블/뷰만 허용 (DuckDB main.main 기준)
SQL_ALLOWED_TABLES = {
    "fact_manufacturing",
    "v_factory_grade_summary",
    "v_edition_kpi",
}
# 사용할 카탈로그/스키마(기본 DuckDB는 main.main)
SQL_CATALOG = "main"
SQL_SCHEMA  = "main"

# ─────────────────────────────────────────────
# 🔹 UI/흐름 옵션
# ─────────────────────────────────────────────
USE_REWRITTEN_FOR_ALL = True  # 리라이팅된 문장을 항상 downstream에 사용

# ─────────────────────────────────────────────
# 🔹 디렉터리 생성 유틸
# ─────────────────────────────────────────────
def ensure_dirs():
    """
    필요한 디렉터리 구조를 생성합니다.
    - ./data
    - ./db
    - ./schema
    - ./workspace/cache
    - ./workspace/sessions
    - ./logs
    """
    for p in [DATA_DIR, DB_DIR, SCHEMA_DIR, CACHE_DIR, SESSIONS_DIR, LOGS_DIR]:
        p.mkdir(parents=True, exist_ok=True)
