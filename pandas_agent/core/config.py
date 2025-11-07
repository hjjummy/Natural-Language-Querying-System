# core/config.py
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

# ─────────────────────────────────────────────
# 🔹 기본 경로 및 설정
# ─────────────────────────────────────────────
DATA_DIR        = Path("./data")
WORKSPACE_ROOT  = Path("./workspace")
CACHE_DIR       = WORKSPACE_ROOT / "cache"
SESSIONS_DIR    = WORKSPACE_ROOT / "sessions"

MODEL_FOR_COL_SELECT   = "gpt-4o"
PANDASAI_LLM_MODEL     = "gpt-4o"
USE_REWRITTEN_FOR_ALL  = True

# ─────────────────────────────────────────────
# 🔹 디렉터리 생성 유틸
# ─────────────────────────────────────────────
def ensure_dirs():
    """
    필요한 디렉터리 구조를 생성합니다.
    - ./data
    - ./workspace/cache
    - ./workspace/sessions
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
