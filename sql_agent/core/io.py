# core/io.py
from __future__ import annotations
import re, json
from pathlib import Path
import pandas as pd

# ─────────────────────────────────────────────
# 🔹 문자열/Markdown 변환 유틸
# ─────────────────────────────────────────────
def _cell_to_str(x) -> str:
    """셀 값을 문자열로 변환 (NaN, None, 리스트 등 안전 처리)"""
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    if isinstance(x, (list, dict, tuple, set)):
        return json.dumps(x, ensure_ascii=False)
    return str(x)

def to_md_table(df: pd.DataFrame, max_rows: int | None = None) -> str:
    """DataFrame → Markdown 표 문자열"""
    if df is None or df.empty:
        return "| (empty) |\n|---|\n| (no rows) |"
    if max_rows:
        df = df.head(max_rows)
    header = "| " + " | ".join(map(str, df.columns)) + " |"
    sep = "|" + "|".join(["---"] * len(df.columns)) + "|"
    body = ["| " + " | ".join(_cell_to_str(v) for v in row) + " |" for _, row in df.iterrows()]
    return "\n".join([header, sep, *body])

def df_to_md_at(df: pd.DataFrame, out_md_path: str, head: int | None = None):
    """DataFrame을 지정 경로에 마크다운 파일로 저장"""
    Path(out_md_path).write_text(to_md_table(df, head), encoding="utf-8")

# ─────────────────────────────────────────────
# 🔹 문자열 추출 유틸
# ─────────────────────────────────────────────
def extract_between_tags(s: str, tag: str) -> str:
    """<tag>...</tag> 사이의 텍스트 추출"""
    m = re.search(rf"<{tag}>\s*(.*?)\s*</{tag}>", s, flags=re.S | re.I)
    return (m.group(1).strip() if m else s.strip())
