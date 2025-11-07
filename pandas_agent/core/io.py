from __future__ import annotations
import re, json
from pathlib import Path
from typing import List
import pandas as pd
import numpy as np

def _cell_to_str(x) -> str:
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except Exception:
        pass
    if isinstance(x, (list, dict, tuple, set, np.ndarray)):
        return json.dumps(x, ensure_ascii=False)
    return str(x)

def to_md_table(df: pd.DataFrame, max_rows: int | None = None) -> str:
    if max_rows: df = df.head(max_rows)
    header = "| " + " | ".join(map(str, df.columns)) + " |"
    sep    = "|" + "|".join(["---"] * len(df.columns)) + "|"
    body   = ["| " + " | ".join(_cell_to_str(v) for v in row) + " |" for _, row in df.iterrows()]
    return "\n".join([header, sep, *body])

# def excel_to_md(input_path: str, outdir: str = "./md_out", head: int | None = None):
#     from pandas import ExcelFile, read_excel, read_csv
#     ip, od = Path(input_path), Path(outdir); od.mkdir(parents=True, exist_ok=True)
#     if ip.suffix.lower() == ".csv":
#         df = read_csv(ip, dtype=str, keep_default_na=False)
#         (od / f"{ip.stem}.md").write_text(to_md_table(df, head), encoding="utf-8"); return
#     xl = ExcelFile(ip)
#     for sheet in xl.sheet_names:
#         df = read_excel(ip, sheet_name=sheet, dtype=str, keep_default_na=False)
#         safe = sheet.replace("/", "_").replace(" ", "_")
#         (od / f"{ip.stem}__{safe}.md").write_text(to_md_table(df, head), encoding="utf-8")

_TABLE_RE = re.compile(r"(\|.*?\|\s*\n(?:\|[-:\s]+?\|\s*\n)?(?:\|.*?\|\s*\n)+)", flags=re.S)
def read_md_table(md_or_path: str) -> pd.DataFrame:
    p = Path(md_or_path)
    text = p.read_text(encoding="utf-8") if p.exists() else md_or_path
    m = _TABLE_RE.search(text)
    if not m: raise ValueError("마크다운 테이블을 찾을 수 없습니다.")
    lines = [ln.strip() for ln in m.group(1).splitlines() if ln.strip()]
    header = lines[0]
    body   = [ln for ln in lines[1:] if not ln.startswith("|:") and not ln.startswith("|-")]
    def split(ln: str): return [c.strip() for c in ln.strip("|").split("|")]
    cols = split(header); rows = [split(ln) for ln in body]
    return pd.DataFrame(rows, columns=cols)

def build_md_subset(md_path: str, selected_cols: list[str], head: int | None = None) -> str:
    df_all = read_md_table(md_path)
    keep = [c for c in selected_cols if c in df_all.columns]
    df_part = df_all[keep] if keep else df_all
    return to_md_table(df_part, head)


def load_excel(path: str, sheet_name: str = None) -> pd.DataFrame:
    path = Path(path)
    ext = path.suffix.lower()

    if ext == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    elif ext in [".xls", ".xlsx"]:
        # 엔진 명시적으로 지정
        return pd.read_excel(path, sheet_name=sheet_name, dtype=str, keep_default_na=False, engine="openpyxl")
    else:
        raise ValueError(f"지원하지 않는 파일 형식입니다: {ext}")

def extract_between_tags(s: str, tag: str) -> str:
    m = re.search(rf"<{tag}>\s*(.*?)\s*</{tag}>", s, flags=re.S | re.I)
    return (m.group(1).strip() if m else s.strip())

# core/io.py — 하단에 유틸 추가
def excel_or_csv_to_md_at(df: pd.DataFrame, out_md_path: str, head: int | None = None):
    """
    이미 로드된 df를 지정된 out_md_path에 마크다운으로 저장.
    (cache/<hash>/<stem>__<sheet>.md 와 같이 정확히 찍기 위해)
    """
    Path(out_md_path).write_text(to_md_table(df, head), encoding="utf-8")
