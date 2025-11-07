from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
from .io import read_md_table
from .llm import chat
from .config import MODEL_FOR_COL_SELECT

# core/schema.py
import os, json
from openai import OpenAI as OpenAIClient

def call_openai_llm(prompt: str, model: str = "gpt-4o", max_tokens: int = 3000) -> str:
    """
    스키마 생성을 위한 간단한 OpenAI 호출 래퍼.
    JSON만 출력하도록 system 메시지를 고정합니다.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError("❌ OPENAI_API_KEY가 필요합니다.")
    client = OpenAIClient(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "당신은 JSON 스키마 분석 전문가입니다. 반드시 JSON만 출력하세요."},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
        max_tokens=max_tokens,
    )
    return resp.choices[0].message.content.strip()

import re

def extract_json(text: str) -> str:
    """
    LLM 응답 문자열에서 JSON 본문만 추출.
    ```json ... ``` 또는 기타 코드펜스 제거.
    """
    if not text:
        return ""
    t = text.strip()

    # ```json ... ``` 또는 ``` ... ``` 제거
    t = re.sub(r"^```[a-zA-Z0-9_+-]*\s*", "", t)
    t = re.sub(r"\s*```$", "", t)

    # 중괄호 기준으로 JSON 본문만 추출
    if "{" in t and "}" in t:
        head = t.index("{")
        tail = t.rfind("}") + 1
        t = t[head:tail]

    return t.strip()


def _col_stats(df: pd.DataFrame) -> dict:
    import re
    stats = {}
    num_like = re.compile(r"^[+-]?\d+(?:\.\d+)?$")
    for c in df.columns:
        s = df[c].astype(str)
        num_vals = [float(v) for v in s if v.strip() and num_like.match(v)]
        if num_vals:
            ser = pd.Series(num_vals, dtype="float64")
            stats[c] = {"dtype_hint":"float","numeric_summary":{
                "count": int(ser.shape[0]), "min": float(ser.min()),
                "max": float(ser.max()), "mean": float(ser.mean()),
                "std": float(ser.std(ddof=0)), "examples": list(pd.unique(ser))[:8]
            }}
        else:
            vc = pd.Series([v.strip() for v in s if v.strip()]).value_counts()
            stats[c] = {"dtype_hint":"string","string_summary":{
                "count": int(vc.sum()), "unique_count": int(vc.shape[0]),
                "examples": vc.head(10).index.tolist()
            }}
    return stats

# --- 교체 시작 ---
def build_schema_prompt(df: pd.DataFrame, purpose: str, preview_rows: int = 50) -> str:
    header = "| " + " | ".join(df.columns) + " |"
    sep    = "| " + " | ".join(["---"] * len(df.columns)) + " |"
    rows   = ["| " + " | ".join(map(str, r)) + " |" for r in df.head(preview_rows).values.tolist()]
    md_block = "\n".join([header, sep, *rows])

    # 숫자/문자 컬럼 통계 (원본 수준)
    import re
    num_like = re.compile(r"^[+-]?\d+(?:\.\d+)?$")
    stats = {}
    for col in df.columns:
        s = df[col].astype(str)
        num_vals = [float(v) for v in s if v.strip() and num_like.match(v)]
        if num_vals:
            ser = pd.Series(num_vals, dtype="float64")
            stats[col] = {
                "dtype_hint": "float",
                "numeric_summary": {
                    "count": int(ser.shape[0]),
                    "min": float(ser.min()),
                    "max": float(ser.max()),
                    "mean": float(ser.mean()),
                    "std": float(ser.std(ddof=0)),
                    "examples": list(pd.unique(ser))[:8],
                },
            }
        else:
            vals = [v.strip() for v in s if v.strip()]
            vc = pd.Series(vals).value_counts()
            stats[col] = {
                "dtype_hint": "string",
                "string_summary": {
                    "count": int(vc.sum()),
                    "unique_count": int(vc.shape[0]),
                    "examples": vc.head(10).index.tolist(),
                },
            }

    example_json = {
        "columns": [
            {
                "name": "A",
                "dtype": "string",
                "definition_2lines": [
                    "[정보] 텍스트 컬럼 'A': 필터/동치 비교 및 그룹 추출에 사용.",
                    "[형식] string; 대표 형식 예: 코드형 (예: AC25, AB17, ...)",
                ],
                "summary": {"count": 999, "unique_count": 123, "examples": ["AC25","AB17","AA03"]},
            },
            {
                "name": "K",
                "dtype": "float",
                "definition_2lines": [
                    "[정보] 수치형 컬럼 'K': 비교/정렬/최댓값·최솟값 검색에 사용.",
                    "[형식] float; 값 범위 ≈ [min, max]",
                ],
                "summary": {"count": 999, "min": 70.58, "max": 191.03, "mean": 126.4, "std": 15.3, "examples": [140.37,119.53,174.14]},
            },
        ]
    }

    rules = """
- JSON만 출력하세요. 코드펜스 금지.
- 각 열은 {name, dtype, definition_2lines(정확히 2문장), summary} 필드 포함.
- dtype은 string 또는 float.
- float의 definition_2lines 두 번째 문장에는 "값 범위 ≈ [min, max]" 그대로 표기.
- string의 definition_2lines 두 번째 문장에는 대표 형식/패턴을 간결히 기술.
- summary는 아래 구조:
  • float: {count, min, max, mean, std, examples(최대 8개)}
  • string: {count, unique_count, examples(최대 10개)}
- 컬럼 순서는 원본 테이블 헤더 순서를 따름.
- string 일 경우 [정보] 칸에 특징을 최대한 자세히 작성.
    """.strip()

    import json as _json
    return f"""
당신은 데이터 스키마 생성 전문가입니다.

[목적]
{purpose}

[규칙]
{rules}

[출력 예시]
{_json.dumps(example_json, ensure_ascii=False, indent=2)}

[데이터 일부]
{md_block}

[열 통계]
{_json.dumps(stats, ensure_ascii=False, indent=2)}
""".strip()

def generate(md_path: str, schema_path: str, model: str = MODEL_FOR_COL_SELECT):
    df = read_md_table(md_path)
    prompt = build_schema_prompt(df, "엑셀 자연어 질의를 정확히 해석하기 위한 열(Column) 정의 스키마 자동 생성.")
    raw = chat(
        model,
        [
            {"role": "system", "content": "당신은 JSON 스키마 분석 전문가입니다. 반드시 JSON만 출력하세요."},
            {"role": "user", "content": prompt},
        ],
        max_tokens=3000,
        temperature=0,
    )
    Path(schema_path).write_text(raw, encoding="utf-8")
# --- 교체 끝 ---


def load(schema_path: str) -> dict:
    return json.loads(Path(schema_path).read_text(encoding="utf-8"))

def columns_info(schema: dict) -> tuple[list[str], str]:
    cols = [c["name"] for c in schema.get("columns", []) if "name" in c]
    desc = "\n".join([f"- {c['name']}: {' '.join(c.get('definition_2lines', []))}" for c in schema.get("columns", [])])
    return cols, desc
