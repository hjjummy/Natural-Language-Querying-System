"""
core/llm.py — OpenAI 래퍼 + Text-to-SQL LLM 인터페이스
"""
from __future__ import annotations
import json, re
from typing import Any, List, Dict
from openai import OpenAI as OpenAIClient
from .config import OPENAI_API_KEY, MODEL_REWRITE, MODEL_SQL, USE_RAG
import re

def _strip_code_fence_and_comments(s: str) -> str:
    if not s:
        return ""
    t = s.strip()
    # 코드펜스 제거
    t = re.sub(r"^```(?:sql|json)?\s*", "", t, flags=re.I)
    t = re.sub(r"\s*```$", "", t, flags=re.I)
    # 주석 제거
    t = re.sub(r"(--.*?$)", "", t, flags=re.M)   # -- inline
    t = re.sub(r"(/\*[\s\S]*?\*/)", "", t)       # /* ... */
    return t.strip().rstrip(";").strip()

def _ensure_select_limit(sql: str, max_limit: int = 500) -> str:
    if not sql:
        return sql
    s = sql.strip()
    # SELECT-only 확인
    if not re.match(r"(?is)^\s*select\b", s):
        return ""
    # 이미 LIMIT가 있으면 최대값 보정
    if re.search(r"(?is)\blimit\s+(\d+)\b", s):
        def _cap(m):
            n = int(m.group(1))
            return f"LIMIT {min(n, max_limit)}"
        s = re.sub(r"(?is)\blimit\s+(\d+)\b", _cap, s)
    else:
        s = f"{s}\nLIMIT {max_limit}"
    return s.strip()

# ─────────────────────────────────────────────
# 🔹 클라이언트 초기화
# ─────────────────────────────────────────────
_client = OpenAIClient(api_key=OPENAI_API_KEY)

# ─────────────────────────────────────────────
# 🔹 기본 챗 호출
# ─────────────────────────────────────────────
def chat(model: str, messages: list[dict], max_tokens=2000, temperature=0) -> str:
    r = _client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return r.choices[0].message.content.strip()

def chat_json(model: str, messages: list[dict], max_tokens=2000, temperature=0) -> dict:
    txt = chat(model, messages, max_tokens, temperature)
    txt = re.sub(r"^```json\s*|\s*```$", "", txt).strip()
    try:
        return json.loads(txt)
    except Exception:
        if "{" in txt and "}" in txt:
            head, tail = txt.index("{"), txt.rfind("}") + 1
            try:
                return json.loads(txt[head:tail])
            except Exception:
                pass
        return {}

# ─────────────────────────────────────────────
# 🔹 질의 리라이팅
# ─────────────────────────────────────────────
def rewrite(history_text: str, question: str, model: str = MODEL_REWRITE) -> dict:
    """
    사용자의 자연어 질의를 단일 명확 문장으로 리라이팅.
    """
    system = (
        "너는 데이터 질의 리라이팅 전문가이다. "
        "이전 대화 내용(<history>)과 현재 질문(<question>)을 읽고, "
        "① 현재 질문이 이전에 대화한 맥락과 연관되는지 판단하고, "
        "② 연관되는 경우, 애매한 대용어나 지시어를 구체화하며, "
        "③ 반드시 JSON만 출력한다."
    )

    user = f"""
[핵심 규칙]
1) 지시어/대용어 해소(Coreference):
   - "그", "해당", "이들", "위에서 구한", "방금 나온", "그 제품들" 등과 같이 현재 질문만으로 알 수 없는 지시어가 등장할 경우,
     기본적으로 "직전 사용자 질문의 결과 집합(이전 결과)"을 가리키는 것으로 해석한다.
[판단 기준]
- "그 중", "그 값", "위에서", "앞 단계" 같은 대용어가 있을 수 있으나,
  연관성 판단은 너의 이해에 기반해 판단하라(규칙 기반 탐지는 사용하지 마라).
- 관련이 없더라도, 숫자/조건/대상 집합이 명확하도록 rewritten을 구체화하되
  원문의 의도를 임의 변경하지 말 것.
- 이전 턴에서 계산한 '집합'이나 '값'을 참조해야 하면, <history>의 answer에서 해당 값을 찾아 그 집합/값을 문장 안에 풀어써라.
- 열 이름 힌트는 스키마를 모를 때도 의미상 추정(예: 'K의 평균' → ["K"]) 가능.

    
[출력 형식]
{{
  "is_related": true|false,
  "reason": "한 줄 근거",
  "rewritten": "리라이팅된 명확 질의",
  "core_columns_hint": ["열명", ...]
}}

[지시]
아래 <history>와 <question>을 바탕으로 위 시스템 규칙을 적용하라.

<history>
{history_text if history_text else "(없음)"}
</history>

<question>
{question}
</question>
""".strip()

    out = chat_json(model, [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ], max_tokens=400)

    return {
        "is_related": bool(out.get("is_related", False)),
        "reason": out.get("reason", ""),
        "rewritten": out.get("rewritten", question),
        "core_columns_hint": out.get("core_columns_hint", []),
    }

# ─────────────────────────────────────────────
# 🔹 Text-to-SQL 생성 (핵심)
# ─────────────────────────────────────────────
def to_sql(question: str,
           schema_partial: dict | None = None,
           shots: list[dict] | None = None,
           model: str = MODEL_SQL,
           history_text: str = "") -> dict:
    """
    자연어 질의 → SQL 코드 생성 (히스토리/스코프 반영).
    Returns:
        {
          "success": bool,
          "sql": str,
          "clean_sql": str,
          "used_model": str,
          "table": str,
          "columns": list[str],
          "reasoning": str,
          "inferred_scope": str,  
          "inferred_filters": str  
        }
    """
    system = (
        "너는 DuckDB용 SQL 생성 전문가이다. 사용자의 질문을 DuckDB SQL 쿼리로 변환하세요.\n"
        "- SELECT-only 쿼리만 생성.\n"
        "- DDL, DML(INSERT/UPDATE/DELETE 등) 금지.\n"
        "- LIMIT 500 이하로 제한.\n"
        "- 제공된 테이블/컬럼 이름만 사용.\n"
        "- SQL 외의 설명, 주석, 코드펜스 금지.\n"
        "- 반드시 다음 JSON 형식으로 답하라.\n"
        "{\n"
        '  "sql": "SELECT ...",\n'
        '  "reasoning": "간단한 생성 근거"\n'
        "}"
    )

    msgs = [{"role": "system", "content": system}]

    # few-shot 예시 주입 (있을 경우)
    if USE_RAG and shots:
        for ex in shots:
            msgs.append({"role": "user", "content": ex["q"]})
            msgs.append({"role": "assistant", "content": json.dumps({"sql": ex["sql"], "reasoning": "샘플 예시"})})

    # 스키마 설명 포함
    schema_str = json.dumps(schema_partial, ensure_ascii=False, indent=2) if schema_partial else "(스키마 없음)"
    prompt = f"""
<rules>
### 공통 해석 규칙
    - <history>의 최근 결과/조건을 참고해 스코프(범위)와 필터를 추정한다.
    - '유사한/가까운/근사한' = 기준값과의 절대오차 최소(|x - target|).
    - '가장 많은/가장 흔한' = 최빈 항목(value_counts / COUNT GROUP BY ORDER BY COUNT DESC).

### 정렬규칙 - 동률(ties) 처리
    - 출력 순서 언급이 따로 없고, 동률일 경우에는 DB에 들어간 순서대로 (ingest_id ASC) 상단부터 순차 출력한다.

### 안전/형변환
    - 숫자 비교 전 NULL 제거(IS NOT NULL), 필요 시 CAST.
    - ORDER BY 후 LIMIT.
</rules>

<history>
{history_text or "(없음)"}
</history>

<schema>
아래는 DuckDB 데이터베이스의 테이블 스키마입니다:
{schema_str}
</schema>

<question>
{question}
</question>

<output_format>
반드시 JSON 형식으로 응답하세요:
{{
  "sql": "SELECT ...",
  "reasoning": "한 줄 요약 근거"
}}
</output_format>


""".strip()

    msgs.append({"role": "user", "content": prompt})
    raw = chat(model, msgs, max_tokens=600, temperature=0)

    # JSON만 추출
    try:
        txt = re.sub(r"^```json\s*|\s*```$", "", raw.strip())
        out = json.loads(txt)
    except Exception:
        # LLM이 JSON 형식 약속을 안 지켜도 fallback
        out = {"sql": raw, "reasoning": ""}

    sql_raw = (out.get("sql") or "").strip()
    sql_clean = _strip_code_fence_and_comments(sql_raw)
    sql_clean = _ensure_select_limit(sql_clean, max_limit=500)

    return {
        "success": bool(sql_clean and sql_clean.lower().lstrip().startswith("select")),
        "sql": sql_raw,
        "clean_sql": sql_clean,
        "used_model": model,
        "table": schema_partial.get("table") if schema_partial else None,
        "columns": [c["name"] for c in (schema_partial.get("columns", []) if schema_partial else [])],
        "reasoning": out.get("reasoning", ""),
    }