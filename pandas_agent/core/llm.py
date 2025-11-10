###OpenAI 래퍼: chat(), chat_json(), rewrite(), select_columns()
# llm.py 

from __future__ import annotations
import json, re
from typing import Any, List
from openai import OpenAI as OpenAIClient
from .config import OPENAI_API_KEY

_client = OpenAIClient(api_key=OPENAI_API_KEY)

def chat(model: str, messages: list[dict], max_tokens=2000, temperature=0) -> str:
    r = _client.chat.completions.create(model=model, messages=messages,
                                        max_tokens=max_tokens, temperature=temperature)
    return r.choices[0].message.content.strip()

def chat_json(model: str, messages: list[dict], max_tokens=2000, temperature=0) -> dict:
    txt = chat(model, messages, max_tokens, temperature)
    txt = re.sub(r"^```json\s*|\s*```$", "", txt).strip()
    try: return json.loads(txt)
    except Exception:
        # 느슨한 추출
        if "{" in txt and "}" in txt:
            head, tail = txt.index("{"), txt.rfind("}") + 1
            try: return json.loads(txt[head:tail])
            except Exception: pass
        return {}

# --- 교체 시작 ---
def rewrite(history_text: str, question: str, model: str) -> dict:
    system = (
        "너는 데이터 질의 리라이팅 전문가이다. "
        "입력된 대화 이력(<history>)과 현재 질문(<question>)을 읽고, "
        "① 현재 질문이 직전 맥락과 연관되는지 판단, "
        "② 애매한 대용어를 구체화하여 단일 문장 질의로 리라이팅, "
        "③ 반드시 JSON만 출력."
    )
    user = f"""
[지침]
- JSON만 출력. 코드펜스 금지.
- keys 및 형식:
{{
  "is_related": true|false,
  "reason": "한 줄 요약 근거",
  "rewritten": "리라이팅된 명확 질의(원문의 의미를 보존하여 구체화)",
  "core_columns_hint": ["열명", ...]  // 있으면, 없으면 []
}}

[판단 기준]
- "그 중", "그 값", "위에서", "앞 단계" 같은 대용어가 있을 수 있으나,
  연관성 판단은 너의 이해에 기반해 판단하라(규칙 기반 탐지는 사용하지 마라).
- 관련이 없더라도, 숫자/조건/대상 집합이 명확하도록 rewritten을 구체화하되
  원문의 의도를 임의 변경하지 말 것.
- 이전 턴에서 계산한 '집합'이나 '값'을 참조해야 하면, <history>의 answer에서 해당 값을 찾아 그 집합/값을 문장 안에 풀어써라.
- 열 이름 힌트는 스키마를 모를 때도 의미상 추정(예: 'K의 평균' → ["K"]) 가능.

<history>
{history_text if history_text else "(없음)"}
</history>

<question>
{question}
</question>
""".strip()
    out = chat_json(model, [{"role":"system","content":system},{"role":"user","content":user}], max_tokens=400)
    return {
        "is_related": bool(out.get("is_related", False)),
        "reason": out.get("reason", ""),
        "rewritten": out.get("rewritten", question),
        "core_columns_hint": out.get("core_columns_hint", []),
    }

def select_columns(question: str, schema_cols: list[str], schema_desc: str,
                   model: str, history_text: str, max_return: int = 999) -> list[str]:
    system = "너는 데이터 질의 해석 보조자이다. 사용자가 참조한 열 이름만 JSON 배열로 반환하라."
    user = f"""
질문: {question}

[대화 이력(최신 우선)]
{history_text if history_text else "(이력 없음)"}

아래는 사용 가능한 열 목록과 요약 정보이다. 질문의 의미와 가장 관련 있는 열 이름들을 모두 JSON 배열로 반환하라.
{schema_desc}

규칙:
1) JSON 배열만 출력 (예: ["A","B","C"])한다. 설명/코드/주석 금지.
2) 질문에 열 이름을 직접적으로 언급하지 않아도, 의미상 관련이 있다고 판단되면 포함 가능
3) 질문에 열의 이름이 아닌, 항목값이 언급되는 경우도 있으므로 열 정보를 이용해 가장 관련이 높은 열을 추측하여 포함
4) 멀티턴 지시어/대용어 처리:
   - 대화 이력의 최근 N턴(권장: 3~5턴)에서 <used_columns>와 <answer> 테이블을 참고하라.
   - 이전 대화에서 사용된 집계 값이 언급되면, **그 집계를 계산한 열(예: A의 평균이면 A)**을 반드시 포함하라.
5) 최대 {max_return}개 이내
6) 반드시 위 목록에 있는 열만 사용
""".strip()
    arr = chat_json(model, [{"role":"system","content":system},{"role":"user","content":user}], max_tokens=200)
    if not isinstance(arr, list): return schema_cols[:]  # 실패 시 전체
    keep = [c for c in arr if c in schema_cols][:max_return]
    # 원본 순서 보존
    return [c for c in schema_cols if c in keep] or schema_cols[:]



