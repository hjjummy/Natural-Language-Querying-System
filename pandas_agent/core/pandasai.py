#pandasai.py
from __future__ import annotations
import pandas as pd
from pandasai import SmartDataframe
from pandasai.llm.openai import OpenAI as PandasAIOpenAI
from .config import OPENAI_API_KEY

def _coerce_numeric(df: pd.DataFrame, numeric_cols: list[str]) -> pd.DataFrame:
    df2 = df.copy()
    for c in numeric_cols:
        if c in df2.columns:
            df2[c] = pd.to_numeric(df2[c].astype(str).str.replace(",", ""), errors="coerce")
    return df2

# --- 교체 시작 ---
def _prompt(question: str, allowed_cols: list[str], context_block: str, history_text: str) -> str:
    return f"""
## system
<system>
당신은 Pandas 코드 생성 전문가입니다.
context은 실제 데이터의 컬럼 정의를 요약한 것이다.
당신의 목표는 사용자의 자연어 질문(question)을 이해하고,
pandas와 numpy만을 이용하여 DataFrame(df)에서 해당 계산을 정확히 수행하는 코드를 작성하는 것이다.

아래 순서에 따라 사고해야 한다.

────────────────────────────
### 사고 순서 (4단계 절차)
────────────────────────────
1️. 연관성 판단 (Context Linking)
- question이 이전 질문과 연관되어 있는지 먼저 판단한다.
- 대용어/참조어가 등장하면 <history>의 결과/조건을 참조한다.
- 연관이 없으면 이번 question만 독립적으로 처리.

2. 질문 유형 판별 (복합 vs 단일)
- 복합 질문: 도출 값이 여러 개인 경우. 각 결과를 행 단위로 누적해 DataFrame으로 반환.
- 단일 질문: 한 번의 계산으로 끝남.

3. 계산 계획 수립
- 필요한 컬럼과 연산 단계를 파악한다.
- 연관된 경우 직전 단계 결과를 scope로 사용.
- "유사한" = |x - target| 최소.

4. 코드 생성
- pandas와 numpy만 사용한다.
- 최종 결과는 항상 DataFrame 형태의 result에 담는다.
────────────────────────────

금지사항:
- 파일 입출력, 네트워크 접근, eval/exec, 시스템 호출
- import (pandas, numpy 외 금지)
</system>

## rules
<rules>
### 기본 규칙
- DataFrame 변수명은 반드시 `df`.
- 사용할 수 있는 컬럼은 다음에 한정: {allowed_cols}
- 수치형 연산 전 `pd.to_numeric(..., errors="coerce")`.
- 동률일 경우 원본 DataFrame의 index 순서를 기준으로 상단 행을 순서로 선택한다.
- 결측치는 원칙적으로 삭제(dropna)하지 않는다.
- 수치 계산 시에는 pd.to_numeric(..., errors="coerce")로 변환 후 자동으로 NaN을 무시하도록 한다.
- 문자열 컬럼은 공백 제거 후 빈 문자열(""), "nan", "None", "<NA>"를 pd.NA로 치환한다.
- 모든 결과는 DataFrame 형태의 `result` 변수에 저장.
- 오직 코드만 출력한다(설명/문장/마크다운 금지).
- 항상 행 전체를 반환하는것이 아니라, 질문에서 요구하는 것을 반환한다. 

### 수치 출력 규칙
- 모든 수치 계산은 float64 정밀도로 수행하며, 소수점 이하를 절대 버리지 않는다.
- 평균·합계·비율 등은 round(x, 6)으로 소수점 6자리까지 반올림한다.
- 개수·순위 등 정수 결과는 int(x)로 출력한다(소수점 붙이지 않음).

### 원본 행 규칙 
- 원본 행 식별용 포인터 컬럼명은 **`__row_idx`** 이다.
- 집계 목적이 아니라 "행 자체"를 보여달라는 요구(예: "행 전체", "모든 열", "행을 알려줘")가 감지되면
  중간 계산 요약 대신 `__row_idx`를 포함한 행 식별 정보를 반환한다.

### 그룹 연산 규칙
- 질문에 "~별로", "각각", "항목별", "그룹별" 등의 표현이 포함되면 반드시 groupby() 연산을 수행한다.
- groupby 기준 컬럼은 "~별로" 앞의 컬럼이며, 대상 컬럼은 이후에 나열된 수치형 컬럼이다.
- 예: "Q 값별로 a, b, c, d의 평균" → df.groupby("Q")[["a","b","c","d"]].mean().reset_index()
- 집계 결과는 groupby 기준 컬럼 + 집계 컬럼 이름을 결합해 df_out으로 반환한다.

### 안전 가이드라인
- 비어있는 결과는 df_out = pd.DataFrame({{"지표":["empty"], "값":["no rows"]}})
- 길이 확인 후 idxmin/iloc/argmax 사용.
- 연산 전 dropna().

### 체인 해석 규칙
- `scope = df.copy()`로 시작.
- 복합 질문 단계 후 `scope = subset_i.copy()`로 갱신.
- “그 중/앞 단계 결과/이 중” 등은 이전 subset에서 이어감.

### 반환 규칙
- result는 {{ "type": "dataframe", "value": df_out }} 형태
- df_out 컬럼: ["지표", "값"]
- 복합질문: 각 결과를 행 단위로 누적하여 하나의 DataFrame으로 반환
- 예시 
df_out = pd.DataFrame({{"지표": ["평균", "최댓값"], "값": [avg_val, max_val]}})
result = {{"type": "dataframe", "value": df_out}}
</rules>

## history
<history>
{history_text or ""}
</history>

## input
<original_question>
{question}
</original_question>

<context>
{context_block or "(열 정의 컨텍스트 없음)"}
</context>

## output
<output>
- **오직 Python 코드만** 출력한다.
- 출력 코드의 시작은 반드시 ```python 으로 시작하고 ``` 로 끝나야 한다.(백틱 개수 주의)
</output>
""".strip()
# --- 교체 끝 ---


def run(df_raw: pd.DataFrame, question: str, allowed_cols: list[str],
        numeric_cols: list[str], context_block: str, history_text: str, llm_model: str) -> dict:
    # 1) 계산용 컬럼 구성 + 숨김 연결키 주입
    keep = [c for c in allowed_cols if c in df_raw.columns] or list(df_raw.columns)
    df_calc = df_raw[keep].copy()

    # ⛳ 원본 행 인덱스 보존용 숨김키 추가
    row_idx = df_raw.reset_index().index
    df_calc.insert(0, "__row_idx", row_idx)

    # allowed 목록에도 강제로 포함 (LLM이 결과에 넣을 수 있도록)
    if "__row_idx" not in keep:
        keep = ["__row_idx"] + keep

    # 수치형 캐스팅
    df_calc = _coerce_numeric(df_calc, numeric_cols or [])

    # 2) LLM 실행
    sdf = SmartDataframe(df_calc, config={
        "llm": PandasAIOpenAI(api_token=OPENAI_API_KEY, model=llm_model),
        "enforce_privacy": True, "use_error_correction_framework": True,
        "enable_cache": False, "save_logs": False
    })
    out = sdf.chat(_prompt(question, keep, context_block, history_text))
    code = getattr(sdf, "last_code_generated", None)

    # 3) 표 형태로 정규화
    def to_df(x):
        if isinstance(x, dict) and "value" in x: x = x["value"]
        if isinstance(x, pd.DataFrame): return x
        if isinstance(x, pd.Series): return x.to_frame().T
        return pd.DataFrame({"answer":[x]})

    df_out = to_df(out)

    # 4) 🔁 후처리: __row_idx로 원본 전체 열 확장
    def _expand_full_rows(df_raw: pd.DataFrame, df_partial: pd.DataFrame) -> pd.DataFrame | None:
        # 4-1) 1순위: __row_idx 기반
        if "__row_idx" in df_partial.columns:
            try:
                idxs = (
                    pd.to_numeric(df_partial["__row_idx"], errors="coerce")
                    .dropna().astype(int).tolist()
                )
                idxs = list(dict.fromkeys(idxs))  # unique & keep order
                if len(idxs) and len(idxs) <= 10:
                    return df_raw.iloc[idxs]
            except Exception:
                pass

        # # 4-2) 2순위(보조): 공통 키로 merge (소수 행일 때만)
        # if len(df_partial) <= 5:
        #     keys = [c for c in df_partial.columns if c in df_raw.columns]
        #     if len(keys) >= 1:
        #         try:
        #             merge_keys_df = df_partial[keys].drop_duplicates()
        #             m = df_raw.merge(merge_keys_df, on=keys, how="inner")
        #             if not m.empty:
        #                 return m.head(10)
        #         except Exception:
        #             pass
        return None

    expanded = _expand_full_rows(df_raw, df_out)

    # 5) 마크다운 생성: 확장 성공 시 원본 모든 열로 교체
    if expanded is not None and not expanded.empty:
        try:
            md = expanded.to_markdown(index=False, disable_numparse=True)
        except Exception:
            from .io import to_md_table
            md = to_md_table(expanded)
    else:
        try:
            md = df_out.to_markdown(index=False, disable_numparse=True)
        except Exception:
            from .io import to_md_table
            md = to_md_table(df_out)

    reason = "```python\n" + (code or "#(no code)") + "\n```"
    return {
        "markdown": md,
        "reason_answer": f"<reason>\n{reason}\n</reason>\n\n<answer>\n{md}\n</answer>",
        "code": code,
        "used_columns": keep
    }


    # df_out = to_df(out)

    # # ✅ 표시 전용: 숫자 셀만 6자리 반올림 + 불필요한 0 제거(정수면 정수로, 소수 있으면 소수로)
    # try:
    #     from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

    #     def _fmt_cell(v):
    #         try:
    #             d = Decimal(str(v))
    #             # 소수점 6자리까지 반올림
    #             d = d.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
    #             s = format(d, "f")                # 예: '98560.030000'
    #             s = s.rstrip('0').rstrip('.')     # 예: '98560.03'
    #             return s if s != "" else "0"
    #         except (InvalidOperation, ValueError, TypeError):
    #             return str(v)

    #     df_disp = df_out.copy().astype(object).applymap(_fmt_cell)
    #     #md = df_disp.to_markdown(index=False)
    #     md = df_disp.to_markdown(index=False, disable_numparse=True)


    # except Exception:
    #     from .io import to_md_table
    #     md = to_md_table(df_out)

    # reason = "```python\n" + (code or "#(no code)") + "\n```"
    # return {
    #     "markdown": md,
    #     "reason_answer": f"<reason>\n{reason}\n</reason>\n\n<answer>\n{md}\n</answer>",
    #     "code": code,
    #     "used_columns": keep,
    # }
