# core/engine.py
from __future__ import annotations
from pathlib import Path
from typing import Optional
import pandas as pd
import time
import re
from dataclasses import dataclass
from datetime import datetime
import json

from .config import USE_REWRITTEN_FOR_ALL
from .io import build_md_subset, extract_between_tags, load_excel
from .schema import load as load_schema, columns_info, build_schema_prompt, call_openai_llm, extract_json
from .llm import rewrite, select_columns
from .pandasai import run as run_pandasai
from .session import SessionManager, Paths

# =====================================================================
# 히스토리 매니저
# =====================================================================
class HistoryManager:
    def __init__(self, max_tokens: int = 3000):
        self.turns = []
        self.max_tokens = max_tokens

    def add(self, q: str, a: str, used: list[str] | None = None):
        self.turns.append({"q": q.strip(), "a": a.strip(), "used": used or []})

    def _format(self, t):
        import json as _json
        return (
            "<turn>\n"
            f"<Q>{t['q']}</Q>\n"
            f"<A>{t['a']}</A>\n"
            f"<used_columns>{_json.dumps(t['used'], ensure_ascii=False)}</used_columns>\n"
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
# (신규) 세션/캐시 기반 준비
# =====================================================================
def prepare_with_session(
    thread_id: str,
    input_path: str,
    sheet_name: Optional[str],
    model_for_col_select: str,
    head_preview_rows: int | None = 20,
    workspace_root: str = "./workspace",
) -> Paths:
    sm = SessionManager(workspace_root)
    paths = sm.prepare_workspace(thread_id, input_path, sheet_name)

    # 1) 원본 로드
    ext = Path(input_path).suffix.lower()
    df = load_excel(input_path, sheet_name if ext != ".csv" else None)

    # 2) 캐시 MD (없을 때만)
    if not paths.cache_md_path.exists():
        header = "| " + " | ".join(map(str, df.columns)) + " |"
        sep    = "|" + "|".join(["---"] * len(df.columns)) + "|"
        rows   = ["| " + " | ".join(map(str, r)) + " |"
                  for r in df.head(head_preview_rows or 20).values.tolist()]
        md = "\n".join([header, sep, *rows])
        paths.cache_md_path.write_text(md, encoding="utf-8")

    # 3) 캐시 스키마 (없을 때만)
    if not paths.cache_schema_path.exists():
        df_preview = df.head(head_preview_rows or 20)
        purpose = "엑셀/CSV 자연어 질의를 위한 컬럼 정의 스키마 생성."
        prompt = build_schema_prompt(df_preview, purpose, preview_rows=(head_preview_rows or 20))
        raw = call_openai_llm(prompt, model=model_for_col_select)
        cleaned = extract_json(raw)
        try:
            import json
            schema = json.loads(cleaned)
            paths.cache_schema_path.write_text(json.dumps(schema, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            # 실패 시 원문 저장(디버깅)
            paths.cache_schema_path.write_text(raw, encoding="utf-8")

    # 4) 세션 폴더 보장 후 링크/복사
    paths.session_dir.mkdir(parents=True, exist_ok=True)
    sm.ensure_link_or_copy(paths.cache_md_path, paths.md_path)
    sm.ensure_link_or_copy(paths.cache_schema_path, paths.schema_path)

    # 세션/캐시 연결 후 즉시 검증
    assert paths.md_path.exists(), f"md not found after link/copy: {paths.md_path}"
    assert paths.schema_path.exists(), f"schema not found after link/copy: {paths.schema_path}"

    return paths

# =====================================================================
# (레거시) prepare — 실수 호출 방지용
# =====================================================================
def prepare(*args, **kwargs):
    """레거시 경로 방지: 세션 구조에서 사용 금지."""
    return  # 의도적으로 아무 것도 하지 않음

# =====================================================================
# 유틸: 빈 결과/모델 실패 응답 판단
# =====================================================================
_EMPTY_PAT = re.compile(r"(?:\bempty\b|no\s*rows)", re.I)

def _is_empty_markdown(md: str) -> bool:
    if not md or not md.strip():
        return True
    if _EMPTY_PAT.search(md):
        return True
    lines = [ln.strip() for ln in md.strip().splitlines() if ln.strip()]
    if len(lines) <= 2 and any("지표" in lines[0] and "값" in lines[0] for _ in [0]):
        return True
    return False

_ERR_PATTERNS = [
    r"Unfortunately, I was not able to answer your question",
    r"single positional indexer is out-of-bounds",
    r"list index out of range",
    r"division by zero",
    r"KeyError:",
    r"ValueError:",
    r"TypeError:",
    r"IndexError:",
    r"ZeroDivisionError",
    r"MemoryError",
    r"cannot convert|could not convert|invalid literal",
]
_MD_TABLE_LINE = re.compile(r"^\|\s*[^|]+\s*\|")

def _looks_like_md_table(md: str) -> bool:
    if not md:
        return False
    for ln in (md or "").splitlines():
        if _MD_TABLE_LINE.search(ln.strip()):
            return True
    return False

def _has_model_error(reason_answer: str, markdown: str) -> bool:
    blob = f"{reason_answer}\n\n{markdown}".lower()
    for pat in _ERR_PATTERNS:
        if re.search(pat.lower(), blob):
            return True
    if markdown and not _looks_like_md_table(markdown):
        return True
    return False

# =====================================================================
# 코어 실행(한 번)
# =====================================================================
def _ask_one_core(
    df_raw: pd.DataFrame,
    question: str,
    schema_path: str,
    md_path: str,
    model_for_col_select: str,
    pandasai_llm_model: str,
    history: HistoryManager,
    use_rewritten_for_all: bool,
    head_rows: int | None,
):
    hist = history.build()

    # 1) 리라이팅
    rw = rewrite(hist, question, model=model_for_col_select)
    q_eff = rw["rewritten"] if use_rewritten_for_all else question

    # 2) 컬럼 선택
    schema = load_schema(schema_path)
    cols, desc = columns_info(schema)
    selected = select_columns(q_eff, cols, desc, model_for_col_select, hist, max_return=999)

    # 힌트 보강
    hints = [h for h in rw.get("core_columns_hint", []) if h in cols]
    selected = [c for c in cols if c in set(selected) | set(hints)]
    numeric_cols = [c["name"] for c in schema.get("columns", []) if c.get("dtype") == "float" and "name" in c]

    # 3) 부분표
    md_subset = None
    if head_rows:
        try:
            md_subset = build_md_subset(md_path, selected, head=head_rows)
        except Exception:
            pass

    # 4) 컨텍스트
    colmap = {c["name"]: c for c in schema.get("columns", []) if "name" in c}
    context = "\n".join(
        [f"- {n} ({colmap[n].get('dtype','')}): " + " ".join(colmap[n].get("definition_2lines", [])) for n in selected]
    )

    # 5) 실행
    res = run_pandasai(df_raw, q_eff, selected, numeric_cols, context, hist, pandasai_llm_model)

    out = {
        "is_related": bool(rw.get("is_related", False)),
        "rewrite_reason": rw.get("reason", ""),
        "rewritten": rw.get("rewritten", question),
        "core_columns_hint": rw.get("core_columns_hint", []),
        "selected_cols": selected,
        "md_subset": md_subset,
        **res,
    }
    return out

# =====================================================================
# 단발 실행 (호환)
# =====================================================================
def ask_one(
    df_raw: pd.DataFrame,
    question: str,
    schema_path: str,
    md_path: str,
    model_for_col_select: str,
    pandasai_llm_model: str,
    history: HistoryManager,
    use_rewritten_for_all: bool = USE_REWRITTEN_FOR_ALL,
    head_rows: int | None = None,
):
    return _ask_one_core(
        df_raw, question, schema_path, md_path, model_for_col_select,
        pandasai_llm_model, history, use_rewritten_for_all, head_rows
    )

# =====================================================================
# 재시도 래퍼
# =====================================================================
@dataclass
class RetryOptions:
    max_retries: int = 2
    backoff_sec: float = 0.5
    retry_on_empty: bool = True
    retry_on_error: bool = True

def ask_one_with_retry(
    df_raw: pd.DataFrame,
    question: str,
    schema_path: str,
    md_path: str,
    model_for_col_select: str,
    pandasai_llm_model: str,
    history: HistoryManager,
    use_rewritten_for_all: bool = USE_REWRITTEN_FOR_ALL,
    head_rows: int | None = None,
    retry: RetryOptions = RetryOptions(),
):
    last_error = None
    attempt = 0
    q_seed = question

    while attempt <= retry.max_retries:
        try:
            q_try = q_seed if attempt == 0 else f"{q_seed}  ※재질문: 수치·조건을 명확히 해석하여 결과가 비지 않게 답해줘."
            out = _ask_one_core(
                df_raw=df_raw,
                question=q_try,
                schema_path=schema_path,
                md_path=md_path,
                model_for_col_select=model_for_col_select,
                pandasai_llm_model=pandasai_llm_model,
                history=history,
                use_rewritten_for_all=use_rewritten_for_all,
                head_rows=head_rows,
            )

            if retry.retry_on_error and _has_model_error(out.get("reason_answer", ""), out.get("markdown", "")):
                attempt += 1
                if attempt > retry.max_retries:
                    return {**out, "retry_info": {"attempts": attempt, "status": "error_final_text"}}
                time.sleep(retry.backoff_sec * (2 ** (attempt - 1)))
                continue

            if retry.retry_on_empty and _is_empty_markdown(out.get("markdown", "")):
                attempt += 1
                if attempt > retry.max_retries:
                    return {**out, "retry_info": {"attempts": attempt, "status": "empty_final"}}
                time.sleep(retry.backoff_sec * (2 ** (attempt - 1)))
                continue

            ans = extract_between_tags(out.get("reason_answer", ""), "answer")
            history.add(
                q=f"(orig) {question} || (rewritten) {out.get('rewritten', question)}",
                a=ans,
                used=out.get("used_columns", []),
            )

            try:
                # schema_path는 항상 cache_dir 내부를 가리키므로, 그 부모 폴더가 캐시 폴더임
                cache_dir = Path(schema_path).parent
                cache_log_path = cache_dir / "query_log.jsonl"

                record = {
                    "timestamp": datetime.now().isoformat(),
                    "question": question,
                    "rewritten": out.get("rewritten", question),
                    "generated_code": out.get("code") or "",
                    "answer": extract_between_tags(out.get("reason_answer", ""), "answer"),
                }

                with open(cache_log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
            except Exception as e:
                print(f"[warn] failed to save query log: {e}")
            return {**out, "retry_info": {"attempts": attempt + 1, "status": "ok"}}

        except Exception as e:
            last_error = e
            if not retry.retry_on_error:
                raise
            attempt += 1
            if attempt > retry.max_retries:
                return {
                    "is_related": False,
                    "rewritten": question,
                    "selected_cols": [],
                    "md_subset": None,
                    "markdown": "",
                    "reason_answer": f"<reason>\n에러 발생: {str(last_error)}\n</reason>\n\n<answer>\n(empty)\n</answer>",
                    "code": None,
                    "used_columns": [],
                    "retry_info": {"attempts": attempt, "status": "error_final", "error": str(last_error)},
                }
            time.sleep(retry.backoff_sec * (2 ** (attempt - 1)))

    return {
        "is_related": False,
        "rewritten": question,
        "selected_cols": [],
        "md_subset": None,
        "markdown": "",
        "reason_answer": "<reason>내부 오류</reason>\n\n<answer>\n(empty)\n</answer>",
        "code": None,
        "used_columns": [],
        "retry_info": {"attempts": attempt, "status": "unknown"},
    }
