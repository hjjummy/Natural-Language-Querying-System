# app.py
from __future__ import annotations
# --- add project root to sys.path (모듈 경로 보장) ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ----------------------------------------------------
import os
import re
import hashlib
import streamlit as st
import pandas as pd
from datetime import datetime
from uuid import uuid4
from dataclasses import dataclass

import time
from contextlib import contextmanager


# config / core
from core.config import (
    MODEL_FOR_COL_SELECT,
    PANDASAI_LLM_MODEL,
    USE_REWRITTEN_FOR_ALL,
    ensure_dirs,
)
from core.io import load_excel
from core.engine import prepare_with_session, ask_one_with_retry, RetryOptions, HistoryManager
from core.session import SessionManager, Paths  # 세션/캐시 정리용

# ------------------- 유틸 -------------------
def _between_tags(s: str, tag: str) -> str:
    m = re.search(rf"<{tag}>\s*(.*?)\s*</{tag}>", s or "", flags=re.S)
    return (m.group(1).strip() if m else (s or "").strip())

def _make_sig_from_uploaded(uploaded) -> str:
    """업로드 파일의 내용 바이트 기준 SHA-256 서명."""
    buf = uploaded.getbuffer()
    return hashlib.sha256(buf).hexdigest()

def _reset_thread(th: "ThreadState"):
    """파일 교체/제거 시 스레드 상태 초기화 + 세션 폴더 정리."""
    try:
        if getattr(th, "thread_id", None):
            SessionManager("./workspace").remove_session(th.thread_id)
    except Exception:
        pass
    th.file_path = None
    th.file_name = None
    th.sheet_name = None
    th.md_path = None
    th.schema_path = None
    th.df_raw = None
    th.paths = None
    th.upload_sig = None

# --- 추가 유틸: 단계별 진행바 + 상태박스 ---
@contextmanager
def step_status(label: str, expanded: bool = False):
    """
    Streamlit 1.27+ 의 st.status 래퍼.
    with step_status("...") as (status, prog, tick):
        tick(0.25, "단계 1 설명")
        ...
    """
    box = st.status(label, expanded=expanded)
    prog = st.progress(0)

    def tick(p: float, msg: str | None = None, delay: float = 0.0):
        prog.progress(min(max(p, 0.0), 1.0))
        if msg:
            box.write(msg)
        if delay > 0:
            time.sleep(delay)

    try:
        yield box, prog, tick
    finally:
    # '중'이 들어있으면 자연스럽게 제거해서 표시
        clean_label = label.replace(" 중", "")
        box.update(label=f"{clean_label} 완료", state="complete")
        prog.progress(1.0)


# --- 추가 유틸: '타자 중...' 플레이스홀더 (응답 생성 중 시각 효과) ---
def typing_placeholder():
    holder = st.empty()
    holder.markdown(
        """
        <div class="typing-wrap">
          <div class="typing-bubble"></div>
          <div class="typing-bubble"></div>
          <div class="typing-bubble"></div>
        </div>
        """,
        unsafe_allow_html=True
    )
    return holder  # holder.empty() 로 제거 가능


# ------------------- 멀티 스레드 상태 -------------------
@dataclass
class ThreadState:
    thread_id: str
    name: str
    created_at: str
    history: HistoryManager
    # 스레드별 데이터 상태
    file_path: str | None = None
    file_name: str | None = None
    sheet_name: str | None = None
    md_path: str | None = None
    schema_path: str | None = None
    df_raw: pd.DataFrame | None = None
    upload_sig: str | None = None   # 현재 업로드된 파일의 서명(내용 해시)
    paths: Paths | None = None      # prepare_with_session 결과

def _ensure_threads():
    ss = st.session_state
    if "threads" not in ss:
        default_hist = ss.get("history", HistoryManager(max_tokens=3000))
        tid = str(uuid4())[:8]
        ss.threads = {
            tid: ThreadState(
                thread_id=tid,
                name="기본 스레드",
                created_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
                history=default_hist,
            )
        }
        ss.current_tid = tid
    if "current_tid" not in ss or ss.current_tid not in ss.threads:
        ss.current_tid = next(iter(ss.threads.keys()))

def _current_thread() -> ThreadState:
    return st.session_state.threads[st.session_state.current_tid]

def _boot_once_cleanup_sessions():
    """새로고침 이후 최초 1회 workspace 세션 폴더 초기화."""
    if "booted" not in st.session_state:
        try:
            SessionManager("./workspace").remove_all_sessions()
        except Exception as e:
            st.warning(f"세션 디렉터리 초기화 중 경고: {e}")
        st.session_state.booted = True

# ------------------- 페이지 설정 -------------------
st.set_page_config(page_title="📊 CHANGSHIN INC", layout="wide")
ensure_dirs()                 # 1) 작업 폴더 구조 보장
_boot_once_cleanup_sessions() # 2) 기존 세션 폴더 정리(초기 1회)

# === 스타일 ===
st.markdown("""
<style>
.sidebar-title { display:flex; align-items:center; gap:.5rem; font-weight:700; }
.sidebar-title .dot { width:10px; height:10px; border-radius:50%; background:#9aa0a6; display:inline-block; }

.thread-item { display:flex; align-items:center; justify-content:space-between;
  padding:.45rem .6rem; border-radius:10px; margin-bottom:.25rem; transition: background 120ms; }
.thread-item:hover { background: rgba(255,255,255,0.06); }
.thread-item.active { background: rgba(72,133,237,0.18); }
.thread-name { white-space:nowrap; overflow:hidden; text-overflow:ellipsis; font-size:.95rem; }
.kebab-btn { background:transparent; border:none; color:#c9cdd2; cursor:pointer;
  font-size:1rem; padding:.2rem .35rem; border-radius:.4rem; }
.kebab-btn:hover { background: rgba(255,255,255,0.10); color:#fff; }
.pop-row { display:flex; gap:.5rem; }
.pop-danger { background:rgba(244, 67, 54, .12); border:1px solid rgba(244,67,54,.25); }
.pop-danger:hover { background:rgba(244, 67, 54, .22); }
.small-note { color:#9aa0a6; font-size:.8rem; }

/* 사이드바 최상단 패딩 거의 제거 */
section[data-testid="stSidebar"] .block-container { margin-top: -60px;  margin-bottom: -60px }
img.side-logo { margin: 0 !important; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
/* --- typing dots --- */
.typing-wrap {
  display: inline-flex; gap: 6px; align-items: center;
  padding: 10px 12px; border-radius: 12px;
  background: rgba(255,255,255,.06); border: 1px solid rgba(255,255,255,.12);
}
.typing-bubble {
  width: 8px; height: 8px; border-radius: 50%;
  background: #c9cdd2; opacity: .7; animation: tb 1.2s infinite;
}
.typing-bubble:nth-child(2){ animation-delay: .15s; }
.typing-bubble:nth-child(3){ animation-delay: .3s; }
@keyframes tb {
  0%{ transform: translateY(0); opacity:.5 }
  25%{ transform: translateY(-4px); opacity:1 }
  50%{ transform: translateY(0); opacity:.5 }
}

/* --- 미세한 skeleton 느낌의 박스 (원하면 활용) --- */
.skel {
  position: relative; overflow: hidden; border-radius: 12px;
  background: linear-gradient(90deg, rgba(255,255,255,.04) 25%, rgba(255,255,255,.08) 37%, rgba(255,255,255,.04) 63%);
  background-size: 400% 100%; animation: shimmer 1.2s ease-in-out infinite;
  height: 22px; margin: 6px 0;
}
@keyframes shimmer {
  0% { background-position: 100% 0; }
  100% { background-position: 0 0; }
}
</style>
""", unsafe_allow_html=True)


# === 사이드바 로고 ===
LOGO_SIDEBAR = Path(__file__).resolve().parent / "assets" / "logo_sidebar.png"
with st.sidebar:
    if LOGO_SIDEBAR.exists():
        st.image(str(LOGO_SIDEBAR), use_container_width=True)
        # st.markdown("---")
    else:
        st.caption("⚠️ assets/logo_sidebar.png 를 찾을 수 없습니다.")

# ------------------- 사이드바: 스레드 박스 -------------------
def render_thread_sidebar():
    _ensure_threads()
    ss = st.session_state
    threads = ss.threads
    cur_tid = ss.current_tid

    st.sidebar.markdown(" ## 대화내역 ")
    st.sidebar.write("")

    for tid, th in threads.items():
        is_active = (tid == cur_tid)

        left, right = st.sidebar.columns([12, 2])
        with left:
            btn_label = f"● {th.name}" if is_active else f"○ {th.name}"
            if st.button(btn_label, key=f"sel_{tid}", use_container_width=True):
                ss.current_tid = tid
                cur_tid = tid

        with right:
            with st.popover("⋯", use_container_width=True):
                st.caption(th.name)
                new_name = st.text_input("이름 바꾸기", value=th.name, key=f"rename_{tid}")
                if st.button("적용", key=f"apply_{tid}"):
                    th.name = new_name.strip() or th.name
                    st.rerun()

                st.divider()
                st.markdown("**관리**")

                # 파일 삭제
                file_disable = not th.file_path or not os.path.exists(th.file_path)
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("파일 삭제", key=f"del_file_{tid}", disabled=file_disable):
                        try:
                            if th.file_path and os.path.exists(th.file_path):
                                os.remove(th.file_path)
                        except Exception as e:
                            st.error(f"파일 삭제 실패: {e}")
                        finally:
                            _reset_thread(th)
                            st.rerun()
                with col2:
                    # 스레드 삭제 (최소 1개는 남기기)
                    dis = (len(threads) <= 1)
                    if st.button("스레드 삭제", key=f"del_thread_{tid}", disabled=dis):
                        try:
                            if th.thread_id:
                                SessionManager("./workspace").remove_session(th.thread_id)
                            del ss.threads[tid]
                            if ss.current_tid == tid:
                                ss.current_tid = next(iter(ss.threads.keys()))
                        finally:
                            st.rerun()

        css_class = "thread-item active" if is_active else "thread-item"
        st.sidebar.markdown(
            f'<div class="{css_class}"><div class="thread-name">{th.name}</div>'
            f'<div class="small-note">{th.created_at}</div></div>',
            unsafe_allow_html=True
        )

    st.sidebar.write("")
    if st.sidebar.button("➕ 새로운 대화", use_container_width=True):
        new_tid = str(uuid4())[:8]
        st.session_state.threads[new_tid] = ThreadState(
            thread_id=new_tid,
            name=f"스레드 {len(st.session_state.threads)+1}",
            created_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
            history=HistoryManager(max_tokens=3000),
        )
        st.session_state.current_tid = new_tid
        st.rerun()

render_thread_sidebar()
cur = _current_thread()

# ------------------- 본문 UI -------------------
st.markdown(
    """
    <div style='text-align: center; margin-top: -60px;'>
        <h2 style='font-weight: 600; margin: 0; color: var(--text-color);'>
            CHANGSHIN INC
        </h2>
        <h4 style='font-weight: 500; color: var(--secondary-text-color); margin-top: 0.2rem;'>
            채팅 서비스 | 엑셀 문서를 이해하고 적절한 답변을 제공해드려요
        </h4>
    </div>
    """,
    unsafe_allow_html=True
)
st.divider()
st.caption(f"현재 스레드: **{cur.name}**  · 생성 {cur.created_at}")

col_left, col_right = st.columns([1, 2], gap="large")

# ===== 좌측: 파일 업로드 + 시트 선택 + 자동 로딩 =====
with col_left:
    st.markdown("### 📂 파일 업로드 ")
    uploaded = st.file_uploader(
        "엑셀 또는 CSV 파일 선택",
        type=["xlsx", "csv"],
        key=f"uploader_{st.session_state.current_tid}"
    )

    # ✅ 항상 정의해 둠
    prev_sig_key = f"last_upload_sig_{cur.thread_id}"
    prev_sig = st.session_state.get(prev_sig_key, None)
    # 1) 새 파일 업로드 시 저장 (원본 파일명 유지, 동일 내용이면 저장 생략)
    if uploaded is not None:
        sig = _make_sig_from_uploaded(uploaded)
        if cur.upload_sig != sig:
            save_dir = Path("./data")
            save_dir.mkdir(parents=True, exist_ok=True)
            save_path = save_dir / uploaded.name
            with open(save_path, "wb") as f:
                f.write(uploaded.getbuffer())

            _reset_thread(cur)
            cur.file_path = str(save_path)
            cur.file_name = save_path.name
            cur.upload_sig = sig
            st.success(f"📁 업로드 완료: {save_path.name}")
        else:
            st.caption("🔁 동일 파일 재업로드 감지 — 저장 생략")

            

    # 2) 시트 목록 표시
    sheet_options: list[str] = []
    if cur.file_path:
        try:
            p = Path(cur.file_path)
            if p.suffix.lower() == ".csv":
                sheet_options = ["(CSV)"]
            else:
                sheet_options = pd.ExcelFile(cur.file_path).sheet_names
        except Exception as e:
            st.error(f"시트 목록을 가져오지 못했습니다: {e}")
            sheet_options = []

    # 3) 시트 선택 → 자동 로딩 & 세션 준비
    if sheet_options:
        prev_sheet = cur.sheet_name
        default_sheet = cur.sheet_name or (sheet_options[0] if sheet_options else None)
        cur.sheet_name = st.selectbox(
            "시트 선택",
            options=sheet_options,
            index=sheet_options.index(default_sheet) if default_sheet in sheet_options else 0,
            key=f"sheet_{st.session_state.current_tid}"
        )

        sheet_changed = (cur.sheet_name != prev_sheet)

        # 원본 로딩 (CSV는 sheet_name=None)
        try:
            p = Path(cur.file_path)
            _sheet = None if p.suffix.lower() == ".csv" else cur.sheet_name

            if sheet_changed or cur.df_raw is None:
                with step_status("데이터 로딩 중", expanded=False) as (box, prog, tick):
                    tick(0.2, "📖 원본 파일에서 데이터 읽는 중...", 0.05)
                    cur.df_raw = load_excel(cur.file_path, _sheet)
                    tick(0.6, f"🧮 데이터프레임 준비: {cur.df_raw.shape}", 0.05)
                    st.success(f"로드 완료: {cur.df_raw.shape}")
        except Exception as e:
            cur.df_raw = None
            st.error(f"엑셀 로딩 실패: {e}")

        # 세션/캐시 준비 (MD/스키마 링크 생성)
        try:
            if sheet_changed or cur.paths is None:
                with step_status("세션 준비 중", expanded=False) as (box, prog, tick):
                    tick(0.3, "🧱 스키마/미리보기 생성...", 0.05)
                    cur.paths = prepare_with_session(
                        thread_id=cur.thread_id,
                        input_path=cur.file_path,
                        sheet_name=(None if Path(cur.file_path).suffix.lower() == ".csv" else cur.sheet_name),
                        model_for_col_select=MODEL_FOR_COL_SELECT,
                        head_preview_rows=50,
                        workspace_root="./workspace",
                    )
                    cur.md_path = str(cur.paths.md_path)
                    cur.schema_path = str(cur.paths.schema_path)
                    tick(0.9, "🔗 세션 경로 연결 완료", 0.05)
                    st.caption("✅ 세션 준비 완료 (MD/스키마 연결)")
        except Exception as e:
            st.error(f"세션 준비 실패: {e}")


    # 4) 미리보기
    if cur.df_raw is not None:
        st.caption("미리보기(상위 10행)")
        st.dataframe(cur.df_raw.head(10), use_container_width=True)
    else:
        st.info("파일을 업로드하고 시트를 선택하면 자동으로 로딩됩니다.")

# ===== 우측: 질의 실행 =====
with col_right:
    st.markdown("### 💬 질의 실행")
    question = st.text_area("질문을 입력하세요", height=120, placeholder="예) 'AC25'의 K 평균은?")
    #head_rows = st.number_input("사용한 데이터 미리보기 행 수", min_value=0, value=0, step=1)

    run_disabled = (cur.df_raw is None) or (cur.paths is None) or (not question.strip())
    if st.button("실행", type="primary", use_container_width=True, disabled=run_disabled):
        t0 = time.perf_counter()

        # 화면에 '타자 중...' 애니메이션 띄우기
        typing = typing_placeholder()

        # 재시도 옵션
        retry_opts = RetryOptions(
            max_retries=2,
            backoff_sec=0.8,
            retry_on_empty=True,
            retry_on_error=True,
        )

        schema_path = str(cur.paths.schema_path)
        md_path = str(cur.paths.md_path)

        with step_status("답변 생성 중", expanded=True) as (box, prog, tick):
            tick(0.15, "🧭 질문 리라이팅 / 연관성 판별...", 0.05)
            # (LLM 내부에서 진행될 단계 - 실제 호출 전 표시용)

            tick(0.35, "🧩 컬럼 선택 / 파이프라인 계획...", 0.05)

            # 실제 호출
            out = ask_one_with_retry(
            df_raw=cur.df_raw,
            question=question.strip(),
            schema_path=schema_path,
            md_path=md_path,
            model_for_col_select=MODEL_FOR_COL_SELECT,
            pandasai_llm_model=PANDASAI_LLM_MODEL,
            history=cur.history,
            use_rewritten_for_all=USE_REWRITTEN_FOR_ALL,
            head_rows=None,
            retry=retry_opts,
            # ✅ 추가: 캐시 경로를 명시적으로 전달 → 항상 cache/YYYYMMDD__해시/query_log.jsonl에 기록됨
            cache_dir_override=str(cur.paths.cache_dir),
        )


            tick(0.75, "🧮 Pandas 코드 실행 / 결과 정리...", 0.05)
            # 이후 요약/렌더링은 기존 로직 그대로 실행됨

        # 타자 애니메이션 제거
        typing.empty()

        elapsed = time.perf_counter() - t0
        st.caption(f"⏱️ 처리 시간: {elapsed:.2f}s")

        
        # # --- 디버깅 블록 추가 ---
        # with st.expander("🧩 LLM 원본 df_out / 코드 확인"):
        #     st.text_area("LLM 생성 Pandas 코드", value=out.get("code") or "", height=200)
        #     st.markdown("**사용된 컬럼:** " + str(out.get("used_columns", [])))

        # with st.expander("📄 최종 마크다운 원문"):
        #     st.code(out.get("markdown") or "", language="markdown")
        # # --- 디버깅 블록 끝 ---


        # --- 요약 박스 ---
        is_related = bool(out.get("is_related"))
        related_text = "이전 질문과 연관된 질문입니다." if is_related else "이전 질문과 연관되지 않은 질문입니다."
        rewrite_reason = out.get("rewrite_reason") or out.get("reason") or out.get("rewriter_reason") or ""
        rewritten = out.get("rewritten", "").strip()
        selected_cols = out.get("selected_cols", [])
        cols_text = ", ".join(selected_cols) if selected_cols else "(없음)"
        ri = out.get("retry_info", {})
        attempts = ri.get("attempts", 1)
        status = ri.get("status", "ok")

        st.markdown("### 💬 답변")
        summary_html = f"""
        <div style="
            border:1px solid rgba(255,255,255,0.15);
            padding:12px 14px;
            border-radius:12px;
            background-color:rgba(255,255,255,0.03);
            ">
            <div><strong>연관 여부</strong> — {related_text}</div>
            {f'<div><strong>근거</strong> — {rewrite_reason}</div>' if rewrite_reason else ''}
            <div><strong>최종 질문(리라이팅)</strong> — {rewritten or "(없음)"}</div>
            <div><strong>계산을 위해 사용한 컬럼</strong> — <code>{cols_text}</code></div>
            <div><small>시도 {attempts}회 · 상태={status}</small></div>
        </div>
        """
        st.markdown(summary_html, unsafe_allow_html=True)

        # # 에러 시 디버그 코드=======================
        # with st.expander("디버그 · 경로 및 파일 상태"):
        #     st.write({"schema_path": str(cur.paths.schema_path), "md_path": str(cur.paths.md_path)})
        #     st.write({
        #         "schema_exists": Path(cur.paths.schema_path).exists(),
        #         "md_exists": Path(cur.paths.md_path).exists(),
        #     })
        #     st.write({"selected_cols": out.get("selected_cols")})

        # ms_err = out.get("md_subset_error")
        # if ms_err:
        #     st.warning(f"md_subset 생성 중 오류: {ms_err}")

        # # 전체 원문(reason/code)도 확인
        # with st.expander("디버그 · reason/code"):
        #     st.markdown(out.get("reason_answer", ""))
        
        # with st.expander("디버그 · 경로 및 파일 상태"):
        #     mdp, shp = Path(cur.paths.md_path), Path(cur.paths.schema_path)
        #     st.write({
        #         "md_path": str(mdp),
        #         "md_exists": mdp.exists(),
        #         "md_size": (mdp.stat().st_size if mdp.exists() else -1),
        #         "schema_path": str(shp),
        #         "schema_exists": shp.exists(),
        #         "schema_size": (shp.stat().st_size if shp.exists() else -1),
        #     })

        # ======================================    

        # 1) 표만 추출
        answer_md = _between_tags(out.get("reason_answer", ""), "answer")
        st.markdown("##### 🔹Answer")
        if answer_md:
            st.markdown(answer_md)
        else:
            st.info("표 형태의 답변이 없습니다.")

        # 2) (선택) 부분 마크다운 표
        if out.get("md_subset"):
            st.write("사용한 데이터 일부")
            st.code(out["md_subset"], language="markdown")

        # 3) 생성된 Pandas 코드
        if out.get("code"):
            with st.expander("계산 과정"):
                st.code(out["code"], language="python")
        
        # ✅ 히스토리 항목에도 코드 저장(ask_one_with_retry가 turns를 추가했다는 전제 하에 보강 저장)
        try:
            if out.get("code") and getattr(cur.history, "turns", None):
                cur.history.turns[-1]["code"] = out["code"]
        except Exception:
            pass

# ===== 히스토리 =====
st.markdown("---")
st.subheader("📜 대화 히스토리")
if not cur.history.turns:
    st.info("아직 히스토리가 없습니다.")
else:
    for i, t in enumerate(cur.history.turns, 1):
        st.markdown(f"**{i}. Q:** {t['q']}")
        st.markdown(f"**A (표):**\n{t['a']}")
        st.caption(f"used_columns = {t.get('used', [])}")
        # ✅ 히스토리에 저장된 판다스 코드 노출
        code_text = t.get("code")
        if code_text:
            with st.expander(f"계산 과정"):
                st.code(code_text, language="python")
        st.divider()
