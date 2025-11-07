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
import streamlit as st
from uuid import uuid4
from datetime import datetime
from dataclasses import dataclass

# core
from core.config import ensure_dirs, SQL_SCHEMA, SQL_ALLOWED_TABLES
from core.llm import rewrite, to_sql
from core.sql_executor import (
    search as sql_search,
    list_tables,
)
from core.engine import HistoryManager

# ================= 유틸 =================
def _between_tags(s: str, tag: str) -> str:
    m = re.search(rf"<{tag}>\s*(.*?)\s*</{tag}>", s or "", flags=re.S | re.I)
    return (m.group(1).strip() if m else (s or "").strip())


# ================= 스레드 상태 =================
@dataclass
class ThreadState:
    thread_id: str
    name: str
    created_at: str
    history: HistoryManager
    table: str = "fact_manufacturing"

def _ensure_threads():
    ss = st.session_state
    if "threads" not in ss:
        tid = str(uuid4())[:8]
        ss.threads = {
            tid: ThreadState(
                thread_id=tid,
                name="기본 스레드",
                created_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
                history=HistoryManager(max_tokens=3000),
            )
        }
        ss.current_tid = tid
    if "current_tid" not in ss or ss.current_tid not in ss.threads:
        ss.current_tid = next(iter(ss.threads.keys()))

def _current_thread() -> ThreadState:
    return st.session_state.threads[st.session_state.current_tid]

# ================= 페이지 설정 =================
st.set_page_config(page_title="📊 CHANGSHIN INC (Text-to-SQL)", layout="wide")
ensure_dirs()

# --- 스타일(상단 여백 축소 등) ---
st.markdown("""
<style>
section[data-testid="stSidebar"] .block-container { margin-top: -60px; margin-bottom: -60px }
.thread-name { white-space:nowrap; overflow:hidden; text-overflow:ellipsis; font-size:.95rem; }
.small-note { color:#9aa0a6; font-size:.8rem; }
.thread-item { padding:.45rem .6rem; border-radius:10px; margin-bottom:.25rem; transition: background 120ms; }
.thread-item:hover { background: rgba(255,255,255,0.06); }
.thread-item.active { background: rgba(72,133,237,0.18); }
</style>
""", unsafe_allow_html=True)

# ================= 사이드바: 스레드 =================
def render_thread_sidebar():
    _ensure_threads()
    ss = st.session_state
    threads = ss.threads
    cur_tid = ss.current_tid

    st.sidebar.markdown("## 대화내역")
    st.sidebar.write("")

    for tid, th in threads.items():
        col_l, col_r = st.sidebar.columns([10, 3])
        with col_l:
            if st.button(("● " if tid == cur_tid else "○ ") + th.name, key=f"sel_{tid}", use_container_width=True):
                ss.current_tid = tid
        with col_r:
            with st.popover("⋯", use_container_width=True):
                new_name = st.text_input("이름 바꾸기", value=th.name, key=f"rename_{tid}")
                if st.button("적용", key=f"apply_{tid}"):
                    th.name = new_name.strip() or th.name
                    st.rerun()
                st.divider()
                if st.button("스레드 삭제", key=f"del_{tid}", disabled=(len(threads) <= 1)):
                    del ss.threads[tid]
                    ss.current_tid = next(iter(ss.threads.keys()))
                    st.rerun()

        css = "thread-item active" if tid == cur_tid else "thread-item"
        st.sidebar.markdown(
            f'<div class="{css}"><div class="thread-name">{th.name}</div>'
            f'<div class="small-note">{th.created_at}</div></div>',
            unsafe_allow_html=True
        )

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

# ================= 헤더 =================
st.markdown(
    """
    <div style='text-align:center; margin-top:-60px;'>
        <h2 style='font-weight:600; margin:0;'>CHANGSHIN INC</h2>
        <h4 style='font-weight:500; margin-top:0.2rem;'>Text-to-SQL 기반 DB 질의 서비스</h4>
    </div>
    """,
    unsafe_allow_html=True,
)
st.divider()
st.caption(f"현재 스레드: **{cur.name}** · 생성 {cur.created_at}")

# ================= 본문 =================
left, right = st.columns([1, 2], gap="large")

# --- 좌: 테이블 선택
with left:
    st.markdown("### ⚙️ 설정")
    try:
        tbls = list_tables(schema=SQL_SCHEMA)
        allowed = sorted([t for t in tbls if t in SQL_ALLOWED_TABLES])
    except Exception as e:
        allowed = []
        st.error(f"테이블 조회 실패: {e}")

    if not allowed:
        st.warning("허용된 테이블이 없습니다. (SQL_ALLOWED_TABLES 확인)")
    else:
        cur.table = st.selectbox("대상 테이블", options=allowed, index=allowed.index(cur.table) if cur.table in allowed else 0)

    # with st.expander("ℹ️ 동작 개요", expanded=False):
    #     st.markdown(
    #         "- 자연어 질의 → 리라이팅 → DuckDB SQL 생성 → 실행 → 표 출력\n"
    #         "- DDL/DML은 차단됨 (SELECT-only)\n"
    #         "- 결과는 상위 50행 미리보기로 표시"
    #     )
    with st.expander("ℹ️ DB 데이터 시나리오", expanded=False):
        st.markdown(
            """
    ##### 📌 데이터 개요
    - 제공받은 샘플 데이터를 기반으로 **생산 공정 시나리오**를 구성함  
    (공장·라인·제품 단위의 효율, 품질, 속도, 온도 등 주요 지표 포함)

    ---

    ##### 🧱 스키마 시나리오
    <div style="overflow-x:auto; border:1px solid #444; border-radius:6px; padding:4px;">
        
    | 원본 | 컬럼명 | 의미(측정 항목) | 타입/단위 | 값 예시·범위 | 비고 |
    | --- | --- | --- | --- | --- | --- |
    | A | `factory_code` | 생산 국가/지역 코드 | TEXT | `AC25`, `AB25`, `AA24` | 지역+사이트 식별 |
    | C | `line_code` | 생산 제품 라인 | TEXT | `AAA157`, `AAA130` | 라인/셀 식별 |
    | E | `product_code` | 제품(SKU) 코드 | TEXT | `CCCCCCC-DD084` | 품번/바코드 역할 |
    | M | `line_grade` | 라인 등급 | TEXT | `U 1st`, `S 3rd` | U/S + 1st~3rd |
    | R | `edition_type` | 제품 생산/유통 유형 | TEXT | `M`,`W`,`J`,`T`,(공백) | 한정/도매/표준 구분 |
    | K | `efficiency_index` | 공정 효율 지수 | DOUBLE (index) | 대략 30~200 | 무단위(정규화 지수) |
    | P | `output_qty` | 생산량 | DOUBLE (pcs) | 0 ~ 375,000 | 개수 |
    | T | `cycle_time_s` | 사이클 타임 | DOUBLE (sec) | 2.0 ~ 180 | 1개 제품당 공정 시간 |
    | U | `mold_temp_c` | 금형 온도 | DOUBLE (°C) | 80 ~ 140 | 성형/프레스 금형 |
    | V | `inj_pressure_bar` | 사출 압력 | DOUBLE (bar) | 50 ~ 180 | 성형 구간 압력 |
    | W | `conv_speed_mps` | 컨베이어 속도 | DOUBLE (m/s) | 0.2 ~ 2.0 | 라인 이송 속도 |
    | X | `inproc_pass_flag` | 공정내 합격 플래그 | INTEGER (0/100) | 0 또는 100 | 이진 플래그(0/100) |
    | Y | `rebound_coeff_pct` | 완성품 반발탄성 계수 | DOUBLE (%) | 5 ~ 150 | 후공정 품질수치로 고정 |
    | Z | `final_perf_score` | 최종 성능 종합점수 | DOUBLE (index) | 0 ~ 120+ | 100 초과 가능 |

    </div>

    ---

    ##### 🏭 A — `factory_code` (공장/사업장)
    | 코드 | 의미 | 설명 |
    | --- | --- | --- |
    | **AA24** | 중국 푸젠 공장 | 파일럿/신소재 테스트 비중 높음 |
    | **AA25** | 중국 광둥 공장 | 표준형·중간단가 라인 중심 |
    | **AB25** | 인도네시아 자카르타 공장 | 대량생산 중심, 효율 위주 |
    | **AC25** | 베트남 동나이 공장 | **주력(Main Factory)**, 물량+품질 핵심 |

    ---

    ##### 🎛️ M — `line_grade` (라인 등급)
    | 코드 | 의미 | 설명 |
    | --- | --- | --- |
    | **U 1st** | 상위 1등급 (Premium) | 자동화/정밀도 최고 |
    | **U 2nd** | 상위 2등급 (Advanced) | 핵심 품목, 일부 수동 |
    | **U 3rd** | 상위 3등급 (General Upper) | 중간 이상 품질 |
    | **S 1st** | 표준 1등급 | 양산 중심, 안정적 품질 |
    | **S 2nd** | 표준 2등급 | 범용, 자동화 중간 |
    | **S 3rd** | 표준 3등급 (Support) | 보조·외주·대량 생산 |

    ---

    ##### 📦 R — `edition_type` (생산/유통 유형)
    | 코드 | 의미 | 설명 |
    | --- | --- | --- |
    | **M** | Main Edition | 주력 표준형 모델 |
    | **W** | Wholesale Edition | 도매·대량 유통형 |
    | **J** | Joint Edition | 합작/공동생산 버전 |
    | **T** | Trial/Test | 시험생산·한정판 |
    | **(공백)** | Standard | 일반 정규 양산품 |
            """,
            unsafe_allow_html=True,
        )


# --- 우: 질의 실행
with right:
    st.markdown("### 💬 질의")
    question = st.text_area("자연어 질문을 입력하세요", height=120, placeholder="예) S 3rd 등급의 평균 효율을 보여줘")

    run_disabled = not question.strip() or not cur.table
    if st.button("실행", type="primary", use_container_width=True, disabled=run_disabled):
        res = sql_search(
            question=question.strip(),
            table=cur.table,
            history=cur.history,
        )

        # 📌 요약 (기존 표시부)
        st.markdown("### 📌 요약")
        with st.container(border=True):
            st.write(f"- **리라이팅**: {res.get('rewritten','') or '(원문 사용)'}")
            gen = res.get("generation", {})
            st.write(f"- **이유**: {gen.get('reasoning','') or '(생략됨)'}")
            st.write(f"- **실행 SQL:**")
            st.code(res.get("executed_sql",""), language="sql")

        if "schema_used" in res:
            with st.expander("💡 사용된 스키마 정보", expanded=False):
                st.json(res["schema_used"])

        st.markdown("### 📄 결과")
        st.markdown(res.get("markdown", "| (empty) |\n|---|\n| (no rows) |"))

        # ✅ 히스토리에 '원본 질문 + 리라이팅 + 결과표 + 사용컬럼 + 실행SQL' 저장
        # cur.history.add(
        #     orig_q=question.strip(),
        #     rewritten_q=res.get("rewritten", question.strip()),
        #     answer_md=res.get("markdown", ""),
        #     used=res.get("used_columns", []),         # 없으면 []로
        #     executed_sql=res.get("executed_sql", ""), # 없으면 ""로
        #     meta={"reasoning": gen.get("reasoning", "")}
        # )


# ================= 히스토리 =================
st.markdown("---")
st.subheader("📜 대화 히스토리")

if not cur.history.turns:
    st.info("아직 히스토리가 없습니다.")
else:
    for i, t in enumerate(cur.history.turns, 1):
        st.markdown(f"**{i}. 원본 질문:** {t.get('orig_q','') or '(비어 있음)'}**")
        st.markdown(f"**↳ 리라이팅:** {t.get('rewritten_q','(원문 사용)')}**")
        st.markdown(f"**A (표):**\n{t.get('a','')}")
        if t.get("executed_sql"):
            with st.expander("💾 실행 SQL 보기", expanded=False):
                st.code(t["executed_sql"], language="sql")
        if t.get("used"):
            st.caption(f"사용된 컬럼: {t['used']}")
        st.divider()
