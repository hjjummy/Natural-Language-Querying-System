# 🧠 SQL Agent — Natural Language → DuckDB SQL

자연어 질의를 **DuckDB용 SQL**로 변환하고, **SELECT-only 가드**와 **LIMIT 보정**을 거쳐 실행 결과를 표로 반환하는 경량 NL→SQL 에이전트임.  

---

## ✨ 핵심 특징

- **리라이팅(Rewrite)** → **Text-to-SQL 생성** → **가드(SELECT-only/LIMIT)** → **DuckDB 실행** → **Markdown 표 변환**
- **가드레일**: DDL/DML 금지, 다중문장 금지, `LIMIT ≤ 500` 강제, 허용 테이블 화이트리스트로 안전 실행
- **스키마 자동 인트로스펙션**: `PRAGMA table_info` + DISTINCT 샘플로 LLM 프롬프트에 투입
- **히스토리 인식**: 멀티턴 맥락(직전 결과/조건)을 반영한 스코프·필터 추정
- **RAG(옵션)**: few-shot 예시 주입(없으면 자동 생략)
- **구조화 출력**: 생성 SQL/클린 SQL/실행 SQL/마크다운/리라이팅 근거 등 메타 포함

---

## 📦 폴더 구조

```
sql_agent/
└── core/
    ├── config.py         # 환경변수/경로/모델/가드 기본설정, ensure_dirs()
    ├── engine.py         # HistoryManager, ask_one_sql() (리라이팅→SQL→실행)
    ├── io.py             # DataFrame → Markdown, 태그 추출 등 유틸
    ├── llm.py            # OpenAI 래퍼(chat/chat_json), rewrite(), to_sql()
    ├── schema.py         # PRAGMA table_info 기반 JSON 스키마 생성/로드
    ├── sql_executor.py   # SELECT-only/LIMIT 가드, DuckDB 실행/스키마/테이블목록
    └── session.py        # cache/sessions 구조 및 schema.json 캐싱/복사
```

---

## ⚙️ 요구 사항

- Python 3.11+
- DuckDB
- OpenAI API Key

**.env 예시**
```
OPENAI_API_KEY=sk-xxxx...
# (옵션) 프록시/전용 엔드포인트 사용 시
# OPENAI_BASE_URL=https://your-endpoint/v1
```

---

## 🔧 설정 주요 포인트 (`core/config.py`)

- **DB 경로**: `DUCKDB_PATH = ./db/manufacturing.db`
- **허용 테이블**: `SQL_ALLOWED_TABLES = {"fact_manufacturing","v_factory_grade_summary","v_edition_kpi"}`
- **가드**: SELECT-only, 금지 키워드, `SQL_DEFAULT_LIMIT=500`
- **모델**: `MODEL_REWRITE`, `MODEL_SQL` (기본 `gpt-4o`)



---

## 🧠 동작 흐름(요약)

1. **히스토리 빌드**: 직전 턴들의 `<Q_original>`, `<Q_rewritten>`, `<A>`, `<executed_sql>`을 합쳐 맥락 문자열 생성  
2. **리라이팅**: 애매한 지시어/대용어를 구체화하여 단일 명령형 질의로 변환(JSON)  
3. **Text-to-SQL**: DuckDB 스키마(JSON)와 히스토리, (옵션) few-shot를 바탕으로 SQL 생성(JSON)  
4. **가드 & 정리**: 주석/코드펜스 제거, SELECT-only 검증, `LIMIT ≤ 500` 보정  
5. **실행 & 포맷팅**: DuckDB 실행 → DataFrame → Markdown 표 변환  
6. **히스토리 적재**: 원질문/리라이팅/SQL/결과표를 히스토리에 저장

---



## 📄 라이선스

© 2025 hjjummy  
**All rights reserved.**  
(무단 복제·수정·배포 금지)
