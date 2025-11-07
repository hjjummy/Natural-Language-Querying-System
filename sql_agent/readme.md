~/test/cs_inc/
├── app.py                           # Streamlit / CLI 진입점
│                                     └─ 사용자가 질문 입력 → LLM이 SQL 생성 → DB 실행
├── setup_duckdb.py                  # ✅스키마 정의 파일로 DB에 데이터 적재 코드
│
├── .env                             #  환경 변수 (DB_PATH, OPENAI_API_KEY 등)
│
├── requirements.txt                 # 의존성 (duckdb, pandas, openai, streamlit 등)
│
├── schema/
│   └── ddl.sql                      # ✅ DB 스키마 정의 (있다고 가정)
│
├── db/
│   └── manufacturing.db             # ✅ 실제 DuckDB 파일 (있다고 가정)
│
├── core/
│   ├── __init__.py
│   ├── config.py                    # 기본 설정값 (경로, 모델명, DB 연결정보 등)
│   ├── llm.py                       # 💬 질문 리라이팅 + SQL 생성
│   ├── engine.py                    # 🚀 LLM → SQL → 실행 → 결과 반환
│   ├── schema.py                    # 엑셀 기반 컬럼 정의 / 스키마 로딩
│   ├── session.py                   # 세션 관리 / 캐시 유지
│   │
│   ├── sql/                         # 🧱 Text-to-SQL 실행 계층
│   │   ├── __init__.py
│   │   ├── db.py                    # DuckDB 연결 (SQLRunner)
│   │   ├── guard.py                 # SELECT-only, LIMIT, 금지어 필터 등 SQL 가드
│   │   ├── executor.py              # SQL 실행 + 결과 DataFrame 반환
│   │   └── inspector.py             # (선택) DB 스키마 introspection
│
├── workspace/                       # 세션별 캐시 / 임시 데이터
│   └── sessions/
│
└── logs/                            # 실행 로그, 오류 로그 (선택)
