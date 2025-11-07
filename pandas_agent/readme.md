~/test/cs_inc/
├── app.py                           # ✅ Streamlit / CLI 진입점
│                                    #  └─ 사용자가 질문을 입력하고 결과를 출력
│                                    #     thread_id 생성 후 prepare_with_session() 호출
│
├── .env                             # ✅ 환경 변수 파일 (OPENAI_API_KEY 등)
│
├── requirements.txt                 # ✅ 의존성 (pandas, openai, streamlit 등)
│
├── core/                            # ✅ 핵심 로직 (엔진 + I/O + 세션)
│   ├── __init__.py
│   ├── config.py                    # 설정값 (EXCEL_PATH, 모델 이름 등)
│   ├── io.py                        # Excel → Markdown 변환, 파일 입출력
│   ├── engine.py                    # LLM + PandasAI 실행 / Retry / History 관리
│   ├── llm.py                       # LLM 호출 래퍼 (rewrite, select_columns 등)
│   ├── pandasai.py                  # PandasAI 연동 및 결과 정리
│   ├── schema.py                    # Schema 생성 및 관리 (LLM 기반 JSON 생성)
│   ├── session.py                   # ✅ 스레드별 workspace & cache 관리 (NEW)
│   └── ...
│
├── data/                            # ✅ 원본 데이터 (엑셀/CSV 등)
│   └── summary_gpt_v2.0.xlsx
│
├── md_out/                          # 마크다운 데이터
│
├── workspace/                       # ✅ 자동 생성 영역 (세션 & 캐시)
│   ├── cache/                       # 파일 해시별 공용 캐시
│   │   └── 23a4c1a9d9.../           # 해시값 = 파일 + 시트 이름
│   │       ├── source.xlsx          # 원본 복사본
│   │       ├── summary__SUMMARY.md  # 변환된 MD
│   │       └── schema.json          # 생성된 스키마
│   │
│   └── sessions/                    # 각 스레드별 독립 폴더
│       ├── 9cf121b2/                # thread_id = UUID 일부
│       │   ├── current.json         # 현재 세션의 캐시 정보
│       │   ├── md                   # → ../../cache/<hash>/summary__SUMMARY.md
│       │   └── schema.json          # → ../../cache/<hash>/schema.json
│       │
│       └── 3a41dc78/                # 또 다른 스레드
│           ├── current.json
│           ├── md
│           └── schema.json
│
├── logs/                            # (선택) 실행 로그, 오류 저장
│
└── README.md                        # 프로젝트 개요 / 실행법
