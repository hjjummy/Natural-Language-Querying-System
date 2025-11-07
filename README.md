# NLQS — Data Insight AI Agents

이 레포지토리는 자연어 질의로 엑셀·DB 데이터를 분석하고,  
AI가 자동으로 SQL/Pandas 코드·집계·인사이트를 도출하는 통합 에이전트 시스템입니다.  

본 프로젝트는 **PandasAI Agent**와 **SQL Agent**를 포함합니다.

---

## 🧩 프로젝트 개요

| 구분 | 설명 |
|------|------|
| **프로젝트명** | Changshin INC Data Insight Agent |
| **목표** | 자연어로 데이터를 질의하면 AI가 자동으로 계산·집계·SQL 실행 결과를 도출 |
| **핵심 기술** | PandasAI, DuckDB, Streamlit, Text-to-SQL, OpenAI GPT-5 |
| **결과물** | 웹 기반 데이터 분석 어시스턴트 (Streamlit UI) |

---

## 📚 구성 구조

```
repo_root/
├── pandas_agent/            # 엑셀 기반 자연어 분석 (PandasAI)
│   ├── core/
│   ├── app.py               # Streamlit 앱
│   ├── README.md
│
├── sql_agent/               # DB 기반 자연어 분석 (DuckDB + Text-to-SQL)
│   ├── core/
│   ├── app.py
│   ├── schema/
│   ├── db/
│   ├── README.md
│
├── data/                    # 샘플 데이터
├── workspace/               # 세션 캐시 / 결과 저장
├── logs/                    # 로그 파일
├── .env                     # OpenAI API Key / 설정 파일
└── README.md                # (현재 파일)
```

---

## 🧠 주요 기능 비교

| 항목 | **Pandas Agent** | **SQL Agent** |
|------|------------------|----------------|
| 데이터 원천 | Excel / CSV | DuckDB / DB 파일 |
| 처리 방식 | Pandas 연산 | Text-to-SQL 변환 후 DuckDB 실행 |
| 모델 | GPT-4o (pandas-chain prompt) | GPT-4o (rewrite + SQL-generation) |
| 출력 형식 | DataFrame (Markdown 변환) | SQL 실행 결과 (Markdown 변환) |
| 특징 | 엑셀 업로드 → AI 계산 자동화 | DB 질의 자동화, SELECT-only 가드 적용 |
| 프레임워크 | Streamlit | Streamlit |
| 고도화 기능 | 열 선택/리라이팅 히스토리 | SQL Guard, 스키마 인트로스펙션 |

---

## ⚙️ 환경 설정

### 1️⃣ 사전 설치
```
python 3.11+
pip install -r requirements.txt
```

### 2️⃣ 환경 변수 (.env)
```
OPENAI_API_KEY=sk-xxxx...
# (선택) 전용 엔드포인트
# OPENAI_BASE_URL=https://your-endpoint/v1
```


## 💻 실행 방법

### ▶ Pandas Agent 실행
```
cd pandas_agent
streamlit run app.py
```
> 엑셀 업로드 후 자연어 질의 → Pandas 코드 자동 생성/실행

---

### ▶ SQL Agent 실행
```
cd sql_agent
streamlit run app.py
```
> DuckDB 스키마를 기반으로 Text-to-SQL → SQL 가드 → 실행 결과 표 출력

---

## 🧠 작동 흐름 요약

### 🔹 Pandas Agent
1. 엑셀/CSV 업로드 → 열 정의 요약  
2. LLM이 자연어 질의를 해석하여 Pandas 코드 생성  
3. DataFrame에서 직접 계산 후 결과 표로 반환  
4. 연속 질의 시, 히스토리를 기반으로 대화형 맥락 유지  

### 🔹 SQL Agent
1. DuckDB 스키마 자동 인식 (PRAGMA table_info)  
2. LLM이 질의를 리라이팅 → Text-to-SQL 변환  
3. SQL 가드(SELECT-only / LIMIT 보정) 적용  
4. DuckDB에서 실행 후 결과를 Markdown 표로 반환  

---

## 📄 라이선스
© 2025 hjjummy  
**All rights reserved.**  
(무단 복제·수정·배포 금지)