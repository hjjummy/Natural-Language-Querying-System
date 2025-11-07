# 🧠 pandas_agent — Natural Language DataFrame Agent

엑셀/CSV 파일을 업로드 후 자연어로 질의하면 LLM이 파일을 분석 하여, Pandas 코드를 생성·실행 후 결과 표를 돌려주는 에이전트입니다.  


---

## 📦 폴더 구조

    pandas_agent/
    └── core/
          ├── engine.py      # 세션 준비,   히스토리, 단발/재시도 실행 오케스트레이션
          ├── io.py          # 엑셀/CSV 로드, 마크다운 테이블 입/출력, 서브셋 빌더
          ├── llm.py         # OpenAI 래퍼(chat/chat_json), rewrite, select_columns
          ├── pandasai.py    # PandasAI 실행, 프롬프트 규칙, __row_idx 확장/마크다운 변환
          ├── schema.py      # 프리뷰 기반 스키마 생성 프롬프트, JSON 추출 유틸
          └── session.py     # cache/sessions 디렉터리 준비·복사·정리

---

## ⚙️ 요구 사항

- Python 3.11+
- OpenAI API 키 (`OPENAI_API_KEY` 환경변수)
- 권장 패키지

      pandas
      numpy
      pandasai
      openpyxl
      openai
      tiktoken

`.env` 예시:

      OPENAI_API_KEY=sk-xxxx...

---

## 🧠 동작 개요 (엔드투엔드)

1. **세션 준비 (`engine.prepare_with_session`)**  
   입력 파일을 로드하고 **프리뷰 마크다운(MD)**, **컬럼 스키마(JSON)**를 생성·캐시 → 세션 디렉터리에 링크/복사

2. **리라이팅 & 컬럼 선택 (`llm.rewrite`, `llm.select_columns`)**  
   질문을 **명확한 단일 질의**로 리라이팅 → 스키마 설명을 바탕으로 **관련 열만** 선택(프롬프트 최소화)

3. **Pandas 코드 생성 & 실행 (`pandasai.run`)**  
   엄격한 **프롬프트 규칙**으로 Pandas-only 코드 생성/실행 → 수치형 변환, 표시 규칙, 빈 결과/에러 감지 및 리트라이

4. **표시 & 원본 행 복원**  
   `__row_idx`가 결과에 있으면 **원본 DataFrame에서 해당 행 전체**를 추출해 표로 반환, 없으면 집계 표 그대로 사용

---

## 🧩 주요 컴포넌트

### `core/engine.py`
- `HistoryManager`: 멀티턴 이력(<Q>, <A>, <used_columns>)을 토큰 한도 내에서 누적·빌드
- `prepare_with_session(...)`: 파일 기반 **MD/스키마 캐시** 생성 및 세션 경로 연결
- `ask_one_with_retry(...)`: 리라이팅 → 컬럼선택 → PandasAI 실행 → **빈결과/에러 자동 재시도**

### `core/pandasai.py`
- **프롬프트 가드레일**: `pandas/numpy` 외 금지, 파일/네트워크 접근 금지, 오직 코드만 출력, `result` 강제
- **수치 캐스팅**: 콤마 제거 후 `pd.to_numeric(..., errors="coerce")`
- **__row_idx 확장**: 소량 행(≤10) 반환 시 **원본 전체 열**로 자동 확장하여 가독성 확보

### `core/schema.py`
- **스키마 프롬프트**: 프리뷰 MD + 간이 통계로 컬럼별 정의(JSON) 생성
- **`extract_json`**: 코드펜스 제거·본문 추출로 JSON 파싱 안정화

### `core/io.py`
- **엑셀/CSV 로더** (`openpyxl` 엔진 명시)  
- **마크다운 테이블** ↔ DataFrame 변환, 선택 컬럼만 서브셋화

### `core/session.py`
- **cache/**: 입력파일·시트 기반 해시폴더에 MD, schema.json 캐시  
- **sessions/**: 스레드별 세션 디렉토리; 캐시 파일을 **복사**하여 격리

---

## 🧪 예시 질문 템플릿

- “**AC25** 공장의 **라인별** `K` 평균과 **상위 3개 라인**을 알려줘”
- “`M == 'U 1st'`인 데이터 중 `P > 0`의 **K 중앙값(Median)**”
- “`Z == 100`과 `Z == 0`의 `K` 평균 차이(절대값)”
- “`X == 100` vs `X == 0`의 `(T+U+V)` 평균 중 어느 쪽이 높나?”

---


## 🏷️ 라이선스
© 2025 hjjummy  
All rights reserved.

