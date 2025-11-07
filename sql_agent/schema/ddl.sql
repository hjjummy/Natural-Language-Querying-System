-- DuckDB 기본 스키마(main.main) 사용
-- manufacturing 전용 테이블 및 뷰 정의

-- 메인 팩트 테이블
CREATE TABLE IF NOT EXISTS fact_manufacturing (
  -- 식별/분류
  factory_code       TEXT    NOT NULL,  -- {AA24,AA25,AB25,AC25}
  line_code          TEXT    NOT NULL,  -- 예: AAA157
  product_code       TEXT    NOT NULL,  -- 예: CCCCCCC-DD084
  line_grade         TEXT    NOT NULL,  -- {U 1st,U 2nd,U 3rd,S 1st,S 2nd,S 3rd}
  edition_type       TEXT,              -- {M,W,J,T,NULL}

  -- 핵심 지표
  efficiency_index   DOUBLE,            -- K: 공정 효율 지수(무단위)
  output_qty         DOUBLE,            -- P: 생산량(pcs)

  -- 공정 센서(단일 의미 확정)
  cycle_time_s       DOUBLE,            -- T: 사이클 타임(초)
  mold_temp_c        DOUBLE,            -- U: 금형 온도(°C)
  inj_pressure_bar   DOUBLE,            -- V: 사출 압력(bar)
  conv_speed_mps     DOUBLE,            -- W: 컨베이어 속도(m/s)
  inproc_pass_flag   INTEGER,           -- X: 공정내 합격 플래그(0/1)

  -- 후공정 품질
  rebound_coeff_pct  DOUBLE,            -- Y: 반발탄성(%)
  final_perf_score   DOUBLE,            -- Z: 최종 성능 종합점수(지수)

  -- 행 삽입 순서 (기본 정렬 기준)
  ingest_order       BIGINT,            -- 원본 행 순서 (1부터 증가)

  -- 생성 시각(선택)
  -- created_at         TIMESTAMPTZ DEFAULT NOW(),

  -- 무결성 제약
  CHECK (factory_code IN ('AA24','AA25','AB25','AC25')),
  CHECK (line_grade   IN ('U 1st','U 2nd','U 3rd','S 1st','S 2nd','S 3rd')),
  CHECK (edition_type IS NULL OR edition_type IN ('M','W','J','T')),
  CHECK (inproc_pass_flag IS NULL OR inproc_pass_flag IN (0,1)),
  CHECK (output_qty IS NULL OR output_qty >= 0),
  CHECK (cycle_time_s IS NULL OR cycle_time_s > 0),
  CHECK (mold_temp_c  IS NULL OR mold_temp_c  > 0),
  CHECK (inj_pressure_bar IS NULL OR inj_pressure_bar > 0),
  CHECK (conv_speed_mps   IS NULL OR conv_speed_mps   > 0),
  CHECK (rebound_coeff_pct IS NULL OR rebound_coeff_pct >= 0),
  CHECK (final_perf_score  IS NULL OR final_perf_score >= 0)
);

-- 인덱스 (조회 성능)
CREATE INDEX IF NOT EXISTS idx_fact_factory   ON fact_manufacturing(factory_code);
CREATE INDEX IF NOT EXISTS idx_fact_line      ON fact_manufacturing(line_code);
CREATE INDEX IF NOT EXISTS idx_fact_product   ON fact_manufacturing(product_code);
CREATE INDEX IF NOT EXISTS idx_fact_grade     ON fact_manufacturing(line_grade);
CREATE INDEX IF NOT EXISTS idx_fact_edition   ON fact_manufacturing(edition_type);
CREATE INDEX IF NOT EXISTS idx_fact_ingest    ON fact_manufacturing(ingest_order);

-- 기존 데이터에 ingest_order 채우기 (1회 실행)
UPDATE fact_manufacturing
SET ingest_order = rowid
WHERE ingest_order IS NULL;

-- 공장 × 등급 요약 뷰
CREATE OR REPLACE VIEW v_factory_grade_summary AS
SELECT
  factory_code,
  line_grade,
  COUNT(*)              AS cnt,
  AVG(efficiency_index) AS avg_efficiency,
  SUM(output_qty)       AS sum_output,
  AVG(rebound_coeff_pct) AS avg_rebound,
  AVG(final_perf_score)  AS avg_final_score
FROM fact_manufacturing
GROUP BY 1, 2;

-- 에디션별 KPI 뷰
CREATE OR REPLACE VIEW v_edition_kpi AS
SELECT
  COALESCE(edition_type, '') AS edition_type,
  COUNT(*)                   AS cnt,
  AVG(efficiency_index)      AS avg_efficiency,
  AVG(cycle_time_s)          AS avg_cycle_time,
  AVG(rebound_coeff_pct)     AS avg_rebound,
  AVG(final_perf_score)      AS avg_final_score
FROM fact_manufacturing
GROUP BY 1;
