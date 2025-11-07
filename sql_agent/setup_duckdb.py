import duckdb
import pandas as pd

# 1. DB 연결 (DuckDB 파일 생성 또는 연결)
con = duckdb.connect("db/manufacturing.db")

# 2. DDL 실행 (스키마 + 테이블 + 뷰 생성)
with open("schema/ddl.sql", "r", encoding="utf-8") as f:
    ddl = f.read()
con.execute(ddl)


# 3. 엑셀 데이터 로드
df = pd.read_excel("data/summary_gpt_v2.0.xlsx", sheet_name="SUMMARY")

# 4. 컬럼 매핑 (원본 A~Z → 최종 컬럼명)
df.columns = [
    "factory_code", "line_code", "product_code", "efficiency_index",
    "line_grade", "output_qty", "shift_code",
    "cycle_time_s", "mold_temp_c", "inj_pressure_bar",
    "conv_speed_mps", "inproc_pass_flag",
    "rebound_coeff_pct", "final_perf_score"
]

# 5. 테이블에 적재
con.register("tmp_df", df)
con.execute("""
INSERT INTO manufacturing.fact_manufacturing
SELECT
  factory_code,
  line_code,
  product_code,
  line_grade,
  shift_code AS edition_type,      -- R → edition_type
  efficiency_index,
  output_qty,
  cycle_time_s,
  mold_temp_c,
  inj_pressure_bar,
  conv_speed_mps,
  CASE WHEN inproc_pass_flag >= 100 THEN 1 ELSE 0 END,
  rebound_coeff_pct,
  final_perf_score
FROM tmp_df
""")

# 6. 테스트
test_df = con.execute("""
SELECT factory_code, COUNT(*) AS rows, AVG(efficiency_index) AS avg_eff
FROM manufacturing.fact_manufacturing
GROUP BY 1
""").df()

print(test_df)
con.close()
print("✅ DuckDB 구축 완료 (manufacturing.db)")

