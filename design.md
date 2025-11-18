基于你这类EDGAR/XBRL 风格的 JSON（concept → units → [facts]），我建议用 DuckDB + Parquet 做主仓（本机分析/批量特征）、再配一个轻量 Postgres/TimescaleDB（如果你要做在线看板或多用户写入）。单机量化迭代时，DuckDB 足够快、零运维，还能直接扫分区化 Parquet。

下面给你一个既“原子化（可追溯）”又“好分析（可透视）”的 schema。你可以只用 DuckDB，后续再把少量聚合表同步到 Postgres。

推荐物理布局
	•	Raw 层：原始 JSON → 归档成 raw/edgar=cik=XXXX/year=YYYY/file=accn.parquet（保留完整原文）
	•	Core 层（规范化）：下表
	•	Mart 层（宽表/视图）：常用指标透视后的宽表（便于回测/特征读取）

Core schema（DuckDB DDL）

```sql
-- 公司主数据
CREATE TABLE dim_entity (
  cik        INTEGER PRIMARY KEY,
  ticker     VARCHAR,
  entity_name VARCHAR
);

-- 申报/报告元数据（一份 10-K/10-Q）
CREATE TABLE dim_filing (
  accn       VARCHAR PRIMARY KEY,         -- "0001193125-09-153165"
  cik        INTEGER REFERENCES dim_entity(cik),
  form       VARCHAR,                     -- 10-K/10-Q/8-K…
  fp         VARCHAR,                     -- FY/Q1/Q2/Q3/Q4
  fy         INTEGER,                     -- fiscal year
  filed_date DATE,                        -- "2009-07-22"
  period_end DATE,                        -- "end": "2009-06-27"
  frame      VARCHAR                      -- 如 CY2009Q2I
);

-- 概念（XBRL concept）
CREATE TABLE dim_concept (
  concept_id  BIGINT PRIMARY KEY,         -- 自增或哈希
  ns          VARCHAR,                    -- "dei" / "us-gaap" / "ifrs"
  name        VARCHAR,                    -- e.g. "EntityCommonStockSharesOutstanding"
  label       VARCHAR,
  description VARCHAR
);

-- 计量单位
CREATE TABLE dim_unit (
  unit_id  BIGINT PRIMARY KEY,
  unit_key VARCHAR UNIQUE,                -- e.g. "shares", "USD", "USD / shares"
  detail   JSON                           -- 可放测量组合
);

-- 事实值（数值型 & 文本型可合一；这里用数值型为主）
CREATE TABLE fact_numeric (
  fact_id     BIGINT PRIMARY KEY,
  cik         INTEGER,
  accn        VARCHAR,
  concept_id  BIGINT,
  unit_id     BIGINT,
  period_start DATE,                      -- 若提供；无则 NULL
  period_end   DATE,
  frame        VARCHAR,
  value        DOUBLE,                    -- "val"
  decimals     INTEGER,                   -- XBRL decimals（可选）
  created_at   TIMESTAMP DEFAULT now(),

  -- 约束/索引
  FOREIGN KEY (accn) REFERENCES dim_filing(accn),
  FOREIGN KEY (concept_id) REFERENCES dim_concept(concept_id),
  FOREIGN KEY (unit_id) REFERENCES dim_unit(unit_id)
);

-- 文本型（如公司描述）可选
CREATE TABLE fact_text (
  fact_id     BIGINT PRIMARY KEY,
  cik         INTEGER,
  accn        VARCHAR REFERENCES dim_filing(accn),
  concept_id  BIGINT REFERENCES dim_concept(concept_id),
  value_text  VARCHAR
);

-- 日行情（便于回测/对齐）
CREATE TABLE fact_price_daily (
  ticker   VARCHAR,
  dt       DATE,
  open     DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
  adj_close DOUBLE, volume BIGINT,
  PRIMARY KEY (ticker, dt)
);

-- 常用索引（DuckDB 会自动向量化，但这些键对 join/过滤仍然有用）
CREATE INDEX idx_fact_numeric_ckp ON fact_numeric(cik, concept_id, period_end);
CREATE INDEX idx_filing_cik_date   ON dim_filing(cik, filed_date);
```

分区建议（Parquet 目录）
	•	fact_numeric/ 按 concept_id= 与 year= 分区（或 cik= + year=）
	•	fact_price_daily/ 按 ticker= + year= 分区
DuckDB 直接 SELECT * FROM 'fact_numeric/concept_id=1234/year=2009/*.parquet' 会很快。

一个最常用的宽表（Mart）

把常用年度/季度指标透视到宽表，便于一次性 join：
```sql
CREATE VIEW mart_quarterly AS
WITH base AS (
  SELECT
    f.cik,
    e.ticker,
    fi.fy, fi.fp, fi.period_end,
    c.name AS concept,
    n.value
  FROM fact_numeric n
  JOIN dim_filing fi ON n.accn = fi.accn
  JOIN dim_entity e  ON fi.cik = e.cik
  JOIN dim_concept c ON n.concept_id = c.concept_id
  WHERE fi.form IN ('10-Q','10-K')
)
SELECT
  cik, ticker, fy, fp, period_end,
  MAX(CASE WHEN concept='EntityCommonStockSharesOutstanding' THEN value END) AS shares_out,
  MAX(CASE WHEN concept='EarningsPerShareDiluted'           THEN value END) AS eps_diluted,
  MAX(CASE WHEN concept='Revenues'                           THEN value END) AS revenue,
  MAX(CASE WHEN concept='NetIncomeLoss'                      THEN value END) AS net_income
FROM base
GROUP BY 1,2,3,4,5;
```

最小可用 ETL（Python → DuckDB）
```python
import json, duckdb, hashlib
from pathlib import Path
import pandas as pd

def hid(s:str) -> int:
    return int(hashlib.md5(s.encode()).hexdigest()[:16], 16)

con = duckdb.connect("fin.duckdb")

# 1) 读取一个 AAPL 的 JSON
j = json.loads(Path("aapl.json").read_text())

cik = int(j["cik"])
entity = pd.DataFrame([{
    "cik": cik,
    "ticker": "AAPL",                    # 若另有映射，可后续更新
    "entity_name": j.get("entityName")
}])

# 2) upsert 公司
con.execute("""
INSERT OR REPLACE INTO dim_entity SELECT * FROM entity
""", {"entity": entity})

rows = []
filings = []
concepts = []
units = []

dei = j["facts"]["dei"]
concept_name = "EntityCommonStockSharesOutstanding"
concept_label = dei[concept_name]["label"]
concept_desc  = dei[concept_name]["description"]
concept_id = hid(f"dei:{concept_name}")
concepts.append(dict(
    concept_id=concept_id, ns="dei", name=concept_name,
    label=concept_label, description=concept_desc
))

unit_key = "shares"
units.append(dict(unit_id=hid(unit_key), unit_key=unit_key, detail=None))

for item in dei[concept_name]["units"][unit_key]:
    accn = item["accn"]
    filed_date = item.get("filed")
    fy  = item.get("fy")
    fp  = item.get("fp")
    form = item.get("form")
    period_end = item["end"]
    frame = item.get("frame")

    filings.append(dict(
        accn=accn, cik=cik, form=form, fp=fp, fy=fy,
        filed_date=filed_date, period_end=period_end, frame=frame
    ))

    rows.append(dict(
        fact_id = hid(f"{accn}:{concept_id}:{period_end}"),
        cik = cik, accn = accn, concept_id = concept_id,
        unit_id = hid(unit_key),
        period_start = None,
        period_end = period_end,
        frame = frame,
        value = float(item["val"]),
        decimals = None
    ))

# 批量写入
con.register("df_concepts", pd.DataFrame(concepts).drop_duplicates("concept_id"))
con.register("df_units",    pd.DataFrame(units).drop_duplicates("unit_id"))
con.register("df_filings",  pd.DataFrame(filings).drop_duplicates("accn"))
con.register("df_facts",    pd.DataFrame(rows))

con.execute("INSERT OR IGNORE INTO dim_concept SELECT * FROM df_concepts")
con.execute("INSERT OR IGNORE INTO dim_unit SELECT * FROM df_units")
con.execute("INSERT OR REPLACE INTO dim_filing SELECT * FROM df_filings")
con.execute("INSERT OR REPLACE INTO fact_numeric SELECT * FROM df_facts")
```

常见查询示例
```sql
SELECT period_end, shares_out
FROM mart_quarterly
WHERE ticker='AAPL'
ORDER BY period_end;
```

对比所有半导体公司的 2025Q2 revenue（假设你有行业标签 dim_entity.industry=‘Semi’）：
```sql 
SELECT e.ticker, m.revenue
FROM mart_quarterly m
JOIN dim_entity e USING (cik)
WHERE e.industry='Semi' AND m.fy=2025 AND m.fp='Q2'
ORDER BY m.revenue DESC;
```

小结
	•	本地量化研究首选 DuckDB + 分区化 Parquet；
	•	以 dim_entity / dim_filing / dim_concept / dim_unit / fact_numeric 为核心抽象，万物皆 fact；
	•	通过 mart 视图 提供宽表入口（对回测/特征最友好）；



