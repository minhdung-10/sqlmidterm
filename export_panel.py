import os
import pandas as pd
from sqlalchemy import create_engine, text
import pwinput

def export_panel_latest(connection_string: str, output_path: str = "outputs/panel_latest.csv"):

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    engine = create_engine(connection_string)

    sql = text("""
    WITH ranked AS (
    SELECT
        ff.firm_id,
        ff.fiscal_year,
        ff.snapshot_id,
        s.snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY ff.firm_id, ff.fiscal_year
            ORDER BY s.snapshot_date DESC, ff.snapshot_id DESC
        ) AS rn
    FROM fact_financial_year ff
    JOIN fact_data_snapshot s 
        ON s.snapshot_id = ff.snapshot_id
),

latest AS (
    SELECT firm_id, fiscal_year, snapshot_id
    FROM ranked
    WHERE rn = 1
)

SELECT
    f.ticker,
    l.fiscal_year,

    -- OWNERSHIP
    own.managerial_inside_own,
    own.state_own,
    own.institutional_own,
    own.foreign_own,

    -- FINANCIAL
    fin.unit_scale,
    fin.currency_code,
    fin.net_sales,
    fin.total_assets,
    fin.selling_expenses,
    fin.general_admin_expenses,
    fin.manufacturing_overhead,
    fin.raw_material_consumption,
    fin.merchandise_purchase_year,
    fin.wip_goods_purchase,
    fin.outside_manufacturing_expenses,
    fin.production_cost,
    fin.intangible_assets_net,
    fin.net_operating_income,
    fin.net_income,
    fin.total_equity,
    fin.total_liabilities,
    fin.long_term_debt,
    fin.current_assets,
    fin.current_liabilities,
    fin.inventory,
    fin.net_ppe,
    fin.cash_and_equivalents,
    fin.rnd_expenditure,
    fin.growth_ratio,

    -- CASHFLOW
    cf.net_cfo,
    cf.net_cfi,
    cf.capex,

    -- MARKET
    mkt.shares_outstanding,
    mkt.share_price,
    mkt.market_value_equity,
    mkt.dividend_cash_paid,
    mkt.eps_basic,

    -- INNOVATION
    inn.product_innovation,
    inn.process_innovation,
    inn.evidence_product,
    inn.evidence_process,

    -- META
    meta.employees_count,
    meta.firm_age

FROM latest l

JOIN dim_firm f ON f.firm_id = l.firm_id

LEFT JOIN fact_financial_year fin
    ON fin.firm_id = l.firm_id
   AND fin.fiscal_year = l.fiscal_year
   AND fin.snapshot_id = l.snapshot_id

LEFT JOIN fact_cashflow_year cf
    ON cf.firm_id = l.firm_id
   AND cf.fiscal_year = l.fiscal_year
   AND cf.snapshot_id = l.snapshot_id

LEFT JOIN fact_market_year mkt
    ON mkt.firm_id = l.firm_id
   AND mkt.fiscal_year = l.fiscal_year
   AND mkt.snapshot_id = l.snapshot_id

LEFT JOIN fact_ownership_year own
    ON own.firm_id = l.firm_id
   AND own.fiscal_year = l.fiscal_year
   AND own.snapshot_id = l.snapshot_id

LEFT JOIN fact_innovation_year inn
    ON inn.firm_id = l.firm_id
   AND inn.fiscal_year = l.fiscal_year
   AND inn.snapshot_id = l.snapshot_id

LEFT JOIN fact_firm_year_meta meta
    ON meta.firm_id = l.firm_id
   AND meta.fiscal_year = l.fiscal_year
   AND meta.snapshot_id = l.snapshot_id

ORDER BY f.ticker, l.fiscal_year;
""")

    with engine.begin() as conn:
        df = pd.read_sql(sql, conn)

    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"✅ Exported panel_latest: {len(df)} rows -> {output_path}")
    return df

if __name__ == "__main__":
    db_host = input("Host (Enter = localhost): ") or "localhost"
    db_port = input("Port (Enter = 3306): ") or "3306"
    db_user = input("User (Enter = root): ") or "root"
    db_name = input("Database name: ")
    db_password = pwinput.pwinput("Password: ", mask="*")

    connection_string = (
        f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    export_panel_latest(connection_string)