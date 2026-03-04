import os
import pandas as pd
from sqlalchemy import create_engine, text

def export_panel_latest(connection_string: str, output_path: str = "outputs/panel_latest.csv"):
    """
    Export dataset panel 'sạch':
    - Output: outputs/panel_latest.csv
    - Columns: ticker, fiscal_year + (các biến)
    - Cho mỗi (firm_id, fiscal_year) chọn snapshot mới nhất (snapshot_date, rồi snapshot_id nếu hòa)
    """

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    engine = create_engine(connection_string)

    # MySQL 8+ dùng window function. Nếu MySQL 5.7: báo mình đưa bản khác.
    sql = text("""
    WITH all_keys AS (
        -- gom tất cả firm-year-snapshot xuất hiện ở các fact
        SELECT firm_id, fiscal_year, snapshot_id FROM fact_financial_year
        UNION
        SELECT firm_id, fiscal_year, snapshot_id FROM fact_cashflow_year
    ),
    ranked AS (
        SELECT
            k.firm_id,
            k.fiscal_year,
            k.snapshot_id,
            s.snapshot_date,
            ROW_NUMBER() OVER (
                PARTITION BY k.firm_id, k.fiscal_year
                ORDER BY s.snapshot_date DESC, k.snapshot_id DESC
            ) AS rn
        FROM all_keys k
        JOIN fact_data_snapshot s ON s.snapshot_id = k.snapshot_id
    ),
    latest AS (
        SELECT firm_id, fiscal_year, snapshot_id
        FROM ranked
        WHERE rn = 1
    )
    SELECT
        f.ticker,
        l.fiscal_year,

        -- =========================
        -- FINANCIAL (theo import của bạn)
        -- =========================
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

        -- =========================
        -- CASHFLOW
        -- =========================
        cf.unit_scale AS cf_unit_scale,
        cf.currency_code AS cf_currency_code,
        cf.net_cfo,
        cf.net_cfi,
        cf.capex

        -- Nếu thầy không muốn snapshot_id trong output thì bỏ dòng dưới:
        -- , l.snapshot_id

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

    ORDER BY f.ticker, l.fiscal_year;
    """)

    with engine.begin() as conn:
        df = pd.read_sql(sql, conn)

    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✅ Exported panel_latest: {len(df)} rows -> {output_path}")
    return df


if __name__ == "__main__":
    # Thay bằng connection string của bạn
    # Ví dụ:
    # mysql+pymysql://root:password@localhost:3306/midterm_proj?charset=utf8mb4
    CONNECTION_STRING = "mysql+pymysql://USER:PASSWORD@HOST:3306/midterm_proj?charset=utf8mb4"

    export_panel_latest(CONNECTION_STRING)
