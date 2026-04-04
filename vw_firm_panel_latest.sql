CREATE OR REPLACE VIEW vw_firm_panel_latest AS

WITH latest_snapshot AS (
    SELECT 
        f.firm_id,
        fy.fiscal_year,
        MAX(fy.snapshot_id) AS snapshot_id
    FROM fact_financial_year fy
    JOIN dim_firm f ON f.firm_id = fy.firm_id
    GROUP BY f.firm_id, fy.fiscal_year
)

SELECT
    df.ticker,
    ls.fiscal_year,

    -- =========================
    -- OWNERSHIP (4)
    -- =========================
    ow.managerial_inside_own,
    ow.state_own,
    ow.institutional_own,
    ow.foreign_own,

    -- =========================
    -- FINANCIAL (core)
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
    fin.rnd_expenditure,
    fin.growth_ratio,

    -- =========================
    -- CASHFLOW
    -- =========================
    cf.net_cfo,
    cf.net_cfi,
    cf.capex,

    -- =========================
    -- MARKET
    -- =========================
    mk.shares_outstanding,
    mk.share_price,
    mk.market_value_equity,
    mk.dividend_cash_paid,
    mk.eps_basic,

    -- =========================
    -- INNOVATION
    -- =========================
    inv.product_innovation,
    inv.process_innovation,

    -- =========================
    -- META
    -- =========================
    meta.employees_count,
    meta.firm_age

FROM latest_snapshot ls

JOIN dim_firm df 
    ON df.firm_id = ls.firm_id

LEFT JOIN fact_ownership_year ow
    ON ow.firm_id = ls.firm_id
    AND ow.fiscal_year = ls.fiscal_year
    AND ow.snapshot_id = ls.snapshot_id

LEFT JOIN fact_financial_year fin
    ON fin.firm_id = ls.firm_id
    AND fin.fiscal_year = ls.fiscal_year
    AND fin.snapshot_id = ls.snapshot_id

LEFT JOIN fact_cashflow_year cf
    ON cf.firm_id = ls.firm_id
    AND cf.fiscal_year = ls.fiscal_year
    AND cf.snapshot_id = ls.snapshot_id

LEFT JOIN fact_market_year mk
    ON mk.firm_id = ls.firm_id
    AND mk.fiscal_year = ls.fiscal_year
    AND mk.snapshot_id = ls.snapshot_id

LEFT JOIN fact_innovation_year inv
    ON inv.firm_id = ls.firm_id
    AND inv.fiscal_year = ls.fiscal_year
    AND inv.snapshot_id = ls.snapshot_id

LEFT JOIN fact_firm_year_meta meta
    ON meta.firm_id = ls.firm_id
    AND meta.fiscal_year = ls.fiscal_year
    AND meta.snapshot_id = ls.snapshot_id;