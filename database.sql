CREATE DATABASE midterm_proj;
USE midterm_proj;

-- =========================
-- DIMENSION TABLES
-- =========================

CREATE TABLE dim_exchange (
    exchange_id INT AUTO_INCREMENT PRIMARY KEY,
    exchange_code VARCHAR(10) UNIQUE NOT NULL,
    exchange_name VARCHAR(100)
);

CREATE TABLE dim_industry_l2 (
    industry_l2_id INT AUTO_INCREMENT PRIMARY KEY,
    industry_l2_name VARCHAR(150) UNIQUE NOT NULL
);

CREATE TABLE dim_firm (
    firm_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(20) UNIQUE NOT NULL,
    firm_name VARCHAR(255),
    exchange_id INT,
    industry_l2_id INT,
    FOREIGN KEY (exchange_id) REFERENCES dim_exchange(exchange_id),
    FOREIGN KEY (industry_l2_id) REFERENCES dim_industry_l2(industry_l2_id)
);

CREATE TABLE dim_data_source (
    data_source_id INT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(150) UNIQUE NOT NULL
);

-- =========================
-- SNAPSHOT TABLE
-- =========================

CREATE TABLE fact_data_snapshot (
    snapshot_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    data_source_id INT NOT NULL,
    snapshot_date DATE NOT NULL,
    version_tag VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (data_source_id) REFERENCES dim_data_source(data_source_id)
);

-- =========================
-- FACT TABLES (38 VARIABLES)
-- =========================

CREATE TABLE fact_ownership_year (
    firm_id BIGINT NOT NULL,
    fiscal_year INT NOT NULL,
    snapshot_id BIGINT NOT NULL,
    managerial_inside_own DECIMAL(10,6),
    state_own DECIMAL(10,6),
    institutional_own DECIMAL(10,6),
    foreign_own DECIMAL(10,6),
    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

CREATE TABLE fact_financial_year (
    firm_id BIGINT NOT NULL,
    fiscal_year INT NOT NULL,
    snapshot_id BIGINT NOT NULL,
    unit_scale BIGINT,
    currency_code VARCHAR(5),
    net_sales DECIMAL(20,2),
    total_assets DECIMAL(20,2),
    selling_expenses DECIMAL(20,2),
    general_admin_expenses DECIMAL(20,2),
    manufacturing_overhead DECIMAL(20,2),
    raw_material_consumption DECIMAL(20,2),
    merchandise_purchase_year DECIMAL(20,2),
    wip_goods_purchase DECIMAL(20,2),
    outside_manufacturing_expenses DECIMAL(20,2),
    production_cost DECIMAL(20,2),
    intangible_assets_net DECIMAL(20,2),
    net_operating_income DECIMAL(20,2),
    net_income DECIMAL(20,2),
    total_equity DECIMAL(20,2),
    total_liabilities DECIMAL(20,2),
    long_term_debt DECIMAL(20,2),
    current_assets DECIMAL(20,2),
    current_liabilities DECIMAL(20,2),
    inventory DECIMAL(20,2),
    net_ppe DECIMAL(20,2),
    cash_and_equivalents DECIMAL(20,2),
    rnd_expenditure DECIMAL(20,2),
    growth_ratio DECIMAL(10,6),
    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

CREATE TABLE fact_cashflow_year (
    firm_id BIGINT NOT NULL,
    fiscal_year INT NOT NULL,
    snapshot_id BIGINT NOT NULL,
    unit_scale BIGINT,
    currency_code VARCHAR(5),
    net_cfo DECIMAL(20,2),
    net_cfi DECIMAL(20,2),
    capex DECIMAL(20,2),
    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

CREATE TABLE fact_market_year (
    firm_id BIGINT NOT NULL,
    fiscal_year INT NOT NULL,
    snapshot_id BIGINT NOT NULL,
    shares_outstanding BIGINT,
    share_price DECIMAL(20,4),
    market_value_equity DECIMAL(20,2),
    dividend_cash_paid DECIMAL(20,2),
    eps_basic DECIMAL(20,6),
    currency_code VARCHAR(5),
    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

CREATE TABLE fact_innovation_year (
    firm_id BIGINT NOT NULL,
    fiscal_year INT NOT NULL,
    snapshot_id BIGINT NOT NULL,
    product_innovation TINYINT,
    process_innovation TINYINT,
    evidence_note TEXT,
    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

CREATE TABLE fact_firm_year_meta (
    firm_id BIGINT NOT NULL,
    fiscal_year INT NOT NULL,
    snapshot_id BIGINT NOT NULL,
    employees_count INT,
    firm_age INT,
    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

CREATE TABLE fact_value_override_log (
    override_id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'Audit key for overrides',

    firm_id BIGINT NOT NULL COMMENT 'Firm identifier',
    fiscal_year SMALLINT NOT NULL COMMENT 'Fiscal year',

    table_name VARCHAR(80) NOT NULL COMMENT 'Target table of override',
    column_name VARCHAR(80) NOT NULL COMMENT 'Target column of override',

    old_value VARCHAR(255) NULL COMMENT 'Old value as text (for audit)',
    new_value VARCHAR(255) NULL COMMENT 'New value as text',

    reason VARCHAR(255) NULL COMMENT 'Reason for override',
    changed_by VARCHAR(80) NULL COMMENT 'User/bot making change',
    changed_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'When override logged',

    -- FOREIGN KEY
    CONSTRAINT fk_override_firm
        FOREIGN KEY (firm_id)
        REFERENCES dim_firm(firm_id)
        ON DELETE CASCADE,

    -- INDEXES
    INDEX idx_override_firm_year (firm_id, fiscal_year),
    INDEX idx_override_table_col (table_name, column_name)
);
