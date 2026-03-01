
SELECT user, host, plugin
FROM mysql.user
WHERE user = 'local';

CREATE DATABASE midterm_proj;
USE midterm_proj;

-- ========================
-- DIM TABLES
-- ========================

CREATE TABLE dim_exchange (
    exchange_id INT AUTO_INCREMENT PRIMARY KEY,
    exchange_code VARCHAR(10) UNIQUE NOT NULL
);

CREATE TABLE dim_industry_l2 (
    industry_l2_id INT AUTO_INCREMENT PRIMARY KEY,
    industry_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE dim_data_source (
    data_source_id INT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE dim_firm (
    firm_id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(20) UNIQUE NOT NULL,
    firm_name VARCHAR(255),
    exchange_id INT,
    industry_l2_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (exchange_id) REFERENCES dim_exchange(exchange_id),
    FOREIGN KEY (industry_l2_id) REFERENCES dim_industry_l2(industry_l2_id)
);

-- ========================
-- SNAPSHOT TABLE
-- ========================

CREATE TABLE fact_data_snapshot (
    snapshot_id INT AUTO_INCREMENT PRIMARY KEY,
    data_source_id INT NOT NULL,
    snapshot_date DATE NOT NULL,
    version_tag VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (data_source_id) REFERENCES dim_data_source(data_source_id)
);

-- ========================
-- FACT TABLES (Composite PK)
-- ========================

CREATE TABLE fact_financial_year (
    firm_id INT,
    fiscal_year INT,
    snapshot_id INT,
    total_assets DOUBLE,
    total_liabilities DOUBLE,
    total_equity DOUBLE,
    revenue DOUBLE,
    operating_income DOUBLE,
    net_income DOUBLE,
    eps DOUBLE,
    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

CREATE TABLE fact_market_year (
    firm_id INT,
    fiscal_year INT,
    snapshot_id INT,
    shares_outstanding DOUBLE,
    share_price DOUBLE,
    market_value_equity DOUBLE,
    dividend_payment DOUBLE,
    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

CREATE TABLE fact_cashflow_year (
    firm_id INT,
    fiscal_year INT,
    snapshot_id INT,
    cfo DOUBLE,
    cfi DOUBLE,
    capex DOUBLE,
    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

CREATE TABLE fact_ownership_year (
    firm_id INT,
    fiscal_year INT,
    snapshot_id INT,
    state_ownership DOUBLE,
    foreign_ownership DOUBLE,
    institutional_ownership DOUBLE,
    managerial_ownership DOUBLE,
    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

CREATE TABLE fact_innovation_year (
    firm_id INT,
    fiscal_year INT,
    snapshot_id INT,
    product_innovation TINYINT,
    process_innovation TINYINT,
    innovation_evidence TEXT,
    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

CREATE TABLE fact_firm_year_meta (
    firm_id INT,
    fiscal_year INT,
    snapshot_id INT,
    employees INT,
    firm_age INT,
    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

-- ========================
-- AUDIT TABLE
-- ========================

CREATE TABLE fact_value_override_log (
    override_id INT AUTO_INCREMENT PRIMARY KEY,
    firm_id INT,
    fiscal_year INT,
    field_name VARCHAR(100),
    old_value DOUBLE,
    new_value DOUBLE,
    override_reason TEXT,
    overridden_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);