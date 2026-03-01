import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine, text

# ======================
# CLEAN NUMBER FUNCTION
# ======================
def clean_number(value):
    if value is None:
        return None

    val = str(value).strip()

    if val == "" or val.lower() == "nan":
        return None

    val = val.replace("\n", "")
    val = val.replace(",", "")

    try:
        return float(val)
    except:
        return None


# ======================
# CREATE SNAPSHOT
# ======================
def run_create_snapshot(source_name, version_tag, connection_string):
    engine = create_engine(connection_string)
    snapshot_date = datetime.date.today()

    with engine.begin() as conn:

        # Ensure data source exists
        conn.execute(text("""
            INSERT INTO dim_data_source (source_name)
            VALUES (:name)
            ON DUPLICATE KEY UPDATE source_name = source_name
        """), {"name": source_name})

        # Get data_source_id
        data_source_id = conn.execute(text("""
            SELECT data_source_id
            FROM dim_data_source
            WHERE source_name = :name
        """), {"name": source_name}).scalar()

        # Create snapshot
        result = conn.execute(text("""
            INSERT INTO fact_data_snapshot (data_source_id, snapshot_date, version_tag)
            VALUES (:ds_id, :date, :version)
        """), {
            "ds_id": data_source_id,
            "date": snapshot_date,
            "version": version_tag,
        })

        snapshot_id = result.lastrowid

    print(f"✅ Snapshot created. SNAPSHOT_ID = {snapshot_id}")
    return snapshot_id


# ======================
# IMPORT PANEL DATA
# ======================
def run_import_panel(excel_path, snapshot_id, connection_string):

    print("📥 Reading Excel...")
    df = pd.read_excel(excel_path, engine="openpyxl")

    # Clean columns
    df.columns = df.columns.str.strip().str.replace('\n', '')
    df['StockCode'] = df['StockCode'].astype(str).str.strip()
    df = df.replace({np.nan: None})

    engine = create_engine(connection_string)

    with engine.begin() as conn:

        # Load firm map
        firm_map = {
            row.ticker: row.firm_id
            for row in conn.execute(
                text("SELECT firm_id, ticker FROM dim_firm")
            )
        }

        print(f"🔎 Loaded {len(firm_map)} firms")

        inserted_rows = 0

        for _, row in df.iterrows():

            if row['StockCode'] not in firm_map or row['Year'] is None:
                continue

            firm_id = firm_map[row['StockCode']]
            fiscal_year = int(row['Year'])

            # ======================
            # 1️⃣ FINANCIAL
            # ======================
            conn.execute(text("""
                INSERT IGNORE INTO fact_financial_year (
                    firm_id, fiscal_year, snapshot_id,
                    total_assets, total_liabilities,
                    total_equity, revenue,
                    operating_income, net_income, eps
                )
                VALUES (
                    :fid, :year, :sid,
                    :ta, :tl, :te, :rev,
                    :op, :ni, :eps
                )
            """), {
                "fid": firm_id,
                "year": fiscal_year,
                "sid": snapshot_id,
                "ta": clean_number(row.get('Total assets')),
                "tl": clean_number(row.get('Total liabilities')),
                "te": clean_number(row.get("Total shareholders' equity")),
                "rev": clean_number(row.get('Total sales revenue')),
                "op": clean_number(row.get('Net operating income')),
                "ni": clean_number(row.get('Net Income')),
                "eps": clean_number(row.get('EPS'))
            })

            # ======================
            # 2️⃣ MARKET
            # ======================
            conn.execute(text("""
                INSERT IGNORE INTO fact_market_year (
                    firm_id, fiscal_year, snapshot_id,
                    shares_outstanding, share_price,
                    market_value_equity, dividend_payment
                )
                VALUES (
                    :fid, :year, :sid,
                    :shares, :price, :mve, :div
                )
            """), {
                "fid": firm_id,
                "year": fiscal_year,
                "sid": snapshot_id,
                "shares": clean_number(row.get('Shares outstanding')),
                "price": clean_number(row.get('Share price')),
                "mve": clean_number(row.get('Market value of equity')),
                "div": clean_number(row.get('Divident payment'))
            })

            # ======================
            # 3️⃣ CASHFLOW
            # ======================
            conn.execute(text("""
                INSERT IGNORE INTO fact_cashflow_year (
                    firm_id, fiscal_year, snapshot_id,
                    cfo, cfi, capex
                )
                VALUES (
                    :fid, :year, :sid,
                    :cfo, :cfi, :capex
                )
            """), {
                "fid": firm_id,
                "year": fiscal_year,
                "sid": snapshot_id,
                "cfo": clean_number(row.get('Net cash from operating activities')),
                "cfi": clean_number(row.get('Cash flows from investing activities')),
                "capex": clean_number(row.get('Capital expenditure'))
            })

            # ======================
            # 4️⃣ OWNERSHIP
            # ======================
            conn.execute(text("""
                INSERT IGNORE INTO fact_ownership_year (
                    firm_id, fiscal_year, snapshot_id,
                    state_ownership, foreign_ownership,
                    institutional_ownership, managerial_ownership
                )
                VALUES (
                    :fid, :year, :sid,
                    :state, :foreign, :inst, :manager
                )
            """), {
                "fid": firm_id,
                "year": fiscal_year,
                "sid": snapshot_id,
                "state": clean_number(row.get('State Ownership')),
                "foreign": clean_number(row.get('Foreign ownership')),
                "inst": clean_number(row.get('Institutional ownership')),
                "manager": clean_number(row.get('Managerial/Inside ownership'))
            })

            # ======================
            # 5️⃣ INNOVATION
            # ======================
            conn.execute(text("""
                INSERT IGNORE INTO fact_innovation_year (
                    firm_id, fiscal_year, snapshot_id,
                    product_innovation, process_innovation,
                    innovation_evidence
                )
                VALUES (
                    :fid, :year, :sid,
                    :prod, :proc, :evidence
                )
            """), {
                "fid": firm_id,
                "year": fiscal_year,
                "sid": snapshot_id,
                "prod": int(row.get('Product innovation') or 0),
                "proc": int(row.get('Process innovation') or 0),
                "evidence": row.get('Innovation evidence')
            })

            # ======================
            # 6️⃣ META
            # ======================
            conn.execute(text("""
                INSERT IGNORE INTO fact_firm_year_meta (
                    firm_id, fiscal_year, snapshot_id,
                    employees, firm_age
                )
                VALUES (
                    :fid, :year, :sid,
                    :emp, :age
                )
            """), {
                "fid": firm_id,
                "year": fiscal_year,
                "sid": snapshot_id,
                "emp": clean_number(row.get('Number of employees')),
                "age": clean_number(row.get('Firm Age'))
            })

            inserted_rows += 1

        print(f"✅ Imported {inserted_rows} firm-year records")

