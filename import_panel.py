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

    import pandas as pd
    import numpy as np
    from sqlalchemy import create_engine, text

    print("📥 Reading Excel...")
    df = pd.read_excel(excel_path, engine="openpyxl")

    df.columns = df.columns.str.strip().str.replace('\n', '')
    df['StockCode'] = df['StockCode'].astype(str).str.strip()

    df = df.replace({np.nan: None})

    engine = create_engine(connection_string)

    with engine.begin() as conn:

        firm_map = {
            row.ticker: row.firm_id
            for row in conn.execute(text(
                "SELECT firm_id, ticker FROM dim_firm"
            ))
        }

        print(f"🔎 Loaded {len(firm_map)} firms")

        inserted_rows = 0

        for _, row in df.iterrows():

            if row['StockCode'] not in firm_map or row['Year'] is None:
                continue

            firm_id = firm_map[row['StockCode']]
            fiscal_year = int(row['Year'])

            # =========================
            # FINANCIAL
            # =========================
            conn.execute(text("""
                INSERT IGNORE INTO fact_financial_year (
                    firm_id, fiscal_year, snapshot_id,
                    unit_scale, currency_code,
                    net_sales, total_assets,
                    selling_expenses, general_admin_expenses,
                    manufacturing_overhead, raw_material_consumption,
                    merchandise_purchase_year, wip_goods_purchase,
                    outside_manufacturing_expenses, production_cost,
                    intangible_assets_net,
                    net_operating_income, net_income,
                    total_equity, total_liabilities,
                    long_term_debt,
                    current_assets, current_liabilities,
                    inventory, net_ppe, cash_and_equivalents,
                    rnd_expenditure, growth_ratio
                )
                VALUES (
                    :fid,:year,:sid,
                    :scale,:cur,
                    :sales,:ta,
                    :sell,:ga,
                    :mo,:rm,
                    :merch,:wip,
                    :outside,:prod,
                    :intan,
                    :op,:ni,
                    :equity,:liab,
                    :ltd,
                    :ca,:cl,
                    :inv,:ppe,:cash,
                    :rnd,:growth
                )
            """),{

                "fid": firm_id,
                "year": fiscal_year,
                "sid": snapshot_id,
                "scale": 1,
                "cur": "VND",

                "sales": clean_number(row.get('Total sales revenue')),
                "ta": clean_number(row.get('Total assets')),
                "sell": clean_number(row.get('Selling expenses')),
                "ga": clean_number(row.get('General and administrative expenditure')),
                "mo": clean_number(row.get('Manufacturing overhead (Indirect cost)')),
                "rm": clean_number(row.get('Consumption of raw material')),
                "merch": clean_number(row.get('Merchandise purchase of the year')),
                "wip": clean_number(row.get('Work-in-progress goods purchase')),
                "outside": clean_number(row.get('Outside manufacturing expenses')),
                "prod": clean_number(row.get('Production cost')),
                "intan": clean_number(row.get('Value of intangible assets')),
                "op": clean_number(row.get('Net operating income')),
                "ni": clean_number(row.get('Net Income')),
                "equity": clean_number(row.get("Total shareholders' equity")),
                "liab": clean_number(row.get('Total liabilities')),
                "ltd": clean_number(row.get('Long-term debt')),
                "ca": clean_number(row.get('Current assets')),
                "cl": clean_number(row.get('Current liabilities')),
                "inv": clean_number(row.get('Total inventory')),
                "ppe": clean_number(row.get('Net plant, property and equipment')),
                "cash": clean_number(row.get('Cash and Cash Equivalents')),
                "rnd": clean_number(row.get('R&D expenditure')),
                "growth": clean_number(row.get('Growth ratio'))
            })


            # =========================
            # CASHFLOW
            # =========================
            conn.execute(text("""
                INSERT IGNORE INTO fact_cashflow_year (
                    firm_id, fiscal_year, snapshot_id,
                    unit_scale, currency_code,
                    net_cfo, net_cfi, capex
                )
                VALUES (
                    :fid,:year,:sid,
                    :scale,:cur,
                    :cfo,:cfi,:capex
                )
            """),{

                "fid": firm_id,
                "year": fiscal_year,
                "sid": snapshot_id,
                "scale": 1,
                "cur": "VND",

                "cfo": clean_number(row.get('Net cash from operating activities')),
                "cfi": clean_number(row.get('Cash flows from investing activities')),
                "capex": clean_number(row.get('Capital expenditure')),
            })


            # =========================
            # MARKET
            # =========================
            conn.execute(text("""
                INSERT IGNORE INTO fact_market_year (
                    firm_id, fiscal_year, snapshot_id,
                    shares_outstanding,
                    market_value_equity,
                    dividend_cash_paid,
                    eps_basic
                )
                VALUES (
                    :fid,:year,:sid,
                    :shares,:mve,:div,:eps
                )
            """),{

                "fid": firm_id,
                "year": fiscal_year,
                "sid": snapshot_id,
                "cur": "VND",

                "shares": clean_number(row.get('Total share outstanding')),
                "mve": clean_number(row.get('Market value of equity')),
                "div": clean_number(row.get('Divident payment')),
                "eps": clean_number(row.get('EPS'))
            })


            # =========================
            # OWNERSHIP
            # =========================
            conn.execute(text("""
                INSERT IGNORE INTO fact_ownership_year (
                    firm_id, fiscal_year, snapshot_id,
                    managerial_inside_own,
                    state_own,
                    institutional_own,
                    foreign_own
                )
                VALUES (
                    :fid,:year,:sid,
                    :manager,:state,:inst,:foreign
                )
            """),{

                "fid": firm_id,
                "year": fiscal_year,
                "sid": snapshot_id,

                "manager": clean_number(row.get('Managerial/Inside ownership')),
                "state": clean_number(row.get('State Ownership')),
                "inst": clean_number(row.get('Institutional ownership')),
                "foreign": clean_number(row.get('Foreign ownership'))
            })


            # =========================
            # INNOVATION
            # =========================
            conn.execute(text("""
                INSERT IGNORE INTO fact_innovation_year (
                    firm_id, fiscal_year, snapshot_id,
                    product_innovation,
                    process_innovation
                )
                VALUES (
                    :fid,:year,:sid,
                    :prod,:proc
                )
            """),{

                "fid": firm_id,
                "year": fiscal_year,
                "sid": snapshot_id,

                "prod": int(row.get('Product innovation') or 0),
                "proc": int(row.get('Process innovation') or 0)
            })


            # =========================
            # META
            # =========================
            conn.execute(text("""
                INSERT IGNORE INTO fact_firm_year_meta (
                    firm_id, fiscal_year, snapshot_id,
                    employees_count,
                    firm_age
                )
                VALUES (
                    :fid,:year,:sid,
                    :emp,:age
                )
            """),{

                "fid": firm_id,
                "year": fiscal_year,
                "sid": snapshot_id,

                "emp": clean_number(row.get('Number of employees')),
                "age": clean_number(row.get('Firm Age'))
            })

            inserted_rows += 1

        print(f"✅ Imported {inserted_rows} firm-year records")
