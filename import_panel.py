import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ==========================================
# VAR MAPPING (GIỮ NGUYÊN)
# ==========================================
VAR_TO_DOC_MAPPING = {
    "BCTC_Kiem_Toan": [
        'Selling expenses','General and administrative expenditure',
        'Manufacturing overhead (Indirect cost)','Consumption of raw material',
        'Merchandise purchase of the year','Work-in-progress goods purchase',
        'Outside manufacturing expenses','Production cost',
        'Capital expenditure','Net plant, property and equipment',
        'Current assets','Total inventory','Value of intangible assets',
        'Total assets','Total liabilities','Current liabilities',
        'Long-term debt',"Total shareholders' equity",
        'Net Income','EPS','Net cash from operating activities',
        'Cash and Cash Equivalents','Net operating income',
        'Total sales revenue','Cash flows from investing activities'
    ],
    "Bao_Cao_Thuong_Nien": [
        'R&D expenditure','Product innovation','Process innovation',
        'Firm Age','Number of employees'
    ],
    "Bao_Cao_Quan_Tri": [
        'Managerial/Inside ownership','State Ownership',
        'Institutional ownership','Foreign ownership'
    ],
    "Du_Lieu_Thi_Truong_Web": [
        'Total share outstanding','Market value of equity',
        'Growth ratio','Divident payment'
    ]
}

# ==========================================
# CLEAN NUMBER
# ==========================================
def clean_number(value):
    if pd.isna(value):
        return None

    val = str(value).strip()

    if val == "" or val.lower() == "nan":
        return None

    val = val.replace("\n", "").replace(",", "").replace('"', '')
    try:
        return float(val)
    except:
        return None

# ==========================================
# SAFE BINARY (0/1)
# ==========================================
def safe_binary(val):
    if pd.isna(val):
        return 0
    try:
        return int(float(val))
    except:
        return 0

# ==========================================
# SAFE STRING
# ==========================================
def safe_str(val):
    if pd.isna(val):
        return None
    return str(val)

# ==========================================
# MAIN FUNCTION
# ==========================================
def run_import_panel(excel_path, connection_string, snapshot_id):

    print("📥 Reading Excel...")
    df = pd.read_excel(excel_path, engine="openpyxl")

    # clean columns
    df.columns = df.columns.str.strip().str.replace('\n', '')

    # validate structure
    required_cols = ['StockCode', 'Year']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"❌ Missing required column: {col}")

    engine = create_engine(connection_string)

    with engine.begin() as conn:

        # load firm map
        firm_map = {
            row.ticker: row.firm_id
            for row in conn.execute(text("SELECT firm_id, ticker FROM dim_firm"))
        }

        inserted_rows = 0
        skipped_rows = 0

        for idx, row in df.iterrows():

            ticker_raw = row.get('StockCode')

            if pd.isna(ticker_raw):
                continue

            ticker = str(ticker_raw).strip().upper()
            year = row.get('Year')

            if ticker not in firm_map:
                raise ValueError(f"❌ Ticker {ticker} not found in dim_firm")

            if pd.isna(year):
                raise ValueError(f"❌ Missing year for ticker {ticker}")

            firm_id = firm_map[ticker]
            fiscal_year = int(year)

            row_has_insert = False

            for doc_type, cols in VAR_TO_DOC_MAPPING.items():

                has_data = any(
                    col in df.columns and clean_number(row.get(col)) is not None
                    for col in cols
                )

                if not has_data:
                    continue

                # ======================
                # DATA SOURCE
                # ======================
                source_name = f"{ticker}_{doc_type}_{fiscal_year}"

                conn.execute(text("""
                    INSERT INTO dim_data_source (source_name)
                    VALUES (:name)
                    ON DUPLICATE KEY UPDATE source_name = source_name
                """), {"name": source_name})

                sid = snapshot_id

                # ======================
                # FINANCIAL
                # ======================
                if doc_type == "BCTC_Kiem_Toan":

                    conn.execute(text("""
                        INSERT INTO fact_financial_year (
                            firm_id,fiscal_year,snapshot_id,
                            unit_scale, currency_code,
                            net_sales,total_assets,
                            selling_expenses,general_admin_expenses,
                            manufacturing_overhead,raw_material_consumption,
                            merchandise_purchase_year,wip_goods_purchase,
                            outside_manufacturing_expenses,production_cost,
                            intangible_assets_net,
                            net_operating_income,net_income,
                            total_equity,total_liabilities,
                            long_term_debt,
                            current_assets,current_liabilities,
                            inventory,net_ppe,cash_and_equivalents,
                            rnd_expenditure,growth_ratio
                        )
                        VALUES (
                            :fid,:year,:sid,
                            :scale, :cur,
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
                        ON DUPLICATE KEY UPDATE
                            unit_scale = VALUES(unit_scale),
                            currency_code = VALUES(currency_code),
                            net_sales = VALUES(net_sales),
                            total_assets = VALUES(total_assets),
                            selling_expenses = VALUES(selling_expenses),
                            general_admin_expenses = VALUES(general_admin_expenses),
                            manufacturing_overhead = VALUES(manufacturing_overhead),
                            raw_material_consumption = VALUES(raw_material_consumption),
                            merchandise_purchase_year = VALUES(merchandise_purchase_year),
                            wip_goods_purchase = VALUES(wip_goods_purchase),
                            outside_manufacturing_expenses = VALUES(outside_manufacturing_expenses),
                            production_cost = VALUES(production_cost),
                            intangible_assets_net = VALUES(intangible_assets_net),
                            net_operating_income = VALUES(net_operating_income),
                            net_income = VALUES(net_income),
                            total_equity = VALUES(total_equity),
                            total_liabilities = VALUES(total_liabilities),
                            long_term_debt = VALUES(long_term_debt),
                            current_assets = VALUES(current_assets),
                            current_liabilities = VALUES(current_liabilities),
                            inventory = VALUES(inventory),
                            net_ppe = VALUES(net_ppe),
                            cash_and_equivalents = VALUES(cash_and_equivalents),
                            rnd_expenditure = VALUES(rnd_expenditure),
                            growth_ratio = VALUES(growth_ratio)
                    """), {
                        "fid": firm_id,
                        "year": fiscal_year,
                        "sid": sid,
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

                    # CASHFLOW
                    conn.execute(text("""
                        INSERT INTO fact_cashflow_year (
                            firm_id,fiscal_year,snapshot_id,
                            unit_scale,currency_code,
                            net_cfo,net_cfi,capex
                        )
                        VALUES (:fid,:year,:sid,:scale,:cur,:cfo,:cfi,:capex)
                        ON DUPLICATE KEY UPDATE
                                unit_scale = VALUES(unit_scale),
                                currency_code = VALUES(currency_code),
                                net_cfo = VALUES(net_cfo),
                                net_cfi = VALUES(net_cfi),
                                capex = VALUES(capex)
                    """), {
                        "fid": firm_id,
                        "year": fiscal_year,
                        "sid": sid,
                        "scale": 1,
                        "cur": "VND",
                        "cfo": clean_number(row.get('Net cash from operating activities')),
                        "cfi": clean_number(row.get('Cash flows from investing activities')),
                        "capex": clean_number(row.get('Capital expenditure'))
                    })

                # ======================
                # MARKET
                # ======================
                elif doc_type == "Du_Lieu_Thi_Truong_Web":

                    conn.execute(text("""
                        INSERT INTO fact_market_year (
                            firm_id,fiscal_year,snapshot_id,
                            currency_code,
                            shares_outstanding,
                            share_price,
                            market_value_equity,
                            dividend_cash_paid,
                            eps_basic
                        )
                        VALUES (:fid,:year,:sid,:cur,:shares,:price,:mve,:div,:eps)
                        ON DUPLICATE KEY UPDATE
                                currency_code = VALUES(currency_code),
                                shares_outstanding = VALUES(shares_outstanding),
                                share_price = VALUES(share_price),
                                market_value_equity = VALUES(market_value_equity),
                                dividend_cash_paid = VALUES(dividend_cash_paid),
                                eps_basic = VALUES(eps_basic)
                    """), {
                        "fid": firm_id,
                        "year": fiscal_year,
                        "sid": sid,
                        "cur": "VND",
                        "shares": clean_number(row.get('Total share outstanding')),
                        "price": clean_number(row.get('Share price')),
                        "mve": clean_number(row.get('Market value of equity')),
                        "div": clean_number(row.get('Divident payment')),
                        "eps": clean_number(row.get('EPS'))
                    })

                # ======================
                # OWNERSHIP
                # ======================
                elif doc_type == "Bao_Cao_Quan_Tri":

                    conn.execute(text("""
                        INSERT INTO fact_ownership_year (
                            firm_id,fiscal_year,snapshot_id,
                            managerial_inside_own,state_own,
                            institutional_own,foreign_own
                        )
                        VALUES (:fid,:year,:sid,:m,:s,:i,:f)
                        ON DUPLICATE KEY UPDATE
                            managerial_inside_own = VALUES(managerial_inside_own),
                            state_own = VALUES(state_own),
                            institutional_own = VALUES(institutional_own),
                            foreign_own = VALUES(foreign_own)        
                    """), {
                        "fid": firm_id,
                        "year": fiscal_year,
                        "sid": sid,
                        "m": clean_number(row.get('Managerial/Inside ownership')),
                        "s": clean_number(row.get('State Ownership')),
                        "i": clean_number(row.get('Institutional ownership')),
                        "f": clean_number(row.get('Foreign ownership'))
                    })

                # ======================
                # INNOVATION + META
                # ======================
                elif doc_type == "Bao_Cao_Thuong_Nien":

                    conn.execute(text("""
                        INSERT INTO fact_innovation_year (
                            firm_id,fiscal_year,snapshot_id,
                            product_innovation,process_innovation,
                            evidence_product, evidence_process
                        )
                        VALUES (:fid,:year,:sid,:p,:pr,:evid_prod,:evid_proc)
                        ON DUPLICATE KEY UPDATE
                            product_innovation = VALUES(product_innovation),
                            process_innovation = VALUES(process_innovation),
                            evidence_product = VALUES(evidence_product),
                            evidence_process = VALUES(evidence_process)              
                    """), {
                        "fid": firm_id,
                        "year": fiscal_year,
                        "sid": sid,
                        "p": safe_binary(row.get('Product innovation')),
                        "pr": safe_binary(row.get('Process innovation')),
                        "evid_prod": safe_str(row.get("Evidence note product")),
                        "evid_proc": safe_str(row.get("Evidence note process"))
                    })

                    conn.execute(text("""
                        INSERT INTO fact_firm_year_meta (
                            firm_id,fiscal_year,snapshot_id,
                            employees_count,firm_age
                        )
                        VALUES (:fid,:year,:sid,:emp,:age)
                        ON DUPLICATE KEY UPDATE
                            employees_count = VALUES(employees_count),
                            firm_age = VALUES(firm_age)      
                    """), {
                        "fid": firm_id,
                        "year": fiscal_year,
                        "sid": sid,
                        "emp": clean_number(row.get('Number of employees')),
                        "age": clean_number(row.get('Firm Age'))
                    })

                row_has_insert = True

            if row_has_insert:
                inserted_rows += 1
            else:
                skipped_rows += 1

        print("====================================")
        print(f"✅ Imported rows: {inserted_rows}")
        print(f"⚠️ Skipped rows: {skipped_rows}")
        print(f"📌 Snapshot used: {snapshot_id}")
        print("====================================")