import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime


def clean_str(x):
    if pd.isna(x) or x is None:
        return None
    return str(x).strip().upper()


def run_import_dim_firm(excel_path, connection_string):
    print(f"\n⏳ IMPORT FIRMS STARTED: {datetime.now()}")
    print(f"📂 File: {excel_path}")

    engine = create_engine(connection_string)

    # ===============================
    # 1. READ + CLEAN DATA
    # ===============================
    df = pd.read_excel(excel_path)

    df.columns = df.columns.str.strip().str.replace('\n', '')

    # Detect columns dynamically
    col_map = {
        "ticker": ["StockCode"],
        "company": ["Company", "Company Name"],
        "exchange_code": ["Exchange Code"],
        "exchange_name": ["Exchange Name"],
        "industry": ["Industry"]
    }

    def find_col(possible_names):
        for name in possible_names:
            if name in df.columns:
                return name
        return None

    ticker_col = find_col(col_map["ticker"])
    name_col = find_col(col_map["company"])
    exchange_code_col = find_col(col_map["exchange_code"])
    exchange_name_col = find_col(col_map["exchange_name"])
    industry_col = find_col(col_map["industry"])

    if not ticker_col:
        raise ValueError("❌ Không tìm thấy cột TICKER trong file")

    # Fix merged cells
    for col in [name_col, exchange_code_col, exchange_name_col, industry_col]:
        if col:
            df[col] = df[col].ffill()

    df = df.where(pd.notnull(df), None)

    total_rows = len(df)
    inserted = 0
    skipped = 0
    errors = 0

    with engine.begin() as conn:

        # ===============================
        # 2. EXCHANGE DIM
        # ===============================
        exchange_map = {}

        if exchange_code_col:
            unique_pairs = set()

            for _, row in df.iterrows():
                code = clean_str(row.get(exchange_code_col))
                name = clean_str(row.get(exchange_name_col))

                if code:
                    unique_pairs.add((code, name))

            for code, name in unique_pairs:
                conn.execute(text("""
                    INSERT INTO dim_exchange (exchange_code, exchange_name)
                    VALUES (:code, :name)
                    ON DUPLICATE KEY UPDATE
                        exchange_name = VALUES(exchange_name)
                """), {"code": code, "name": name})

            rows = conn.execute(text("""
                SELECT exchange_id, exchange_code FROM dim_exchange
            """)).fetchall()

            exchange_map = {clean_str(r[1]): r[0] for r in rows}

        # ===============================
        # 3. INDUSTRY DIM
        # ===============================
        industry_map = {}

        if industry_col:
            unique_inds = set(clean_str(x) for x in df[industry_col] if x)

            for ind in unique_inds:
                conn.execute(text("""
                    INSERT INTO dim_industry_l2 (industry_l2_name)
                    VALUES (:ind)
                    ON DUPLICATE KEY UPDATE industry_l2_name = industry_l2_name
                """), {"ind": ind})

            rows = conn.execute(text("""
                SELECT industry_l2_id, industry_l2_name FROM dim_industry_l2
            """)).fetchall()

            industry_map = {clean_str(r[1]): r[0] for r in rows}

        # ===============================
        # 4. DIM_FIRM
        # ===============================
        processed_tickers = set()

        for i, row in df.iterrows():
            try:
                ticker = clean_str(row.get(ticker_col))

                # QC CHECK
                if not ticker or ticker in ['NONE', 'NAN']:
                    print(f"⚠️ Row {i}: Invalid ticker → SKIP")
                    skipped += 1
                    continue

                if ticker in processed_tickers:
                    skipped += 1
                    continue

                firm_name = clean_str(row.get(name_col))
                ex_code = clean_str(row.get(exchange_code_col))
                ind_val = clean_str(row.get(industry_col))

                ex_id = exchange_map.get(ex_code)
                ind_id = industry_map.get(ind_val)

                conn.execute(text("""
                    INSERT INTO dim_firm (
                        ticker, firm_name, exchange_id, industry_l2_id
                    )
                    VALUES (:ticker, :name, :ex_id, :ind_id)
                    ON DUPLICATE KEY UPDATE
                        firm_name = VALUES(firm_name),
                        exchange_id = VALUES(exchange_id),
                        industry_l2_id = VALUES(industry_l2_id)
                """), {
                    "ticker": ticker,
                    "name": firm_name,
                    "ex_id": ex_id,
                    "ind_id": ind_id
                })

                processed_tickers.add(ticker)
                inserted += 1

            except Exception as e:
                print(f"❌ Row {i} ERROR: {e}")
                errors += 1

    # ===============================
    # 5. REPORT
    # ===============================
    print(f"""
    ✅ IMPORT COMPLETED

    📊 SUMMARY
    - Total rows: {total_rows}
    - Inserted/Updated: {inserted}
    - Skipped: {skipped}
    - Errors: {errors}
    """)