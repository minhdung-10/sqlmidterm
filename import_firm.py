import pandas as pd
from sqlalchemy import create_engine, text

def run_import_dim_firm(excel_path, connection_string):
    print(f"⏳ Đang nạp danh sách công ty, Sàn và Ngành từ file: {excel_path}")
    engine = create_engine(connection_string)
    
    # 1. Đọc Excel
    df = pd.read_excel(excel_path)

    # 2. Clean column names
    df.columns = df.columns.str.strip().str.replace('\n', '')

    # 3. Detect columns
    name_col = 'Company' if 'Company' in df.columns else None
    exchange_code_col = 'Exchange Code' if 'Exchange Code' in df.columns else None
    exchange_name_col = 'Exchange Name' if 'Exchange Name' in df.columns else None
    industry_col = 'Industry' if 'Industry' in df.columns else None

    # ===============================================================
    # FIX MERGED CELLS (QUAN TRỌNG)
    # ===============================================================
    if name_col:
        df[name_col] = df[name_col].ffill()

    if exchange_code_col:
        df[exchange_code_col] = df[exchange_code_col].ffill()

    if exchange_name_col:
        df[exchange_name_col] = df[exchange_name_col].ffill()

    if industry_col:
        df[industry_col] = df[industry_col].ffill()

    # Convert NaN → None
    df = df.where(pd.notnull(df), None)

    with engine.begin() as conn:

        # ==========================================
        # 1. INSERT EXCHANGE (code + name)
        # ==========================================
        exchange_map = {}

        if exchange_code_col:
            unique_pairs = set()

            for _, row in df.iterrows():
                code = row.get(exchange_code_col)
                name = row.get(exchange_name_col) if exchange_name_col else None

                code = str(code).strip() if code else None
                name = str(name).strip() if name else None

                if code:
                    unique_pairs.add((code, name))

            for code, name in unique_pairs:
                conn.execute(text("""
                    INSERT INTO dim_exchange (exchange_code, exchange_name)
                    VALUES (:code, :name)
                    ON DUPLICATE KEY UPDATE
                        exchange_name = VALUES(exchange_name)
                """), {
                    "code": code,
                    "name": name
                })

            rows = conn.execute(text("""
                SELECT exchange_id, exchange_code FROM dim_exchange
            """)).fetchall()

            exchange_map = {str(r[1]).strip(): r[0] for r in rows}

        # ==========================================
        # 2. INSERT INDUSTRY
        # ==========================================
        industry_map = {}

        if industry_col:
            unique_industries = [x for x in df[industry_col].unique() if x]

            for ind in unique_industries:
                conn.execute(text("""
                    INSERT INTO dim_industry_l2 (industry_l2_name) 
                    VALUES (:ind)
                    ON DUPLICATE KEY UPDATE industry_l2_name = industry_l2_name
                """), {"ind": str(ind).strip()})

            ind_rows = conn.execute(text("""
                SELECT industry_l2_id, industry_l2_name FROM dim_industry_l2
            """)).fetchall()

            industry_map = {str(r[1]).strip(): r[0] for r in ind_rows}

        # ==========================================
        # 3. INSERT DIM_FIRM
        # ==========================================
        inserted_count = 0
        processed_tickers = set()

        for _, row in df.iterrows():

            ticker_raw = row.get('StockCode')
            ticker = str(ticker_raw).strip().upper() if ticker_raw else None

            if not ticker or ticker in processed_tickers or ticker in ['NONE', 'NAN']:
                continue

            firm_name = str(row.get(name_col)).strip() if name_col and row.get(name_col) else None
            ex_code = str(row.get(exchange_code_col)).strip() if exchange_code_col and row.get(exchange_code_col) else None
            ind_val = str(row.get(industry_col)).strip() if industry_col and row.get(industry_col) else None

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
            inserted_count += 1

    print(f"✅ Đã cập nhật {inserted_count} công ty.")