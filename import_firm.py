import pandas as pd
from sqlalchemy import create_engine, text

def run_import_dim_firm(excel_path, connection_string):

    df = pd.read_excel(excel_path)

    # Clean column names
    df.columns = df.columns.str.strip()

    # Forward fill merged cells
    df['Company'] = df['Company'].ffill()

    # Keep needed columns
    df = df[['StockCode', 'Company']]

    # Clean ticker kỹ hơn (loại ký tự ẩn)
    df['StockCode'] = (
        df['StockCode']
        .astype(str)
        .str.strip()
        .str.replace(r'\s+', '', regex=True)
    )

    df['Company'] = df['Company'].astype(str).str.strip()

    # Drop null
    df = df.dropna(subset=['StockCode', 'Company'])

    # Unique ticker
    df = df.drop_duplicates(subset=['StockCode'])

    print("Unique firms found:", len(df))

    engine = create_engine(connection_string)

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO dim_firm (ticker, firm_name)
                VALUES (:ticker, :name)
            """), {
                "ticker": row['StockCode'],
                "name": row['Company']
            })

    print("✅ Import completed")