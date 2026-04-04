import os
import pandas as pd
from sqlalchemy import create_engine, text
import pwinput

def export_panel_latest(connection_string: str, output_path: str = "outputs/panel_latest.csv"):

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    engine = create_engine(connection_string)

    sql = text("SELECT * FROM vw_firm_panel_latest ORDER BY ticker, fiscal_year")
    
    with engine.begin() as conn:
        df = pd.read_sql(sql, conn)

    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"✅ Exported panel_latest: {len(df)} rows -> {output_path}")
    return df

if __name__ == "__main__":
    db_host = input("Host (Enter = localhost): ") or "localhost"
    db_port = input("Port (Enter = 3306): ") or "3306"
    db_user = input("User (Enter = root): ") or "root"
    db_name = input("Database name: ")
    db_password = pwinput.pwinput("Password: ", mask="*")

    connection_string = (
        f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    export_panel_latest(connection_string)
