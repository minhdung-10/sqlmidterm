import datetime
from sqlalchemy import create_engine, text

def run_create_snapshot(source_name, fiscal_year, version_tag, connection_string):
    engine = create_engine(connection_string)
    snapshot_date = datetime.date.today()

    with engine.begin() as conn:
        # 1. Insert hoặc đảm bảo data source tồn tại
        conn.execute(text("""
            INSERT INTO dim_data_source (source_name)
            VALUES (:name)
            ON DUPLICATE KEY UPDATE source_name = source_name
        """), {"name": source_name})

        # 2. Lấy data_source_id
        data_source_id = conn.execute(text("""
            SELECT data_source_id
            FROM dim_data_source
            WHERE source_name = :name
        """), {"name": source_name}).scalar()

        # 3. Tạo snapshot
        result = conn.execute(text("""
            INSERT INTO fact_data_snapshot (data_source_id, fiscal_year, snapshot_date, version_tag)
            VALUES (:ds_id, :year, :date, :version)
        """), {
            "ds_id": data_source_id,
            "year": fiscal_year,
            "date": snapshot_date,
            "version": version_tag
        })

        snapshot_id = result.lastrowid

    print(f"✅ Snapshot created. SNAPSHOT_ID = {snapshot_id}")
    return snapshot_id