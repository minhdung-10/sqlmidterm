import datetime
from sqlalchemy import create_engine, text


def run_create_snapshot(source_name, version_tag, connection_string):

    engine = create_engine(connection_string)
    snapshot_date = datetime.date.today()

    with engine.begin() as conn:

        # ======================================
        # 1. ENSURE DATA SOURCE EXISTS
        # ======================================
        conn.execute(text("""
            INSERT INTO dim_data_source (source_name)
            VALUES (:name)
            ON DUPLICATE KEY UPDATE source_name = source_name
        """), {"name": source_name})

        # ======================================
        # 2. GET DATA_SOURCE_ID
        # ======================================
        data_source_id = conn.execute(text("""
            SELECT data_source_id
            FROM dim_data_source
            WHERE source_name = :name
        """), {"name": source_name}).scalar()

        if data_source_id is None:
            raise RuntimeError("❌ Failed to get data_source_id")

        # ======================================
        # 3. CHECK EXISTING SNAPSHOT (ANTI-DUPLICATE)
        # ======================================
        existing_snapshot = conn.execute(text("""
            SELECT snapshot_id
            FROM fact_data_snapshot
            WHERE data_source_id = :ds_id
              AND version_tag = :version
        """), {
            "ds_id": data_source_id,
            "version": version_tag
        }).scalar()

        if existing_snapshot:
            print(f"⚠️ Snapshot already exists → SNAPSHOT_ID = {existing_snapshot}")
            return existing_snapshot

        # ======================================
        # 4. CREATE NEW SNAPSHOT
        # ======================================
        result = conn.execute(text("""
            INSERT INTO fact_data_snapshot (
                data_source_id,
                snapshot_date,
                version_tag
            )
            VALUES (:ds_id, :date, :version)
        """), {
            "ds_id": data_source_id,
            "date": snapshot_date,
            "version": version_tag
        })

        snapshot_id = result.lastrowid

    print(f"✅ Snapshot created. SNAPSHOT_ID = {snapshot_id}")
    return snapshot_id