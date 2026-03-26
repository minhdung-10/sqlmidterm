import datetime
from sqlalchemy import create_engine, text

def run_create_snapshot(source_name, version_tag, connection_string):
    engine = create_engine(connection_string)
    snapshot_time = datetime.datetime.now()

    with engine.begin() as conn:

        # 1. Ensure data source exists
        conn.execute(text("""
            INSERT INTO dim_data_source (source_name)
            VALUES (:name)
            ON DUPLICATE KEY UPDATE source_name = source_name
        """), {"name": source_name})

        # 2. Get source_id
        data_source_id = conn.execute(text("""
            SELECT data_source_id
            FROM dim_data_source
            WHERE source_name = :name
        """), {"name": source_name}).scalar()

        if data_source_id is None:
            raise RuntimeError("❌ source_id not found")

        # 4. Insert snapshot
        result = conn.execute(text("""
            INSERT INTO fact_data_snapshot (
                data_source_id,
                snapshot_date,
                version_tag,
                created_at
            )
            VALUES (:sid, :date, :version, :created_at)
        """), {
            "sid": data_source_id,
            "date": snapshot_time,
            "version": version_tag,
            "created_at": snapshot_time
        })

        snapshot_id = result.lastrowid

    print(f"[SNAPSHOT] source={source_name}  version={version_tag} id={snapshot_id}")
    return snapshot_id