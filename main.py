import time
import pwinput

from import_firm import run_import_dim_firm
from create_snapshot import run_create_snapshot
from import_panel import run_import_panel


def run_pipeline():

    print("=" * 60)
    print("🚀 RUNNING FULL FINANCIAL ETL PIPELINE 🚀")
    print("=" * 60)

    db_host = input("Host (Enter = localhost): ") or "localhost"
    db_port = input("Port (Enter = 3306): ") or "3306"
    db_user = input("User (Enter = root): ") or "root"
    db_name = input("Database name: ")
    db_password = pwinput.pwinput("Password: ", mask="*")

    connection_string = (
        f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    EXCEL_FILE = "panel_2020_2024.xlsx"
    VERSION_TAG = "v1"

    try:
        print("\n▶ STEP 1: Import DIM_FIRM")
        time.sleep(1)

        run_import_dim_firm(EXCEL_FILE, connection_string)

        print("\n▶ STEP 2: Create Snapshot")
        time.sleep(1)

        snapshot_id = run_create_snapshot(
            source_name=EXCEL_FILE,
            version_tag=VERSION_TAG,
            connection_string=connection_string
        )

        print("\n▶ STEP 3: Import Panel Data")
        time.sleep(1)

        run_import_panel(
            excel_path=EXCEL_FILE,
            connection_string=connection_string,
            snapshot_id= snapshot_id
        )

        print("\n" + "=" * 60)
        print("🎉 PIPELINE COMPLETED SUCCESSFULLY 🎉")
        print("=" * 60)

    except Exception as e:
        print("\n❌ PIPELINE FAILED")
        print("Error:", e)


if __name__ == "__main__":
    
    run_pipeline()

