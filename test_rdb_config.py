# test_rdb_config.py

import os
from core.rdb_saver import save_rdb
from core.rdb_loader import load_rdb
from config import RDB_FILE

def test_rdb_save_load():
    print("🧪 Testing RDB Config Integration...")

    test_data = {
        "name": "Alice",
        "age": "30"
    }

    print("💾 Saving test data...")
    save_rdb(RDB_FILE, test_data)

    print("📥 Loading test data back...")
    loaded_data = load_rdb(RDB_FILE)

    assert loaded_data == test_data, "❌ Data mismatch!"
    print("✅ Test passed! RDB config is working correctly.")

    # Optional: Clean up test file
    if os.path.exists(RDB_FILE):
        os.remove(RDB_FILE)
        print("🧹 Cleanup complete.")

if __name__ == "__main__":
    test_rdb_save_load()