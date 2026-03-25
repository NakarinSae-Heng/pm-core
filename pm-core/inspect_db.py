#!/usr/bin/env python3
import sqlite3
import os
import json

DB_PATH = "/opt/pm-core/data/pm_reports.db"

def inspect():
    if not os.path.exists(DB_PATH):
        print(f"❌ ไม่พบไฟล์ Database ที่: {DB_PATH}")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        print("=== 1. Columns in 'latest_reports_v' (VIEW) ===")
        # ดึงรายชื่อคอลัมน์ทั้งหมดใน View นี้
        cur.execute("PRAGMA table_info(latest_reports_v)")
        cols = cur.fetchall()
        col_names = []
        for c in cols:
            print(f"- {c['name']} ({c['type']})")
            col_names.append(c['name'])
            
        print("\n=== 2. Sample Data (1 Row) ===")
        # ดึงตัวอย่างข้อมูลจริงมา 1 แถว เพื่อดู Format
        cur.execute("SELECT * FROM latest_reports_v LIMIT 1")
        row = cur.fetchone()
        if row:
            # แปลงเป็น dict เพื่อแสดงผลให้อ่านง่าย
            data = dict(row)
            # ตัดข้อมูลยาวๆ ทิ้งเพื่อความกระชับ (เช่น json ก้อนใหญ่ๆ ถ้ามี)
            for k, v in data.items():
                val_str = str(v)
                if len(val_str) > 100: 
                    val_str = val_str[:100] + "...(truncated)"
                print(f"{k}: {val_str}")
        else:
            print("No data found in view.")

        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    inspect()
