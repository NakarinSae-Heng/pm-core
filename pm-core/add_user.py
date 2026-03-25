import sqlite3
import hashlib
import argparse
from datetime import datetime

# ยืนยัน Path ตามที่คุณระบุ
DB_PATH = "/opt/pm-core/web/pm_users.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def hash_password(password):
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

def cmd_add(args):
    pw_hash = hash_password(args.password)
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn = get_conn()
        conn.execute("INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
                     (args.username, pw_hash, args.role, created_at))
        conn.commit()
        print(f"✅ เพิ่มผู้ใช้ '{args.username}' (Role: {args.role}) เรียบร้อยแล้ว")
    except sqlite3.IntegrityError:
        print(f"❌ Error: มีชื่อผู้ใช้ '{args.username}' อยู่ในระบบแล้ว")
    finally:
        conn.close()

def cmd_delete(args):
    conn = get_conn()
    res = conn.execute("DELETE FROM users WHERE username=?", (args.username,))
    conn.commit()
    if res.rowcount > 0:
        print(f"✅ ลบผู้ใช้ '{args.username}' เรียบร้อยแล้ว")
    else:
        print(f"⚠️ ไม่พบผู้ใช้ '{args.username}'")
    conn.close()

def cmd_reset(args):
    pw_hash = hash_password(args.new_password)
    conn = get_conn()
    res = conn.execute("UPDATE users SET password_hash=? WHERE username=?", (pw_hash, args.username))
    conn.commit()
    if res.rowcount > 0:
        print(f"✅ รีเซ็ตรหัสผ่านของ '{args.username}' เรียบร้อยแล้ว")
    else:
        print(f"⚠️ ไม่พบผู้ใช้ '{args.username}'")
    conn.close()

def cmd_list(args):
    conn = get_conn()
    query = "SELECT username, role, created_at FROM users"
    params = []
    if args.role:
        query += " WHERE role = ?"
        params.append(args.role)
    
    rows = conn.execute(query, params).fetchall()
    conn.close()

    print("\n{:<20} | {:<12} | {:<20}".format("Username", "Role", "Created At"))
    print("-" * 60)
    for r in rows:
        print("{:<20} | {:<12} | {:<20}".format(r['username'], r['role'], r['created_at']))
    print(f"\nรวมทั้งหมด: {len(rows)} รายการ\n")

def main():
    parser = argparse.ArgumentParser(description="เครื่องมือจัดการผู้ใช้ระบบ PM-Core (Path: /opt/pm-core/add_user.py)")
    subparsers = parser.add_subparsers(dest="command", help="คำสั่งที่ใช้งาน")

    # Command: add
    p_add = subparsers.add_parser("add", help="เพิ่มผู้ใช้ใหม่")
    p_add.add_argument("username", help="ชื่อผู้ใช้")
    p_add.add_argument("password", help="รหัสผ่าน")
    p_add.add_argument("-r", "--role", default="admin", choices=["admin", "engineer", "user"], help="สิทธิ์การใช้งาน (default: admin)")
    p_add.set_defaults(func=cmd_add)

    # Command: delete
    p_del = subparsers.add_parser("delete", help="ลบผู้ใช้")
    p_del.add_argument("username", help="ชื่อผู้ใช้ที่ต้องการลบ")
    p_del.set_defaults(func=cmd_delete)

    # Command: reset
    p_res = subparsers.add_parser("reset", help="รีเซ็ตรหัสผ่าน")
    p_res.add_argument("username", help="ชื่อผู้ใช้")
    p_res.add_argument("new_password", help="รหัสผ่านใหม่")
    p_res.set_defaults(func=cmd_reset)

    # Command: list
    p_list = subparsers.add_parser("list", help="แสดงรายชื่อผู้ใช้")
    p_list.add_argument("-r", "--role", choices=["admin", "engineer", "user"], help="กรองตามสิทธิ์")
    p_list.set_defaults(func=cmd_list)

    args = parser.parse_args()
    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
