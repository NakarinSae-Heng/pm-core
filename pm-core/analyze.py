#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze.py — ประมวลผลไฟล์ .tar.gz จาก /data/pm_upload/incoming
เวอร์ชัน: 2025-11-01 PM-CORE detail ready

สิ่งที่ทำเพิ่มจากของเดิม
- ยังกระทำแบบเดิมทุกอย่าง (insert -> reports)
- เพิ่มคอลัมน์ใน reports แบบปลอดภัย: swap_pct, mem_used_mb, mem_free_mb, mem_total_mb
- เก็บประวัติทุกครั้งลง metrics_raw (ไว้ทำกราฟ/ETA)
- ingest ไฟล์เสริมจาก client ตัวใหม่:
    mounts.txt     -> host_mounts
    top_cpu.txt    -> host_top_cpu
    top_mem.txt    -> host_top_mem
    lvm_vgs.txt    -> host_lvm_vgs
    lvm_lvs.txt    -> host_lvm_lvs
    lvm_pvs.txt    -> host_lvm_pvs
    services.txt   -> host_services
    disks.txt      -> host_disks
    ntp_status.txt -> host_ntp_status
- ถ้าไม่พบไฟล์เหล่านี้ จะไม่ error และจะยังเขียน reports ปกติ
"""

import os, tarfile, shutil, sqlite3, re
from datetime import datetime, timezone

# ---------------------------------------------------------------------
# PATH หลักของระบบ
# ---------------------------------------------------------------------
PROJECT_ROOT = "/opt/pm-core"
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DB_PATH = os.path.join(DATA_DIR, "pm_reports.db")

UPLOAD_DIR = "/data/pm_upload/incoming"
PROC_DIR   = "/data/pm_upload/processing"
FAILED_DIR = "/data/pm_upload/failed"
DONE_DIR   = "/data/pm_processed"

os.makedirs(PROC_DIR, exist_ok=True)
os.makedirs(FAILED_DIR, exist_ok=True)
os.makedirs(DONE_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)


# ---------------------------------------------------------------------
# ฟังก์ชัน DB เบื้องต้น
# ---------------------------------------------------------------------
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def add_col_if_missing(cur, table, col, typ):
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")

def ensure_schema():
    """
    สร้าง/เสริม schema ขั้นต่ำที่เว็บใช้ + ตารางใหม่ที่ ingest ต้องใช้
    """
    conn = get_db()
    cur = conn.cursor()

    # ตารางหลักของเดิม
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS reports (id INTEGER PRIMARY KEY)")
    except Exception:
        pass

    # คอลัมน์เดิม + คอลัมน์ที่เพิ่มใหม่
    base_cols = [
        ("hostname","TEXT"),
        ("timestamp","TEXT"),
        ("processed_utc","TEXT"),
        ("cpu","REAL"),
        ("mem","REAL"),
        ("disk","REAL"),
        ("status","TEXT"),
        ("os_name","TEXT"),
        ("os_version","TEXT"),
        ("kernel","TEXT"),
        ("ip_address","TEXT"),
        ("ip_all","TEXT"),
        ("ip_address_all","TEXT"),
        ("uptime","TEXT"),
        ("uptime_secs","INTEGER"),
        ("machine_id","TEXT"),
        ("hostid","TEXT"),
        # เพิ่มใหม่จาก client รุ่นล่าสุด
        ("swap_pct","REAL"),
        ("mem_used_mb","INTEGER"),
        ("mem_free_mb","INTEGER"),
        ("mem_total_mb","INTEGER"),
    ]
    for col, typ in base_cols:
        add_col_if_missing(cur, "reports", col, typ)

    # ตารางเก็บประวัติ metrics (ทำกราฟ, ETA)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metrics_raw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hostname TEXT NOT NULL,
            collected_utc TEXT NOT NULL,
            processed_utc TEXT NOT NULL,
            cpu_pct REAL,
            mem_pct REAL,
            swap_pct REAL,
            disk_root_pct REAL,
            mem_used_mb INTEGER,
            mem_total_mb INTEGER
        )
    """)

    # รายละเอียด disks/mounts
    cur.execute("""
        CREATE TABLE IF NOT EXISTS host_mounts (
            hostname TEXT,
            mountpoint TEXT,
            fstype TEXT,
            size TEXT,
            used TEXT,
            avail TEXT,
            used_pct TEXT,
            collected_utc TEXT
        )
    """)

    # top processes
    cur.execute("""
        CREATE TABLE IF NOT EXISTS host_top_cpu (
            hostname TEXT,
            collected_utc TEXT,
            pid INTEGER,
            cmd TEXT,
            cpu REAL,
            mem REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS host_top_mem (
            hostname TEXT,
            collected_utc TEXT,
            pid INTEGER,
            cmd TEXT,
            cpu REAL,
            mem REAL
        )
    """)

    # LVM
    cur.execute("""
        CREATE TABLE IF NOT EXISTS host_lvm_vgs (
            hostname TEXT,
            collected_utc TEXT,
            vg_name TEXT,
            lv_count INTEGER,
            pv_count INTEGER,
            vg_size TEXT,
            vg_free TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS host_lvm_lvs (
            hostname TEXT,
            collected_utc TEXT,
            lv_name TEXT,
            vg_name TEXT,
            lv_size TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS host_lvm_pvs (
            hostname TEXT,
            collected_utc TEXT,
            pv_name TEXT,
            vg_name TEXT,
            pv_size TEXT,
            pv_free TEXT
        )
    """)

    # services จาก systemctl
    cur.execute("""
        CREATE TABLE IF NOT EXISTS host_services (
            hostname TEXT,
            collected_utc TEXT,
            service TEXT,
            load TEXT,
            active TEXT,
            sub TEXT,
            description TEXT
        )
    """)

    # disks จาก lsblk
    cur.execute("""
        CREATE TABLE IF NOT EXISTS host_disks (
            hostname TEXT,
            collected_utc TEXT,
            name TEXT,
            dtype TEXT,
            size TEXT,
            mountpoint TEXT
        )
    """)

    # ntp/time sync
    cur.execute("""
        CREATE TABLE IF NOT EXISTS host_ntp_status (
            hostname TEXT,
            collected_utc TEXT,
            ntp_type TEXT,
            raw TEXT
        )
    """)

    # system / hardware (virtualization, manufacturer, product, serial)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS host_system_hw (
            hostname TEXT,
            collected_utc TEXT,
            virt_type TEXT,
            manufacturer TEXT,
            product_name TEXT,
            serial_number TEXT
        )
    """)

    # cpu detail (model, arch, sockets, cores, threads, mhz)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS host_cpu_detail (
            hostname TEXT,
            collected_utc TEXT,
            model TEXT,
            arch TEXT,
            sockets INTEGER,
            cores_per_socket INTEGER,
            threads_per_core INTEGER,
            cpu_mhz REAL
        )
    """)

    # network extra (gateway + dns summary)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS host_network_extra (
            hostname TEXT,
            collected_utc TEXT,
            gateway TEXT,
            dns TEXT
        )
    """)

    # repository / patch status
    # repository / patch status
    cur.execute("""
        CREATE TABLE IF NOT EXISTS host_repo_status (
            hostname TEXT,
            collected_utc TEXT,
            pkg_manager TEXT,
            enabled_repos INTEGER,
            updates_available INTEGER,
            last_update TEXT
        )
    """)

    # --- migrate host_repo_status (add new columns if missing) ---
    try:
        cols = [r[1] for r in cur.execute("PRAGMA table_info(host_repo_status);").fetchall()]
    except Exception:
        cols = []

    def _add_col(colname, coltype):
        if colname not in cols:
            try:
                cur.execute(f"ALTER TABLE host_repo_status ADD COLUMN {colname} {coltype}")
            except Exception:
                pass

    _add_col("repo_health", "TEXT")
    _add_col("repo_health_reason", "TEXT")
    _add_col("online_repos", "INTEGER")
    _add_col("offline_repos", "INTEGER")
    _add_col("local_repos", "INTEGER")
    _add_col("repolist_b64", "TEXT")
    _add_col("reposrc_b64", "TEXT")
    _add_col("makecache_rc", "INTEGER")
    _add_col("makecache_out_b64", "TEXT")

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------
# ตัวช่วย parsing
# ---------------------------------------------------------------------
def parse_os_release(s: str):
    name, ver = None, None
    for line in s.splitlines():
        line = line.strip()
        if line.startswith("NAME=") and not name:
            name = line.split("=",1)[1].strip().strip('"')
        if (line.startswith("VERSION_ID=") or line.startswith("VERSION=")) and not ver:
            ver = line.split("=",1)[1].strip().strip('"')
    return name, ver

def parse_uptime_secs(s: str):
    if not s: return 0
    s = s.lower()
    total = 0
    m = re.findall(r"(\d+)\s*(day|days|hour|hours|minute|minutes|sec|second|seconds)", s)
    if m:
        for val, unit in m:
            n = int(val)
            if unit.startswith("day"): total += n*86400
            elif unit.startswith("hour"): total += n*3600
            elif unit.startswith("minute"): total += n*60
            elif unit.startswith("sec"): total += n
        return total
    m2 = re.findall(r"(\d+)\s*(d|h|m|s)", s)
    if m2:
        for val, unit in m2:
            n = int(val)
            if unit == "d": total += n*86400
            elif unit == "h": total += n*3600
            elif unit == "m": total += n*60
            elif unit == "s": total += n
    return total

def read_file_text(base, name):
    p = os.path.join(base, name)
    try:
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            return f.read().strip()
    except Exception:
        return ""

def extract_ipv4_from_ip_dump(dump: str):
    ips = []
    for line in dump.splitlines():
        line = line.strip()
        if "inet " in line:
            try:
                part = line.split("inet ", 1)[1].split()[0]
                ip = part.split("/")[0]
                if ip and ip not in ips:
                    ips.append(ip)
            except Exception:
                pass
    return ips

def choose_primary_ip(ip_all_dump: str, ip_text: str):
    ips = extract_ipv4_from_ip_dump(ip_all_dump) if ip_all_dump else []
    if ips:
        return ips[0]
    return (ip_text or "").strip()


# ---------------------------------------------------------------------
# ฟังก์ชัน ingest ย่อยแต่ละไฟล์
# ---------------------------------------------------------------------
def ingest_mem_detail(conn, hostname, collected_utc, mem_detail_txt):
    if not mem_detail_txt:
        return (None, None, None)
    used_mb = free_mb = total_mb = None
    used_pct = None
    for line in mem_detail_txt.splitlines():
        line = line.strip()
        if not line or "=" not in line: continue
        k, v = line.split("=", 1)
        if k == "USED_MB":
            used_mb = int(v)
        elif k == "FREE_MB":
            free_mb = int(v)
        elif k == "TOTAL_MB":
            total_mb = int(v)
        elif k == "USED_PCT":
            used_pct = float(v)
    # เราแค่คืนค่าไปให้ caller เอาไปอัปเดต reports + metrics_raw
    return (used_mb, free_mb, total_mb)

def ingest_mounts(conn, hostname, collected_utc, mounts_txt):
    if not mounts_txt:
        return
    cur = conn.cursor()
    # ลบของเก่าเฉพาะรอบนี้ (hostname + collected_utc)
    cur.execute("DELETE FROM host_mounts WHERE hostname=? AND collected_utc=?", (hostname, collected_utc))
    for line in mounts_txt.splitlines():
        parts = line.strip().split("|")
        if len(parts) != 6:
            continue
        mountpoint, fstype, size, used, avail, used_pct = parts
        cur.execute("""
            INSERT INTO host_mounts
                (hostname, mountpoint, fstype, size, used, avail, used_pct, collected_utc)
            VALUES (?,?,?,?,?,?,?,?)
        """, (hostname, mountpoint, fstype, size, used, avail, used_pct, collected_utc))
    conn.commit()

def ingest_top_procs(conn, hostname, collected_utc, top_cpu_txt, top_mem_txt):
    cur = conn.cursor()
    # ลบของเก่าในรอบนี้
    cur.execute("DELETE FROM host_top_cpu WHERE hostname=? AND collected_utc=?", (hostname, collected_utc))
    cur.execute("DELETE FROM host_top_mem WHERE hostname=? AND collected_utc=?", (hostname, collected_utc))
    # top cpu
    if top_cpu_txt:
        for line in top_cpu_txt.splitlines()[1:]:  # ข้าม header
            parts = line.split(None, 3)
            if len(parts) < 4:
                continue
            pid, cmd, cpu, mem = parts
            try: pid_i = int(pid)
            except: pid_i = None
            try: cpu_f = float(cpu)
            except: cpu_f = None
            try: mem_f = float(mem)
            except: mem_f = None
            cur.execute("""
                INSERT INTO host_top_cpu (hostname, collected_utc, pid, cmd, cpu, mem)
                VALUES (?,?,?,?,?,?)
            """, (hostname, collected_utc, pid_i, cmd, cpu_f, mem_f))
    # top mem
    if top_mem_txt:
        for line in top_mem_txt.splitlines()[1:]:
            parts = line.split(None, 3)
            if len(parts) < 4:
                continue
            pid, cmd, cpu, mem = parts
            try: pid_i = int(pid)
            except: pid_i = None
            try: cpu_f = float(cpu)
            except: cpu_f = None
            try: mem_f = float(mem)
            except: mem_f = None
            cur.execute("""
                INSERT INTO host_top_mem (hostname, collected_utc, pid, cmd, cpu, mem)
                VALUES (?,?,?,?,?,?)
            """, (hostname, collected_utc, pid_i, cmd, cpu_f, mem_f))
    conn.commit()

def ingest_lvm(conn, hostname, collected_utc, vgs_txt, lvs_txt, pvs_txt):
    cur = conn.cursor()
    # ลบรอบนี้ก่อน
    cur.execute("DELETE FROM host_lvm_vgs WHERE hostname=? AND collected_utc=?", (hostname, collected_utc))
    cur.execute("DELETE FROM host_lvm_lvs WHERE hostname=? AND collected_utc=?", (hostname, collected_utc))
    cur.execute("DELETE FROM host_lvm_pvs WHERE hostname=? AND collected_utc=?", (hostname, collected_utc))
    # vgs
    if vgs_txt:
        for line in vgs_txt.splitlines():
            parts = line.strip().split("|")
            if len(parts) < 5:
                continue
            vg_name, lv_count, pv_count, vg_size, vg_free = parts[:5]
            cur.execute("""
                INSERT INTO host_lvm_vgs
                    (hostname, collected_utc, vg_name, lv_count, pv_count, vg_size, vg_free)
                VALUES (?,?,?,?,?,?,?)
            """, (hostname, collected_utc, vg_name.strip(), int(lv_count), int(pv_count), vg_size.strip(), vg_free.strip()))
    # lvs
    if lvs_txt:
        for line in lvs_txt.splitlines():
            parts = line.strip().split("|")
            if len(parts) < 3:
                continue
            lv_name, vg_name, lv_size = parts[:3]
            cur.execute("""
                INSERT INTO host_lvm_lvs
                    (hostname, collected_utc, lv_name, vg_name, lv_size)
                VALUES (?,?,?,?,?)
            """, (hostname, collected_utc, lv_name.strip(), vg_name.strip(), lv_size.strip()))
    # pvs
    if pvs_txt:
        for line in pvs_txt.splitlines():
            parts = line.strip().split("|")
            if len(parts) < 4:
                continue
            pv_name, vg_name, pv_size, pv_free = parts[:4]
            cur.execute("""
                INSERT INTO host_lvm_pvs
                    (hostname, collected_utc, pv_name, vg_name, pv_size, pv_free)
                VALUES (?,?,?,?,?,?)
            """, (hostname, collected_utc, pv_name.strip(), vg_name.strip(), pv_size.strip(), pv_free.strip()))
    conn.commit()

def ingest_services(conn, hostname, collected_utc, services_txt):
    if not services_txt:
        return
    cur = conn.cursor()
    cur.execute("DELETE FROM host_services WHERE hostname=? AND collected_utc=?", (hostname, collected_utc))
    for line in services_txt.splitlines():
        line = line.strip()
        if not line or line.startswith("UNIT "):
            continue
        parts = line.split(None, 4)
        if len(parts) < 5:
            # บางดิสโทรอาจมีรูปแบบไม่ครบ ให้เก็บดิบ ๆ เผื่อใช้วิเคราะห์ทีหลัง
            cur.execute("""
                INSERT INTO host_services (hostname, collected_utc, service, load, active, sub, description)
                VALUES (?,?,?,?,?,?,?)
            """, (hostname, collected_utc, line, None, None, None, None))
            continue
        unit, load, active, sub, desc = parts
        cur.execute("""
            INSERT INTO host_services (hostname, collected_utc, service, load, active, sub, description)
            VALUES (?,?,?,?,?,?,?)
        """, (hostname, collected_utc, unit, load, active, sub, desc))
    conn.commit()

def ingest_disks(conn, hostname, collected_utc, disks_txt):
    if not disks_txt:
        return
    cur = conn.cursor()
    cur.execute("DELETE FROM host_disks WHERE hostname=? AND collected_utc=?", (hostname, collected_utc))
    # รูปแบบจาก lsblk -o NAME,TYPE,SIZE,MOUNTPOINT
    for line in disks_txt.splitlines()[1:]:
        parts = line.split()
        if not parts:
            continue
        name = parts[0]; dtype=None; size=None; mountpoint=None
        if len(parts) >= 2: dtype = parts[1]
        if len(parts) >= 3: size = parts[2]
        if len(parts) >= 4: mountpoint = parts[3]
        cur.execute("""
            INSERT INTO host_disks (hostname, collected_utc, name, dtype, size, mountpoint)
            VALUES (?,?,?,?,?,?)
        """, (hostname, collected_utc, name, dtype, size, mountpoint))
    conn.commit()

def ingest_ntp(conn, hostname, collected_utc, ntp_txt):
    if not ntp_txt:
        return
    ntp_type = "UNKNOWN"
    for line in ntp_txt.splitlines():
        if line.startswith("TYPE="):
            ntp_type = line.split("=",1)[1].strip()
            break
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM host_ntp_status WHERE hostname=? AND collected_utc=?
    """, (hostname, collected_utc))
    cur.execute("""
        INSERT INTO host_ntp_status (hostname, collected_utc, ntp_type, raw)
        VALUES (?,?,?,?)
    """, (hostname, collected_utc, ntp_type, ntp_txt))
    conn.commit()

def ingest_system_hw(conn, hostname, collected_utc, system_hw_txt: str):
    """
    ingest system_hw.txt
    รูปแบบไฟล์ (จาก pm_collect.sh):
        VIRT_TYPE=vmware
        MANUFACTURER=VMware, Inc.
        PRODUCT_NAME=VMware Virtual Platform
        SERIAL_NUMBER=XXXX
    """
    if not system_hw_txt:
        return
    kv = {}
    for line in system_hw_txt.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        kv[k.strip()] = v.strip()

    virt_type    = kv.get("VIRT_TYPE")
    manufacturer = kv.get("MANUFACTURER")
    product_name = kv.get("PRODUCT_NAME")
    serial       = kv.get("SERIAL_NUMBER")

    cur = conn.cursor()
    cur.execute("""
        DELETE FROM host_system_hw
        WHERE hostname=? AND collected_utc=?
    """, (hostname, collected_utc))
    cur.execute("""
        INSERT INTO host_system_hw
            (hostname, collected_utc, virt_type, manufacturer, product_name, serial_number)
        VALUES (?,?,?,?,?,?)
    """, (hostname, collected_utc, virt_type, manufacturer, product_name, serial))
    conn.commit()


def ingest_cpu_detail(conn, hostname, collected_utc, cpu_detail_txt: str):
    """
    ingest cpu_detail.txt
    รูปแบบไฟล์:
        MODEL=Intel(R) Xeon(...)
        ARCH=x86_64
        SOCKETS=1
        CORES_PER_SOCKET=4
        THREADS_PER_CORE=2
        CPU_MHZ=2100.000
    """
    if not cpu_detail_txt:
        return
    kv = {}
    for line in cpu_detail_txt.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        kv[k.strip()] = v.strip()

    model  = kv.get("MODEL")
    arch   = kv.get("ARCH")

    def _to_int(val):
        try:
            return int(val)
        except Exception:
            return None

    def _to_float(val):
        try:
            return float(val)
        except Exception:
            return None

    sockets          = _to_int(kv.get("SOCKETS"))
    cores_per_socket = _to_int(kv.get("CORES_PER_SOCKET"))
    threads_per_core = _to_int(kv.get("THREADS_PER_CORE"))
    cpu_mhz          = _to_float(kv.get("CPU_MHZ"))

    cur = conn.cursor()
    cur.execute("""
        DELETE FROM host_cpu_detail
        WHERE hostname=? AND collected_utc=?
    """, (hostname, collected_utc))
    cur.execute("""
        INSERT INTO host_cpu_detail
            (hostname, collected_utc, model, arch,
             sockets, cores_per_socket, threads_per_core, cpu_mhz)
        VALUES (?,?,?,?,?,?,?,?)
    """, (hostname, collected_utc, model, arch,
          sockets, cores_per_socket, threads_per_core, cpu_mhz))
    conn.commit()


def ingest_network_extra(conn, hostname, collected_utc, net_extra_txt: str):
    """
    ingest network_extra.txt
    รูปแบบไฟล์:
        GATEWAY=192.168.56.1
        DNS=8.8.8.8,1.1.1.1
    """
    if not net_extra_txt:
        return
    kv = {}
    for line in net_extra_txt.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        kv[k.strip()] = v.strip()

    gateway = kv.get("GATEWAY")
    dns     = kv.get("DNS")

    cur = conn.cursor()
    cur.execute("""
        DELETE FROM host_network_extra
        WHERE hostname=? AND collected_utc=?
    """, (hostname, collected_utc))
    cur.execute("""
        INSERT INTO host_network_extra
            (hostname, collected_utc, gateway, dns)
        VALUES (?,?,?,?)
    """, (hostname, collected_utc, gateway, dns))
    conn.commit()


def ingest_repo_status(conn, hostname, collected_utc, repo_status_txt: str):
    """
    ingest repo_status.txt

    Phase 1 keys (เพิ่มเติมจากเดิม):
        REPOLIST_B64=...
        REPOSRC_B64=...
        MAKECACHE_RC=0
        MAKECACHE_OUT_B64=...
    """
    if not repo_status_txt:
        return

    kv = {}
    for line in repo_status_txt.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        kv[k.strip()] = v.strip()

    pkg_manager = kv.get("PKG_MANAGER")

    def _to_int(val):
        try:
            return int(val)
        except Exception:
            return None

    enabled_repos      = _to_int(kv.get("ENABLED_REPOS"))
    updates_available  = _to_int(kv.get("UPDATES_AVAILABLE"))
    last_update        = kv.get("LAST_UPDATE") or "unknown"

    repolist_b64       = kv.get("REPOLIST_B64")
    reposrc_b64        = kv.get("REPOSRC_B64")
    makecache_rc       = _to_int(kv.get("MAKECACHE_RC"))
    makecache_out_b64  = kv.get("MAKECACHE_OUT_B64")
    repo_health_raw    = (kv.get("REPO_HEALTH") or "").strip()
    repo_health_reason = (kv.get("REPO_HEALTH_REASON") or "").strip()

    # ---------- compute LOCAL vs NETWORK counts ----------
    local_repos = 0
    enabled_from_src = 0

    def _b64_to_text(s):
        if not s:
            return ""
        try:
            import base64
            return base64.b64decode(s).decode("utf-8", errors="ignore")
        except Exception:
            return ""

    reposrc_text = _b64_to_text(reposrc_b64)

    # reposrc format from client:
    #   [repoid|src|enabled|type]
    for raw in reposrc_text.splitlines():
        raw = raw.strip()
        if not raw:
            continue
        if raw.startswith("[") and raw.endswith("]"):
            raw = raw[1:-1]
        parts = raw.split("|")
        if len(parts) < 2:
            continue

        repoid = (parts[0] or "").strip()
        src    = (parts[1] or "").strip()
        en     = (parts[2] or "").strip() if len(parts) >= 3 else ""

        # treat enabled empty as enabled=1 (ตามที่ client ใส่ไว้เป็น placeholder)
        is_enabled = True
        if en != "":
            is_enabled = (en == "1" or en.lower() == "true" or en.lower() == "yes")

        if is_enabled and repoid:
            enabled_from_src += 1
            if src.startswith("file://"):
                local_repos += 1

    # prefer ENABLED_REPOS from client; fallback to parsed enabled count
    if enabled_repos is None:
        enabled_repos = enabled_from_src if enabled_from_src > 0 else None

    network_repos = None
    if enabled_repos is not None:
        network_repos = max(int(enabled_repos) - int(local_repos), 0)

    # ---------- Health logic (Phase 1, overall) ----------
    # rules:
    # - ถ้าไม่มี network repo -> OK (LOCAL only)
    # - ถ้ามี network repo และ makecache_rc==0 -> OK
    # - ถ้ามี network repo และ makecache_rc!=0:
    #     - ถ้ามี local ด้วย -> DEGRADED (🟠)
    #     - ไม่มี local -> OFFLINE (🔴)

    # ---------- repo_health ----------
    repo_health = "unknown"
    online_repos = None
    offline_repos = None

    # 1) Prefer health sent by client (C2)
    h = repo_health_raw.upper()
    if h == "OK":
        repo_health = "ok"
        online_repos = network_repos
        offline_repos = 0
    elif h == "FAIL":
        repo_health = "offline"
        online_repos = 0
        offline_repos = network_repos
    elif h in ("WARN", "WARNING", "DEGRADED"):
        repo_health = "degraded"
        # keep online/offline as None (ambiguous / partial)

    # 2) Fallback to legacy logic if client didn't send REPO_HEALTH
    if repo_health == "unknown":
        if makecache_rc is None:
            repo_health = "unknown"
        elif makecache_rc == 0:
            repo_health = "ok"
            online_repos = network_repos
            offline_repos = 0
        else:
            if local_repos > 0:
                repo_health = "degraded"
            else:
                repo_health = "offline"
                online_repos = 0
                offline_repos = network_repos


    cur = conn.cursor()
    cur.execute("""
        DELETE FROM host_repo_status
        WHERE hostname=? AND collected_utc=?
    """, (hostname, collected_utc))

    cur.execute("""
        INSERT INTO host_repo_status
            (hostname, collected_utc, pkg_manager,
             enabled_repos, updates_available, last_update,
             repo_health, repo_health_reason, online_repos, offline_repos, local_repos,
             repolist_b64, reposrc_b64, makecache_rc, makecache_out_b64)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        hostname, collected_utc, pkg_manager,
        enabled_repos, updates_available, last_update,
        repo_health, repo_health_reason, online_repos, offline_repos, local_repos,
        repolist_b64, reposrc_b64, makecache_rc, makecache_out_b64
    ))
    conn.commit()



# ---------------------------------------------------------------------
# ประมวลผลไฟล์เดียว
# ---------------------------------------------------------------------
def process_one(tar_path: str):
    ensure_schema()
    base_name = os.path.basename(tar_path)
    work_dir = os.path.join(PROC_DIR, base_name.replace(".tar.gz",""))
    os.makedirs(work_dir, exist_ok=True)

    # แตกไฟล์
    try:
        with tarfile.open(tar_path, "r:gz") as tf:
            tf.extractall(work_dir)
    except Exception as e:
        shutil.move(tar_path, os.path.join(FAILED_DIR, base_name))
        return f"FAIL: tar extract: {e}"

    # อ่านไฟล์หลัก
    hostname      = read_file_text(work_dir, "hostname.txt")
    machine_id    = read_file_text(work_dir, "machine_id.txt")
    hostid        = read_file_text(work_dir, "hostid.txt")
    os_release    = read_file_text(work_dir, "os_release.txt")
    kernel        = read_file_text(work_dir, "kernel.txt")
    uptime_str    = read_file_text(work_dir, "uptime.txt")
    collected_utc = read_file_text(work_dir, "collected_utc.txt")
    ip_text       = read_file_text(work_dir, "ip.txt")
    ip_all_dump   = read_file_text(work_dir, "ip_all.txt")

    # metrics หลัก
    def to_num(s):
        s = (s or "").strip().replace("%","")
        try:
            return float(s)
        except Exception:
            return 0.0

    cpu  = to_num(read_file_text(work_dir, "cpu.txt"))
    mem  = to_num(read_file_text(work_dir, "mem.txt"))
    disk = to_num(read_file_text(work_dir, "disk_root.txt"))
    swap_pct = to_num(read_file_text(work_dir, "swap_pct.txt"))

    # memory แบบละเอียด
    mem_detail_txt = read_file_text(work_dir, "mem_detail.txt")
    mem_used_mb, mem_free_mb, mem_total_mb = ingest_mem_detail(None, None, None, mem_detail_txt)

    # os name/version
    os_name, os_version = parse_os_release(os_release)
    uptime_secs = parse_uptime_secs(uptime_str)

    # ip รวม/หลัก
    ip_all = ip_all_dump or ip_text
    ip_address = choose_primary_ip(ip_all_dump, ip_text)

    # เวลา
    processed_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if not collected_utc:
        collected_utc = processed_utc

    # เริ่มเขียน DB
    conn = get_db()
    cur = conn.cursor()

    # อัปเดตรายงานหลัก (reports)
    cur.execute("""
        INSERT INTO reports(
            hostname, timestamp, processed_utc,
            cpu, mem, disk, status,
            os_name, os_version, kernel,
            ip_address, ip_all, ip_address_all,
            uptime, uptime_secs, hostid, machine_id,
            swap_pct, mem_used_mb, mem_free_mb, mem_total_mb
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        hostname or "unknown",
        collected_utc,
        processed_utc,
        cpu, mem, disk, None,
        os_name, os_version, kernel,
        ip_address, ip_all, ip_all,
        uptime_str, int(uptime_secs), hostid, machine_id,
        swap_pct, mem_used_mb, mem_free_mb, mem_total_mb
    ))

    # เก็บประวัติ (metrics_raw)
    cur.execute("""
        INSERT INTO metrics_raw
            (hostname, collected_utc, processed_utc,
             cpu_pct, mem_pct, swap_pct, disk_root_pct,
             mem_used_mb, mem_total_mb)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        hostname or "unknown",
        collected_utc,
        processed_utc,
        cpu,
        mem,
        swap_pct,
        disk,
        mem_used_mb,
        mem_total_mb
    ))
    conn.commit()

    # ingest ไฟล์เสริม (ทำแบบมี-ค่อย-เขียน)
    mounts_txt    = read_file_text(work_dir, "mounts.txt")
    top_cpu_txt   = read_file_text(work_dir, "top_cpu.txt")
    top_mem_txt   = read_file_text(work_dir, "top_mem.txt")
    lvm_vgs_txt   = read_file_text(work_dir, "lvm_vgs.txt")
    lvm_lvs_txt   = read_file_text(work_dir, "lvm_lvs.txt")
    lvm_pvs_txt   = read_file_text(work_dir, "lvm_pvs.txt")
    services_txt  = read_file_text(work_dir, "services.txt")
    disks_txt     = read_file_text(work_dir, "disks.txt")
    ntp_txt       = read_file_text(work_dir, "ntp_status.txt")
    system_hw_txt   = read_file_text(work_dir, "system_hw.txt")
    cpu_detail_txt  = read_file_text(work_dir, "cpu_detail.txt")
    net_extra_txt   = read_file_text(work_dir, "network_extra.txt")
    repo_status_txt = read_file_text(work_dir, "repo_status.txt")

    if mounts_txt:
        ingest_mounts(conn, hostname, collected_utc, mounts_txt)
    if top_cpu_txt or top_mem_txt:
        ingest_top_procs(conn, hostname, collected_utc, top_cpu_txt, top_mem_txt)
    if lvm_vgs_txt or lvm_lvs_txt or lvm_pvs_txt:
        ingest_lvm(conn, hostname, collected_utc, lvm_vgs_txt, lvm_lvs_txt, lvm_pvs_txt)
    if services_txt:
        ingest_services(conn, hostname, collected_utc, services_txt)
    if disks_txt:
        ingest_disks(conn, hostname, collected_utc, disks_txt)
    if ntp_txt:
        ingest_ntp(conn, hostname, collected_utc, ntp_txt)
    if system_hw_txt:
        ingest_system_hw(conn, hostname, collected_utc, system_hw_txt)
    if cpu_detail_txt:
        ingest_cpu_detail(conn, hostname, collected_utc, cpu_detail_txt)
    if net_extra_txt:
        ingest_network_extra(conn, hostname, collected_utc, net_extra_txt)
    if repo_status_txt:
        ingest_repo_status(conn, hostname, collected_utc, repo_status_txt)

    conn.close()

    # ย้ายไฟล์สำเร็จ
    shutil.move(tar_path, os.path.join(DONE_DIR, base_name))
    shutil.rmtree(work_dir, ignore_errors=True)
    return "OK"


# ---------------------------------------------------------------------
# main loop
# ---------------------------------------------------------------------
def main():
    ensure_schema()
    files = [f for f in os.listdir(UPLOAD_DIR) if f.endswith(".tar.gz")]
    files.sort()
    for f in files:
        src = os.path.join(UPLOAD_DIR, f)
        # ย้ายเข้าคิวก่อน
        dst = os.path.join(PROC_DIR, f)
        try:
            shutil.move(src, dst)
        except Exception:
            # อาจมีโปรเซสอื่นมาอ่านพร้อมกัน ให้ข้ามไป
            continue
        res = process_one(dst)
        if not res.startswith("OK"):
            try:
                shutil.move(dst, os.path.join(FAILED_DIR, f))
            except Exception:
                pass

if __name__ == "__main__":
    main()



