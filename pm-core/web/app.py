#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# /opt/pm-core/web/app.py

import os, re, sqlite3, hashlib, subprocess, mimetypes, base64, json
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, request, redirect, url_for, session, jsonify, send_file, abort

# ----------------------------- CONFIG -----------------------------
REPORT_DB_PATH = "/opt/pm-core/data/pm_reports.db"
USER_DB_PATH   = "/opt/pm-core/web/pm_users.db"
EXPORT_SCRIPT  = "/opt/pm-core/export_pdf.py"
REPORT_DIR     = "/opt/pm-core/reports"

AGE_OFFLINE_MIN = 360  # 6 ชั่วโมง

CPU_WARN, CPU_DEGRADE  = 80, 90
MEM_WARN, MEM_DEGRADE  = 80, 90
DISK_WARN_SUMMARY, DISK_DEGRADE_SUMMARY = 80, 90

SKIP_FS_TABLE = {"proc", "sysfs", "cgroup"}
MNT_WARN = 80.0
MNT_CRIT = 90.0
MNT_FULL = 99.5

app = Flask(
    __name__,
    template_folder="/opt/pm-core/web/templates",
    static_folder="/opt/pm-core/web/static",
)
app.secret_key = "pm-core-secret-please-change"
def _b64_to_text(s):
    if not s:
        return ""
    try:
        return base64.b64decode(s).decode("utf-8", errors="ignore")
    except Exception:
        return ""

def _parse_repolist_text(txt):
    """
    Return list of repos from dnf/yum repolist output.
    Output: [{"id": "...", "name": "...", "status_col": "..."}]
    """
    out = []
    for line in (txt or "").splitlines():
        l = line.rstrip()
        if not l.strip():
            continue
        low = l.lower().strip()

        # noise / headers
        if low.startswith("not root, subscription management"):
            continue
        if low.startswith("repo id"):
            continue
        if low.startswith("loaded plugins:"):
            continue
        if low.startswith("loading mirror speeds"):
            continue
        if low.startswith("repolist:"):
            continue

        # split by 2+ spaces (works for both dnf and yum)
        parts = re.split(r"\s{2,}", l.strip())
        if len(parts) >= 2:
            rid = parts[0].strip()
            rname = parts[1].strip()
            status_col = parts[2].strip() if len(parts) >= 3 else ""
            if rid and rname:
                out.append({"id": rid, "name": rname, "status_col": status_col})
    return out

def _parse_reposrc_text(txt):
    """
    reposrc format (รองรับหลายแบบ):
      1) [repoid|baseurl|enabled|type]         (ถ้ามี)
      2) [repoid|src|enabled|type]             (ของเดิมบางเวอร์ชัน)
      3) repoid|baseurl|enabled|type            (ไม่มี [])
    return dict: repoid -> {"baseurl": str, "enabled": int|None, "src": str, "is_local": bool}
    """
    m = {}
    for raw in (txt or "").splitlines():
        raw = raw.strip()
        if not raw:
            continue

        # ตัด [] ถ้ามี
        if raw.startswith("[") and raw.endswith("]"):
            raw = raw[1:-1].strip()

        parts = [p.strip() for p in raw.split("|")]
        if len(parts) < 2:
            continue

        rid = (parts[0] or "").strip()
        if not rid:
            continue

        # normalize repoid: ตัด [ ] ที่ค้างอยู่/หลุดรูปแบบ
        rid = rid.strip().lstrip("[").rstrip("]")

        p1 = parts[1] if len(parts) > 1 else ""
        enabled = None
        if len(parts) > 2 and parts[2] != "":
            try:
                enabled = int(parts[2])
            except Exception:
                enabled = None
        rtype = parts[3] if len(parts) > 3 else ""

        # baseurl heuristic:
        # - ถ้า field 2 เป็น url/file:// ให้ถือเป็น baseurl
        # - ถ้าไม่ใช่ ให้ baseurl ว่าง แต่ยังเก็บ src/rtype ไว้
        baseurl = p1 if ("://" in p1 or p1.startswith("file://")) else ""

        m[rid] = {
            "baseurl": baseurl,
            "enabled": enabled,
            "src": rtype or p1,   # เก็บไว้เผื่อ debug
            "is_local": baseurl.startswith("file://")
        }
    return m

def _norm_repoid(rid: str) -> str:
    rid = (rid or "").strip()
    rid = rid.lstrip("[").rstrip("]")
    return rid

def _repo_health_badge(health):
    # return (icon, label)
    h = (health or "").lower()
    if h == "ok":
        return ("🟢", "OK")
    if h == "degraded":
        return ("🟠", "Degraded")
    if h == "offline":
        return ("🔴", "Offline")
    return ("⚪", "Unknown")

def get_report_conn():
    c = sqlite3.connect(REPORT_DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def get_user_conn():
    c = sqlite3.connect(USER_DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def safe_get(row, key, default=None):
    if row is None: return default
    try: return row[key]
    except Exception: return default

def parse_any_ts(s):
    if not s: return None
    s = str(s).strip()
    for f in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
        try: return datetime.strptime(s, f).replace(tzinfo=timezone.utc)
        except: pass
    return None

def to_th_time(s):
    dt = parse_any_ts(s)
    if not dt: return "-" if not s else str(s)
    return dt.astimezone(timezone(timedelta(hours=7))).strftime("%Y-%m-%d %H:%M:%S")

def age_minutes(s):
    dt = parse_any_ts(s)
    if not dt: return None
    return round((datetime.now(timezone.utc) - dt).total_seconds()/60.0, 2)

def age_hours_num(s):
    m = age_minutes(s)
    if m is None: return None
    return round(m/60.0, 2)

def human_age_str(ts_str):
    dt = parse_any_ts(ts_str)
    if not dt:
        return "-"
    delta = datetime.now(timezone.utc) - dt
    mins = int(delta.total_seconds() // 60)
    if mins < 60:
        return f"{mins} mins"
    hours = mins // 60
    mins  = mins % 60
    if hours < 24:
        return f"{hours} hours, {mins} mins"
    days  = hours // 24
    hours = hours % 24
    return f"{days} days, {hours} hours"

def login_required(view):
    def wrapper(*a, **kw):
        if not session.get("user"):
            return redirect(url_for("login"))
        return view(*a, **kw)
    wrapper.__name__ = view.__name__
    return wrapper

def check_user(username, password):
    # แปลงรหัสผ่านที่รับมาจากฟอร์มให้เป็น SHA-256 Hash
    pw_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()
    
    conn = get_user_conn()
    try:
        # ตรวจสอบโดยใช้คอลัมน์ password_hash เพียงอย่างเดียวตาม Schema จริง
        r = conn.execute(
            "SELECT 1 FROM users WHERE username=? AND password_hash=?", 
            (username, pw_hash)
        ).fetchone()
        
        # ส่งค่ากลับเป็น True หากพบข้อมูล หรือ False หากไม่พบ (รหัสผิด)
        return bool(r)
    finally:
        conn.close()


def _to_num(v):
    try: return int(round(float(v)))
    except: return 0

def score_from_resources(cpu, mem, disk):
    lvl = 0
    try: c = float(cpu or 0)
    except: c = 0.0
    if   c >= CPU_DEGRADE: lvl = max(lvl, 2)
    elif c >= CPU_WARN:    lvl = max(lvl, 1)

    try: m = float(mem or 0)
    except: m = 0.0
    if   m >= MEM_DEGRADE: lvl = max(lvl, 2)
    elif m >= MEM_WARN:    lvl = max(lvl, 1)

    try: d = float(disk or 0)
    except: d = 0.0
    if   d >= DISK_DEGRADE_SUMMARY: lvl = max(lvl, 2)
    elif d >= DISK_WARN_SUMMARY:    lvl = max(lvl, 1)
    return lvl

def level_to_status(lvl):
    return ["up","warning","degraded","offline"][max(0, min(3, int(lvl)))]

def filter_ip_list(raw):
    if not raw: return []
    toks = re.split(r"[,\s]+", str(raw))
    out, seen = [], set()
    for t in toks:
        if not t: continue
        base = t.split("/")[0]
        octs = base.split(".")
        if len(octs) != 4: continue
        try: o = list(map(int, octs))
        except: continue
        if o[0]==127: continue
        if o[0]==169 and o[1]==254: continue
        if o[3] in (0,255): continue
        if t in seen: continue
        seen.add(t); out.append(t)
    return out

def classify_mount(fs, used_pct):
    fs = (fs or "").lower()
    try:
        u = float(re.sub(r"[^0-9.]+","", str(used_pct) if used_pct is not None else "0"))
    except:
        u = 0.0

    if u >= MNT_FULL:       status = "full"
    elif u >= MNT_CRIT:     status = "critical"
    elif u >= MNT_WARN:     status = "warning"
    else:                   status = "ok"

    note = None
    if fs == "iso9660":
        if u < MNT_FULL:
            u = 100.0
            status = "full"
        note = "ISO image"

    if fs in {"nfs","nfs4","cifs","smbfs","fuse.sshfs"}:
        note = f"{(note + ' / ') if note else ''}remote"

    return status, u, note

def fetch_latest_hosts_from_reports():
    conn = get_report_conn()
    rows = conn.execute("""
        SELECT r.rowid AS id, r.hostname,
               COALESCE(r.timestamp, r.collected_utc, r.processed_utc) AS client_ts,
               r.processed_utc AS processed_ts,
               r.cpu AS cpu_usage, r.mem AS mem_usage, r.disk AS disk_usage,
               r.uptime AS uptime_str, r.uptime_secs
        FROM reports r
        JOIN (
           SELECT hostname, MAX(COALESCE(timestamp,collected_utc,processed_utc)) AS max_ts
           FROM reports WHERE hostname IS NOT NULL GROUP BY hostname
        ) x ON x.hostname=r.hostname AND x.max_ts=COALESCE(r.timestamp,r.collected_utc,r.processed_utc)
        ORDER BY r.hostname
    """).fetchall()

    hosts = []; total=up=warning=degraded=offline=0
    for r in rows:
        hn   = safe_get(r, "hostname", "-")
        cts  = safe_get(r, "client_ts", None)
        cpu  = safe_get(r, "cpu_usage", 0) or 0
        mem  = safe_get(r, "mem_usage", 0) or 0
        disk = safe_get(r, "disk_usage", 0) or 0

        a_min = age_minutes(cts)
        a_hr  = age_hours_num(cts)
        lvl_age = 0 if a_min is None else (3 if a_min > AGE_OFFLINE_MIN else 0)

        lvl_res = score_from_resources(cpu, mem, disk)
        status  = level_to_status(max(lvl_age, lvl_res))

        total += 1
        if   status=="up":       up+=1
        elif status=="warning":  warning+=1
        elif status=="degraded": degraded+=1
        else:                    offline+=1

        hosts.append({
            "id": safe_get(r,"id"),
            "hostname": hn, "display_name": hn,
            "status": status,
            "cpu_usage": cpu, "mem_usage": mem, "disk_usage": disk,
            "uptime": safe_get(r,"uptime_str","-"),
            "collected_th": to_th_time(cts),
            "age_hours": a_hr if a_hr is not None else 0.0,
        })

    counts = {"total":total,"up":up,"warning":warning,"degraded":degraded,"offline":offline}
    last_refresh_th = to_th_time(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    conn.close()
    return hosts, counts, last_refresh_th

def fetch_host_detail_from_reports(hostname):
    conn = get_report_conn()
    row = conn.execute("""
        SELECT r.rowid AS id, r.hostname, r.hostid,
               COALESCE(r.timestamp, r.collected_utc, r.processed_utc) AS client_ts,
               r.processed_utc AS processed_ts,
               r.cpu AS cpu_usage, r.mem AS mem_usage, r.disk AS disk_usage,
               r.uptime AS uptime_str, r.uptime_secs,
               r.kernel, r.os_name, r.os_version,
               r.ip_address, r.ip_all, r.ip_address_all, r.machine_id
        FROM reports r
        WHERE r.hostname=? ORDER BY COALESCE(r.timestamp,r.collected_utc,r.processed_utc) DESC LIMIT 1
    """,(hostname,)).fetchone()

    if not row:
        alt = hostname[:-6] if hostname.endswith(".local") else (hostname + ".local")
        row = conn.execute("""
            SELECT r.rowid AS id, r.hostname, r.hostid,
                   COALESCE(r.timestamp, r.collected_utc, r.processed_utc) AS client_ts,
                   r.processed_utc AS processed_ts,
                   r.cpu AS cpu_usage, r.mem AS mem_usage, r.disk AS disk_usage,
                   r.uptime AS uptime_str, r.uptime_secs,
                   r.kernel, r.os_name, r.os_version,
                   r.ip_address, r.ip_all, r.ip_address_all, r.machine_id
            FROM reports r
            WHERE r.hostname=? ORDER BY COALESCE(r.timestamp,r.collected_utc,r.processed_utc) DESC LIMIT 1
        """,(alt,)).fetchone()

    if not row:
        conn.close(); return None, [], ""

    hn   = safe_get(row,"hostname","-")
    cts  = safe_get(row,"client_ts",None)
    pts  = safe_get(row,"processed_ts",None)
    cpu  = safe_get(row,"cpu_usage",0) or 0
    mem  = safe_get(row,"mem_usage",0) or 0
    disk = safe_get(row,"disk_usage",0) or 0

    mounts=[]
    try:
        mrows = conn.execute("""
            SELECT mountpoint, used_pct, fstype, size, used, avail
            FROM host_mounts
            WHERE hostname=? AND collected_utc=? ORDER BY mountpoint
        """,(hn,cts)).fetchall()
        for m in mrows:
            mp = safe_get(m,"mountpoint","/")
            fs = (safe_get(m,"fstype","") or "").lower()
            if fs in SKIP_FS_TABLE:
                continue
            try:
                upct_raw = safe_get(m,"used_pct",0.0)
                upct = float(re.sub(r"[^0-9.]+","",str(upct_raw))) if upct_raw is not None else 0.0
            except:
                upct = 0.0
            status_mnt, upct_norm, note = classify_mount(fs, upct)
            mounts.append({
                "mountpoint": mp, "fstype": fs, "used_pct": upct_norm,
                "used_pct_str": f"{upct_norm:.0f}%",
                "status": status_mnt, "note": note,
                "size": safe_get(m,"size",None), "used": safe_get(m,"used",None), "avail": safe_get(m,"avail",None),
            })
    except sqlite3.OperationalError:
        mounts=[]

    a_min = age_minutes(cts)
    lvl_age = 3 if (a_min is not None and a_min > AGE_OFFLINE_MIN) else 0
    lvl_res = score_from_resources(cpu, mem, disk)
    status  = level_to_status(max(lvl_age, lvl_res))

    host = {
        "hostname": hn, "status": status,
        "hostid": safe_get(row, "hostid", "-"),
        "client_ts": cts, "processed_ts": pts,
        "client_th": to_th_time(cts), "processed_th": to_th_time(pts),
        "collected_th": to_th_time(cts), "last_collected_th": to_th_time(cts),
        "uptime": safe_get(row,"uptime_str","-"),
        "kernel": safe_get(row,"kernel","-"),
        "os_name": safe_get(row,"os_name","-"),
        "os_version": safe_get(row,"os_version","-"),
        "os_display": f"{safe_get(row,'os_name','-')} {safe_get(row,'os_version','')}".strip(),
        "machine_id": safe_get(row,"machine_id","-"),
        "cpu": cpu, "mem": mem, "disk": disk,
        "cpu_usage": cpu, "mem_usage": mem, "disk_usage": disk,
        "ip_list": filter_ip_list(
            safe_get(row,"ip_all") or safe_get(row,"ip_address_all") or safe_get(row,"ip_address") or ""
        ),
    }

    conn.close(); return host, mounts, ""

@app.route("/")
def root():
    if not session.get("user"): return redirect(url_for("login"))
    return redirect(url_for("dashboard"))

@app.route("/login", methods=["GET", "POST"])
def login():
    # --- ส่วนที่เพิ่มใหม่: โหลด Config ---
    config_path = "/opt/pm-core/pm_report_config.json"
    config_data = {}
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config_data = json.load(f)
    except Exception as e:
        print(f"Config Load Error: {e}")
    # ----------------------------------

    if request.method == "POST":
        u = request.form.get("username", "")
        p = request.form.get("password", "")
        if check_user(u, p):
            session["user"] = u
            # ดึงบทบาทมาเก็บไว้ใน session ด้วย (ถ้าต้องการใช้เช็คสิทธิ์ในหน้าอื่น)
            conn = get_user_conn()
            r = conn.execute("SELECT role FROM users WHERE username=?", (u,)).fetchone()
            session["role"] = r[0] if r else "user"
            conn.close()
            return redirect(url_for("dashboard"))
        else:
            # ส่ง config_data ไปด้วยแม้ล็อกอินผิด
            return render_template("login.html", error="Invalid credentials", config=config_data)
            
    return render_template("login.html", config=config_data) # ส่ง config_data ไปที่ Template

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html")

@app.route("/api/hosts")
@login_required
def api_hosts():
    hosts, counts, last_refresh_th = fetch_latest_hosts_from_reports()
    return jsonify({
        "ok": True,
        "data": hosts, "hosts": hosts,
        "counts": counts,
        "last_refresh_th": last_refresh_th,
        "cycle": "-"
    })

@app.route("/api/metrics/<hostname>")
@login_required
def api_metrics(hostname):
    days = request.args.get("range","30")
    try: D = int(days)
    except: D = 30
    D = max(1, min(365, D))
    since = (datetime.utcnow()-timedelta(days=D)).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_report_conn()
    rows = conn.execute("""
        SELECT COALESCE(timestamp,collected_utc,processed_utc) AS ts, cpu, mem, disk
        FROM reports
        WHERE hostname=? AND COALESCE(timestamp,collected_utc,processed_utc) >= ?
        ORDER BY COALESCE(timestamp,collected_utc,processed_utc)
    """,(hostname, since)).fetchall()
    conn.close()
    data = [{"ts":safe_get(r,"ts",""),
             "cpu":_to_num(safe_get(r,"cpu",0) or 0),
             "mem":_to_num(safe_get(r,"mem",0) or 0),
             "disk":_to_num(safe_get(r,"disk",0) or 0)} for r in rows]
    return jsonify({"ok":True,"data":data})
    
# ======================= NEW API FOR METRICS RANGE =======================

@app.route("/api/metrics_data/<hostname>")
@login_required
def api_metrics_data(hostname):
    """
    Return metrics (CPU, Memory, Disk) for a specific host
    within a custom datetime range, used for Resource Trends chart.
    Expected query parameters: start, end (format: YYYY-MM-DDTHH:MM)

    NOTE:
    - ใช้ตาราง reports โดยตรง (เหมือน /api/metrics และ detail())
    - ใช้ COALESCE(timestamp,collected_utc,processed_utc) ตาม baseline
    """
    start = request.args.get("start")
    end   = request.args.get("end")

    if not start or not end:
        return jsonify({"error": "Missing 'start' or 'end' parameters"}), 400

    try:
        # start/end จาก frontend ใช้รูปแบบ YYYY-MM-DDTHH:MM
        dt_start = datetime.strptime(start, "%Y-%m-%dT%H:%M")
        dt_end   = datetime.strptime(end,   "%Y-%m-%dT%H:%M")
    except ValueError:
        return jsonify({"error": "Datetime format must be YYYY-MM-DDTHH:MM"}), 400

    if dt_end <= dt_start:
        return jsonify({"error": "End must be after start"}), 400

    # แปลงจากเวลา Local (Asia/Bangkok, UTC+7) → UTC ก่อน แล้วค่อย format เป็น string
    # สมมติว่าค่า start/end ที่มาจาก frontend เป็นเวลาไทย (TH) ตามที่ UI แสดง
    local_tz = timezone(timedelta(hours=7))

    # ผูก timezone ให้ dt_start/dt_end เป็นเวลาไทย (ไม่มีการเลื่อนเวลา ณ จุดนี้)
    dt_start_local = dt_start.replace(tzinfo=local_tz)
    dt_end_local   = dt_end.replace(tzinfo=local_tz)

    # แปลงไปเป็นเวลา UTC เพื่อให้ตรงกับรูปแบบเวลาที่เก็บใน DB (collected_utc/processed_utc เป็น UTC)
    dt_start_utc = dt_start_local.astimezone(timezone.utc)
    dt_end_utc   = dt_end_local.astimezone(timezone.utc)

    # สุดท้าย format เป็น string ที่ใช้กับ DB
    start_str = dt_start_utc.strftime("%Y-%m-%d %H:%M:%S")
    end_str   = dt_end_utc.strftime("%Y-%m-%d %H:%M:%S")


    conn = get_report_conn()
    try:
        rows = conn.execute("""
            SELECT COALESCE(timestamp,collected_utc,processed_utc) AS ts,
                   cpu, mem, disk
            FROM reports
            WHERE hostname = ?
              AND COALESCE(timestamp,collected_utc,processed_utc)
                    BETWEEN ? AND ?
            ORDER BY COALESCE(timestamp,collected_utc,processed_utc)
        """, (hostname, start_str, end_str)).fetchall()
    finally:
        conn.close()

    if not rows:
        return jsonify({
            "hostname": hostname,
            "range_start": start,
            "range_end": end,
            "metrics": []
        }), 200

    metrics = []
    for r in rows:
        ts = safe_get(r, "ts", "")
        metrics.append({
            "timestamp": ts,
            "cpu":  _to_num(safe_get(r, "cpu",  0) or 0),
            "mem":  _to_num(safe_get(r, "mem",  0) or 0),
            "disk": _to_num(safe_get(r, "disk", 0) or 0),
        })

    return jsonify({
        "hostname": hostname,
        "range_start": start,
        "range_end": end,
        "metrics": metrics
    }), 200

@app.route("/detail/<hostname>")
@login_required
def detail(hostname):
    def _pct_to_num(x):
        try:
            s = (str(x) or "").strip().replace("%", "")
            return float(s) if s else 0.0
        except Exception:
            return 0.0

    # ----- ดึง host + mounts ล่าสุด -----
    host, mounts, note_text = fetch_host_detail_from_reports(hostname)
    if not host:
        return "Host not found", 404

    try:
        ip_display = (host.get("ip_list") and ", ".join(host.get("ip_list"))) or host.get("ip_address") or "-"
    except Exception:
        ip_display = "-"

    age_str = human_age_str(host.get("client_ts") or host.get("processed_ts"))

    cpu  = float(host.get("cpu_usage", host.get("cpu", 0)) or 0)
    mem  = float(host.get("mem_usage", host.get("mem", 0)) or 0)
    disk = float(host.get("disk_usage", host.get("disk", 0)) or 0)

    # ----- เลือก label ของ Disk -----
    disk_label = "Disk Usage"
    root_rows = [m for m in mounts if (m.get("mountpoint") == "/" or m.get("mount_point") == "/")]
    if disk <= 0.0:
        if root_rows:
            disk = float(root_rows[0].get("used_pct", 0) or 0)
            disk_label = "Disk Usage (/)"
        else:
            mx = max([_pct_to_num(m.get("used_pct", 0)) for m in mounts], default=0.0)
            disk = mx
            disk_label = "Max Disk Usage (any mount)"
    else:
        if root_rows and abs(disk - float(root_rows[0].get("used_pct", 0) or 0)) < 0.5:
            disk_label = "Disk Usage (/)"
        else:
            disk_label = "Max Disk Usage (any mount)"

    # ----- เตรียมข้อมูล mounts + severity -----
    for m in mounts:
        up = _pct_to_num(m.get("used_pct", 0))
        if up >= 100.0:
            sev = "bad2"
        elif up >= 90.0:
            sev = "bad"
        elif up >= 80.0:
            sev = "warn"
        else:
            sev = "ok"
        m["used_pct_num"] = up
        m["sev"] = sev
        if not m.get("mountpoint"): m["mountpoint"] = m.get("mount_point")
        if not m.get("fstype"):     m["fstype"]     = m.get("fs_type")
        if "used_pct_str" not in m: m["used_pct_str"] = f"{up:.0f}%"

    # sort ตาม %used มาก → น้อย
    mounts_full = sorted(
        mounts,
        key=lambda x: x.get("used_pct_num", _pct_to_num(x.get("used_pct", 0))),
        reverse=True
    )
    mounts_top5 = mounts_full[:5]

    # ----- item สำหรับฝั่ง template / legacy -----
    item = {
        "hostname":       host.get("hostname", hostname),
        "hostid":         host.get("hostid", "-"),
        "status":         host.get("status", "Unknown"),
        "processed_utc":  host.get("processed_ts", "-"),
        "collected_utc":  host.get("client_ts",    "-"),
        "kernel":         host.get("kernel", "-"),
        "os_display":     host.get("os_display", host.get("os_name","-")),
        "os_version":     host.get("os_version", "-"),
        "uptime_human":   host.get("uptime", "-"),
        "ip_display":     ip_display,
        "machine_id":     host.get("machine_id", "-"),
        "cpu_usage":      cpu,
        "mem_usage":      mem,
        "disk_usage":     disk,
        "age":            age_str,
    }

    # head_ctx ให้ตรงกับสิ่งที่ detail.html ใช้
    head_ctx = {
        "hostname":   item["hostname"],
        "hostid":     item.get("hostid", "-"),
        "status":     item["status"],
        "processed_th": host.get("processed_th") or host.get("client_th") or "-",
        "collected_th": host.get("collected_th") or host.get("client_th") or "-",
        "kernel":     host.get("kernel", "-"),
        "os":         host.get("os_display") or host.get("os_name") or "-",
        "os_name":    host.get("os_display") or host.get("os_name") or "-",
        "uptime":     host.get("uptime", "-"),
        "machine_id": host.get("machine_id", "-"),
        "ip_addrs":   host.get("ip_list") or [],
        "cpu_pct":    cpu,
        "mem_pct":    mem,
        "disk_pct":   disk,
        "age_text":   age_str,
    }

    latest = {
        "hostname": item["hostname"],
        "status":   item["status"],
        "uptime":   item["uptime_human"],
        "cpu":        item["cpu_usage"],
        "mem":        item["mem_usage"],
        "disk":       item["disk_usage"],
        "cpu_usage":  item["cpu_usage"],
        "mem_usage":  item["mem_usage"],
        "disk_usage": item["disk_usage"],
        "cpuPercent": item["cpu_usage"],
        "memPercent": item["mem_usage"],
        "diskPercent":item["disk_usage"],
    }

    # ----- เตรียมข้อมูล Row3: System / CPU / Mem / Storage / Network / Time / Repo / Top Procs -----
    # ใช้ timestamp เดียวกับ snapshot ล่าสุดของ host (client_ts) เพื่อผูกกับ host_* tables
    hn  = host.get("hostname")
    cts = host.get("client_ts")

    system_hw     = None
    cpu_detail    = None
    network_extra = None
    ntp_status    = None
    repo_status   = None
    disks         = []
    lvm_vgs       = []
    lvm_lvs       = []
    lvm_pvs       = []
    top_cpu       = []
    top_mem       = []
    mem_info      = None

    if hn and cts:
        conn_row3 = get_report_conn()
        try:
            # ----- System / Virtualization / HW (ถ้ามี) -----
            try:
                system_hw = conn_row3.execute("""
                    SELECT *
                    FROM host_system_hw
                    WHERE hostname=? AND collected_utc=?
                    ORDER BY rowid DESC
                    LIMIT 1
                """, (hn, cts)).fetchone()
            except Exception:
                system_hw = None  # กันกรณีตารางนี้ยังไม่มีในบางระบบ

            # ----- CPU Detail (model / arch / core/thread) -----
            try:
                cpu_detail = conn_row3.execute("""
                    SELECT *
                    FROM host_cpu_detail
                    WHERE hostname=? AND collected_utc=?
                    ORDER BY rowid DESC
                    LIMIT 1
                """, (hn, cts)).fetchone()
            except Exception:
                cpu_detail = None

            # ----- Network Extra (GW / DNS) -----
            try:
                network_extra = conn_row3.execute("""
                    SELECT *
                    FROM host_network_extra
                    WHERE hostname=? AND collected_utc=?
                    ORDER BY rowid DESC
                    LIMIT 1
                """, (hn, cts)).fetchone()
            except Exception:
                network_extra = None

            # ----- Time Sync (NTP / Chrony / อื่น ๆ) -----
            try:
                ntp_status = conn_row3.execute("""
                    SELECT *
                    FROM host_ntp_status
                    WHERE hostname=? AND collected_utc=?
                    ORDER BY rowid DESC
                    LIMIT 1
                """, (hn, cts)).fetchone()
            except Exception:
                ntp_status = None

            # ----- Repository Status -----
            try:
                repo_status = conn_row3.execute("""
                    SELECT *
                    FROM host_repo_status
                    WHERE hostname=? AND collected_utc=?
                    ORDER BY rowid DESC
                    LIMIT 1
                """, (hn, cts)).fetchone()
            except Exception:
                repo_status = None

            # ----- Repository Status (decoded view) -----
            repo_items = []
            repo_health_icon = ""
            repo_health_label = ""
            repo_health_note = "from makecache/refresh"

            def _clean_repo_id(rid: str) -> str:
                rid = (rid or "").strip()
                if not rid:
                    return ""
                # กันเคส [BaseOS] หรือ [centos79]
                if rid.startswith("[") and rid.endswith("]") and len(rid) >= 3:
                    rid = rid[1:-1].strip()
                return rid

            def _repo_type_from_baseurl(baseurl: str) -> str:
                u = (baseurl or "").strip().lower()
                if u.startswith("file://"):
                    return "LOCAL"
                if u.startswith("http://") or u.startswith("https://"):
                    return "NET"
                return ""
            def _pick_repo_src(rs: dict) -> str:
                """
                prefer baseurl > metalink > mirrorlist
                """
                if not isinstance(rs, dict):
                    return ""
                for k in ("baseurl", "metalink", "mirrorlist", "src"):
                    v = (rs.get(k) or "").strip()
                    if v:
                        return v
                return ""



            if repo_status:
                repolist_txt = _b64_to_text(repo_status["repolist_b64"]) if "repolist_b64" in repo_status.keys() else ""
                reposrc_txt  = _b64_to_text(repo_status["reposrc_b64"])  if "reposrc_b64"  in repo_status.keys() else ""

                repolist_raw = _parse_repolist_text(repolist_txt)
                reposrc_raw  = _parse_reposrc_text(reposrc_txt)

                # --- normalize repolist -> dict[rid] = (name, status_txt)
                repolist_map = {}
                if isinstance(repolist_raw, dict):
                    repolist_map = repolist_raw
                elif isinstance(repolist_raw, list):
                    for r in repolist_raw:
                        if isinstance(r, dict) and "id" in r:
                            rid = _clean_repo_id(r.get("id"))
                            if rid:
                                repolist_map[rid] = (r.get("name",""), r.get("status",""))

                # --- normalize reposrc -> dict[rid] = (baseurl, src)
                reposrc_map = {}
                if isinstance(reposrc_raw, dict):
                    reposrc_map = reposrc_raw
                elif isinstance(reposrc_raw, list):
                    for r in reposrc_raw:
                        if isinstance(r, dict) and "id" in r:
                            rid = _clean_repo_id(r.get("id"))
                            if rid:
                                reposrc_map[rid] = (r.get("baseurl",""), r.get("src",""))

                # merge เป็น list สำหรับ template (normalize repoid)
                health = (repo_status["repo_health"] if "repo_health" in repo_status.keys() else "") or ""
                reason = (repo_status["repo_health_reason"] if "repo_health_reason" in repo_status.keys() else "") or ""
                
                ids = set(_norm_repoid(k) for k in repolist_map.keys()) | set(_norm_repoid(k) for k in reposrc_map.keys())
                
                for rid in sorted(ids):
                    # repolist: (name, status_txt) หรือ dict แล้วแต่ parser
                    name, status_txt = ("", "")
                    if rid in repolist_map:
                        v = repolist_map.get(rid)
                    else:
                        # เผื่อ repolist_map เก็บ key แบบมี []
                        v = repolist_map.get(f"[{rid}]")
                    if isinstance(v, (tuple, list)) and len(v) >= 2:
                        name, status_txt = v[0], v[1]
                    elif isinstance(v, dict):
                        name = v.get("name", "")
                        status_txt = v.get("status", "")
                
                    # reposrc: dict
                    rs = reposrc_map.get(rid) or reposrc_map.get(f"[{rid}]") or {}
                    
                    # pick URL for display: baseurl > metalink > mirrorlist
                    src_url = _pick_repo_src(rs)
                    
                    # enabled: reposrc_raw เราเก็บเฉพาะ enabled=1 อยู่แล้ว (จาก pm_collect.sh)
                    # ถ้า parser ไม่ได้ส่ง enabled มา ให้ default เป็น 1 เมื่อมี rs
                    enabled = rs.get("enabled", 1) if isinstance(rs, dict) and rs else None
                    
                    # local repo if file://
                    is_local = bool((src_url or "").startswith("file://"))


                    # per-repo status (for display)
                    net_status = ""
                    status_icon = ""
                    if is_local:
                        status_icon = "🟢"
                    else:
                        if health.lower() == "ok":
                            net_status  = "ONLINE"
                            status_icon = "🟢"
                        else:
                            net_status  = "OFFLINE"
                            status_icon = "🔴"
                
                    repo_items.append({
                        "id": rid,
                        "name": name or "-",
                        "status": status_txt or "",
                        "baseurl": src_url or "",
                        "enabled": enabled,
                        "is_local": is_local,
                        "net_status": net_status,
                        "status_icon": status_icon,
                    })


                # --- Repo Health badge (ภาพรวมจาก makecache/refresh) ---
                
                if health.lower() == "ok":
                    repo_health_icon  = "🟢"
                    repo_health_label = "Repo cache refresh succeeded"
                elif health.lower() in ("offline", "fail"):
                    repo_health_icon  = "🔴"
                    if reason == "DNS_FAIL":
                        repo_health_label = "Repo refresh failed (DNS / Network)"
                    elif reason == "REPO_MD_FAIL":
                        repo_health_label = "Repo metadata unavailable"
                    else:
                        repo_health_label = "Repo refresh failed"
                else:
                    repo_health_icon  = "⚪"
                    repo_health_label = "Repo health unknown"

                
                # ----- Repo Breakdown (recalculate from repo_items) -----
                repo_breakdown = {
                    "online": 0,
                    "offline": 0,
                    "local": 0,
                }

                is_health_ok = (health.lower() == "ok")
                
                for r in repo_items:
                    if r.get("enabled") == 0:
                        continue
                
                    if r.get("is_local"):
                        repo_breakdown["local"] += 1
                    else:
                        if r.get("net_status") == "ONLINE":
                            repo_breakdown["online"] += 1
                        elif r.get("net_status") == "OFFLINE":
                            repo_breakdown["offline"] += 1



            # ----- Disks / Mounts / LVM -----
            try:
                disks = conn_row3.execute("""
                    SELECT hostname, collected_utc, name, dtype, size, mountpoint
                    FROM host_disks
                    WHERE hostname=? AND collected_utc=?
                    ORDER BY name
                """, (hn, cts)).fetchall()
            except Exception:
                disks = []

            try:
                lvm_vgs = conn_row3.execute("""
                    SELECT hostname, collected_utc, vg_name, lv_count, pv_count, vg_size, vg_free
                    FROM host_lvm_vgs
                    WHERE hostname=? AND collected_utc=?
                    ORDER BY vg_name
                """, (hn, cts)).fetchall()
            except Exception:
                lvm_vgs = []

            try:
                lvm_lvs = conn_row3.execute("""
                    SELECT hostname, collected_utc, lv_name, vg_name, lv_size
                    FROM host_lvm_lvs
                    WHERE hostname=? AND collected_utc=?
                    ORDER BY vg_name, lv_name
                """, (hn, cts)).fetchall()
            except Exception:
                lvm_lvs = []

            try:
                lvm_pvs = conn_row3.execute("""
                    SELECT hostname, collected_utc, pv_name, vg_name, pv_size, pv_free
                    FROM host_lvm_pvs
                    WHERE hostname=? AND collected_utc=?
                    ORDER BY pv_name
                """, (hn, cts)).fetchall()
            except Exception:
                lvm_pvs = []

            # ----- Top Processes (CPU / MEM) -----
            try:
                top_cpu = conn_row3.execute("""
                    SELECT hostname, collected_utc, pid, cmd, cpu, mem
                    FROM host_top_cpu
                    WHERE hostname=? AND collected_utc=?
                    ORDER BY cpu DESC, pid
                """, (hn, cts)).fetchall()
            except Exception:
                top_cpu = []

            try:
                top_mem = conn_row3.execute("""
                    SELECT hostname, collected_utc, pid, cmd, cpu, mem
                    FROM host_top_mem
                    WHERE hostname=? AND collected_utc=?
                    ORDER BY mem DESC, pid
                """, (hn, cts)).fetchall()
            except Exception:
                top_mem = []

            # ----- Memory Detail (รวมจาก reports: total / used / free / swap_pct) -----
            try:
                mem_info = conn_row3.execute("""
                    SELECT mem_total_mb, mem_used_mb, mem_free_mb, swap_pct
                    FROM reports
                    WHERE hostname=? AND COALESCE(timestamp,collected_utc,processed_utc)=?
                    ORDER BY rowid DESC
                    LIMIT 1
                """, (hn, cts)).fetchone()
            except Exception:
                mem_info = None

        finally:
            conn_row3.close()

    # ----- ดึงกราฟ 24 ชั่วโมงล่าสุด -----
    conn = get_report_conn()
    since = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute("""
        SELECT COALESCE(timestamp,collected_utc,processed_utc) AS ts, cpu, mem, disk
        FROM reports
        WHERE hostname=? AND COALESCE(timestamp,collected_utc,processed_utc) >= ?
        ORDER BY COALESCE(timestamp,collected_utc,processed_utc)
    """, (item["hostname"], since)).fetchall()
    conn.close()

    labels, cpu_series, mem_series, disk_series = [], [], [], []
    for r in rows:
        ts = safe_get(r, "ts", "")
        dt = parse_any_ts(ts)
        if dt:
            dt = dt.astimezone(timezone(timedelta(hours=7)))
            label = dt.strftime("%Y-%m-%d %H:%M")
        else:
            label = ts
        labels.append(label)
        cpu_series.append(_to_num(safe_get(r, "cpu", 0) or 0))
        mem_series.append(_to_num(safe_get(r, "mem", 0) or 0))
        disk_series.append(_to_num(safe_get(r, "disk", 0) or 0))

    chart = {
        "labels": labels,
        "cpu": cpu_series,
        "mem": mem_series,
        "disk": disk_series,
    }

    return render_template(
        "detail.html",
        item=item,
        head=head_ctx,
        disk_label=disk_label,
        host=host,
        mounts=mounts_full,
        mounts_full=mounts_full,
        mounts_top5=mounts_top5,
        latest=latest,
        note_text=note_text,
        chart=chart,
        
        # ----- Row3 context -----
        system_hw=system_hw,
        cpu_detail=cpu_detail,
        mem_info=mem_info,
        network_extra=network_extra,
        ntp_status=ntp_status,
        repo_status=repo_status,
        repo_items=repo_items,
        repo_health_icon=repo_health_icon,
        repo_health_label=repo_health_label,
        repo_health_note=repo_health_note,
        repo_breakdown=repo_breakdown,
        disks=disks,
        lvm_vgs=lvm_vgs,
        lvm_lvs=lvm_lvs,
        lvm_pvs=lvm_pvs,
        top_cpu=top_cpu,
        top_mem=top_mem,        
    )

def _run_export(payload):
    rtype = (payload or {}).get("type","summary")
    fmt   = (payload or {}).get("fmt","pdf").lower()
    hosts = (payload or {}).get("hosts")

    if not os.path.exists(EXPORT_SCRIPT):
        return {"ok":False,"error":"export script not found", "path":"", "filename":""}

    try:
        cmd = ["python3", EXPORT_SCRIPT, "--type", rtype, "--fmt", fmt]
        if hosts:
            if isinstance(hosts, list): hosts = ",".join(hosts)
            cmd += ["--hosts", str(hosts)]
        subprocess.check_call(cmd)

        latest_path, latest_m = None, -1
        for f in os.listdir(REPORT_DIR):
            p = os.path.join(REPORT_DIR, f)
            if os.path.isfile(p):
                m = os.path.getmtime(p)
                if m > latest_m: latest_m, latest_path = m, p
        if not latest_path:
            return {"ok":False,"error":"no report generated", "path":"", "filename":""}

        return {"ok":True,"path":latest_path, "filename":os.path.basename(latest_path)}
    except subprocess.CalledProcessError as e:
        return {"ok":False,"error":f"export failed: {e}", "path":"", "filename":""}
    except Exception as e:
        return {"ok":False,"error":f"unexpected error: {e}", "path":"", "filename":""}

@app.route("/api/export_report", methods=["POST"])
@login_required
def api_export_report():
    return jsonify(_run_export(request.get_json(silent=True) or {}))

@app.route("/api/report", methods=["POST"])
@login_required
def api_report_compat():
    return jsonify(_run_export(request.get_json(silent=True) or {}))

@app.route("/download")
@login_required
def download():
    p = request.args.get("path","")
    if not p: abort(400)
    p = os.path.abspath(p)
    if not p.startswith(os.path.abspath(REPORT_DIR)): abort(403)
    if not os.path.exists(p) or not os.path.isfile(p): abort(404)
    mime = mimetypes.guess_type(p)[0] or "application/octet-stream"
    return send_file(p, as_attachment=True, download_name=os.path.basename(p), mimetype=mime)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)


