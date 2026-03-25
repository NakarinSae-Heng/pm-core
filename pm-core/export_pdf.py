#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PM-Core Report Exporter V9.4 (Smart Analyst Edition)
- Baseline: V9.3.3 (Layout & Core Logic preserved).
- Upgrade: "Smart Disk Analysis" - Replaced passive text with active, specific analysis.
    - Detects "Space Recovery" (Cleanup events).
    - Lists ALL Critical/Warning partitions specifically (e.g., "Critical on /opt, Warning on /var").
- Upgrade: "Specific Recommendations" - Recommendations now loop through all OS partitions to give specific advice.
- Layout: Preserved V9.3.3 Layout (Vertical Status Stack, Balanced Columns).
"""

import os
import sys
import sqlite3
import argparse
import re
import traceback
import base64
import json
import tempfile
from datetime import datetime, timedelta

# ----------------- CONFIG -----------------
ROOT_DIR = "/opt/pm-core"
DATA_DIR = os.path.join(ROOT_DIR, "data")
DB_PATH  = os.path.join(DATA_DIR, "pm_reports.db")
REPORT_DIR = os.path.join(ROOT_DIR, "reports")
CONFIG_PATH = os.path.join(ROOT_DIR, "pm_report_config.json") 
LOGO_PATH = os.path.join(ROOT_DIR, "web/static/logo.png")
os.makedirs(REPORT_DIR, exist_ok=True)

# OS PARTITION WHITELIST
OS_PARTITIONS = ['/', '/var', '/usr', '/tmp', '/opt', '/boot', '/home']

# DEFAULT CONFIG
DEFAULT_CONFIG = {
    "client_name": "Client Company Name",
    "provider_name": "Service Provider Co., Ltd.",
    "report_title": "PREVENTIVE MAINTENANCE REPORT",
    "contact": {
        "team_name": "Support Team",
        "email": "support@example.com",
        "tel": "02-xxx-xxxx"
    }
}

# REPORT TITLES
TYPE_MAP = {
    'summary': 'EXECUTIVE SUMMARY',
    'select': 'SELECTED HOSTS REPORT',
    'full': 'FULL TECHNICAL REPORT'
}

# THEME COLORS
THEME_COLOR = "#0F172A"
COLOR_UP = "#10B981"     # Green
COLOR_WARN = "#F59E0B"   # Orange
COLOR_DEG = "#EF4444"    # Red
COLOR_OFF = "#94A3B8"    # Grey

# ----------------- LIBRARY CHECK -----------------
try:
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT, TA_JUSTIFY
    from reportlab.pdfgen import canvas
    from reportlab.platypus import (BaseDocTemplate, PageTemplate, Frame, Table, TableStyle, Paragraph, 
                                    Spacer, PageBreak, Flowable, Image as RLImage, KeepTogether, NextPageTemplate)
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
except ImportError as e:
    sys.stderr.write(f"CRITICAL ERROR: Missing library. {e}\n")
    sys.exit(1)

# ----------------- UTILS -----------------
def load_config():
    paths = ["pm_report_config.json", CONFIG_PATH]
    for p in paths:
        if os.path.exists(p):
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
    return DEFAULT_CONFIG

def extract_real_ips(raw_text):
    if not raw_text: return "-"
    pattern = r"inet\s+(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"
    found_ips = re.findall(pattern, str(raw_text))
    real_ips = [ip for ip in found_ips if not ip.startswith("127.")]
    if real_ips:
        return "\n".join(sorted(list(set(real_ips))))
    elif found_ips:
        return found_ips[0]
    clean_text = str(raw_text).replace('\n', ' ').strip()
    return clean_text[:40] + "..." if len(clean_text)>40 else clean_text

def decode_base64_utf8(b64_str):
    try:
        if not b64_str: return ""
        return base64.b64decode(b64_str).decode('utf-8', errors='ignore').strip()
    except:
        return ""

def to_thai_datetime(utc_str):
    if not utc_str: return None
    try:
        dt_utc = datetime.strptime(str(utc_str), "%Y-%m-%d %H:%M:%S")
        return dt_utc + timedelta(hours=7)
    except:
        return None

def get_effective_ts_str(row):
    return row.get('timestamp') or row.get('collected_utc') or row.get('processed_utc')

def parse_repo_info(repo_row):
    if not repo_row: return []
    raw_list = decode_base64_utf8(repo_row.get('repolist_b64', ''))
    raw_src = decode_base64_utf8(repo_row.get('reposrc_b64', ''))
    repos = []
    lines = raw_list.split('\n')
    ignore_starts = ["loaded plugins", "loading mirror", "repo id", "not root", "subscription management", "repolist:", "this system is"]
    for line in lines:
        clean_line = line.strip()
        if not clean_line: continue
        is_garbage = False
        for prefix in ignore_starts:
            if clean_line.lower().startswith(prefix):
                is_garbage = True
                break
        if is_garbage: continue
        parts = clean_line.split(maxsplit=1)
        if len(parts) >= 2:
            rid, rname = parts[0], parts[1]
            rtype = "Network"
            if raw_src:
                if f"[{rid}|file://" in raw_src or f"{rid}|file://" in raw_src:
                    rtype = "Local"
            repos.append({'id': rid, 'name': rname, 'type': rtype})
    return repos

def parse_usage_pct(val):
    if val is None or val == "": return 0.0
    try:
        clean_val = str(val).replace('%', '').strip()
        return float(clean_val)
    except:
        return 0.0

# ----------------- DB & DATA BASE -----------------
def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def fetch_cycle_name():
    try:
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("SELECT name FROM pm_cycles WHERE closed_at IS NULL ORDER BY id DESC LIMIT 1;")
        r = cur.fetchone()
        conn.close()
        return r["name"] if r else "Current Cycle"
    except: return "-"

# ----------------- SMART ANALYTICS & TEXT GEN (V9.4) -----------------
def summarize_list_with_count(items, limit=3):
    """Returns string like: "HostA, HostB, and 5 additional hosts"."""
    if not items: return ""
    count = len(items)
    if count <= limit:
        return ", ".join(items)
    else:
        top = ", ".join(items[:limit])
        rem = count - limit
        return f"{top}, and {rem} additional hosts"

def generate_key_observations(data):
    """Generates bullet points with Scalability and Composite Logic."""
    cpu_crit, cpu_warn = [], []
    mem_crit, mem_warn = [], []
    disk_crit, disk_warn = [], []
    
    for row in data:
        h = row['hostname']
        if row['cpu'] >= 90: cpu_crit.append(h)
        elif row['cpu'] >= 80: cpu_warn.append(h)
        if row['mem'] >= 90: mem_crit.append(h)
        elif row['mem'] >= 80: mem_warn.append(h)
        d_val = row['disk']
        d_mp = row['disk_max_mp']
        if d_val >= 86: disk_crit.append(f"{h} ({d_mp} {int(d_val)}%)")
        elif d_val >= 80: disk_warn.append(f"{h} ({d_mp} {int(d_val)}%)")

    obs_bullets = []

    # CPU
    if not cpu_crit and not cpu_warn:
        obs_bullets.append("<b>CPU:</b> Usage remains optimal across all nodes, with workloads distributed within expected baselines.")
    else:
        parts = []
        if cpu_crit:
            txt = summarize_list_with_count(cpu_crit)
            parts.append(f"High utilization peaks (>=90%) were observed on <b>{txt}</b>.")
        if cpu_warn:
            txt = summarize_list_with_count(cpu_warn)
            parts.append(f"Elevated load levels (Warning) were detected on <b>{txt}</b>.")
        obs_bullets.append("<b>CPU:</b> " + " ".join(parts))

    # Memory
    if not mem_crit and not mem_warn:
        obs_bullets.append("<b>Memory:</b> Memory usage is stable across the infrastructure (<80%).")
    else:
        parts = []
        if mem_crit:
            txt = summarize_list_with_count(mem_crit)
            parts.append(f"Critical memory consumption (>=90%) detected on <b>{txt}</b>.")
        if mem_warn:
            txt = summarize_list_with_count(mem_warn)
            parts.append(f"High memory usage warnings on <b>{txt}</b>.")
        obs_bullets.append("<b>Memory:</b> " + " ".join(parts))

    # Disk
    if not disk_crit and not disk_warn:
        obs_bullets.append("<b>Storage:</b> Disk capacity is healthy. No partitions are approaching critical thresholds.")
    else:
        parts = []
        if disk_crit:
            txt = summarize_list_with_count(disk_crit)
            parts.append(f"Critical storage usage detected. Immediate cleanup is required on: <b>{txt}</b>.")
        if disk_warn:
            txt = summarize_list_with_count(disk_warn)
            parts.append(f"High storage usage observed. Capacity planning recommended for: <b>{txt}</b>.")
        obs_bullets.append("<b>Storage:</b> " + " ".join(parts))

    return "<br/>".join([f"&bull; {b}" for b in obs_bullets])

def generate_executive_paragraph(score, stats):
    """Generates the narrative paragraph."""
    health_level = "Excellent" if score >= 90 else "Good" if score >= 70 else "Fair" if score >= 50 else "Critical"
    total = stats['Total']
    healthy_cnt = stats['Up']
    
    para = f"The overall system health is rated as <b>{health_level} ({score}%)</b>. Routine checks indicate that <b>{healthy_cnt} out of {total} hosts</b> are operating normally. "
    
    if score >= 90:
        para += "No critical outages or sustained resource bottlenecks were detected during this cycle."
    elif score >= 70:
        para += "Some systems are showing signs of elevated load, but operations remain stable."
    else:
        para += "<b>Attention Required:</b> Critical resource bottlenecks or outages have been detected."
        
    return para

def generate_auto_analysis(metric_type, stats, disk_rows=None):
    """
    Context-aware analysis text.
    [V9.4] Enhanced Disk Logic: Checks for Recovery, Critical, and Warning in specific partitions.
    """
    if metric_type == 'DISK':
        if not disk_rows: return "No disk data available."
        
        recoveries = []
        criticals = []
        warnings = []
        
        for row in disk_rows:
            mp = row['mount']
            curr = row['current']
            peak = row['peak']
            
            # Priority 1: Recovery (Peak was Critical, Current is Normal)
            if peak >= 90 and curr < 80:
                recoveries.append(f"{mp} ({peak:.0f}%->{curr:.0f}%)")
            # Priority 2: Critical
            elif curr >= 86:
                criticals.append(f"{mp} ({curr:.0f}%)")
            # Priority 3: Warning
            elif curr >= 80:
                warnings.append(f"{mp} ({curr:.0f}%)")
                
        analysis = []
        if recoveries:
            analysis.append(f"<b>Positive Maintenance:</b> Space recovery detected on <b>{', '.join(recoveries)}</b>. Successful cleanup indicated.")
        
        if criticals:
            analysis.append(f"<b>Critical Bottlenecks:</b> Storage exhaustion detected on <b>{', '.join(criticals)}</b>. Immediate capacity expansion or cleanup is required.")
            
        if warnings:
            analysis.append(f"<b>High Utilization:</b> High usage levels observed on <b>{', '.join(warnings)}</b>. Capacity planning recommended.")
            
        if not criticals and not warnings and not recoveries:
            analysis.append("<b>Healthy Storage:</b> All monitored partitions are within safe operating limits (<80%).")
            
        return "<br/><br/>".join(analysis)

    # CPU/MEM Logic (Standard)
    curr = stats.get('current', 0)
    peak = stats.get('max', 0)
    avg = stats.get('avg', 0)
    
    analysis = []
    if avg >= 80:
        analysis.append(f"<b>System Under-sized:</b> The system sustained a very high average load ({avg:.1f}%) throughout the period. This indicates a resource saturation that may impact performance.")
    elif peak >= 95 and avg < 40:
        analysis.append(f"<b>Intermittent Spikes:</b> Intermittent {metric_type} spikes reaching {peak:.1f}% were recorded. These appear to be short-lived batch processes.")
    elif curr < 20 and peak > 85:
        analysis.append(f"<b>Load Normalized:</b> Current usage ({curr:.1f}%) has returned to optimal levels after previous high peaks ({peak:.1f}%).")
    elif avg <= 5:
        analysis.append(f"<b>Low Utilization:</b> The system is currently under-utilized (Avg: {avg:.1f}%).")
    else:
        analysis.append(f"<b>Healthy Workload:</b> Performance indicates a healthy and stable workload (Avg: {avg:.1f}%). No anomalies detected.")
        
    return "<br/><br/>".join(analysis)

def get_disk_assessment(current, peak):
    if current >= 90: return "Critical: Space Exhausted"
    if current >= 80: return "Warning: High Usage"
    if current < 80 and peak >= 90: return "Observation: Space Recovered"
    if current > 50 and (current - peak) > -2: return "Observation: Continuous Growth"
    return "Normal Operation"

# ----------------- DATA ENGINE -----------------
def get_real_disk_max(hostname):
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT MAX(collected_utc) FROM host_mounts WHERE hostname=?", (hostname,))
        latest_ts = cur.fetchone()[0]
        if not latest_ts: return 0, "/"
        sql = "SELECT mountpoint, used_pct FROM host_mounts WHERE hostname=? AND collected_utc=?"
        cur.execute(sql, (hostname, latest_ts))
        rows = cur.fetchall()
        max_val = 0.0
        max_mp = "/"
        for r in rows:
            mp = r['mountpoint']
            if mp not in OS_PARTITIONS: continue
            val = parse_usage_pct(r['used_pct'])
            if val > max_val:
                max_val = val
                max_mp = mp
        return max_val, max_mp
    except: return 0, "/"
    finally: conn.close()

def compute_status_and_age(row):
    try:
        cpu = int(parse_usage_pct(row['cpu']))
        mem = int(parse_usage_pct(row['mem']))
        disk = int(parse_usage_pct(row['disk']))
        target_ts = get_effective_ts_str(row)
    except: return "Offline", 999
    is_offline = True
    if target_ts:
        try:
            dt = datetime.strptime(target_ts, "%Y-%m-%d %H:%M:%S")
            if (datetime.utcnow() - dt).total_seconds() < 6*3600:
                is_offline = False
        except: pass
    if is_offline: return "Offline", 999
    mx = max(cpu, mem, disk)
    if mx >= 86: return "Degraded", 0
    if mx >= 80: return "Warning", 0
    return "Up", 0

def get_report_data(hosts_filter=None):
    conn = connect_db()
    cur = conn.cursor()
    sql = """
        SELECT * FROM reports r
        WHERE r.id = (
            SELECT id FROM reports r2
            WHERE r2.hostname = r.hostname
            ORDER BY COALESCE(r2.timestamp, r2.collected_utc, r2.processed_utc) DESC, r2.id DESC
            LIMIT 1
        )
    """
    if hosts_filter:
        ph = ','.join(f"'{h}'" for h in hosts_filter)
        sql += f" AND r.hostname IN ({ph})"
    sql += " ORDER BY r.hostname ASC"
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    data = []
    stats = {"Total": 0, "Up": 0, "Warning": 0, "Degraded": 0, "Offline": 0}
    for r_obj in rows:
        r = dict(r_obj)
        disk_max, disk_mp = get_real_disk_max(r['hostname'])
        r['disk'] = disk_max
        r['disk_max_mp'] = disk_mp
        st, _ = compute_status_and_age(r)
        stats["Total"] += 1
        if st in stats: stats[st] += 1
        raw_ip = r.get('ip_address', '')
        if r.get('ip_addresses'): raw_ip = r['ip_addresses']
        elif r.get('ip_address_all'): raw_ip = r['ip_address_all']
        data.append({
            "hostname": r['hostname'],
            "ip": extract_real_ips(raw_ip),
            "os": f"{r.get('os_name','') or ''} {r.get('os_version','') or ''}".strip(),
            "kernel": r.get('kernel', '-'),
            "hostid": r.get('hostid', '-'),
            "status": st,
            "cpu": parse_usage_pct(r['cpu']), 
            "mem": parse_usage_pct(r['mem']), 
            "disk": disk_max,
            "disk_max_mp": disk_mp,
            "uptime": r.get('uptime', '-'),
            "timestamp": r.get('timestamp', None),
            "collected_utc": r.get('collected_utc', None),
            "processed_utc": r.get('processed_utc', None)
        })
    score = 0
    if stats["Total"] > 0:
        try:
            pts = (stats["Up"] * 10) + (stats["Warning"] * 7) + (stats["Degraded"] * 3)
            denominator = float(stats["Total"] * 10)
            score = int((pts / denominator) * 100) if denominator > 0 else 0
        except: score = 0
    return data, stats, score

def fetch_host_details(hostname):
    conn = connect_db()
    cur = conn.cursor()
    details = {}
    def get_one(query, params):
        try:
            cur.execute(query, params)
            r = cur.fetchone()
            return dict(r) if r else None
        except: return None
    def get_list_smart(table, hostname):
        try:
            cur.execute(f"SELECT MAX(collected_utc) as ts FROM {table} WHERE hostname=?", (hostname,))
            ts_res = cur.fetchone()
            if not ts_res or not ts_res['ts']: return []
            latest_ts = ts_res['ts']
            cur.execute(f"SELECT * FROM {table} WHERE hostname=? AND collected_utc=?", (hostname, latest_ts))
            return [dict(r) for r in cur.fetchall()]
        except: return []
    details['hw'] = get_one("SELECT * FROM host_system_hw WHERE hostname=? ORDER BY collected_utc DESC LIMIT 1", (hostname,))
    details['net'] = get_one("SELECT * FROM host_network_extra WHERE hostname=? ORDER BY collected_utc DESC LIMIT 1", (hostname,))
    details['repo_raw'] = get_one("SELECT * FROM host_repo_status WHERE hostname=? ORDER BY collected_utc DESC LIMIT 1", (hostname,))
    details['top_cpu'] = sorted(get_list_smart("host_top_cpu", hostname), key=lambda x: x.get('cpu', 0), reverse=True)[:5]
    details['top_mem'] = sorted(get_list_smart("host_top_mem", hostname), key=lambda x: x.get('mem', 0), reverse=True)[:5]
    details['disks'] = sorted(get_list_smart("host_disks", hostname), key=lambda x: x.get('name', ''))
    details['mounts'] = sorted(get_list_smart("host_mounts", hostname), key=lambda x: parse_usage_pct(x.get('used_pct', 0)), reverse=True)
    details['repo_list'] = get_list_smart("host_repo_status", hostname)
    conn.close()
    return details

# ----------------- DATA HISTORIES & GRAPHS -----------------
def fetch_host_history_v9(hostname, days):
    conn = connect_db()
    cur = conn.cursor()
    now = datetime.utcnow()
    start_dt = now - timedelta(days=days)
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    try:
        cur.execute("SELECT swap FROM reports LIMIT 1")
        has_swap = True
    except: has_swap = False
    query = "SELECT collected_utc, timestamp, cpu, mem, disk"
    if has_swap: query += ", swap"
    query += " FROM reports WHERE hostname = ? AND (collected_utc >= ? OR timestamp >= ?) ORDER BY collected_utc ASC"
    try:
        cur.execute(query, (hostname, start_str, start_str))
        rows = cur.fetchall()
        dates, cpu, mem, disk, swap = [], [], [], [], []
        for r in rows:
            ts_str = r['collected_utc'] or r['timestamp']
            if not ts_str: continue
            dt_thai = to_thai_datetime(ts_str)
            if not dt_thai: continue
            dates.append(dt_thai)
            cpu.append(parse_usage_pct(r['cpu']))
            mem.append(parse_usage_pct(r['mem']))
            disk.append(parse_usage_pct(r['disk']))
            if has_swap: swap.append(parse_usage_pct(r['swap']))
        now_th = now + timedelta(hours=7)
        start_th = now_th - timedelta(days=days)
        return {'dates': dates, 'cpu': cpu, 'mem': mem, 'disk': disk, 'swap': swap, 'period': f"Scope: {start_th.strftime('%d %b %Y')} - {now_th.strftime('%d %b %Y')}", 'xlim': (start_th, now_th)}
    except: return None
    finally: conn.close()

def fetch_disk_partition_history(hostname, days):
    conn = connect_db()
    cur = conn.cursor()
    now = datetime.utcnow()
    start_dt = now - timedelta(days=days)
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    sql = "SELECT collected_utc, mountpoint, used_pct, size FROM host_mounts WHERE hostname = ? AND collected_utc >= ? ORDER BY collected_utc ASC"
    try:
        cur.execute(sql, (hostname, start_str))
        rows = cur.fetchall()
        partitions = {} 
        for r in rows:
            mp = r['mountpoint']
            if mp not in OS_PARTITIONS: continue
            ts_str = r['collected_utc']
            dt_thai = to_thai_datetime(ts_str)
            if not dt_thai: continue
            if mp not in partitions: partitions[mp] = {'dates': [], 'values': [], 'size': r['size']}
            partitions[mp]['dates'].append(dt_thai)
            partitions[mp]['values'].append(parse_usage_pct(r['used_pct']))
            partitions[mp]['size'] = r['size']
        now_th = now + timedelta(hours=7)
        start_th = now_th - timedelta(days=days)
        return {'data': partitions, 'period': f"Scope: {start_th.strftime('%d %b %Y')} - {now_th.strftime('%d %b %Y')}", 'xlim': (start_th, now_th)}
    except: return None
    finally: conn.close()

def generate_hero_graph(hostname, metric_type, days, width_in=7.5, height_in=4):
    data = fetch_host_history_v9(hostname, days)
    if not data or not data['dates']: return None, None
    if metric_type == 'CPU': y_data, color_line, color_fill = data['cpu'], '#2563EB', '#3B82F6'
    elif metric_type == 'MEM': y_data, color_line, color_fill = data['mem'], '#10B981', '#34D399'
    else: return None, None
    if not y_data: return None, None
    stats = {'max': max(y_data), 'min': min(y_data), 'avg': sum(y_data)/len(y_data), 'current': y_data[-1], 'swap_max': max(data['swap']) if data.get('swap') else 0}
    fig, ax = plt.subplots(figsize=(width_in, height_in))
    ax.plot(data['dates'], y_data, color=color_line, linewidth=1.5, marker='o', markersize=3, markeredgecolor=color_line, markerfacecolor='white', markeredgewidth=1.0)
    ax.fill_between(data['dates'], y_data, 0, color=color_fill, alpha=0.15)
    ax.set_xlim(data['xlim'])
    ax.set_ylim(0, 110)
    ax.grid(True, linestyle=':', linewidth=0.5, alpha=0.7, color='#94A3B8')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#CBD5E1')
    ax.spines['bottom'].set_color('#CBD5E1')
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))
    plt.xticks(rotation=90, fontsize=8, color='#475569')
    plt.yticks(fontsize=8, color='#475569')
    plt.tight_layout()
    fd, path = tempfile.mkstemp(suffix=".png")
    os.close(fd)
    plt.savefig(path, dpi=150, transparent=False)
    plt.close(fig)
    return path, stats

def generate_disk_multiline_graph(hostname, days, width_in=7.5, height_in=4):
    res = fetch_disk_partition_history(hostname, days)
    if not res or not res['data']: return None, None
    fig, ax = plt.subplots(figsize=(width_in, height_in))
    colors_map = {'/': '#2563EB', '/var': '#F59E0B', '/boot': '#10B981', '/usr': '#8B5CF6', '/tmp': '#EC4899', '/opt': '#06B6D4', '/home': '#64748B'}
    stats_list = []
    for mp, info in res['data'].items():
        if not info['values']: continue
        col = colors_map.get(mp, '#64748B')
        ax.plot(info['dates'], info['values'], label=mp, color=col, linewidth=1.5, marker='o', markersize=2, markeredgecolor=col, markerfacecolor=col)
        curr, peak = info['values'][-1], max(info['values'])
        stats_list.append({'mount': mp, 'size': info['size'], 'current': curr, 'peak': peak, 'assess': get_disk_assessment(curr, peak)})
    ax.set_xlim(res['xlim'])
    ax.set_ylim(0, 110)
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.12), ncol=4, frameon=False, fontsize=8)
    ax.grid(True, linestyle=':', linewidth=0.5, alpha=0.7, color='#94A3B8')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#CBD5E1')
    ax.spines['bottom'].set_color('#CBD5E1')
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))
    plt.xticks(rotation=90, fontsize=8, color='#475569')
    plt.yticks(fontsize=8, color='#475569')
    plt.tight_layout()
    fd, path = tempfile.mkstemp(suffix=".png")
    os.close(fd)
    plt.savefig(path, dpi=150, transparent=False)
    plt.close(fig)
    return path, {'period': res['period'], 'rows': stats_list}

# ----------------- GRAPHICS -----------------
class UsageBar(Flowable):
    def __init__(self, value, width=40, height=8):
        try: self.value = min(max(float(value or 0), 0), 100)
        except: self.value = 0
        self.width, self.height = width, height
    def wrap(self, w, h): return self.width, self.height
    def draw(self):
        fill_col = colors.HexColor(COLOR_UP)
        if self.value >= 86: fill_col = colors.HexColor(COLOR_DEG)
        elif self.value >= 80: fill_col = colors.HexColor(COLOR_WARN)
        self.canv.setFillColor(colors.HexColor("#E2E8F0"))
        self.canv.roundRect(0, 0, self.width, self.height, 2, fill=1, stroke=0)
        bar_w = (self.value / 100.0) * self.width
        if bar_w > 0:
            self.canv.setFillColor(fill_col)
            self.canv.roundRect(0, 0, bar_w, self.height, 2, fill=1, stroke=0)
        self.canv.setFillColor(colors.black)
        self.canv.setFont("Helvetica", 7)
        self.canv.drawString(self.width + 3, 1, f"{int(self.value)}%")

class StatusDot(Flowable):
    def __init__(self, status):
        self.status = str(status).lower() if status else "offline"
        self.radius, self.width, self.height = 3, 6, 6
    def wrap(self, w, h): return self.width, self.height
    def draw(self):
        col = colors.HexColor(COLOR_OFF)
        if self.status in ['up', 'enabled', '1', 'online', 'ok', 'local']: col = colors.HexColor(COLOR_UP)
        elif self.status in ['warning', 'warn']: col = colors.HexColor(COLOR_WARN)
        elif self.status in ['degraded', 'disabled', '0', 'offline', 'critical', 'full']: col = colors.HexColor(COLOR_DEG)
        self.canv.setFillColor(col)
        self.canv.circle(self.radius, self.radius, self.radius, fill=1, stroke=0)

class HealthScoreGauge(Flowable):
    def __init__(self, score):
        self.score, self.size = score, 80
        self.width, self.height = 80, 80
    def wrap(self, w, h): return self.width, self.height
    def draw(self):
        col = colors.HexColor(COLOR_UP)
        status_text = "EXCELLENT"
        if self.score < 50: col, status_text = colors.HexColor(COLOR_DEG), "CRITICAL"
        elif self.score < 70: col, status_text = colors.HexColor(COLOR_WARN), "WARNING"
        elif self.score < 90: col, status_text = colors.HexColor("#FDE047"), "GOOD"
        cx, cy = self.size/2, self.size/2
        self.canv.setStrokeColor(colors.HexColor("#E2E8F0"))
        self.canv.setLineWidth(6)
        self.canv.circle(cx, cy, 35, stroke=1, fill=0)
        self.canv.setStrokeColor(col)
        self.canv.circle(cx, cy, 35, stroke=1, fill=0)
        self.canv.setFillColor(col)
        self.canv.setFont("Helvetica-Bold", 20)
        self.canv.drawCentredString(cx, cy - 2, f"{self.score}%")
        self.canv.setFillColor(col)
        self.canv.setFont("Helvetica-Bold", 7)
        self.canv.drawCentredString(cx, cy - 14, status_text)
        self.canv.setFillColor(colors.gray)
        self.canv.setFont("Helvetica", 6)
        self.canv.drawCentredString(cx, cy + 18, "HEALTH SCORE")

class DonutChart(Flowable):
    def __init__(self, value, label, color, size=60, sub_label=None):
        try: self.value = min(max(float(value or 0), 0), 100)
        except: self.value = 0
        self.label, self.color, self.size, self.sub_label = label, color, size, sub_label
        self.width, self.height = size, size + 15
    def wrap(self, w, h): return self.width, self.height
    def draw(self):
        cx, cy = self.size/2, self.size/2 + 10
        radius = self.size/2
        self.canv.setStrokeColor(colors.HexColor("#E2E8F0"))
        self.canv.setLineWidth(5)
        self.canv.circle(cx, cy, radius, stroke=1, fill=0)
        if self.value > 0:
            p = self.canv.beginPath()
            p.arc(cx-radius, cy-radius, cx+radius, cy+radius, 90, -(self.value / 100.0) * 360)
            self.canv.setStrokeColor(self.color)
            self.canv.setLineWidth(5)
            self.canv.drawPath(p, stroke=1, fill=0)
        self.canv.setFillColor(colors.black)
        self.canv.setFont("Helvetica-Bold", 10)
        self.canv.drawCentredString(cx, cy - 3, f"{int(self.value)}%")
        self.canv.setFillColor(colors.gray)
        self.canv.setFont("Helvetica", 8)
        self.canv.drawCentredString(cx, 0, self.label)
        if self.sub_label:
            self.canv.setFont("Helvetica", 6)
            self.canv.drawCentredString(cx, cy - 12, self.sub_label)

# ----------------- PAGE FUNCTIONS -----------------
def create_cover_page(elements, styles, stats, cycle, conf, rtype):
    elements.append(Spacer(1, 10*mm))
    if os.path.exists(LOGO_PATH):
        im = RLImage(LOGO_PATH, width=60*mm, height=25*mm)
        im.hAlign = 'RIGHT'
        elements.append(im)
    elements.append(Spacer(1, 30*mm))
    main_title = conf.get("report_title", "PREVENTIVE MAINTENANCE REPORT")
    sub_title = "System Health & Performance Assessment" 
    if rtype == 'full': sub_title = "Full Technical Assessment & System Health"
    elif rtype == 'summary': sub_title = "Executive Summary: Technical Assessment & System Health"
    elif rtype == 'select': sub_title = "Targeted Host: Technical Assessment & System Health"
    elements.append(Paragraph(main_title, ParagraphStyle('CT', parent=styles['Title'], fontSize=28, leading=34, alignment=TA_RIGHT, spaceAfter=10)))
    elements.append(Paragraph(sub_title, ParagraphStyle('CS', parent=styles['Normal'], fontSize=16, textColor=colors.gray, alignment=TA_RIGHT)))
    elements.append(Spacer(1, 30*mm))
    elements.append(Paragraph("PREPARED FOR:", ParagraphStyle('CL', parent=styles['Normal'], fontSize=10, textColor=colors.HexColor(THEME_COLOR), alignment=TA_RIGHT)))
    elements.append(Paragraph(conf.get("client_name"), ParagraphStyle('CN', parent=styles['Heading2'], fontSize=20, alignment=TA_RIGHT, spaceAfter=20)))
    elements.append(Paragraph("PREPARED BY:", ParagraphStyle('PL', parent=styles['Normal'], fontSize=10, textColor=colors.HexColor(THEME_COLOR), alignment=TA_RIGHT)))
    elements.append(Paragraph(conf.get("provider_name"), ParagraphStyle('PN', parent=styles['Heading2'], fontSize=18, alignment=TA_RIGHT)))
    elements.append(Spacer(1, 50*mm))
    p_meta = ParagraphStyle('Meta', parent=styles['Normal'], fontSize=12, alignment=TA_RIGHT, leading=16)
    elements.append(Paragraph(f"<b>Report Cycle:</b> {cycle}", p_meta))
    elements.append(Paragraph(f"<b>Generated Date:</b> {datetime.now().strftime('%Y-%m-%d %H:%M')}", p_meta))
    elements.append(Paragraph(f"<b>Total Hosts Monitored:</b> {stats['Total']}", p_meta))
    elements.append(Spacer(1, 15*mm))
    elements.append(Paragraph(f"© Copyright {datetime.now().year} {conf.get('provider_name')}. All Rights Reserved.", ParagraphStyle('Cpy', parent=styles['Normal'], fontSize=9, textColor=colors.gray, alignment=TA_CENTER)))
    elements.append(NextPageTemplate('Portrait')) 
    elements.append(PageBreak())

def create_preface_page(elements, styles, rtype, conf, sel_str):
    elements.append(Paragraph("DOCUMENT CONTROL & PREFACE", styles['Heading1']))
    elements.append(Spacer(1, 10*mm))
    elements.append(Paragraph("1. Document Information", styles['Heading3']))
    doc_info = [["Report Type:", TYPE_MAP.get(rtype, rtype.upper())], ["System Scope:", "PM-Core Automation Monitoring"], ["Classification:", "Confidential"]]
    if rtype == 'select':
        hosts = [h.strip() for h in sel_str.split(',') if h.strip()]
        target_str = ", ".join(hosts) if len(hosts) <= 5 else f"{len(hosts)} Hosts Selected"
        doc_info.append(["Target Hosts:", Paragraph(target_str, styles['Normal'])])
    t_info = Table(doc_info, colWidths=[40*mm, 100*mm])
    t_info.setStyle(TableStyle([('FONTNAME', (0,0), (0,-1), 'Helvetica-Bold'), ('ALIGN', (0,0), (-1,-1), 'LEFT'), ('VALIGN', (0,0), (-1,-1), 'TOP')]))
    elements.append(t_info)
    elements.append(Spacer(1, 10*mm))
    elements.append(Paragraph("2. Objective & Scope", styles['Heading3']))
    if rtype == 'summary':
        obj_text = """This document provides a <b>high-level executive overview</b> of the system health status. The objectives are to highlight critical risks, summarize resource utilization trends, and support management decision-making regarding IT infrastructure."""
    else:
        obj_text = """This document provides a <b>comprehensive technical health check analysis</b> of the computer systems. The objectives are:<br/>&bull; To analyze detailed system resource utilization (CPU, Memory, Disk).<br/>&bull; To verify system availability and identify operational risks.<br/>&bull; To serve as operational evidence for <b>ISO 20000 / ISO 27001</b> compliance audits."""
    elements.append(Paragraph(obj_text, ParagraphStyle('Obj', parent=styles['Normal'], leading=14, alignment=TA_JUSTIFY)))
    elements.append(Spacer(1, 10*mm))
    elements.append(Paragraph("3. Contact Information", styles['Heading3']))
    contact = conf.get("contact", {})
    c_text = f"For any inquiries regarding this report, please contact:<br/><b>{contact.get('team_name', 'Support Team')}</b><br/>Email: {contact.get('email', '-')}<br/>Tel: {contact.get('tel', '-')}"
    elements.append(Paragraph(c_text, ParagraphStyle('Ct', parent=styles['Normal'], leading=14)))
    elements.append(PageBreak())

def create_summary_section(elements, styles, data, stats, score, cycle):
    # [V9.3.3] FIX: Stacked Vertical Layout for Breakdown to fit in 70mm
    elements.append(Paragraph("Executive Summary", styles['Heading2']))
    
    # 1.1 Status Breakdown Table (VERTICAL Stack for narrow column)
    breakdown_data = [
        [StatusDot("Up"), "Healthy Systems", str(stats['Up'])],
        [StatusDot("Warning"), "Warning Systems", str(stats['Warning'])],
        [StatusDot("Degraded"), "Critical Systems", str(stats['Degraded'])],
        [StatusDot("Offline"), "Offline / No Data", str(stats['Offline'])]
    ]
    t_breakdown = Table(breakdown_data, colWidths=[10*mm, 45*mm, 10*mm])
    t_breakdown.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('ALIGN', (2,0), (2,-1), 'RIGHT'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('TOPPADDING', (0,0), (-1,-1), 2),
        ('BOTTOMPADDING', (0,0), (-1,-1), 2),
    ]))

    # 1.2 Left Column Content
    left_col = [
        HealthScoreGauge(score),
        Spacer(1, 8*mm),
        t_breakdown
    ]

    # 1.3 Right Column Content
    para_text = generate_executive_paragraph(score, stats)
    obs_text = generate_key_observations(data)
    
    right_col = [
        Paragraph("<b>Executive Summary:</b>", styles['Normal']),
        Paragraph(para_text, ParagraphStyle('ExecTxt', parent=styles['Normal'], leading=14, alignment=TA_JUSTIFY, spaceAfter=12)),
        Paragraph("<b>Key Observations:</b>", styles['Normal']),
        Paragraph(obs_text, ParagraphStyle('ObsTxt', parent=styles['Normal'], leading=14, leftIndent=0)) 
    ]
    
    # 2. Master Table
    t_master = Table([[left_col, right_col]], colWidths=[70*mm, 115*mm])
    t_master.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('TOPPADDING', (0,0), (-1,-1), 0),
    ]))
    elements.append(t_master)
    elements.append(Spacer(1, 12*mm))
    
    # 3. Host Table
    elements.append(Paragraph("Host Status Overview", styles['Heading2']))
    tbl_data = [["Hostname / IP", "OS", "Last Seen", "Status", "CPU", "MEM", "DISK (Max)"]]
    style_n = ParagraphStyle('S', parent=styles['Normal'], fontSize=8)
    style_ip = ParagraphStyle('SIP', parent=styles['Normal'], fontSize=6, textColor=colors.gray)
    style_tiny = ParagraphStyle('Tiny', parent=styles['Normal'], fontSize=6, textColor=colors.gray)
    style_ts = ParagraphStyle('STS', parent=styles['Normal'], fontSize=7, alignment=TA_CENTER)
    
    for row in data:
        host_cell = [Paragraph(row['hostname'], style_n), Paragraph(row['ip'], style_ip)]
        last_seen_str = "-"
        target_ts = get_effective_ts_str(row)
        if target_ts:
            dt_thai = to_thai_datetime(target_ts)
            last_seen_str = dt_thai.strftime("%d %b\n%H:%M") if dt_thai else str(target_ts)
        st_cell = Table([[StatusDot(row['status']), Paragraph(row['status'], style_n)]], colWidths=[6*mm, 15*mm])
        st_cell.setStyle(TableStyle([('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('LEFTPADDING', (0,0), (-1,-1), 0)]))
        
        disk_cell_content = [UsageBar(row['disk'], width=15)]
        if row['disk_max_mp']:
            disk_cell_content.append(Paragraph(f"(max: {row['disk_max_mp']})", style_tiny))
        
        tbl_data.append([
            host_cell, Paragraph(row['os'][:25], style_n), Paragraph(last_seen_str, style_ts),
            st_cell, UsageBar(row['cpu'], width=15), UsageBar(row['mem'], width=15), disk_cell_content
        ])
    col_w = [45*mm, 35*mm, 20*mm, 25*mm, 18*mm, 18*mm, 18*mm]
    t_main = Table(tbl_data, colWidths=col_w, repeatRows=1)
    t_main.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor(THEME_COLOR)),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('ALIGN', (0,0), (-1,0), 'CENTER'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor("#F8FAFC")]),
        ('FONTSIZE', (0,0), (-1,-1), 7), 
    ]))
    elements.append(t_main)

def create_host_detail_page(elements, styles, host_data, details):
    elements.append(NextPageTemplate('Portrait'))
    elements.append(PageBreak())
    elements.append(Paragraph(f"Host Detail: {host_data['hostname']}", styles['Heading1']))
    
    def get_color(val):
        if val >= 86: return colors.HexColor(COLOR_DEG)
        if val >= 80: return colors.HexColor(COLOR_WARN)
        return colors.HexColor(COLOR_UP)
    
    t_donut = Table([[
        DonutChart(host_data['cpu'], "CPU Usage", get_color(host_data['cpu'])),
        DonutChart(host_data['mem'], "Memory Usage", get_color(host_data['mem'])),
        DonutChart(host_data['disk'], "Max Disk Usage", get_color(host_data['disk']), sub_label=f"({host_data['disk_max_mp']})")
    ]], colWidths=[50*mm]*3)
    t_donut.setStyle(TableStyle([('ALIGN',(0,0),(-1,-1),'CENTER')]))
    elements.append(t_donut)
    
    elements.append(Paragraph("System Information", styles['Heading3']))
    hw = details.get('hw') or {}
    kernel_val = hw.get('kernel_release') or hw.get('kernel') or host_data.get('kernel', '-')
    snapshot_display = "-"
    target_ts = get_effective_ts_str(host_data)
    if target_ts:
        dt_thai = to_thai_datetime(target_ts)
        snapshot_display = f"{dt_thai.strftime('%Y-%m-%d %H:%M:%S')} (Thai Time)" if dt_thai else target_ts
    sys_data = [
        ["Hostname:", host_data['hostname']], ["Data Snapshot:", snapshot_display],
        ["OS:", host_data['os']], ["Kernel:", kernel_val],
        ["Uptime:", host_data['uptime']], ["IP:", Paragraph(host_data['ip'], styles['Normal'])],
        ["Platform / Virt:", hw.get('product_name','-')], ["Host ID:", host_data['hostid']]
    ]
    t_sys = Table(sys_data, colWidths=[45*mm, 130*mm]) 
    t_sys.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 0.25, colors.grey), ('FONTNAME', (0,0), (0,-1), 'Helvetica-Bold'), ('BACKGROUND', (0,0), (0,-1), colors.HexColor("#F1F5F9"))]))
    elements.append(t_sys)
    elements.append(Spacer(1, 5*mm))
    
    if details.get('mounts'):
        elements.append(Paragraph("Mount Points", styles['Heading3']))
        m_data = [["Mount", "Type", "Size", "Used", "Usage %", "Status"]]
        for m in details['mounts']:
            usage = parse_usage_pct(m.get('used_pct', 0))
            if usage >= 86: st, st_col = "Full/Crit", "critical"
            elif usage >= 80: st, st_col = "Warning", "warn"
            else: st, st_col = "OK", "ok"
            m_data.append([m.get('mountpoint'), m.get('fstype'), m.get('size'), m.get('used'), f"{usage}%", Table([[StatusDot(st_col), st]], colWidths=[5*mm, 20*mm])])
        t_mnt = Table(m_data, colWidths=[50*mm, 20*mm, 20*mm, 20*mm, 20*mm, 35*mm])
        t_mnt.setStyle(TableStyle([('BACKGROUND', (0,0), (-1,0), colors.HexColor(THEME_COLOR)), ('TEXTCOLOR', (0,0), (-1,0), colors.white), ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor("#F8FAFC")]), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
        elements.append(t_mnt)
        elements.append(Spacer(1, 5*mm))
    if details.get('top_cpu'):
        t_cpu_data = [["Top 5 Processes by CPU Usage", "%"]]
        for p in details.get('top_cpu', []): t_cpu_data.append([Paragraph(str(p.get('cmd','-'))[:30], styles['Normal']), f"{p.get('cpu')}%"])
        t_mem_data = [["Top 5 Processes by Memory Usage", "%"]]
        for p in details.get('top_mem', []): t_mem_data.append([Paragraph(str(p.get('cmd','-'))[:30], styles['Normal']), f"{p.get('mem')}%"])
        t_c = Table(t_cpu_data, colWidths=[65*mm, 20*mm])
        t_c.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 0.25, colors.grey), ('BACKGROUND', (0,0), (0,0), colors.HexColor("#E2E8F0"))]))
        t_m = Table(t_mem_data, colWidths=[65*mm, 20*mm])
        t_m.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 0.25, colors.grey), ('BACKGROUND', (0,0), (0,0), colors.HexColor("#E2E8F0"))]))
        elements.append(Paragraph("Resource Usage: Top Processes", styles['Heading3']))
        elements.append(Table([[t_c, Spacer(5*mm,0), t_m]], colWidths=[85*mm, 5*mm, 85*mm]))
        elements.append(Spacer(1, 5*mm))
    if details.get('repo_raw'):
        repos = parse_repo_info(details['repo_raw'])
        if repos:
            r_data = [["Repo ID", "Name", "Type"]]
            for r in repos: r_data.append([Paragraph(r['id'][:30], styles['Normal']), Paragraph(r['name'][:50], styles['Normal']), r['type']])
            t_repo = Table(r_data, colWidths=[50*mm, 90*mm, 30*mm])
            t_repo.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 0.25, colors.grey), ('BACKGROUND', (0,0), (0,-1), colors.HexColor("#E2E8F0")), ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold')]))
            elements.append(KeepTogether([Paragraph("Repositories", styles['Heading3']), t_repo]))
    
    elements.append(Spacer(1, 5*mm))
    
    # [V9.4] Smart Recommendations Loop
    recs = []
    # CPU
    if host_data['cpu'] >= 90: recs.append("- <b>CPU Critical:</b> Immediate investigation required.")
    elif host_data['cpu'] >= 80: recs.append("- <b>CPU Warning:</b> High load detected.")
    
    # DISK (Specific)
    disk_issues = False
    if details.get('mounts'):
        for m in details['mounts']:
            u = parse_usage_pct(m.get('used_pct', 0))
            mp = m.get('mountpoint')
            if mp not in OS_PARTITIONS: continue
            
            if u >= 86:
                recs.append(f"- <b>Disk Critical:</b> Partition <b>{mp} ({int(u)}%)</b> is near capacity. Immediate cleanup required.")
                disk_issues = True
            elif u >= 80:
                recs.append(f"- <b>Disk Warning:</b> High usage on <b>{mp} ({int(u)}%)</b>. Plan expansion.")
                disk_issues = True
    
    # Fallback Disk
    if not disk_issues and host_data['disk'] >= 80:
         recs.append(f"- <b>Disk Check:</b> Max usage is high ({int(host_data['disk'])}%), check partitions.")

    if not recs: recs.append("- System health parameters are within optimal ranges.")
    
    elements.append(KeepTogether([Paragraph("Recommendations", styles['Heading3'])]))
    for r in recs: elements.append(Paragraph(r, ParagraphStyle('Rec', parent=styles['Normal'], textColor=colors.HexColor(THEME_COLOR))))

    metrics = [
        {'type': 'CPU', 'title': 'CPU Performance Analysis', 'unit': '%'},
        {'type': 'MEM', 'title': 'Memory Usage Analysis', 'unit': '%'},
        {'type': 'DISK', 'title': 'Disk Usage Analysis (OS Partitions)', 'unit': '%'}
    ]

    for m in metrics:
        elements.append(NextPageTemplate('Portrait'))
        elements.append(PageBreak())
        
        if m['type'] == 'DISK':
            img_path, res = generate_disk_multiline_graph(host_data['hostname'], 30, width_in=7.5, height_in=4)
            if img_path:
                elements.append(Paragraph(f"{m['title']}: {host_data['hostname']}", styles['Heading2']))
                elements.append(Paragraph(res['period'], ParagraphStyle('Sub', parent=styles['Normal'], fontSize=10, textColor=colors.gray)))
                elements.append(Spacer(1, 5*mm))
                elements.append(RLImage(img_path, width=180*mm, height=96*mm))
                elements.append(Spacer(1, 10*mm))
                elements.append(Paragraph("Statistics Summary", styles['Heading3']))
                t_rows = [["Mount Point", "Size", "Current", "Peak (30d)", "Assessment"]]
                for row in res['rows']:
                    t_rows.append([row['mount'], row['size'], f"{row['current']:.1f}%", f"{row['peak']:.1f}%", row['assess']])
                t_stat = Table(t_rows, colWidths=[30*mm, 25*mm, 25*mm, 25*mm, 65*mm])
                t_stat.setStyle(TableStyle([('BACKGROUND', (0,0), (-1,0), colors.HexColor(THEME_COLOR)), ('TEXTCOLOR', (0,0), (-1,0), colors.white), ('ALIGN', (0,0), (-1,-1), 'CENTER'), ('GRID', (0,0), (-1,-1), 0.25, colors.grey), ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor("#F8FAFC")]), ('FONTSIZE', (0,0), (-1,-1), 9)]))
                elements.append(t_stat)
                elements.append(Spacer(1, 10*mm))
                elements.append(Paragraph("Analysis & Observations", styles['Heading3']))
                # [V9.4] Pass rows for smart analysis
                analysis_text = generate_auto_analysis(m['type'], {}, res['rows'])
                t_analysis = Table([[Paragraph(analysis_text, styles['Normal'])]], colWidths=[170*mm])
                t_analysis.setStyle(TableStyle([('BOX', (0,0), (-1,-1), 1, colors.HexColor("#CBD5E1")), ('BACKGROUND', (0,0), (-1,-1), colors.HexColor("#F8FAFC")), ('PADDING', (0,0), (-1,-1), 10)]))
                elements.append(t_analysis)
            else:
                elements.append(Paragraph(f"No Disk History Data for {host_data['hostname']}", styles['Normal']))
        else:
            img_path, stats = generate_hero_graph(host_data['hostname'], m['type'], 30, width_in=7.5, height_in=4)
            data = fetch_host_history_v9(host_data['hostname'], 30)
            if img_path and stats and data:
                elements.append(Paragraph(f"{m['title']}: {host_data['hostname']}", styles['Heading2']))
                elements.append(Paragraph(data['period'], ParagraphStyle('Sub', parent=styles['Normal'], fontSize=10, textColor=colors.gray)))
                elements.append(Spacer(1, 5*mm))
                elements.append(RLImage(img_path, width=180*mm, height=96*mm))
                elements.append(Spacer(1, 10*mm))
                elements.append(Paragraph("Statistics Summary", styles['Heading3']))
                mx, avg = stats['max'], stats['avg']
                curr = stats['current']
                assess = "Normal"
                if m['type'] == 'CPU': 
                    if avg > 80: assess = "Critical: Overloaded"
                    elif mx > 90: assess = "Transient Peak"
                else:
                    if avg > 85: assess = "High Usage"
                stat_data = [["Metric", "Current", "Peak (30d)", "Average", "Assessment"], ["Usage", f"{curr:.1f}%", f"{mx:.1f}%", f"{avg:.1f}%", assess]]
                if m['type'] == 'MEM':
                    swap_val = stats.get('swap_max', 0)
                    stat_data.append(["Swap", "-", f"{swap_val:.1f}%", "-", "Normal" if swap_val < 50 else "High"])
                t_stat = Table(stat_data, colWidths=[30*mm, 30*mm, 30*mm, 30*mm, 50*mm])
                t_stat.setStyle(TableStyle([('BACKGROUND', (0,0), (-1,0), colors.HexColor(THEME_COLOR)), ('TEXTCOLOR', (0,0), (-1,0), colors.white), ('ALIGN', (0,0), (-1,-1), 'CENTER'), ('GRID', (0,0), (-1,-1), 0.25, colors.grey), ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor("#F8FAFC")])]))
                elements.append(t_stat)
                elements.append(Spacer(1, 10*mm))
                elements.append(Paragraph("Analysis & Observations", styles['Heading3']))
                analysis_text = generate_auto_analysis(m['type'], stats)
                t_analysis = Table([[Paragraph(analysis_text, styles['Normal'])]], colWidths=[170*mm])
                t_analysis.setStyle(TableStyle([('BOX', (0,0), (-1,-1), 1, colors.HexColor("#CBD5E1")), ('BACKGROUND', (0,0), (-1,-1), colors.HexColor("#F8FAFC")), ('PADDING', (0,0), (-1,-1), 10)]))
                elements.append(t_analysis)
            else:
                elements.append(Paragraph(f"No Data for {m['title']}", styles['Normal']))
    elements.append(NextPageTemplate('Portrait'))

def create_appendix_page(elements, styles, stats, score, total_hosts):
    elements.append(NextPageTemplate('Portrait'))
    elements.append(PageBreak())
    elements.append(Paragraph("APPENDIX: METHODOLOGY & CRITERIA", styles['Heading1']))
    elements.append(Spacer(1, 8*mm))
    style_th = ParagraphStyle('THead', parent=styles['Normal'], textColor=colors.white)
    style_cell = ParagraphStyle('TCell', parent=styles['Normal'], fontSize=9, leading=11)
    style_c_w = ParagraphStyle('TCellW', parent=styles['Normal'], fontSize=9, leading=11, textColor=colors.white, alignment=TA_CENTER)
    style_c_b = ParagraphStyle('TCellB', parent=styles['Normal'], fontSize=9, leading=11, textColor=colors.black, alignment=TA_CENTER)
    elements.append(Paragraph("1. Health Score Interpretation", styles['Heading3']))
    data_interp = [["Score Range", "Level", "Meaning"], ["90% - 100%", "Excellent", "All systems are healthy or have minimal load."], ["70% - 89%", "Good / Fair", "System operational but monitor warning signs."], ["50% - 69%", "Warning", "Multiple hosts are degraded or critical resources detected."], ["0% - 49%", "Critical", "Major outages or widespread failures. Immediate action required."]]
    t_int = Table([[Paragraph(c, style_th) if i==0 else Paragraph(c, style_cell) for c in r] for i, r in enumerate(data_interp)], colWidths=[45*mm, 40*mm, 100*mm])
    t_int.setStyle(TableStyle([('BACKGROUND', (0,0), (-1,0), colors.HexColor(THEME_COLOR)), ('GRID', (0,0), (-1,-1), 0.25, colors.grey), ('ALIGN', (0,0), (-1,0), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BACKGROUND', (0,1), (0,1), colors.HexColor(COLOR_UP)), ('BACKGROUND', (0,2), (0,2), colors.HexColor("#FDE047")), ('BACKGROUND', (0,3), (0,3), colors.HexColor(COLOR_WARN)), ('BACKGROUND', (0,4), (0,4), colors.HexColor(COLOR_DEG))]))
    elements.append(t_int)
    elements.append(Spacer(1, 8*mm))
    elements.append(Paragraph("2. Status Definitions & Logic", styles['Heading3']))
    data_def = [
        [Paragraph("Status", style_th), Paragraph("Weight", style_th), Paragraph("Condition / Logic", style_th)],
        [Paragraph("<b>Up</b>", style_c_w), "10", Paragraph("Normal Operation. Online (< 6h). Resource Usage (CPU, MEM, <b>Max Disk Part</b>) < 80%.", style_cell)],
        [Paragraph("<b>Warning</b>", style_c_b), "7", Paragraph("High Load. Online. Resource Usage (CPU, MEM, <b>Max Disk Part</b>) 80% - 85%.", style_cell)],
        [Paragraph("<b>Degraded</b>", style_c_w), "3", Paragraph("Critical Load. Online. Resource Usage (CPU, MEM, <b>Max Disk Part</b>) >= 86%.", style_cell)],
        [Paragraph("<b>Offline</b>", style_c_w), "0", Paragraph("Connection Lost. No data > 6 Hours (Last Seen).", style_cell)]
    ]
    t_def = Table(data_def, colWidths=[35*mm, 20*mm, 130*mm])
    t_def.setStyle(TableStyle([('BACKGROUND', (0,0), (-1,0), colors.HexColor(THEME_COLOR)), ('GRID', (0,0), (-1,-1), 0.25, colors.grey), ('ALIGN', (0,0), (-1,0), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BACKGROUND', (0,1), (0,1), colors.HexColor(COLOR_UP)), ('BACKGROUND', (0,2), (0,2), colors.HexColor("#FDE047")), ('BACKGROUND', (0,3), (0,3), colors.HexColor(COLOR_DEG)), ('BACKGROUND', (0,4), (0,4), colors.HexColor(COLOR_OFF)), ('ALIGN', (1,1), (1,-1), 'CENTER')]))
    elements.append(t_def)
    elements.append(Spacer(1, 8*mm))
    elements.append(Paragraph("3. Calculation Trace (Current Report)", styles['Heading3']))
    t_fraction = Table([["SUM (Count x Weight)"], ["Total Hosts x 10"]], colWidths=[50*mm])
    t_fraction.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('LINEBELOW', (0,0), (-1,0), 1.2, colors.black), ('FONTSIZE', (0,0), (-1,-1), 9), ('BOTTOMPADDING', (0,0), (-1,0), 2), ('TOPPADDING', (0,1), (-1,1), 2)]))
    elements.append(Table([[Paragraph("<b>Health Score = </b>", styles['Normal']), t_fraction, Paragraph("<b> x 100</b>", styles['Normal'])]]))
    elements.append(Spacer(1, 6*mm))
    s_up, s_warn, s_deg, s_off = stats['Up'], stats['Warning'], stats['Degraded'], stats['Offline']
    w_sum = (s_up*10) + (s_warn*7) + (s_deg*3)
    max_score = total_hosts * 10
    final_color = colors.HexColor(COLOR_UP) if score>=90 else colors.HexColor("#FDE047") if score>=70 else colors.HexColor(COLOR_WARN) if score>=50 else colors.HexColor(COLOR_DEG)
    calc_data = [[Paragraph("<b>Current Data:</b>", styles['Normal']), Paragraph(f"Total: {total_hosts} | Up: {s_up} | Warn: {s_warn} | Deg: {s_deg} | Off: {s_off}", styles['Normal'])], [Paragraph("<b>Step 1: Weighted Sum</b>", styles['Normal']), Paragraph(f"({s_up}x10) + ({s_warn}x7) + ({s_deg}x3) + ({s_off}x0) = <b>{w_sum}</b>", styles['Normal'])], [Paragraph("<b>Step 2: Max Score</b>", styles['Normal']), Paragraph(f"{total_hosts} Hosts x 10 Points = <b>{max_score}</b>", styles['Normal'])], [Paragraph("<b>Step 3: Final Calculation</b>", styles['Normal']), Paragraph(f"({w_sum} / {max_score}) * 100 = <b>{score}%</b>", ParagraphStyle('Res', parent=styles['Normal'], textColor=final_color, fontSize=12))]]
    t_calc = Table(calc_data, colWidths=[50*mm, 120*mm])
    t_calc.setStyle(TableStyle([('BOX', (0,0), (-1,-1), 1, colors.grey), ('INNERGRID', (0,0), (-1,-1), 0.25, colors.lightgrey), ('BACKGROUND', (0,0), (0,-1), colors.HexColor("#F1F5F9")), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('PADDING', (0,0), (-1,-1), 6)]))
    elements.append(t_calc)
    elements.append(Spacer(1, 8*mm))
    elements.append(Paragraph("4. Standards Alignment & Compliance Support", styles['Heading3']))
    elements.append(Paragraph("This document is generated to serve as verifiable operational evidence, designed to support the organization's compliance with international standards regarding IT service management and information security.", styles['Normal']))
    elements.append(Spacer(1, 3*mm))
    std_items = ["<b>Metric Framework & Methodology:</b><br/>The Health Score is calculated based on <b>Apdex (Application Performance Index)</b> principles for performance scoring, aligned with <b>NIST SP 800-137</b> guidelines.", "<b>ISO/IEC 20000-1:2018 (IT Service Management):</b><br/>&bull; <b>Clause 9.2 (Service Reporting):</b> Provides comprehensive reports on service performance and suitability.", "<b>ISO/IEC 27001:2022 (Information Security):</b><br/>&bull; <b>Control 5.37 (Documented Operating Procedures):</b> Serves as a record of routine preventive maintenance.<br/>&bull; <b>Control 8.6 (Capacity Management):</b> Monitoring resource utilization (CPU, Memory, Disk) to prevent system overload."]
    for item in std_items: elements.append(Paragraph(item, ParagraphStyle('StdItem', parent=styles['Normal'], leading=14, spaceAfter=6)))

def create_signoff_page(elements, styles, conf):
    elements.append(PageBreak())
    elements.append(Paragraph("5. DOCUMENT SIGN-OFF", styles['Heading1']))
    elements.append(Spacer(1, 10*mm))
    elements.append(Paragraph("<b>Reference Service Report ID:</b> [_______________________]", styles['Normal']))
    elements.append(Spacer(1, 15*mm))
    style_th = ParagraphStyle('SigH', parent=styles['Normal'], fontName='Helvetica-Bold', fontSize=10)
    style_label = ParagraphStyle('SigL', parent=styles['Normal'], fontName='Helvetica-Bold', fontSize=10, spaceBefore=6)
    style_val = ParagraphStyle('SigV', parent=styles['Normal'], fontSize=10, spaceBefore=2)
    style_sign = ParagraphStyle('SigLine', parent=styles['Normal'], alignment=TA_CENTER, leading=14)
    sig_line = "<br/><br/>____________________________________<br/>(Signature)<br/><br/>"
    col_prov = [Paragraph(sig_line, style_sign), Paragraph("Name: ________________________________", style_label), Paragraph("Position: _____________________________", style_label), Paragraph("Company:", style_label), Paragraph(conf.get('provider_name'), style_val), Paragraph("Date: _______ / _______ / __________", style_label)]
    col_cust = [Paragraph(sig_line, style_sign), Paragraph("Name: ________________________________", style_label), Paragraph("Position: _____________________________", style_label), Paragraph("Company:", style_label), Paragraph(conf.get('client_name'), style_val), Paragraph("Date: _______ / _______ / __________", style_label)]
    sig_data = [[Paragraph("Verified By (Service Provider)", style_th), Paragraph("Acknowledged By (Customer)", style_th)], [col_prov, col_cust]]
    t_sig = Table(sig_data, colWidths=[85*mm, 85*mm])
    t_sig.setStyle(TableStyle([('BOX', (0,0), (0,-1), 1, colors.black), ('BOX', (1,0), (1,-1), 1, colors.black), ('VALIGN', (0,0), (-1,-1), 'TOP'), ('TOPPADDING', (0,0), (-1,-1), 10), ('BOTTOMPADDING', (0,0), (-1,-1), 15), ('LEFTPADDING', (0,0), (-1,-1), 10), ('RIGHTPADDING', (0,0), (-1,-1), 10), ('LINEBELOW', (0,0), (-1,0), 1, colors.black)]))
    elements.append(t_sig)
    elements.append(Spacer(1, 10*mm))
    elements.append(Paragraph("<i>Note: This report is a technical attachment to the maintenance service reference above.</i>", ParagraphStyle('Note', parent=styles['Normal'], fontSize=9, textColor=colors.gray)))

def create_pdf(filepath, rtype, data, stats, score, cycle, sel_str):
    doc = BaseDocTemplate(filepath, pagesize=A4)
    frame_portrait = Frame(10*mm, 15*mm, 190*mm, 272*mm, id='portrait_frame', showBoundary=0)
    frame_landscape = Frame(10*mm, 10*mm, 277*mm, 190*mm, id='landscape_frame', showBoundary=0)
    template_portrait = PageTemplate(id='Portrait', frames=frame_portrait, pagesize=A4)
    template_landscape = PageTemplate(id='Landscape', frames=frame_landscape, pagesize=landscape(A4))
    doc.addPageTemplates([template_portrait, template_landscape])
    styles = getSampleStyleSheet()
    elements = []
    conf = load_config()
    create_cover_page(elements, styles, stats, cycle, conf, rtype)
    create_preface_page(elements, styles, rtype, conf, sel_str)
    create_summary_section(elements, styles, data, stats, score, cycle)
    if rtype in ['select', 'full']:
        target_data = data
        if rtype == 'select':
            sel_list = [x.strip() for x in sel_str.split(',')]
            target_data = [d for d in data if d['hostname'] in sel_list]
        for host in target_data:
            details = fetch_host_details(host['hostname'])
            create_host_detail_page(elements, styles, host, details)
    create_appendix_page(elements, styles, stats, score, stats['Total'])
    if rtype == 'full': create_signoff_page(elements, styles, conf)
    def add_footer(canvas, doc):
        pw, ph = doc.pagesize
        if doc.page == 1: return 
        canvas.saveState()
        canvas.setFont('Helvetica', 8)
        canvas.setFillColor(colors.gray)
        footer_text = f"PM-Core Automation {TYPE_MAP.get(rtype, rtype).upper()} Report"
        if pw < ph: 
            canvas.drawString(10*mm, 10*mm, footer_text)
            page_num = doc.page - 1
            if page_num > 0: canvas.drawRightString(pw-10*mm, 10*mm, f"Page {page_num}")
            canvas.setStrokeColor(colors.HexColor(THEME_COLOR))
            canvas.line(10*mm, ph-12*mm, pw-10*mm, ph-12*mm)
            if os.path.exists(LOGO_PATH): canvas.drawImage(LOGO_PATH, pw-30*mm, ph-10*mm, width=20*mm, height=8*mm, mask='auto')
        canvas.restoreState()
    template_portrait.onPage = add_footer
    template_landscape.onPage = add_footer
    doc.build(elements)

def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--type", default="summary")
        parser.add_argument("--hosts", default="")
        parser.add_argument("--fmt", default="pdf")
        parser.add_argument("--cycle", default="")
        args = parser.parse_args()
        cycle = fetch_cycle_name() if not args.cycle else args.cycle
        hosts_list = [h.strip() for h in args.hosts.split(',')] if args.hosts else None
        data, stats, score = get_report_data(hosts_list if args.type == 'select' else None)
        ts = datetime.now().strftime("%Y%m%d-%H%M")
        host_lbl = ""
        if args.type == "select" and hosts_list:
            host_lbl = f"-{hosts_list[0]}"
            if len(hosts_list)>1: host_lbl += f"_plus_{len(hosts_list)-1}"
        filename = f"pmcore-{args.type}{host_lbl}-{ts}.pdf"
        filepath = os.path.join(REPORT_DIR, filename)
        sel_str = args.hosts.replace(",", ", ") if args.hosts else "-"
        create_pdf(filepath, args.type, data, stats, score, cycle, sel_str)
        print(filepath) 
    except Exception as e:
        sys.stderr.write(f"Error: {traceback.format_exc()}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
