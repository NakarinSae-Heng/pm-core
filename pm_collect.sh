#!/bin/bash
# ==========================================================
# pm_collect.sh  (PM-Core client)
# Version: 2025-11-01-planB-memdetail-fix1
# - อ่าน /etc/pm-collect.conf
# - ส่ง metric หลักเป็น %
# - เพิ่ม mem_detail.txt (MB + %)
# - เก็บ mounts/top/lvm/ntp/services/disks
# - ส่งด้วย SCP
# ==========================================================

CONF_FILE="/etc/pm-collect.conf"

if [ -f "$CONF_FILE" ]; then
    . "$CONF_FILE"
else
    echo "[ERR] Config file $CONF_FILE not found. Abort."
    exit 1
fi

if [ -z "$PMCORE_HOST" ] || [ -z "$PMCORE_USER" ] || [ -z "$REMOTE_DIR" ]; then
    echo "[ERR] Missing PMCORE_HOST/USER/REMOTE_DIR in $CONF_FILE"
    exit 1
fi

PMCORE_PORT=${PMCORE_PORT:-22}
REMOTE_USER=${REMOTE_USER:-$PMCORE_USER}

HOSTNAME=$(hostname)
TS=$(date +"%Y%m%d_%H%M%S")
WORKDIR="/tmp/pm_${HOSTNAME}_${TS}"
ARCHIVE="pm_${HOSTNAME}_${TS}.tar"
ARCHIVE_GZ="${ARCHIVE}.gz"
LOGFILE="/tmp/pm_collect.log"

mkdir -p "$WORKDIR"

echo "[INFO] Collecting snapshot at $TS" | tee "$LOGFILE"

# ----------------------------------------------------------
# 1) Basic info + time
# ----------------------------------------------------------
date "+%Y-%m-%d %H:%M:%S %z" > "${WORKDIR}/client_time_local.txt"
date -u "+%Y-%m-%d %H:%M:%S UTC" > "${WORKDIR}/client_time_utc.txt"
date -u "+%Y-%m-%d %H:%M:%S" > "${WORKDIR}/collected_utc.txt"

hostname > "${WORKDIR}/hostname.txt"
cat /etc/machine-id 2>/dev/null > "${WORKDIR}/machine_id.txt"
# Host ID (for reports.hostid) - use `hostid` command if available
if command -v hostid >/dev/null 2>&1; then
  hostid 2>/dev/null > "${WORKDIR}/hostid.txt"
else
  # fallback (some systems may not have hostid in PATH)
  cat /etc/hostid 2>/dev/null > "${WORKDIR}/hostid.txt"
fi
cat /etc/os-release 2>/dev/null > "${WORKDIR}/os_release.txt"
uname -r 2>/dev/null > "${WORKDIR}/kernel.txt"

if uptime -p >/dev/null 2>&1; then
  uptime -p | sed 's/^up //g' > "${WORKDIR}/uptime.txt"
else
  uptime > "${WORKDIR}/uptime.txt"
fi

# ----------------------------------------------------------
# 2) Metrics หลัก (เป็น %)
# ----------------------------------------------------------

# CPU usage (%)
read -r _ user nice system idle iowait irq softirq steal guest guest_nice < /proc/stat
sleep 1
read -r _ user2 nice2 system2 idle2 iowait2 irq2 softirq2 steal2 guest2 guest_nice2 < /proc/stat
TOTAL=$(( (user2-user) + (nice2-nice) + (system2-system) + (idle2-idle) + (iowait2-iowait) + (irq2-irq) + (softirq2-softirq) + (steal2-steal) ))
IDLE=$(( (idle2-idle) + (iowait2-iowait) ))
CPU_PCT=0
if [ "$TOTAL" -gt 0 ]; then
  CPU_PCT=$(( 100 * (TOTAL - IDLE) / TOTAL ))
fi
echo "$CPU_PCT" > "${WORKDIR}/cpu.txt"

# Memory usage (%) + รายละเอียดเป็น MB
MT=$(awk '/MemTotal:/ {print $2}' /proc/meminfo)        # kB
MA=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo)    # kB
MEM_PCT=0
if [ -n "$MT" ] && [ "$MT" -gt 0 ]; then
  USED=$(( MT - MA ))                  # kB
  MEM_PCT=$(( 100 * USED / MT ))
else
  USED=0
fi
echo "$MEM_PCT" > "${WORKDIR}/mem.txt"

USED_MB=$(( USED / 1024 ))
FREE_MB=$(( MA / 1024 ))
TOTAL_MB=$(( MT / 1024 ))
{
  echo "USED_MB=${USED_MB}"
  echo "FREE_MB=${FREE_MB}"
  echo "TOTAL_MB=${TOTAL_MB}"
  echo "USED_PCT=${MEM_PCT}"
} > "${WORKDIR}/mem_detail.txt"

# Swap usage (%)
SWAP_TOTAL=$(awk '/SwapTotal:/ {print $2}' /proc/meminfo)
SWAP_FREE=$(awk '/SwapFree:/ {print $2}' /proc/meminfo)
SWAP_PCT=0
if [ -n "$SWAP_TOTAL" ] && [ "$SWAP_TOTAL" -gt 0 ]; then
  SWAP_USED=$(( SWAP_TOTAL - SWAP_FREE ))
  SWAP_PCT=$(( 100 * SWAP_USED / SWAP_TOTAL ))
fi
echo "$SWAP_PCT" > "${WORKDIR}/swap_pct.txt"

# Disk root (% used)
df -P / | awk 'NR==2 {gsub("%","",$5); print $5}' > "${WORKDIR}/disk_root.txt"

# ----------------------------------------------------------
# 2.1) System / Hardware / CPU detail
# ----------------------------------------------------------

SYS_HW_FILE="${WORKDIR}/system_hw.txt"
CPU_DETAIL_FILE="${WORKDIR}/cpu_detail.txt"

# --- Virtualization / Hardware Info ---
virt_type="unknown"

if command -v systemd-detect-virt >/dev/null 2>&1; then
    virt_raw=$(systemd-detect-virt 2>/dev/null || echo "none")
    if [ "$virt_raw" = "none" ] || [ "$virt_raw" = "openvz" ]; then
        virt_type="physical"
    else
        virt_type="$virt_raw"
    fi
fi

manufacturer=""
product_name=""
serial_number=""

# Prefer sysfs (works without sudo on most distros/VMs)
if [ -r /sys/class/dmi/id/sys_vendor ]; then
  manufacturer=$(cat /sys/class/dmi/id/sys_vendor 2>/dev/null | head -n 1)
fi
if [ -r /sys/class/dmi/id/product_name ]; then
  product_name=$(cat /sys/class/dmi/id/product_name 2>/dev/null | head -n 1)
fi

# --- Serial Number (root-only on many distros; add fallbacks for pmusr) ---
serial_number=""

# 1) direct read (if readable)
if [ -r /sys/class/dmi/id/product_serial ]; then
    serial_number=$(cat /sys/class/dmi/id/product_serial 2>/dev/null | head -n 1)
fi

# 2) sudo cat (only if allowed; do not prompt password)
if [ -z "$serial_number" ] && command -v sudo >/dev/null 2>&1; then
    serial_number=$(sudo -n /bin/cat /sys/class/dmi/id/product_serial 2>/dev/null | head -n 1)
fi

# 3) dmidecode fallback (only if root OR sudo allowed; do not prompt password)
if [ -z "$serial_number" ]; then
    if [ "$(/usr/bin/id -u)" -eq 0 ] && command -v dmidecode >/dev/null 2>&1; then
        serial_number=$(dmidecode -s system-serial-number 2>/dev/null | head -n 1)
    elif command -v sudo >/dev/null 2>&1 && command -v dmidecode >/dev/null 2>&1; then
        serial_number=$(sudo -n dmidecode -s system-serial-number 2>/dev/null | head -n 1)
    fi
fi

# normalize common empty/meaningless values to "-"
case "$serial_number" in
    ""|"None"|"none"|"Not Specified"|"NotSpecified")
        serial_number="-"
        ;;
esac



{
    echo "VIRT_TYPE=${virt_type}"
    [ -n "$manufacturer" ] && echo "MANUFACTURER=${manufacturer}"
    [ -n "$product_name" ] && echo "PRODUCT_NAME=${product_name}"
    [ -n "$serial_number" ] && echo "SERIAL_NUMBER=${serial_number}"
} > "${SYS_HW_FILE}"

# --- CPU Detail ---
# หน่วยความถี่: MHz (ตัวเลขลอย เช่น 2100.000)
# จำนวน core / thread ใช้ค่าตรงจาก lscpu

cpu_model=""
cpu_arch=""
cpu_sockets=""
cpu_cores_per_socket=""
cpu_threads_per_core=""
cpu_mhz=""

if command -v lscpu >/dev/null 2>&1; then
    cpu_model=$(lscpu 2>/dev/null | awk -F: '/Model name/ {sub(/^ +/,"",$2); print $2; exit}')
    cpu_arch=$(lscpu 2>/dev/null | awk -F: '/Architecture/ {sub(/^ +/,"",$2); print $2; exit}')
    cpu_sockets=$(lscpu 2>/dev/null | awk -F: '/Socket\(s\)/ {sub(/^ +/,"",$2); print $2; exit}')
    cpu_cores_per_socket=$(lscpu 2>/dev/null | awk -F: '/Core\(s\) per socket/ {sub(/^ +/,"",$2); print $2; exit}')
    cpu_threads_per_core=$(lscpu 2>/dev/null | awk -F: '/Thread\(s\) per core/ {sub(/^ +/,"",$2); print $2; exit}')
    cpu_mhz=$(lscpu 2>/dev/null | awk -F: '/CPU MHz/ {sub(/^ +/,"",$2); print $2; exit}')
fi

{
    [ -n "$cpu_model" ] && echo "MODEL=${cpu_model}"
    [ -n "$cpu_arch" ] && echo "ARCH=${cpu_arch}"
    [ -n "$cpu_sockets" ] && echo "SOCKETS=${cpu_sockets}"
    [ -n "$cpu_cores_per_socket" ] && echo "CORES_PER_SOCKET=${cpu_cores_per_socket}"
    [ -n "$cpu_threads_per_core" ] && echo "THREADS_PER_CORE=${cpu_threads_per_core}"
    [ -n "$cpu_mhz" ] && echo "CPU_MHZ=${cpu_mhz}"
} > "${CPU_DETAIL_FILE}"


# ----------------------------------------------------------
# 3) Network
# ----------------------------------------------------------
hostname -I 2>/dev/null | awk '{print $1}' > "${WORKDIR}/ip.txt"

if command -v ip >/dev/null 2>&1; then
    ip -4 addr show 2>/dev/null > "${WORKDIR}/ip_all.txt"
else
    /sbin/ip -4 addr show 2>/dev/null > "${WORKDIR}/ip_all.txt"
fi

: > "${WORKDIR}/net_link.txt"
if ls /sys/class/net >/dev/null 2>&1; then
  for iface in $(ls /sys/class/net); do
      state=$(cat /sys/class/net/$iface/operstate 2>/dev/null)
      speed=$(cat /sys/class/net/$iface/speed 2>/dev/null)
      echo "$iface|${state:-unknown}|${speed:-unknown}" >> "${WORKDIR}/net_link.txt"
  done
fi

# ----------------------------------------------------------
# 3.1) Network extra (gateway + DNS)
# ----------------------------------------------------------
NET_EXTRA_FILE="${WORKDIR}/network_extra.txt"

# default gateway
gateway_ip=""
if command -v ip >/dev/null 2>&1; then
    gateway_ip=$(ip route 2>/dev/null | awk '/^default/ {print $3; exit}')
else
    gateway_ip=$(/sbin/ip route 2>/dev/null | awk '/^default/ {print $3; exit}')
fi

# DNS (จาก resolv.conf)
dns_list=""
if [ -f /etc/resolv.conf ]; then
    dns_list=$(awk '/^nameserver/ {printf("%s%s", (NR>1?",":""), $2)} END {print ""}' /etc/resolv.conf)
fi

{
    [ -n "$gateway_ip" ] && echo "GATEWAY=${gateway_ip}"
    [ -n "$dns_list" ] && echo "DNS=${dns_list}"
} > "${NET_EXTRA_FILE}"

# ----------------------------------------------------------
# 4) Mounts
# ----------------------------------------------------------
df -hP -T | awk 'NR>1 {print $7"|"$2"|"$3"|"$4"|"$5"|"$6}' > "${WORKDIR}/mounts.txt"

# ----------------------------------------------------------
# 5) Top processes
# ----------------------------------------------------------
ps -eo pid,comm,pcpu,pmem --sort=-pcpu | head -n 11 > "${WORKDIR}/top_cpu.txt"
ps -eo pid,comm,pcpu,pmem --sort=-pmem | head -n 11 > "${WORKDIR}/top_mem.txt"

# ----------------------------------------------------------
# 6) LVM
# ----------------------------------------------------------
if command -v vgs >/dev/null 2>&1; then
    sudo vgs --noheadings --units g --separator '|' \
      -o vg_name,lv_count,pv_count,vg_size,vg_free 2>/dev/null > "${WORKDIR}/lvm_vgs.txt"
else
    : > "${WORKDIR}/lvm_vgs.txt"
fi

if command -v lvs >/dev/null 2>&1; then
    sudo lvs --noheadings --units g --separator '|' \
      -o lv_name,vg_name,lv_size 2>/dev/null > "${WORKDIR}/lvm_lvs.txt"
else
    : > "${WORKDIR}/lvm_lvs.txt"
fi

if command -v pvs >/dev/null 2>&1; then
    sudo pvs --noheadings --units g --separator '|' \
      -o pv_name,vg_name,pv_size,pv_free 2>/dev/null > "${WORKDIR}/lvm_pvs.txt"
else
    : > "${WORKDIR}/lvm_pvs.txt"
fi

# ----------------------------------------------------------
# 7) Time sync detect
# ----------------------------------------------------------
NTP_FILE="${WORKDIR}/ntp_status.txt"
if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet chronyd 2>/dev/null; then
    echo "TYPE=CHRONY" > "$NTP_FILE"
    chronyc tracking 2>/dev/null >> "$NTP_FILE"
elif command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet ntpd 2>/dev/null; then
    echo "TYPE=NTP" > "$NTP_FILE"
    ntpq -p 2>/dev/null >> "$NTP_FILE"
elif command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet systemd-timesyncd 2>/dev/null; then
    echo "TYPE=SYSTEMD" > "$NTP_FILE"
    timedatectl show 2>/dev/null | grep -E 'SystemClockSynchronized|NTPSynchronized|Timezone' >> "$NTP_FILE"
else
    echo "TYPE=UNKNOWN" > "$NTP_FILE"
    echo "No time sync service detected" >> "$NTP_FILE"
fi

# ----------------------------------------------------------
# 8) Optional: services + disks
# ----------------------------------------------------------
# services (ตรงนี้แหละที่สะกดผิด ตอนนี้แก้แล้วเป็น /dev/null)
if command -v systemctl >/dev/null 2>&1; then
    systemctl list-units --type=service --state=running --no-pager 2>/dev/null > "${WORKDIR}/services.txt"
else
    : > "${WORKDIR}/services.txt"
fi

# disks (inventory)
if command -v lsblk >/dev/null 2>&1; then
    lsblk -o NAME,TYPE,SIZE,MOUNTPOINT 2>/dev/null > "${WORKDIR}/disks.txt"
else
    : > "${WORKDIR}/disks.txt"
fi

# ----------------------------------------------------------
# 8.1) Repository / Patch status
# ----------------------------------------------------------
REPO_STATUS_FILE="${WORKDIR}/repo_status.txt"

pkg_mgr=""
if command -v dnf >/dev/null 2>&1; then
    pkg_mgr="dnf"
elif command -v yum >/dev/null 2>&1; then
    pkg_mgr="yum"
fi

enabled_repos=""
updates_available=""
last_update=""

# --- enabled repos count ---
if [ "$pkg_mgr" = "dnf" ]; then
    enabled_repos=$(dnf repolist enabled 2>/dev/null | awk 'END{print (NF>0 ? NR-1 : 0)}')
elif [ "$pkg_mgr" = "yum" ]; then
    enabled_repos=$(yum repolist enabled 2>/dev/null | awk 'END{print (NF>0 ? NR-1 : 0)}')
fi

# --- updates available (rough but deterministic count) ---
# นับเฉพาะบรรทัดที่มีรูปแบบ "pkg arch version repo"
tmp_updates="${WORKDIR}/_check_updates.tmp"

if [ "$pkg_mgr" = "dnf" ]; then
    dnf -q check-update >"$tmp_updates" 2>/dev/null
    rc=$?
    if [ $rc -eq 0 ]; then
        updates_available=0
    elif [ $rc -eq 100 ]; then
        updates_available=$(awk '
            /^[[:space:]]*$/        {next}
            /^Last metadata/        {next}
            /^Obsoleting Packages/  {exit}
            /^Security:/            {next}
            /^Upgrade[[:space:]]/   {next}
            /^Loaded plugins:/      {next}
            NF>=3                   {c++}
            END {print (c+0)}
        ' "$tmp_updates")
    else
        updates_available="unknown"
    fi
elif [ "$pkg_mgr" = "yum" ]; then
    yum -q check-update >"$tmp_updates" 2>/dev/null
    rc=$?
    if [ $rc -eq 0 ]; then
        updates_available=0
    elif [ $rc -eq 100 ]; then
        updates_available=$(awk '
            /^[[:space:]]*$/       {next}
            /^Loaded plugins:/     {next}
            /^Security:/           {next}
            /^Obsoleting Packages/ {exit}
            NF>=3                  {c++}
            END {print (c+0)}
        ' "$tmp_updates")
    else
        updates_available="unknown"
    fi
fi

# --- last update date (จาก yum.log หรือ dnf.log ถ้ามี) ---
if [ -f /var/log/dnf.log ]; then
    last_update=$(grep -E "Upgrade:|Updated:" /var/log/dnf.log 2>/dev/null | awk 'END{print $1}')
elif [ -f /var/log/yum.log ]; then
    last_update=$(grep -E "Updated:|Upgrade:" /var/log/yum.log 2>/dev/null | awk 'END{print $1}')
fi

rm -f "$tmp_updates" 2>/dev/null || true

  # --- capture repolist (enabled) ---
  repolist_raw=""
  if [ "$pkg_mgr" = "dnf" ]; then
      repolist_raw=$(dnf repolist enabled 2>&1 || true)
  elif [ "$pkg_mgr" = "yum" ]; then
      repolist_raw=$(yum repolist enabled 2>&1 || true)
  fi
  repolist_b64=$(printf "%s" "$repolist_raw" | base64 -w0 2>/dev/null || printf "%s" "$repolist_raw" | base64 2>/dev/null | tr -d '\n')

  # --- capture repo source (enabled=1) from /etc/yum.repos.d/*.repo ---
  reposrc_raw=""
  if [ -d /etc/yum.repos.d ]; then
      # format: repoid|baseurl|metalink|mirrorlist (only enabled=1 blocks)
      reposrc_raw=$(awk '
        BEGIN{RS="\\n\\["; FS="\\n"}
        {
          # restore [section]
          section="["$1
          sub(/^[[]/,"",section); sub(/].*/,"",section)
          enabled=""; baseurl=""; metalink=""; mirrorlist=""
          for(i=2;i<=NF;i++){
            line=$i
            sub(/^[ \t]+/,"",line)
            if(line ~ /^enabled[ \t]*=/){ split(line,a,"="); enabled=a[2] }
            else if(line ~ /^baseurl[ \t]*=/){ split(line,a,"="); baseurl=a[2] }
            else if(line ~ /^metalink[ \t]*=/){ split(line,a,"="); metalink=a[2] }
            else if(line ~ /^mirrorlist[ \t]*=/){ split(line,a,"="); mirrorlist=a[2] }
          }
          gsub(/[ \t\r]/,"",enabled)
          if(enabled=="1"){
            print section "|" baseurl "|" metalink "|" mirrorlist
          }
        }
      ' /etc/yum.repos.d/*.repo 2>/dev/null || true)
  fi
  reposrc_b64=$(printf "%s" "$reposrc_raw" | base64 -w0 2>/dev/null || printf "%s" "$reposrc_raw" | base64 2>/dev/null | tr -d '\n')

  # --- makecache health probe (Phase 1) ---
  makecache_out=""
  makecache_rc=127
  if [ "$pkg_mgr" = "dnf" ]; then
      makecache_out=$(dnf -q makecache --refresh 2>&1)
      makecache_rc=$?
  elif [ "$pkg_mgr" = "yum" ]; then
      makecache_out=$(yum -q makecache 2>&1)
      makecache_rc=$?
  else
      makecache_out="no-pkg-manager"
      makecache_rc=127
  fi
  makecache_out_b64=$(printf "%s" "$makecache_out" | base64 -w0 2>/dev/null || printf "%s" "$makecache_out" | base64 2>/dev/null | tr -d '\n')

    # --- derive repo health (Phase 2) ---
    repo_health="FAIL"
    repo_health_reason="ERROR"

    if [ "${makecache_rc:-127}" -eq 0 ]; then
        repo_health="OK"
        repo_health_reason="OK"
    else
        # classify common errors from makecache output
        if printf "%s" "$makecache_out" | grep -qiE "Could not resolve host|Couldn't resolve host|Curl error \(6\)"; then
            repo_health_reason="DNS_FAIL"
        elif printf "%s" "$makecache_out" | grep -qiE "Connection timed out|Timed out"; then
            repo_health_reason="NET_TIMEOUT"
        elif printf "%s" "$makecache_out" | grep -qiE "No route to host|Network is unreachable"; then
            repo_health_reason="NET_UNREACHABLE"
        elif printf "%s" "$makecache_out" | grep -qiE "Cannot download repomd\.xml|All mirrors were tried"; then
            repo_health_reason="REPO_MD_FAIL"
        elif printf "%s" "$makecache_out" | grep -qiE "Failed to download metadata|Cannot prepare internal mirrorlist|Cannot download metadata"; then
            repo_health_reason="METADATA_FAIL"
        elif [ "${makecache_rc:-127}" -eq 127 ] || printf "%s" "$makecache_out" | grep -qiE "no-pkg-manager"; then
            repo_health_reason="NO_PKG_MGR"
        else
            repo_health_reason="ERROR"
        fi
    fi

  {
      [ -n "$pkg_mgr" ] && echo "PKG_MANAGER=${pkg_mgr}"
      [ -n "$enabled_repos" ] && echo "ENABLED_REPOS=${enabled_repos}"
      [ -n "$updates_available" ] && echo "UPDATES_AVAILABLE=${updates_available}"
      echo "LAST_UPDATE=${last_update:-unknown}"
      echo "REPO_HEALTH=${repo_health}"
      echo "REPO_HEALTH_REASON=${repo_health_reason}"

      echo "REPOLIST_B64=${repolist_b64}"
      echo "REPOSRC_B64=${reposrc_b64}"
      echo "MAKECACHE_RC=${makecache_rc}"
      echo "MAKECACHE_OUT_B64=${makecache_out_b64}"
  } > "${REPO_STATUS_FILE}"


# ----------------------------------------------------------
# 9) Pack
# ----------------------------------------------------------
cd /tmp || exit 1
tar cf "$ARCHIVE" -C "$WORKDIR" .
gzip "$ARCHIVE"

# ----------------------------------------------------------
# 10) Upload (SCP)
# ----------------------------------------------------------
echo "[INFO] Uploading to ${REMOTE_USER}@${PMCORE_HOST}:${REMOTE_DIR} ..." | tee -a "$LOGFILE"
scp -o StrictHostKeyChecking=no -P "$PMCORE_PORT" "$ARCHIVE_GZ" \
    "${REMOTE_USER}@${PMCORE_HOST}:${REMOTE_DIR}/" 2>>"$LOGFILE"

if [ $? -eq 0 ]; then
    echo "[OK] Upload success: $ARCHIVE_GZ" | tee -a "$LOGFILE"
else
    echo "[ERR] Upload failed" | tee -a "$LOGFILE"
fi

# ----------------------------------------------------------
# 11) Cleanup
# ----------------------------------------------------------
rm -rf "$WORKDIR"
exit 0



