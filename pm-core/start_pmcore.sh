#!/bin/bash
# Start PM Core (Gunicorn) — ปรับ timeout เพื่อบรรเทา worker timeout error
cd /opt/pm-core/web || exit

# ลบ --graceful-timeout (ไม่รองรับใน gunicorn 23)
# ใช้เฉพาะ --timeout ที่เราต้องการเท่านั้น
gunicorn -w 2 -b 0.0.0.0:5000 app:app \
  --timeout 120 \
  > /opt/pm-core/web/gunicorn.log 2>&1 &

echo "✅ PM Core started on http://0.0.0.0:5000 (2 workers, timeout=120s)"

