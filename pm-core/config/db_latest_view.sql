-- /opt/pm-core/config/db_latest_view.sql
-- Create performant "latest per host" view & helpful indexes

PRAGMA foreign_keys=OFF;

-- Indexes: ช่วยเร่ง query/ORDER
CREATE INDEX IF NOT EXISTS idx_reports_hostname      ON reports(hostname);
CREATE INDEX IF NOT EXISTS idx_reports_processed     ON reports(processed_utc);
CREATE INDEX IF NOT EXISTS idx_reports_host_proc     ON reports(hostname, processed_utc);

-- View: เลือก "ล่าสุดต่อเครื่อง" กันซ้ำด้วย ROW_NUMBER()
DROP VIEW IF EXISTS latest_reports_v;
CREATE VIEW latest_reports_v AS
WITH ranked AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      PARTITION BY r.hostname
      ORDER BY r.processed_utc DESC, r.id DESC
    ) AS rn
  FROM reports r
  WHERE r.hostname IS NOT NULL
)
SELECT *
FROM ranked
WHERE rn = 1;

-- View: สรุปตัวเลขบนการ์ดจาก "ล่าสุดต่อเครื่อง"
DROP VIEW IF EXISTS latest_counts_v;
CREATE VIEW latest_counts_v AS
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN status='Up' THEN 1 ELSE 0 END)                  AS up,
  SUM(CASE WHEN status IN ('Degraded','Down') THEN 1 ELSE 0 END) AS degraded
FROM latest_reports_v;

