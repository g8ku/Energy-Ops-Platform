-- =============================================================================
-- schema.sql  –  Energy Operations Analytics Platform
-- =============================================================================
-- Creates the four core tables and a set of analytical views used by the
-- Python pipeline (analytics/pipeline.py) and the dashboard.
-- Compatible with SQLite and PostgreSQL (minor type changes needed for PG).
-- =============================================================================

-- ── Tables ───────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS assets (
    asset_id              TEXT PRIMARY KEY,
    asset_type            TEXT NOT NULL,          -- 'Wind Turbine' | 'Solar Array' | 'Substation'
    region                TEXT NOT NULL,
    capacity_kw           REAL NOT NULL,
    install_year          INTEGER,
    manufacturer          TEXT,
    rated_wind_speed_ms   REAL                    -- NULL for non-turbine assets
);

CREATE TABLE IF NOT EXISTS sensor_readings (
    reading_id      TEXT PRIMARY KEY,
    asset_id        TEXT NOT NULL REFERENCES assets(asset_id),
    date            DATE NOT NULL,
    output_kw       REAL,
    efficiency      REAL,                         -- 0.0 – 1.0
    temperature_c   REAL,
    vibration_mm    REAL,
    uptime_hours    REAL,
    UNIQUE(asset_id, date)
);

CREATE TABLE IF NOT EXISTS maintenance_logs (
    log_id              TEXT PRIMARY KEY,
    asset_id            TEXT NOT NULL REFERENCES assets(asset_id),
    date                DATE NOT NULL,
    maintenance_type    TEXT,                     -- Preventive | Corrective | Emergency | Inspection
    duration_hours      REAL,
    cost_cad            REAL,
    technician          TEXT,
    resolved            INTEGER,                  -- 1 = resolved, 0 = open
    notes               TEXT
);

CREATE TABLE IF NOT EXISTS efficiency_metrics (
    asset_id                TEXT NOT NULL REFERENCES assets(asset_id),
    month                   TEXT NOT NULL,        -- YYYY-MM
    avg_efficiency          REAL,
    total_output_kwh        REAL,
    avg_uptime              REAL,
    peak_output_kw          REAL,
    maintenance_events      INTEGER,
    total_maintenance_cost  REAL,
    maintenance_hours       REAL,
    asset_type              TEXT,
    region                  TEXT,
    capacity_kw             REAL,
    availability_pct        REAL,
    capacity_factor_pct     REAL,
    PRIMARY KEY (asset_id, month)
);


-- ── Analytical Views ──────────────────────────────────────────────────────────

-- Fleet-wide monthly performance summary
CREATE VIEW IF NOT EXISTS v_fleet_monthly AS
SELECT
    month,
    asset_type,
    COUNT(DISTINCT asset_id)            AS asset_count,
    ROUND(AVG(avg_efficiency) * 100, 2) AS avg_efficiency_pct,
    ROUND(SUM(total_output_kwh), 0)     AS total_output_kwh,
    ROUND(AVG(availability_pct), 2)     AS avg_availability_pct,
    ROUND(AVG(capacity_factor_pct), 2)  AS avg_capacity_factor_pct,
    SUM(maintenance_events)             AS total_maintenance_events,
    ROUND(SUM(total_maintenance_cost), 2) AS total_maintenance_cost_cad
FROM efficiency_metrics
GROUP BY month, asset_type
ORDER BY month, asset_type;


-- Assets ranked by efficiency (worst performers first — useful for targeting maintenance)
CREATE VIEW IF NOT EXISTS v_asset_efficiency_rank AS
SELECT
    asset_id,
    asset_type,
    region,
    ROUND(AVG(avg_efficiency) * 100, 2)    AS ytd_avg_efficiency_pct,
    ROUND(SUM(total_output_kwh), 0)        AS ytd_output_kwh,
    SUM(maintenance_events)                AS ytd_maintenance_events,
    ROUND(SUM(total_maintenance_cost), 2)  AS ytd_maintenance_cost_cad,
    ROUND(AVG(availability_pct), 2)        AS ytd_availability_pct,
    RANK() OVER (
        PARTITION BY asset_type
        ORDER BY AVG(avg_efficiency) ASC
    )                                      AS efficiency_rank_asc   -- 1 = worst
FROM efficiency_metrics
GROUP BY asset_id, asset_type, region;


-- Maintenance cost breakdown by type
CREATE VIEW IF NOT EXISTS v_maintenance_by_type AS
SELECT
    a.asset_type,
    m.maintenance_type,
    COUNT(*)                         AS event_count,
    ROUND(AVG(m.duration_hours), 1)  AS avg_duration_hrs,
    ROUND(SUM(m.cost_cad), 2)        AS total_cost_cad,
    ROUND(AVG(m.cost_cad), 2)        AS avg_cost_per_event_cad,
    SUM(CASE WHEN m.resolved = 1 THEN 1 ELSE 0 END) AS resolved_count
FROM maintenance_logs m
JOIN assets a ON m.asset_id = a.asset_id
GROUP BY a.asset_type, m.maintenance_type
ORDER BY total_cost_cad DESC;


-- Regional efficiency comparison
CREATE VIEW IF NOT EXISTS v_regional_efficiency AS
SELECT
    region,
    asset_type,
    COUNT(DISTINCT asset_id)                AS asset_count,
    ROUND(AVG(avg_efficiency) * 100, 2)     AS avg_efficiency_pct,
    ROUND(SUM(total_output_kwh) / 1000, 1)  AS total_output_mwh,
    ROUND(SUM(total_maintenance_cost), 2)   AS total_cost_cad
FROM efficiency_metrics
GROUP BY region, asset_type
ORDER BY avg_efficiency_pct DESC;


-- ── Useful Ad-hoc Queries ─────────────────────────────────────────────────────

-- Top 5 most expensive maintenance events
-- SELECT asset_id, date, maintenance_type, duration_hours, cost_cad
-- FROM maintenance_logs
-- ORDER BY cost_cad DESC
-- LIMIT 5;

-- Assets with efficiency below 75% in any month (potential flags)
-- SELECT asset_id, month, ROUND(avg_efficiency * 100, 2) AS efficiency_pct
-- FROM efficiency_metrics
-- WHERE avg_efficiency < 0.75
-- ORDER BY avg_efficiency ASC;

-- Monthly output trend for a specific asset
-- SELECT month, total_output_kwh, avg_efficiency, availability_pct
-- FROM efficiency_metrics
-- WHERE asset_id = 'WTG-001'
-- ORDER BY month;
