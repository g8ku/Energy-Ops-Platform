"""
pipeline.py
-----------
Energy Operations Analytics Pipeline

Connects to the SQLite database, runs the core analytical queries,
computes KPIs, flags underperforming assets, and exports a summary
report to CSV. This module is also imported by the dashboard.

Usage:
    python analytics/pipeline.py

Outputs:
    data/report_summary.csv    — monthly KPI summary
    data/report_flags.csv      — assets flagged for review
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT     = Path(__file__).parent
DB_PATH  = ROOT / "energy_ops.db"
DATA_DIR = ROOT 


# ── Database helpers ──────────────────────────────────────────────────────────

def get_connection() -> sqlite3.Connection:
    """Return a SQLite connection with row_factory set for dict-like access."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Execute a SQL query and return results as a DataFrame."""
    with get_connection() as conn:
        return pd.read_sql_query(sql, conn, params=params)


# ── KPI Computations ──────────────────────────────────────────────────────────

def fleet_summary() -> dict:
    """
    Compute high-level fleet KPIs for the full year:
      - total output (MWh)
      - average fleet efficiency (%)
      - average availability (%)
      - total maintenance cost (CAD)
      - number of emergency events
    """
    df = query("SELECT * FROM efficiency_metrics")

    return {
        "total_output_mwh":       round(df["total_output_kwh"].sum() / 1000, 1),
        "avg_fleet_efficiency":   round(df["avg_efficiency"].mean() * 100, 2),
        "avg_availability_pct":   round(df["availability_pct"].mean(), 2),
        "total_maintenance_cost": round(df["total_maintenance_cost"].sum(), 2),
        "total_assets":           df["asset_id"].nunique(),
    }


def monthly_fleet_trend() -> pd.DataFrame:
    """
    Return month-by-month aggregated fleet performance, broken out
    by asset type. Used for the primary trend chart in the dashboard.
    """
    return query("""
        SELECT
            month,
            asset_type,
            ROUND(AVG(avg_efficiency) * 100, 2)      AS avg_efficiency_pct,
            ROUND(SUM(total_output_kwh) / 1000, 2)   AS output_mwh,
            ROUND(AVG(availability_pct), 2)           AS availability_pct,
            ROUND(AVG(capacity_factor_pct), 2)        AS capacity_factor_pct,
            SUM(maintenance_events)                   AS maintenance_events,
            ROUND(SUM(total_maintenance_cost), 2)     AS maintenance_cost_cad
        FROM efficiency_metrics
        GROUP BY month, asset_type
        ORDER BY month, asset_type
    """)


def asset_performance_ranking() -> pd.DataFrame:
    """
    Rank all assets by year-to-date average efficiency.
    Returns a DataFrame sorted worst → best so underperformers
    appear at the top for easy review.
    """
    return query("""
        SELECT
            em.asset_id,
            em.asset_type,
            em.region,
            ROUND(AVG(em.avg_efficiency) * 100, 2)    AS ytd_efficiency_pct,
            ROUND(SUM(em.total_output_kwh) / 1000, 2) AS ytd_output_mwh,
            SUM(em.maintenance_events)                 AS ytd_maintenance_events,
            ROUND(SUM(em.total_maintenance_cost), 2)   AS ytd_cost_cad,
            ROUND(AVG(em.availability_pct), 2)         AS ytd_availability_pct
        FROM efficiency_metrics em
        GROUP BY em.asset_id, em.asset_type, em.region
        ORDER BY ytd_efficiency_pct ASC
    """)


def maintenance_breakdown() -> pd.DataFrame:
    """
    Break down maintenance events by type and asset class,
    showing cost and resolution rate.
    """
    return query("""
        SELECT
            a.asset_type,
            m.maintenance_type,
            COUNT(*)                               AS event_count,
            ROUND(AVG(m.duration_hours), 1)        AS avg_duration_hrs,
            ROUND(SUM(m.cost_cad), 2)              AS total_cost_cad,
            ROUND(
                SUM(CASE WHEN m.resolved = 1 THEN 1.0 ELSE 0 END)
                / COUNT(*) * 100, 1
            )                                      AS resolution_rate_pct
        FROM maintenance_logs m
        JOIN assets a ON m.asset_id = a.asset_id
        GROUP BY a.asset_type, m.maintenance_type
        ORDER BY total_cost_cad DESC
    """)


def regional_comparison() -> pd.DataFrame:
    """Compare efficiency and output across grid regions."""
    return query("""
        SELECT
            region,
            asset_type,
            COUNT(DISTINCT asset_id)                   AS assets,
            ROUND(AVG(avg_efficiency) * 100, 2)        AS avg_efficiency_pct,
            ROUND(SUM(total_output_kwh) / 1000, 1)     AS total_output_mwh,
            ROUND(SUM(total_maintenance_cost), 2)      AS total_cost_cad
        FROM efficiency_metrics
        GROUP BY region, asset_type
        ORDER BY avg_efficiency_pct DESC
    """)


def flag_underperformers(efficiency_threshold: float = 0.78) -> pd.DataFrame:
    """
    Identify assets whose YTD average efficiency falls below the given
    threshold. These are candidates for maintenance review or inspection.

    Parameters
    ----------
    efficiency_threshold : float
        Efficiency ratio below which an asset is flagged (default 0.78 = 78%).
    """
    df = asset_performance_ranking()
    flagged = df[df["ytd_efficiency_pct"] < efficiency_threshold * 100].copy()
    flagged["flag_reason"] = flagged["ytd_efficiency_pct"].apply(
        lambda e: "Critical (<70%)" if e < 70 else "Below threshold"
    )
    return flagged.reset_index(drop=True)


def sensor_anomalies() -> pd.DataFrame:
    """
    Detect sensor readings where vibration or temperature exceed
    normal operating thresholds — a proxy for fault detection.
    """
    return query("""
        SELECT
            s.asset_id,
            s.date,
            s.temperature_c,
            s.vibration_mm,
            s.efficiency,
            a.asset_type,
            a.region,
            CASE
                WHEN s.vibration_mm > 2.5 AND s.temperature_c > 70
                    THEN 'High Risk'
                WHEN s.vibration_mm > 2.5 OR s.temperature_c > 70
                    THEN 'Medium Risk'
                ELSE 'Low Risk'
            END AS risk_level
        FROM sensor_readings s
        JOIN assets a ON s.asset_id = a.asset_id
        WHERE s.vibration_mm > 2.5 OR s.temperature_c > 70
        ORDER BY s.vibration_mm DESC, s.temperature_c DESC
        LIMIT 500
    """)


# ── Report Export ─────────────────────────────────────────────────────────────

def export_reports():
    """Generate and save CSV reports to the data directory."""
    monthly_fleet_trend().to_csv(DATA_DIR / "report_monthly_trend.csv", index=False)
    asset_performance_ranking().to_csv(DATA_DIR / "report_asset_ranking.csv", index=False)
    maintenance_breakdown().to_csv(DATA_DIR / "report_maintenance.csv", index=False)
    flag_underperformers().to_csv(DATA_DIR / "report_flags.csv", index=False)
    regional_comparison().to_csv(DATA_DIR / "report_regional.csv", index=False)


# ── CLI Entry Point ───────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  Energy Operations Analytics Pipeline")
    print(f"  Run date: {date.today()}")
    print("=" * 55)

    # Fleet summary
    kpis = fleet_summary()
    print("\n── Fleet KPIs (YTD) ──────────────────────────────────")
    for key, val in kpis.items():
        label = key.replace("_", " ").title()
        print(f"  {label:<30} {val:>12,}")

    # Underperformers
    flags = flag_underperformers()
    print(f"\n── Underperforming Assets (efficiency < 78%) ─────────")
    if flags.empty:
        print("  None flagged — all assets above threshold.")
    else:
        print(flags[["asset_id", "asset_type", "region", "ytd_efficiency_pct", "flag_reason"]].to_string(index=False))

    # Anomalies
    anomalies = sensor_anomalies()
    high_risk = anomalies[anomalies["risk_level"] == "High Risk"]
    print(f"\n── Sensor Anomaly Summary ────────────────────────────")
    print(f"  Total anomalous readings : {len(anomalies):,}")
    print(f"  High-risk readings       : {len(high_risk):,}")

    # Export
    print("\n── Exporting Reports ─────────────────────────────────")
    export_reports()
    print("  ✓ report_monthly_trend.csv")
    print("  ✓ report_asset_ranking.csv")
    print("  ✓ report_maintenance.csv")
    print("  ✓ report_flags.csv")
    print("  ✓ report_regional.csv")
    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
