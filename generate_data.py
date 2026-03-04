"""
generate_data.py
----------------
Generates realistic synthetic operational data for the Energy Operations Analytics Platform.
Simulates 12 months of sensor readings, maintenance logs, and efficiency metrics
across a fleet of energy assets (wind turbines, solar arrays, substations).

Usage:
    python data/generate_data.py

Outputs:
    data/assets.csv
    data/sensor_readings.csv
    data/maintenance_logs.csv
    data/efficiency_metrics.csv
"""

import pandas as pd
import numpy as np
import sqlite3
from pathlib import Path
import random
from datetime import datetime, timedelta

# ── Reproducibility ──────────────────────────────────────────────────────────
SEED = 42
np.random.seed(SEED)
random.seed(SEED)

# ── Constants ─────────────────────────────────────────────────────────────────
START_DATE = datetime(2023, 1, 1)
END_DATE   = datetime(2023, 12, 31)
N_TURBINES = 12
N_SOLAR    = 8
N_SUBSTATIONS = 4

ASSET_REGIONS = ["Northern Grid", "Southern Grid", "Eastern Grid", "Western Grid"]
MAINTENANCE_TYPES = ["Preventive", "Corrective", "Emergency", "Inspection"]
TECHNICIANS = [f"Tech_{i:02d}" for i in range(1, 9)]

OUTPUT_DIR = Path(__file__).parent


# ── 1. Assets ─────────────────────────────────────────────────────────────────
def generate_assets() -> pd.DataFrame:
    records = []

    for i in range(1, N_TURBINES + 1):
        records.append({
            "asset_id":       f"WTG-{i:03d}",
            "asset_type":     "Wind Turbine",
            "region":         random.choice(ASSET_REGIONS),
            "capacity_kw":    random.choice([2000, 2500, 3000, 3500]),
            "install_year":   random.randint(2010, 2020),
            "manufacturer":   random.choice(["Vestas", "Siemens Gamesa", "GE Renewable"]),
            "rated_wind_speed_ms": random.uniform(11, 14),
        })

    for i in range(1, N_SOLAR + 1):
        records.append({
            "asset_id":       f"SPV-{i:03d}",
            "asset_type":     "Solar Array",
            "region":         random.choice(ASSET_REGIONS),
            "capacity_kw":    random.choice([500, 750, 1000, 1250]),
            "install_year":   random.randint(2015, 2022),
            "manufacturer":   random.choice(["SunPower", "First Solar", "Canadian Solar"]),
            "rated_wind_speed_ms": None,
        })

    for i in range(1, N_SUBSTATIONS + 1):
        records.append({
            "asset_id":       f"SUB-{i:03d}",
            "asset_type":     "Substation",
            "region":         random.choice(ASSET_REGIONS),
            "capacity_kw":    random.choice([10000, 15000, 20000]),
            "install_year":   random.randint(2005, 2018),
            "manufacturer":   random.choice(["ABB", "Siemens", "Schneider Electric"]),
            "rated_wind_speed_ms": None,
        })

    return pd.DataFrame(records)


# ── 2. Sensor Readings (daily, per asset) ─────────────────────────────────────
def generate_sensor_readings(assets: pd.DataFrame) -> pd.DataFrame:
    records = []
    dates = pd.date_range(START_DATE, END_DATE, freq="D")

    for _, asset in assets.iterrows():
        base_efficiency = np.random.uniform(0.78, 0.95)

        for date in dates:
            # Seasonal factor: wind turbines peak in winter/spring, solar in summer
            day_of_year = date.timetuple().tm_yday
            if asset["asset_type"] == "Wind Turbine":
                seasonal = 1.0 + 0.15 * np.cos(2 * np.pi * (day_of_year - 30) / 365)
            elif asset["asset_type"] == "Solar Array":
                seasonal = 1.0 + 0.25 * np.sin(2 * np.pi * (day_of_year - 80) / 365)
            else:
                seasonal = 1.0

            # Random daily noise + occasional fault dip
            noise = np.random.normal(0, 0.03)
            fault_dip = -0.25 if np.random.rand() < 0.02 else 0.0

            efficiency = float(np.clip(base_efficiency * seasonal + noise + fault_dip, 0.1, 1.0))
            output_kw  = asset["capacity_kw"] * efficiency * np.random.uniform(0.6, 1.0)
            temperature_c = np.random.uniform(20, 80) + (20 if fault_dip else 0)
            vibration_mm  = np.random.exponential(0.5) + (3.0 if fault_dip else 0)

            records.append({
                "reading_id":    f"{asset['asset_id']}-{date.strftime('%Y%m%d')}",
                "asset_id":      asset["asset_id"],
                "date":          date.date(),
                "output_kw":     round(output_kw, 2),
                "efficiency":    round(efficiency, 4),
                "temperature_c": round(temperature_c, 1),
                "vibration_mm":  round(vibration_mm, 3),
                "uptime_hours":  round(np.random.uniform(18, 24), 1),
            })

    return pd.DataFrame(records)


# ── 3. Maintenance Logs ───────────────────────────────────────────────────────
def generate_maintenance_logs(assets: pd.DataFrame) -> pd.DataFrame:
    records = []
    log_id = 1

    for _, asset in assets.iterrows():
        # Each asset gets 2-8 maintenance events over the year
        n_events = random.randint(2, 8)
        event_dates = sorted(
            random.sample(
                pd.date_range(START_DATE, END_DATE, freq="D").tolist(),
                n_events
            )
        )

        for event_date in event_dates:
            mtype = random.choices(
                MAINTENANCE_TYPES,
                weights=[0.50, 0.30, 0.10, 0.10]
            )[0]
            duration_hrs = {
                "Preventive":  random.uniform(2, 8),
                "Corrective":  random.uniform(4, 24),
                "Emergency":   random.uniform(8, 48),
                "Inspection":  random.uniform(1, 4),
            }[mtype]
            cost = duration_hrs * random.uniform(150, 400)

            records.append({
                "log_id":          f"MNT-{log_id:04d}",
                "asset_id":        asset["asset_id"],
                "date":            event_date.date(),
                "maintenance_type": mtype,
                "duration_hours":  round(duration_hrs, 1),
                "cost_cad":        round(cost, 2),
                "technician":      random.choice(TECHNICIANS),
                "resolved":        random.choices([True, False], weights=[0.92, 0.08])[0],
                "notes":           f"{mtype} work on {asset['asset_id']} — {asset['asset_type']}",
            })
            log_id += 1

    return pd.DataFrame(records)


# ── 4. Monthly Efficiency Metrics (aggregated) ────────────────────────────────
def generate_efficiency_metrics(
    sensors: pd.DataFrame,
    maintenance: pd.DataFrame,
    assets: pd.DataFrame,
) -> pd.DataFrame:
    sensors["date"] = pd.to_datetime(sensors["date"])
    sensors["month"] = sensors["date"].dt.to_period("M")

    monthly = (
        sensors.groupby(["asset_id", "month"])
        .agg(
            avg_efficiency=("efficiency", "mean"),
            total_output_kwh=("output_kw", "sum"),
            avg_uptime=("uptime_hours", "mean"),
            peak_output_kw=("output_kw", "max"),
        )
        .reset_index()
    )

    maintenance["date"] = pd.to_datetime(maintenance["date"])
    maintenance["month"] = maintenance["date"].dt.to_period("M")
    maint_monthly = (
        maintenance.groupby(["asset_id", "month"])
        .agg(
            maintenance_events=("log_id", "count"),
            total_maintenance_cost=("cost_cad", "sum"),
            maintenance_hours=("duration_hours", "sum"),
        )
        .reset_index()
    )

    metrics = monthly.merge(maint_monthly, on=["asset_id", "month"], how="left")
    metrics = metrics.merge(assets[["asset_id", "asset_type", "region", "capacity_kw"]], on="asset_id")

    metrics["maintenance_events"]      = metrics["maintenance_events"].fillna(0).astype(int)
    metrics["total_maintenance_cost"]  = metrics["total_maintenance_cost"].fillna(0)
    metrics["maintenance_hours"]       = metrics["maintenance_hours"].fillna(0)
    metrics["month"]                   = metrics["month"].astype(str)
    metrics["availability_pct"]        = (metrics["avg_uptime"] / 24 * 100).round(2)
    metrics["capacity_factor_pct"]     = (
        metrics["total_output_kwh"] / (metrics["capacity_kw"] * 24 * 30) * 100
    ).round(2)

    return metrics


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("Generating synthetic energy operations data...")

    assets      = generate_assets()
    sensors     = generate_sensor_readings(assets)
    maintenance = generate_maintenance_logs(assets)
    metrics     = generate_efficiency_metrics(sensors, maintenance, assets)

    # Save CSVs
    assets.to_csv(OUTPUT_DIR / "assets.csv", index=False)
    sensors.to_csv(OUTPUT_DIR / "sensor_readings.csv", index=False)
    maintenance.to_csv(OUTPUT_DIR / "maintenance_logs.csv", index=False)
    metrics.to_csv(OUTPUT_DIR / "efficiency_metrics.csv", index=False)

    # Save SQLite DB (convert date/Period columns to strings for compatibility)
    db_path = OUTPUT_DIR / "energy_ops.db"
    conn = sqlite3.connect(db_path)

    def prep(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in df.columns:
            if hasattr(df[col], "dt") or str(df[col].dtype) in ("object",):
                df[col] = df[col].astype(str)
            elif str(df[col].dtype).startswith("period"):
                df[col] = df[col].astype(str)
        # catch any remaining Period dtype
        for col in df.select_dtypes(include=["period[M]", "period[D]"]).columns:
            df[col] = df[col].astype(str)
        return df

    assets.to_sql("assets", conn, if_exists="replace", index=False)
    prep(sensors).to_sql("sensor_readings", conn, if_exists="replace", index=False)
    prep(maintenance).to_sql("maintenance_logs", conn, if_exists="replace", index=False)
    prep(metrics).to_sql("efficiency_metrics", conn, if_exists="replace", index=False)
    conn.close()

    print(f"  ✓ assets.csv              ({len(assets)} rows)")
    print(f"  ✓ sensor_readings.csv     ({len(sensors):,} rows)")
    print(f"  ✓ maintenance_logs.csv    ({len(maintenance)} rows)")
    print(f"  ✓ efficiency_metrics.csv  ({len(metrics)} rows)")
    print(f"  ✓ energy_ops.db           (SQLite)")
    print("Done.")


if __name__ == "__main__":
    main()
