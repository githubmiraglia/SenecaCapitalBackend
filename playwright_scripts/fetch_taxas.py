import os
import sys
import django
from pathlib import Path
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from workalendar.america import Brazil  


# --- Setup Django ---
BASE_DIR = Path(__file__).resolve().parent.parent  # project root (TaxadeRemunercaoCRI)
sys.path.append(str(BASE_DIR))  # make backend_api importable
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend_api.settings")
django.setup()
from core.models import IPCADiario


# -----------------------------
# 1. Baixar série IPCA mensal
# -----------------------------
def get_ipca_monthly():
    url = "https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/63/p/all"
    r = requests.get(url)
    r.raise_for_status()  # raise error if API fails
    data = r.json()

    df = pd.DataFrame(data[1:])  # skip metadata
    df["date"] = pd.to_datetime(df["D3C"], format="%Y%m")
    df["ipca_pct"] = pd.to_numeric(df["V"], errors="coerce")
    return df[["date", "ipca_pct"]].sort_values("date").reset_index(drop=True)


# -----------------------------
# 2. Preencher estimativas
# -----------------------------
def fill_missing_with_estimates(ipca, estimate_current=0.25, estimate_previous=0.30):
    today = datetime.today()
    current_month = pd.Period(today, freq="M")
    previous_month = current_month - 1
    last_available = ipca["date"].dt.to_period("M").max()

    if today.day < 10 and last_available < previous_month:
        ipca = pd.concat(
            [
                ipca,
                pd.DataFrame(
                    {"date": [previous_month.to_timestamp()], "ipca_pct": [estimate_previous]}
                ),
            ],
            ignore_index=True,
        )

    if last_available < current_month:
        ipca = pd.concat(
            [
                ipca,
                pd.DataFrame(
                    {"date": [current_month.to_timestamp()], "ipca_pct": [estimate_current]}
                ),
            ],
            ignore_index=True,
        )

    return ipca.sort_values("date").reset_index(drop=True)


# -----------------------------
# 3. Construir índice diário
# -----------------------------
def build_daily_ipca_index(start="2011-01-03"):

    ipca = get_ipca_monthly()
    ipca = fill_missing_with_estimates(ipca)

    cal = Brazil()
    start_date = pd.Timestamp(start)
    end_date = pd.Timestamp.today().normalize()

    # lista de dias úteis
    dates = []
    d = start_date
    while d <= end_date:
        if cal.is_working_day(d.date()):
            dates.append(d)
        d += timedelta(days=1)

    df_days = pd.DataFrame({"date": dates})
    df_days["month"] = df_days["date"].dt.to_period("M")
    ipca["month"] = ipca["date"].dt.to_period("M")

    df = df_days.merge(ipca[["month", "ipca_pct"]], on="month", how="left")
    df["business_day_in_month"] = df.groupby("month").cumcount() + 1
    df["total_bd_in_month"] = df.groupby("month")["date"].transform("count")

    # fatores
    df["monthly_factor"] = 1 + df["ipca_pct"] / 100.0
    df["daily_factor"] = df["monthly_factor"] ** (1 / df["total_bd_in_month"])

    df = df.sort_values("date").reset_index(drop=True)
    df["ipca_index"] = df["daily_factor"].cumprod()

    base = df.loc[df["date"] == pd.Timestamp(start), "ipca_index"].iloc[0]
    df["ipca_index"] = df["ipca_index"] / base

    return df[["date", "ipca_index", "ipca_pct"]]


# -----------------------------
# 4. Upsert into IPCADiario
# -----------------------------
def upsert_ipca():
    df = build_daily_ipca_index("2011-01-03")

    print(f"Upserting {len(df)} rows into IPCADiario...")

    count = 0
    for _, row in df.iterrows():
        IPCADiario.objects.update_or_create(
            data=row["date"].date(),
            defaults={
                "index": round(float(row["ipca_index"]), 6),
                "variacao_pct": round(float(row["ipca_pct"]), 6) if row["ipca_pct"] is not None else None,
            },
        )
        count += 1

    print(f"Upserted {count} rows into IPCADiario")


# -----------------------------
# Always run when called
# -----------------------------
if __name__ == "__main__":
    print("Starting IPCADiario upsert (standalone)...")
    upsert_ipca()
else:
    # ensure it's executed even when called via run_playwright
    print("Starting IPCADiario upsert (imported)...")
    upsert_ipca()
