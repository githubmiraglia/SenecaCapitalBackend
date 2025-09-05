# parallelShift.py
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import date
from workalendar.america import Brazil

# -----------------------------
# Config
# -----------------------------
COUPON_RATE_ANNUAL = 0.06       # 6% p.a., NTNB 2035
COUPON_FREQ = 2                  # semiannual
FACE = 100.0
MATURITY = pd.Timestamp(2035, 5, 15)

# Load Brazil calendar once
cal = Brazil()

# Default paths
POSSIBLE_DIRS = [Path("downloads"), Path("Downloads")]
NTNB_FILE_NAME = "ntnb2035_monthly_yields.csv"
CURVE_FILE_NAME = "CurvaZero.xlsx"

# -----------------------------
# Utilities
# -----------------------------
def find_existing_path(filename: str) -> Path:
    for d in POSSIBLE_DIRS:
        p = d / filename
        if p.exists():
            return p
    return Path(filename)

def year_fraction_workdays(start: pd.Timestamp, end: pd.Timestamp) -> float:
    """Year fraction using Brazilian working days (252 basis)."""
    nb = cal.get_working_days_delta(start.date(), end.date())
    return nb / 252.0

def generate_coupon_schedule(settlement: pd.Timestamp) -> list:
    years = range(settlement.year, MATURITY.year + 1)
    raw_dates = []
    for y in years:
        raw_dates.append(pd.Timestamp(y, 5, 15))
        raw_dates.append(pd.Timestamp(y, 11, 15))
    return [d for d in sorted(raw_dates) if (d > settlement) and (d <= MATURITY)]

def macaulay_duration_ntnb35(settlement: pd.Timestamp, y_real_annual: float) -> float:
    if settlement >= MATURITY:
        raise ValueError("Settlement date is on/after maturity.")

    coupons = generate_coupon_schedule(settlement)
    coupon_amt = FACE * COUPON_RATE_ANNUAL / COUPON_FREQ

    times, cfs = [], []
    for dt in coupons:
        t = year_fraction_workdays(settlement, dt)
        cf = coupon_amt
        if dt == MATURITY:
            cf += FACE
        times.append(t)
        cfs.append(cf)

    times, cfs = np.array(times), np.array(cfs)
    dfs = (1.0 + y_real_annual) ** (-times)
    pv = cfs * dfs
    pv_total = pv.sum()
    return np.sum(times * pv) / pv_total

# -----------------------------
# Load data ONCE
# -----------------------------
ntnb_path = find_existing_path(NTNB_FILE_NAME)
curve_path = find_existing_path(CURVE_FILE_NAME)

# NTNB 2035 CSV (monthly historical real yield)
ntnb = pd.read_csv(ntnb_path)
ntnb['Data Base'] = pd.to_datetime(ntnb['Data Base'], format='%Y-%m', errors='coerce')

# detect coluna de yield
ycol = [c for c in ntnb.columns if 'yield' in c.lower()][0]
ntnb['RealYield'] = ntnb[ycol] / (100.0 if ntnb[ycol].mean() > 1 else 1.0)

# CurvaZero XLSX (estrutura: Business Day, taxa_real, taxa_nominal, inflacao_implicita)
curve = pd.read_excel(curve_path)
curve.rename(columns={
    'Business Day': 'du',
    'taxa_real': 'taxa_real',
    'taxa_nominal': 'taxa_nominal',
    'inflacao_implicita': 'inflacao'
}, inplace=True)

# garantir que todas estão em decimal
curve['taxa_real'] = pd.to_numeric(curve['taxa_real'], errors="coerce") / 100.0
curve['taxa_nominal'] = pd.to_numeric(curve['taxa_nominal'], errors="coerce") / 100.0
curve['inflacao'] = pd.to_numeric(curve['inflacao'], errors="coerce") / 100.0

# -----------------------------
# Core function
# -----------------------------
def run_parallel_shift(input_date_str: str):
    target_date = pd.to_datetime(input_date_str, errors="coerce")
    if pd.isna(target_date):
        raise ValueError(f"Could not parse {input_date_str}")

    # Closest NTNB35 yield (monthly data)
    idx = (ntnb['Data Base'] - target_date).abs().idxmin()
    row_ntnb = ntnb.loc[idx]
    y_ntnb = float(row_ntnb['RealYield'])
    settle_dt = pd.Timestamp(row_ntnb['Data Base'])

    # Duration in years and business days
    dur_years = macaulay_duration_ntnb35(settle_dt, y_ntnb)
    dur_workdays = int(round(dur_years * 252))

    # Closest tenor in curva zero
    idx_near = (curve['du'] - dur_workdays).abs().idxmin()
    real_at_dur = float(curve.loc[idx_near, 'taxa_real'])

    # parallel shift delta
    delta = y_ntnb - real_at_dur

    # apply shift to all real curve
    real_shifted = curve['taxa_real'] + delta
    infl = curve['inflacao']
    nominal = (1 + real_shifted) * (1 + infl) - 1

    out = pd.DataFrame({
        "ntnb_data_base": [settle_dt.date()] * len(curve),
        "du": curve['du'],
        "real_shifted_%": (real_shifted * 100).round(4),
        "inflacao_%": (infl * 100).round(4),
        "nominal_%": (nominal * 100).round(4),
    }).sort_values("du").reset_index(drop=True)

    #print(f"\n== Parallel Shift Summary ==")
    #print(f"Target date: {target_date.date()}")
    #print(f"Closest NTNB date: {settle_dt.date()}  (real ytm = {y_ntnb*100:.2f} %)")
    #print(f"Macaulay duration: {dur_years:.2f} yrs (~{dur_workdays} workdays)")
    #print(f"Real@dur in curve: {real_at_dur*100:.2f} %")
    #print(f"Parallel shift delta: {delta*100:.2f} %\n")

    return out

# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python parallelShift.py YYYY-MM-DD")
        sys.exit(1)
    df = run_parallel_shift(sys.argv[1])
    print(df.head(15))