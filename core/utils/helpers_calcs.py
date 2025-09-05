# core/utils/cashflow.py
from __future__ import annotations

import os, django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend_api.settings")  # adjust project name
django.setup()

import numpy as np
import pandas as pd
from dataclasses import dataclass
from datetime import date
from typing import Iterable, Literal, Optional, Tuple
from core.utils.rates import get_rate_structure
from core.utils.business_days import business_days_between
from core.utils.rates import interpolar_taxas
from core.utils.helpers_dates import _add_months, _annual_to_monthly_effective, _months_diff_approx, _to_date, _xirr, _npv
from core.utils.helpers_dates import PrincipalMethod, CRICashflowInput

import openpyxl

# ============================================================
# ===================== Valuation Utils ======================
# ============================================================

from core.utils.helpers_dates import _to_date
from datetime import date
import pandas as pd
import numpy as np

def current_xirr(
    cashflows: pd.DataFrame,
    reference_date: date | str,
    pu: float | None = None,
    quantity: float | None = None,
    guess: float = 0.15,
):
    """
    Calcula XIRR usando convenção 30/360 (Excel DAYS360).
    Retorna:
      - IRR anual equivalente
      - DataFrame com colunas extras: dias360, t_years, pv

    Se 'pu' e 'quantity' forem fornecidos, insere saída sintética
    no reference_date correspondente ao preço de compra.
    """
    if cashflows.empty:
        return np.nan, cashflows

    ref = _to_date(reference_date)
    df = cashflows.copy()
    df["date"] = df["date"].apply(_to_date)
    df = df.sort_values("date").reset_index(drop=True)

    # Apenas fluxos futuros
    future = df[df["date"] >= ref]
    if future.empty:
        return np.nan, df

    dates = future["date"].tolist()
    values = future["pagamento"].astype(float).tolist()

    # Preço de compra sintético (inicial negativo)
    if pu is not None and quantity is not None:
        dates = [ref] + dates
        values = [-float(pu) * float(quantity)] + values

    try:
        irr, df_enriched = _xirr(dates, values, guess=guess)
        return irr, df_enriched
    except Exception:
        return np.nan, df


# ============================================================
# Current NPV com dias úteis (252)
# ============================================================

def current_npv(
    cashflows: pd.DataFrame,
    reference_date: date | str,
    rate: float,
) -> tuple[float, float, pd.DataFrame]:
    """
    NPV (30/360, Excel DAYS360) e Macaulay Duration (apenas recebimentos positivos).

    Assumes DataFrame já contém todos os fluxos
    (inclusive o investimento inicial em reference_date).
    """
    if cashflows.empty:
        return np.nan, np.nan, cashflows

    ref = _to_date(reference_date)
    df = cashflows.copy()
    df["date"] = df["date"].apply(_to_date)
    df = df.sort_values("date").reset_index(drop=True)

    future = df[df["date"] >= ref]
    if future.empty:
        return 0.0, np.nan, df

    cashflow_list = list(zip(future["date"], future["pagamento"].astype(float)))
    return _npv(cashflow_list, ref, rate)

# ============================================================
# NPV ANBIMA com curva e dias úteis
# ============================================================

def npv_anbima( 
    cashflows: pd.DataFrame,
    reference_date: date | str,
    tipo_taxa: str = "taxa_nominal",
    pu: float | None = None,
    quantity: float | None = None,
    security_duration: float | None = None,
) -> tuple[float, float, float, pd.DataFrame]:
    """
    Calcula NPV de fluxos usando curva ANBIMA (base 252 dias úteis por ano).
    Se security_duration for informado, usa a taxa do ponto da curva
    mais próximo (em dias úteis) como taxa fixa para todos os fluxos.

    Args:
        cashflows: DataFrame com colunas ["date", "pagamento"]
        reference_date: data de referência
        tipo_taxa: coluna a ser usada da curva ANBIMA (ex.: "taxa_nominal", "taxa_real")
        pu: preço unitário (opcional)
        quantity: quantidade de unidades (opcional)
        security_duration: duração do título em anos (opcional)

    Returns:
        (npv_result, lookup_rate, macaulay_duration, df_enriquecido)
    """
    if cashflows.empty:
        return np.nan, np.nan, np.nan, cashflows

    ref = _to_date(reference_date)
    df = cashflows.copy()
    df["date"] = df["date"].apply(_to_date)
    df = df.sort_values("date").reset_index(drop=True)

    future = df[df["date"] >= ref].copy()
    if future.empty:
        return 0.0, np.nan, np.nan, df

    df_curve = get_rate_structure(ref)
    if df_curve.empty:
        raise ValueError(f"Nenhuma curva ANBIMA encontrada para {ref}")
    if "dias_uteis" not in df_curve.columns:
        raise KeyError("df_curve precisa ter coluna 'dias_uteis' para lookup da curva.")

    lookup_rate = np.nan
    fixed_taxa = None
    if security_duration is not None:
        dias_target = int(round(security_duration * 252))
        idx = (df_curve["dias_uteis"] - dias_target).abs().idxmin()
        lookup_rate = float(df_curve.loc[idx, tipo_taxa]) / 100.0
        fixed_taxa = lookup_rate

    rows = []
    pv_total = 0.0

    for _, row in future.iterrows():
        fluxo_date = row["date"]
        valor = float(row["pagamento"])

        dias_uteis = business_days_between(ref, fluxo_date)
        t_years = dias_uteis / 252.0

        if fixed_taxa is not None:
            taxa = fixed_taxa
        else:
            taxas = interpolar_taxas(df_curve, ref, fluxo_date)
            taxa = taxas[tipo_taxa] / 100.0

        pv = valor / ((1 + taxa) ** t_years)

        rows.append({
            "date": fluxo_date,
            "valor": valor,
            "dias_uteis": dias_uteis,
            "t_years": t_years,
            "taxa_interp": taxa,
            "pv": pv,
        })

        pv_total += pv

    df_out = pd.DataFrame(rows)

    # NPV líquido (se pu e quantity fornecidos)
    if pu is not None and quantity is not None:
        npv_result = pv_total - (float(pu) * float(quantity))
    else:
        npv_result = pv_total

    # Macaulay duration só com fluxos positivos
    df_pos = df_out[df_out["valor"] > 0]
    if not df_pos.empty:
        pv_pos_total = df_pos["pv"].sum()
        if pv_pos_total > 0:
            macaulay_duration = (df_pos["pv"] * df_pos["t_years"]).sum() / pv_pos_total
        else:
            macaulay_duration = np.nan
    else:
        macaulay_duration = np.nan

    return npv_result, lookup_rate, macaulay_duration, df_out


# ============================================================
# Resolver XSOV
# ============================================================
def xsov_anbima(
    cashflows: pd.DataFrame,
    reference_date: date | str,
    tipo_taxa: str,
    pu: float | None = None,
    quantity: float | None = None,
    security_duration: float | None = None,
    guess: float = 0.15,
    pre_xirr: Optional[float] = None,
    pre_df_xirr: Optional[pd.DataFrame] = None,
    pre_npv_market: Optional[float] = None,
    pre_duration_xirr: Optional[float] = None,
    pre_df_npv: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Calcula o spread sobre a curva ANBIMA (sov) e retorna todos os cálculos.
    Pode receber cálculos pré-computados para evitar duplicação.
    """

    # --- XIRR de mercado ---
    if pre_xirr is not None and pre_df_xirr is not None:
        xirr, df_xirr = pre_xirr, pre_df_xirr
    else:
        xirr, df_xirr = current_xirr(
            cashflows, reference_date, pu=pu, quantity=quantity, guess=guess
        )
    if np.isnan(xirr):
        raise ValueError("Não foi possível calcular o XIRR do fluxo de caixa.")

    # --- Duration de mercado ---
    if pre_npv_market is not None and pre_duration_xirr is not None and pre_df_npv is not None:
        npv_market, duration_xirr, df_npv = pre_npv_market, pre_duration_xirr, pre_df_npv
    else:
        npv_market, duration_xirr, df_npv = current_npv(
            cashflows, reference_date, rate=xirr
        )

    # --- NPV via curva ANBIMA ---
    npv_anb, lookup_rate, duration_anbima, df_anb = npv_anbima(
        cashflows,
        reference_date,
        tipo_taxa=tipo_taxa,
        pu=pu,
        quantity=quantity,
        security_duration=security_duration,
    )
    if np.isnan(lookup_rate):
        raise ValueError("Não foi possível obter taxa lookup da curva ANBIMA.")

    sov = xirr - lookup_rate

    # ✅ garantir que todos os scalars sejam floats nativos
    return {
        "sov": float(sov),
        "xirr": float(xirr),
        "lookup_rate": float(lookup_rate),
        "duration_xirr": float(duration_xirr) if duration_xirr is not None else None,
        "duration_anbima": float(duration_anbima) if duration_anbima is not None else None,
        "npv_market": float(npv_market) if npv_market is not None else None,
        "npv_anbima": float(npv_anb) if npv_anb is not None else None,
        "df_xirr": df_xirr,
        "df_npv": df_npv,
        "df_anb": df_anb,
    }


# ==============================================================
# WRAPPER XSOV TO USE NEW CLASS OF SECURITY INCLUDING INDEXATION
# ==============================================================
def analyze_CRI(
    security: CRICashflowInput,
    reference_date: date | str,
    pu: float | None = None,
    quantity: float | None = None,
    guess: float = 0.15,
) -> dict:
    """
    Workflow completo para um CRI:
      1. Gera fluxos de caixa a partir do security.
      2. Ajusta PU conforme indexação (ex.: divide por inflação acumulada se IPCA).
      3. Calcula XIRR e NPV de mercado.
      4. Calcula XSOV comparando contra ANBIMA.
    """

    # ✅ Import atrasado para evitar circular import
    from core.utils.cashflow import build_cri_cashflow  

    # ✅ Generate cashflows and summary from CRICashflowInput
    cashflows, summary = build_cri_cashflow(security)

    tipo_taxa = security.get_tipo_taxa()
    if tipo_taxa is None:
        raise ValueError(f"Indexação {security.indexation} não suportada ainda.")

    # ✅ Ajuste PU se IPCA
    if security.indexation == "IPCA" and pu is not None:
        from core.models import IPCADiario
        idx_ini = IPCADiario.objects.get(data=security.data_emissao).index
        idx_ref = IPCADiario.objects.get(data=_to_date(reference_date)).index
        fator = float(idx_ref) / float(idx_ini)
        pu = pu / fator
        print("MODIFIED PU ", pu)

    # ➕ continue sua lógica de cálculos (XIRR, NPV, XSOV etc.)

    print("QUANTITY ",quantity)
    print("PU", pu)
    print("QUANTITY * PU", float(quantity) * float(pu))
    print("INDEXATION", security.indexation)
    print(cashflows.head())

    # --- XIRR + NPV de mercado ---
    xirr, df_xirr = current_xirr(
        cashflows, reference_date, pu=pu, quantity=quantity, guess=guess
    )

    cashflows.to_excel("cashflows.xlsx")
    df_xirr.to_excel("df_xirr.xlsx")

    if np.isnan(xirr):
        raise ValueError("Não foi possível calcular XIRR.")

    npv_market, macaulay_market, df_npv = current_npv(
        cashflows, reference_date, rate=xirr
    )

    # --- XSOV vs ANBIMA ---
    results = xsov_anbima(
        cashflows,
        reference_date,
        tipo_taxa=tipo_taxa,
        pu=pu,
        quantity=quantity,
        security_duration=macaulay_market,
        guess=guess,
        pre_xirr=xirr,
        pre_df_xirr=df_xirr,
        pre_npv_market=npv_market,
        pre_duration_xirr=macaulay_market,
        pre_df_npv=df_npv,
    )

    return {
        "xirr": float(xirr),
        "macaulay_market": float(macaulay_market),
        "summary": summary,   # ✅ keep the summary info too
        **results,            # sov, lookup_rate, npvs, durations, etc.
    }
