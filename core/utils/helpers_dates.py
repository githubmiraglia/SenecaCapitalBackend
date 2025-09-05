# core/utils/cashflow.py
from __future__ import annotations

import os, django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend_api.settings")  # adjust project name
django.setup()

import numpy as np
import pandas as pd
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, Literal, Optional, Tuple
from core.utils.rates import get_rate_structure
from core.utils.business_days import business_days_between
from core.utils.rates import interpolar_taxas
import openpyxl


# ============================================================
# ========================= Helpers ==========================
# ============================================================

# ============================================================
# Business Day Utilities
# ============================================================

def is_business_day(dt: date) -> bool:
    """Verifica se é dia útil (segunda-sexta)."""
    return dt.weekday() < 5  # 0=Mon ... 6=Sun

def business_days_between(start, end) -> int:
    """
    Conta dias úteis entre duas datas (exclui a inicial, inclui a final).
    Aceita tanto datetime.date quanto pandas.Timestamp.
    """
    start = pd.Timestamp(start)
    end = pd.Timestamp(end)

    if start > end:
        start, end = end, start

    bdays = pd.bdate_range(start + pd.Timedelta(days=1), end)
    return len(bdays)


# ============================================================
# Ajustado: NPV e XIRR genéricos usando dias úteis (252)
# ============================================================

def _npv(
    cashflows: list[tuple[date, float]],
    ref_date: date,
    rate: float
) -> tuple[float, float, pd.DataFrame]:
    """
    NPV usando convenção 30/360 (Excel DAYS360) e
    Macaulay Duration (apenas recebimentos positivos).

    Args:
        cashflows: lista de (date, valor)
        ref_date: data de referência (date ou str)
        rate: taxa efetiva anual

    Returns:
        tuple:
          - npv_total
          - macaulay_duration_em_anos
          - DataFrame com colunas auxiliares
    """
    if not cashflows:
        return np.nan, np.nan, pd.DataFrame(columns=["date", "valor", "dias360", "t_years", "pv"])

    ref_date = _to_date(ref_date)
    df = pd.DataFrame(cashflows, columns=["date", "valor"])
    df["date"] = pd.to_datetime(df["date"])

    # Dias 30/360 e anos equivalentes
    df["dias360"] = df["date"].apply(lambda d: days360(ref_date, d))
    df["t_years"] = df["dias360"] / 360.0

    # Valor presente
    df["pv"] = df["valor"] / ((1 + rate) ** df["t_years"])

    npv_total = df["pv"].sum()

    # Duration só com fluxos positivos
    df_pos = df[df["valor"] > 0]
    if not df_pos.empty:
        pv_pos_total = df_pos["pv"].sum()
        if pv_pos_total > 0:
            weighted_time_sum = (df_pos["pv"] * df_pos["t_years"]).sum()
            macaulay_duration = weighted_time_sum / pv_pos_total
        else:
            macaulay_duration = np.nan
    else:
        macaulay_duration = np.nan

    return npv_total, macaulay_duration, df


###### XIRR #####

def _xirr(
    dates: Iterable[date],
    cashflows: Iterable[float],
    guess: float = 0.15
) -> Tuple[float, pd.DataFrame]:
    """
    Calcula XIRR usando convenção 30/360 (Excel DAYS360).
    Retorna:
      - IRR anual equivalente
      - DataFrame com colunas extras: dias360, t_years, pv
    """
    dates = list(dates)
    cashflows = list(cashflows)

    if len(dates) != len(cashflows):
        raise ValueError("dates e cashflows devem ter o mesmo tamanho")
    if len(dates) < 2:
        return np.nan, pd.DataFrame({"date": dates, "pagamento": cashflows})
    if not (any(cf > 0 for cf in cashflows) and any(cf < 0 for cf in cashflows)):
        # precisa ter entradas e saídas
        return np.nan, pd.DataFrame({"date": dates, "pagamento": cashflows})

    t0 = pd.Timestamp(dates[0])

    # Usar dias360/360 como base para t_years
    times = [days360(t0.date(), pd.Timestamp(d).date()) / 360.0 for d in dates]

    def f(r: float) -> float:
        return sum(cf / (1 + r) ** t for cf, t in zip(cashflows, times))

    def df(r: float) -> float:
        return sum(-t * cf / (1 + r) ** (t + 1) for cf, t in zip(cashflows, times))

    r = guess
    for _ in range(100):
        fv = f(r)
        dv = df(r)
        if abs(fv) < 1e-8:
            break
        if dv == 0:
            return np.nan, pd.DataFrame({"date": dates, "pagamento": cashflows})
        r_next = r - fv / dv
        if not np.isfinite(r_next):
            return np.nan, pd.DataFrame({"date": dates, "pagamento": cashflows})
        if abs(r_next - r) < 1e-8:
            r = r_next
            break
        r = r_next

    irr = float(r)

    # Construir DataFrame enriquecido
    df = pd.DataFrame({"date": dates, "pagamento": cashflows})
    dias_list = []
    pv_list = []
    for d, cf, t in zip(dates, cashflows, times):
        dias = days360(t0.date(), pd.Timestamp(d).date())
        dias_list.append(dias)
        pv_list.append(cf / ((1 + irr) ** t))

    df["dias360"] = dias_list
    df["t_years"] = [t for t in times]
    df["pv"] = pv_list

    return irr, df


# ============================================================
#  Calendar Days Utilities
# ============================================================

def days360(start_date: date, end_date: date, european: bool = False) -> int:
    """
    Calcula diferença em dias entre duas datas usando convenção 30/360.
    Compatível com Excel DAYS360 (default = US method).

    Args:
        start_date: data inicial
        end_date: data final
        european: se True, usa método europeu; senão, método US (Excel default)

    Returns:
        Número de dias 30/360
    """
    d1, d2 = start_date.day, end_date.day
    m1, m2 = start_date.month, end_date.month
    y1, y2 = start_date.year, end_date.year

    if not european:
        if d1 == 31:
            d1 = 30
        if d2 == 31 and d1 == 30:
            d2 = 30
    else:
        if d1 == 31:
            d1 = 30
        if d2 == 31:
            d2 = 30

    return (y2 - y1) * 360 + (m2 - m1) * 30 + (d2 - d1)



def _to_date(d) -> date:
    """
    Converte entrada (date | str | Timestamp) para datetime.date.
    """
    if isinstance(d, date):
        return d
    return pd.to_datetime(d).date()

def _add_months(d: date, m: int) -> date:
    """
    Soma m meses a uma data (preservando o dia sempre que possível).
    """
    return (pd.Timestamp(d) + pd.DateOffset(months=m)).date()

def _months_diff_approx(d1: date, d2: date) -> int:
    """
    Diferença aproximada em meses cheios entre d1 -> d2.
    Se o dia do vencimento for anterior ao dia de emissão, desconta 1 mês.
    """
    d1 = _to_date(d1); d2 = _to_date(d2)
    y = d2.year - d1.year
    m = d2.month - d1.month
    months = y * 12 + m
    if d2.day < d1.day:
        months -= 1
    return max(0, months)

def _annual_to_monthly_effective(rate_aa: float) -> float:
    """
    Converte taxa anual efetiva para mensal efetiva.
    Exemplo: 12% a.a. -> ~0.949% a.m.
    """
    return (1.0 + rate_aa) ** (1.0 / 12.0) - 1.0


# ============================================================
# ========================= Tipos ============================
# ============================================================

PrincipalMethod = Literal["SAC", "Bullet"]          # Usado quando tabela='Integral'
TabelaJuros = Literal["Integral", "Price"]          # Amortização/juros
Freq = int                                          # meses (1=mensal, 3=trimestral, ...)
# ============================================================
# ===================== Dataclass Input ======================
# ============================================================

@dataclass
class CRICashflowInput:
    """
    Estrutura de entrada para geração de cronograma de CRI.
    """
    principal: float
    data_emissao: date | str

    # Se 'numero_parcelas' não vier, pode vir 'data_vencimento' para derivar.
    numero_parcelas: Optional[int] = None
    data_vencimento: Optional[date | str] = None

    # Juros: use OU taxa_nominal_aa, OU taxa_real_aa + inflacao_mensal
    taxa_nominal_aa: Optional[float] = None
    taxa_real_aa: Optional[float] = None
    inflacao_mensal: Optional[Iterable[float]] = None

    # Carência e frequências
    carencia_meses: int = 0                          # carência de principal
    freq_principal_meses: Freq = 1
    freq_juros_meses: Freq = 1

    # Tabela de juros
    tabela_juros: Optional[TabelaJuros] = "Integral" # None/""/"Nenhuma" -> Integral
    principal_method: PrincipalMethod = "SAC"        # válido para Integral

    # Integralizações (capital calls)
    periodo_integralizacao: Optional[int] = None     # meses até última integralização
    freq_integralizacao: Optional[int] = None        # frequência (meses) das chamadas

    # Novo: Indexador
    indexation: Literal["CDI", "IPCA", "TR"] = "CDI"

    def get_tipo_taxa(self) -> Optional[str]:
        """
        Map indexation to tipo_taxa para uso na curva ANBIMA.
        """
        if self.indexation == "CDI":
            return "taxa_nominal"
        elif self.indexation == "IPCA":
            return "taxa_real"
        elif self.indexation == "TR":
            return None   # não tratado por enquanto
        return None