# core/utils/cashflow.py
from __future__ import annotations

import os, django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend_api.settings")
django.setup()

import numpy as np
import pandas as pd
from dataclasses import dataclass
from datetime import date
from typing import Literal, Optional, Tuple
from core.utils.helpers_dates import (
    _add_months,
    _annual_to_monthly_effective,
    _months_diff_approx,
    _to_date,
)
from core.utils.helpers_calcs import analyze_CRI

# ============================================================
# ========================= Tipos ============================
# ============================================================

PrincipalMethod = Literal["SAC", "Bullet"]
TabelaJuros = Literal["Integral", "Price"]
Freq = int


# ============================================================
# ===================== Dataclass Input ======================
# ============================================================

@dataclass
class CRICashflowInput:
    principal: float
    data_emissao: date | str
    numero_parcelas: Optional[int] = None
    data_vencimento: Optional[date | str] = None

    taxa_nominal_aa: Optional[float] = None  # ✅ only nominal

    carencia_meses: int = 0
    freq_principal_meses: Freq = 1
    freq_juros_meses: Freq = 1

    tabela_juros: Optional[TabelaJuros] = "Integral"
    principal_method: PrincipalMethod = "SAC"

    periodo_integralizacao: Optional[int] = None
    freq_integralizacao: Optional[int] = None

    indexation: Literal["CDI", "IPCA", "TR"] = "CDI"

    def get_tipo_taxa(self) -> Optional[str]:
        if self.indexation == "CDI":
            return "taxa_nominal"
        elif self.indexation == "IPCA":
            return "taxa_real"
        elif self.indexation == "TR":
            return None
        return None


# ============================================================
# ========================= Engine ===========================
# ============================================================

def build_cri_cashflow_br_from_security(
    security: CRICashflowInput
) -> tuple[pd.DataFrame, dict]:
    """
    Gera cronograma de CRI a partir de um objeto CRICashflowInput.
    """
    df, resumo = build_cri_cashflow_br(
        valor=security.principal,
        data_emissao=_to_date(security.data_emissao),
        prazo_meses=security.numero_parcelas,   # ✅ instead of numero_parcelas
        data_vencimento=security.data_vencimento,
        taxa_nominal_aa=security.taxa_nominal_aa,
        carencia_principal=security.carencia_meses,
        frequencia_amortizacao_principal=security.freq_principal_meses,
        frequencia_juros=security.freq_juros_meses,
        tabela_de_juros=security.tabela_juros,
        principal_method=security.principal_method,
        periodo_integralizacao=security.periodo_integralizacao,
        freq_integralizacao=security.freq_integralizacao,
    )
    resumo["indexation"] = security.indexation
    return df, resumo

# Função para construir o input de cashflow a partir de um CRI gravado na base de dados
def build_cashflow_input_from_cri(op):
    """
    Accepts either a dict (from .values()) or a CRIOperacao model instance.
    Returns a CRICashflowInput object.
    """
    # If it's a model instance, convert to dict
    if not isinstance(op, dict):
        op = {
            field.name: getattr(op, field.name)
            for field in op._meta.fields
        }

    # ✅ Now we can safely access as dict
    return CRICashflowInput(
        principal=float(op.get("montante_emitido", 0) or 0),
        data_emissao=op.get("data_emissao"),
        numero_parcelas=op.get("prazo_meses"),
        taxa_nominal_aa=float(op.get("spread_aa", 0) or 0),
        carencia_meses=int(op.get("carencia_principal_meses", 0) or 0),
        freq_principal_meses=int(op.get("frequencia_principal", 1) or 1),
        freq_juros_meses=int(op.get("frequencia_juros", 1) or 1),
        tabela_juros=op.get("tabela_juros", "Integral"),
        principal_method=op.get("metodo_principal", "SAC"),
        periodo_integralizacao=int(op.get("periodo_integralizacao", 0) or 0),
        freq_integralizacao=int(op.get("frequencia_integralizacao", 0) or 0),
        indexation=op.get("remuneracao", "IPCA"),
    )


def build_cri_cashflow(inp: CRICashflowInput) -> Tuple[pd.DataFrame, dict]:
    notional = float(inp.principal)
    issue = _to_date(inp.data_emissao)

    # ---------------- Prazo ----------------
    n = inp.numero_parcelas
    if n is None:
        if inp.data_vencimento is None:
            raise ValueError("Informe 'numero_parcelas' ou 'data_vencimento'.")
        n = _months_diff_approx(issue, _to_date(inp.data_vencimento))
        if n <= 0:
            raise ValueError("Prazo em meses inválido (<=0).")

    g = max(0, int(inp.carencia_meses))
    freq_p = max(1, int(inp.freq_principal_meses))
    freq_j = max(1, int(inp.freq_juros_meses))

    # ---------------- Tabela ----------------
    tab = (inp.tabela_juros or "Integral").strip().title()
    if tab in ("Nenhuma", "None", ""):
        tab = "Integral"
    if tab not in ("Integral", "Price"):
        raise ValueError("tabela_juros deve ser 'Integral' ou 'Price'.")

    # ---------------- Capital Calls ----------------
    cap_calls = np.zeros(n + 1)  # inclui período 0
    if inp.periodo_integralizacao and inp.freq_integralizacao:
        steps = inp.periodo_integralizacao // inp.freq_integralizacao
        call_value = notional / steps
        for k in range(steps + 1):
            idx = k * inp.freq_integralizacao
            if idx <= n:
                cap_calls[idx] = call_value
    else:
        cap_calls[0] = notional

    # ---------------- Taxa mensal ----------------
    if inp.taxa_nominal_aa is None:
        raise ValueError("Informe taxa_nominal_aa.")
    i_m = np.full(n, _annual_to_monthly_effective(float(inp.taxa_nominal_aa)))

    # ---------------- Datas ----------------
    dates = [_add_months(issue, k) for k in range(0, n + 1)]

    # ---------------- Arrays ----------------
    saldo = 0.0
    rows = []
    juros_accum = 0.0

    parcela_price = None
    if tab == "Price" and (n - g) > 0:
        i_ref = float(i_m[g])
        k = n - g
        if i_ref == 0:
            parcela_price = notional / k
        else:
            parcela_price = notional * (i_ref * (1 + i_ref) ** k) / ((1 + i_ref) ** k - 1)

    # =====================================================
    # Loop períodos (0..n)
    # =====================================================
    for t in range(0, n + 1):
        capital_call = cap_calls[t]
        saldo += capital_call

        rate = i_m[t - 1] if t > 0 else 0.0
        juros = saldo * rate if t > 0 else 0.0

        pay_interest = (t > 0) and (t % freq_j == 0)
        pay_principal = (t > g) and (t % freq_p == 0)

        amort = 0.0
        parcela = 0.0
        juros_pago_no_periodo = 0.0

        if t > 0:  # pagamentos começam no período 1
            if tab == "Integral":
                juros_accum += juros
                if pay_principal:
                    if inp.principal_method == "SAC":
                        remaining = max(1, n - t + 1)
                        amort = saldo / remaining
                    elif inp.principal_method == "Bullet":
                        amort = saldo if t == n else 0.0
                if pay_interest:
                    parcela += juros_accum
                    juros_pago_no_periodo = juros_accum
                    juros_accum = 0.0
                parcela += amort
                saldo_end = max(0.0, saldo - amort)

            else:  # PRICE
                juros_accum += juros
                if t <= g:
                    if pay_interest:
                        parcela = juros_accum
                        juros_pago_no_periodo = juros_accum
                        juros_accum = 0.0
                    saldo_end = saldo
                else:
                    if pay_principal:
                        raw_juros = juros_accum
                        amort = max(0.0, parcela_price - raw_juros)
                        amort = min(amort, saldo)
                        parcela = raw_juros + amort
                        juros_pago_no_periodo = raw_juros
                        juros_accum = 0.0
                        saldo_end = max(0.0, saldo - amort)
                    else:
                        if pay_interest:
                            parcela = juros_accum
                            juros_pago_no_periodo = juros_accum
                            juros_accum = 0.0
                        saldo_end = saldo
        else:
            saldo_end = saldo

        rows.append(
            {
                "period": t,
                "date": dates[t],
                "capital_call": -capital_call,  # negativo = saída
                "i_mensal": rate,
                "saldo_inicial": round(saldo, 2),
                "juros_teorico": round(juros, 2),
                "juros_pago_no_periodo": round(juros_pago_no_periodo, 2),
                "amortizacao": round(amort, 2),
                "pagamento": round(parcela + (-capital_call if capital_call else 0), 2),
                "saldo_final": round(saldo_end, 2),
            }
        )
        saldo = saldo_end

    df = pd.DataFrame(rows)

    # ---------------- IRR ----------------
    dates_cf = df["date"].tolist()
    cfs = df["pagamento"].tolist()
    try:
        irr_m = np.irr(cfs)  # type: ignore
    except Exception:
        irr_m = np.nan
    irr_aa_from_m = float((1 + irr_m) ** 12 - 1) if np.isfinite(irr_m) else np.nan

    summary = {
        "principal": notional,
        "parcelas": n,
        "carencia_meses": g,
        "tabela_juros": tab,
        "principal_method": inp.principal_method,
        "freq_principal_meses": freq_p,
        "freq_juros_meses": freq_j,
        "irr_anual_equivalente": irr_aa_from_m,
        "total_pagamentos": float(df["pagamento"].sum()),
        "total_juros_pagos": float(df["juros_pago_no_periodo"].sum()),
        "total_amortizacao": float(df["amortizacao"].sum()),
        "capital_calls_total": float(-df["capital_call"].sum()),
    }
    return df, summary


# ============================================================
# ==================== Wrapper PT-BR =========================
# ============================================================

def build_cri_cashflow_br(
    *,
    valor: float,
    data_emissao: date | str,
    data_vencimento: Optional[date | str] = None,
    prazo_meses: Optional[int] = None,
    carencia_principal: int = 0,
    frequencia_amortizacao_principal: int = 1,
    tabela_de_juros: Optional[str] = None,
    principal_method: PrincipalMethod = "SAC",
    frequencia_juros: int = 1,
    taxa_nominal_aa: Optional[float] = None,
    periodo_integralizacao: Optional[int] = None,
    freq_integralizacao: Optional[int] = None,
):
    if prazo_meses is None:
        if data_vencimento is None:
            raise ValueError("Informe 'prazo_meses' ou 'data_vencimento'.")
        prazo_meses = _months_diff_approx(_to_date(data_emissao), _to_date(data_vencimento))

    tab = (tabela_de_juros or "Integral").strip().title()
    if tab in ("Nenhuma", "None", ""):
        tab = "Integral"
    inp = CRICashflowInput(
        principal=float(valor),
        data_emissao=data_emissao,
        numero_parcelas=int(prazo_meses),
        data_vencimento=data_vencimento,
        taxa_nominal_aa=taxa_nominal_aa,
        carencia_meses=int(carencia_principal),
        freq_principal_meses=int(frequencia_amortizacao_principal),
        freq_juros_meses=int(frequencia_juros),
        tabela_juros=tab,
        principal_method=principal_method,
        periodo_integralizacao=periodo_integralizacao,
        freq_integralizacao=freq_integralizacao,
    )
    return build_cri_cashflow(inp)

# ============================================================
# ========================= Demo =============================
# ============================================================

if __name__ == "__main__":
    cri_security = CRICashflowInput(
        principal=1_000_000,
        data_emissao="2024-05-29",
        numero_parcelas=205,
        taxa_nominal_aa=0.09,
        tabela_juros="Integral",
        principal_method="SAC",
        indexation="IPCA",  # ✅ works now
    )

    df, resumo = build_cri_cashflow_br_from_security(cri_security)

    results = analyze_CRI(
        cashflows=df,
        reference_date="2024-06-28",
        security=cri_security,
        pu=1000,
        quantity=1000,
    )

