import pandas as pd
from datetime import date
from core.models import Indice
from core.utils.business_days import business_days_between
import numpy as np

def get_rate_structure(reference_date: date) -> pd.DataFrame:
    qs = Indice.objects.filter(data_da_tabela=reference_date).order_by("dias_uteis")
    if not qs.exists():
        return pd.DataFrame(columns=[
            "data_da_tabela", "dias_uteis", "taxa_real", "taxa_nominal", "inflacao_implicita"
        ])

    return pd.DataFrame.from_records(
        qs.values("data_da_tabela", "dias_uteis", "taxa_real", "taxa_nominal", "inflacao_implicita")
    )

def interpolar_taxas(df: pd.DataFrame, data_referencia: pd.Timestamp | str | date, data_do_fluxo_futuro: pd.Timestamp | str | date) -> dict:
    """
    Interpola taxas (nominal, real, implícita) da curva ANBIMA para o número de dias úteis
    entre a data de referência e a data do fluxo futuro.

    Args:
        df: DataFrame com colunas ["data_da_tabela", "dias_uteis", "taxa_nominal", "taxa_real", "inflacao_implicita"]
        data_referencia: data da curva ANBIMA (ex.: '2025-08-28')
        data_do_fluxo_futuro: data de vencimento/cashflow

    Returns:
        dict com taxas interpoladas {"taxa_nominal": x, "taxa_real": y, "inflacao_implicita": z}
    """
    # normaliza datas
    ref_date = pd.to_datetime(data_referencia).date()
    fluxo_date = pd.to_datetime(data_do_fluxo_futuro).date()

    # filtra curva da data de referência
    df_date = df[df["data_da_tabela"] == ref_date].copy()
    if df_date.empty:
        raise ValueError(f"Nenhuma curva encontrada para data_da_tabela={ref_date}")

    # número de dias úteis entre data referência e fluxo futuro
    dias_uteis_target = business_days_between(ref_date, fluxo_date)

    # ordena e extrai pontos
    df_date = df_date.sort_values("dias_uteis")
    xp = df_date["dias_uteis"].to_numpy()

    def exp_interp(x, xp_arr, yp_arr):
        yp = np.asarray(yp_arr, dtype=float)
        yp[yp <= 0] = 1e-12
        return float(np.exp(np.interp(x, xp_arr, np.log(yp))))

    taxas = {}
    for col in ["taxa_nominal", "taxa_real", "inflacao_implicita"]:
        taxas[col] = exp_interp(dias_uteis_target, xp, df_date[col].to_numpy())

    return taxas
