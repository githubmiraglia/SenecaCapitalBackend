import pandas as pd
import requests
from datetime import date, datetime, timedelta
from workalendar.america import Brazil

# -----------------------------
# 1. Baixar série IPCA mensal (SIDRA/IBGE)
# -----------------------------
def get_ipca_monthly():
    """
    Baixa IPCA mensal (% variação) via API SIDRA/IBGE.
    Tabela 1737, Variável 63 = IPCA (variação mensal).
    """
    url = "https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/63/p/all"
    r = requests.get(url)
    data = r.json()

    # primeira linha é metadados, descartar
    df = pd.DataFrame(data[1:])
    # D3C = período AAAAMM
    df["date"] = pd.to_datetime(df["D3C"], format="%Y%m")
    df["ipca_pct"] = pd.to_numeric(df["V"], errors="coerce")
    return df[["date", "ipca_pct"]].sort_values("date").reset_index(drop=True)

# -----------------------------
# 2. Preencher estimativas faltantes
# -----------------------------
def fill_missing_with_estimates(ipca, estimate_current=0.25, estimate_previous=0.30):
    """
    Preenche IPCA estimado para:
    - mês anterior, se ainda não publicado (antes do dia 10)
    - mês corrente (sempre estimado até divulgação oficial no próximo mês)
    """
    today = datetime.today()
    current_month = pd.Period(today, freq="M")
    previous_month = current_month - 1

    # Último mês oficial disponível
    last_available = ipca["date"].dt.to_period("M").max()

    # Caso 1: mês anterior ainda não saiu (antes do dia 10)
    if today.day < 10 and last_available < previous_month:
        ipca = pd.concat([
            ipca,
            pd.DataFrame({
                "date": [previous_month.to_timestamp()],
                "ipca_pct": [estimate_previous]
            })
        ], ignore_index=True)

    # Caso 2: mês corrente sempre precisa de estimativa
    if last_available < current_month:
        ipca = pd.concat([
            ipca,
            pd.DataFrame({
                "date": [current_month.to_timestamp()],
                "ipca_pct": [estimate_current]
            })
        ], ignore_index=True)

    return ipca.sort_values("date").reset_index(drop=True)

# -----------------------------
# 3. Construir índice diário
# -----------------------------
def build_daily_ipca_index(start="2011-01-03"):
    # 1. pegar IPCA oficial
    ipca = get_ipca_monthly()

    # 2. preencher estimativas se faltar
    ipca = fill_missing_with_estimates(ipca, estimate_current=0.25, estimate_previous=0.30)

    # 3. calendário brasileiro
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

    # merge dias com IPCA do mês
    df = df_days.merge(ipca[["month", "ipca_pct"]], on="month", how="left")

    # contagem de dias úteis do mês
    df["business_day_in_month"] = df.groupby("month").cumcount() + 1
    df["total_bd_in_month"] = df.groupby("month")["date"].transform("count")

    # fator mensal (ex: 0.45% = 1.0045)
    df["monthly_factor"] = 1 + df["ipca_pct"] / 100.0
    # fator diário = raiz do fator mensal pelo nº de dias úteis
    df["daily_factor"] = df["monthly_factor"] ** (1 / df["total_bd_in_month"])

    # índice acumulado
    df = df.sort_values("date").reset_index(drop=True)
    df["ipca_index"] = df["daily_factor"].cumprod()

    # normaliza para 1 em 2011-01-03
    base = df.loc[df["date"] == pd.Timestamp(start), "ipca_index"].iloc[0]
    df["ipca_index"] = df["ipca_index"] / base

    return df[["date", "ipca_index", "ipca_pct"]]

# -----------------------------
# Execução principal
# -----------------------------
if __name__ == "__main__":
    df_ipca = build_daily_ipca_index("2011-01-03")
    print(df_ipca.tail(15))  # mostra últimos dias
    df_ipca.to_csv("ipca_daily_index.csv", index=False)
    print("\n✅ Série diária salva em ipca_daily_index.csv")

    #INSERT DO QUE FOR NOVO
    # PROCESSO DIARIO

    # 
