from datetime import date
import bizdays

# Cria o calendário ANBIMA (já embutido no pacote)
CAL_ANBIMA = bizdays.Calendar.load('ANBIMA')

def business_days_between(start: date, end: date) -> int:
    """
    Calcula o número de dias úteis (calendário ANBIMA) entre duas datas.
    Inclui a data inicial? Não. Inclui a data final? Sim, se for dia útil.

    Exemplo:
        business_days_between(date(2025, 1, 1), date(2025, 1, 31))
    """
    return CAL_ANBIMA.bizdays(start, end)
