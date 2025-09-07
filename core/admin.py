# core/admin.py
from django.contrib import admin
from .models import CRIOperacao
from .models import Indice
from .models import Investidor  
from .models import IPCADiario
from .models import Preco

@admin.register(CRIOperacao)
class CRIOperacaoAdmin(admin.ModelAdmin):
    # Colunas que aparecem na lista
    list_display = (
        "codigo_if",
        "operacao",
        "securitizadora",
        "classe_titulo",
        "emissao",
        "serie",
        "data_emissao",
        "montante_emitido",
        "remuneracao",
        "spread_aa",
        "prazo_meses",
        "ativo_lastro",
        "tipo_devedor",
        "agente_fiduciario",
        "tipo_oferta",
        "regime_fiduciario",
        "pulverizado",
        "qtd_emitida",
        "segmento_imobiliario",
        "certificacao_esg",
        "agencia_certificadora_esg",
        "contrato_lastro",
        "isin",
        "cedentes",
        "lider_distribuicao",
        "carencia_principal_meses",
        "frequencia_principal",
        "tabela_juros",
        "frequencia_juros",
        "metodo_principal",
        "periodo_integralizacao",
        "frequencia_integralizacao",
        "duration",
        "spread",
        "taxa",
    )

    # (opcional) filtros e busca para facilitar a navegação
    list_filter = (
        "securitizadora",
        "regime_fiduciario",
        "certificacao_esg",
        "pulverizado",
        "tipo_oferta",
        "segmento_imobiliario",
    )
    search_fields = ("codigo_if", "isin", "operacao", "securitizadora", "cedentes")
    ordering = ("-data_emissao",)
    list_per_page = 50  # evita uma lista gigantesca por página

from django.contrib import admin
from .models import Indice

@admin.register(Indice)
class IndiceAdmin(admin.ModelAdmin):
    list_display = ("data_da_tabela", "dias_uteis", "taxa_real", "taxa_nominal", "inflacao_implicita")
    list_filter = ("data_da_tabela",)
    search_fields = ("data_da_tabela",)

@admin.register(Investidor)
class InvestidorAdmin(admin.ModelAdmin):
    list_display = (
        "isin",
        "codigo_if",
        "fii_investidor",
        "quantidade",
        "valor_mercado",
        "serie_investida",
        "classe_investida",
        "mes_referencia",
        "nome_operacao",
        "created_at",
    )
    list_filter = ("isin", "codigo_if", "classe_investida", "mes_referencia")
    search_fields = ("isin", "codigo_if", "fii_investidor", "nome_operacao")

@admin.register(IPCADiario)
class IPCADiarioAdmin(admin.ModelAdmin):
    list_display = ("data", "index", "variacao_pct")   # ✅ colunas visíveis
    search_fields = ("data",)
    list_filter = ("data",)

# core/admin.py

from django.contrib import admin
from .models import Preco

@admin.register(Preco)
class PrecoAdmin(admin.ModelAdmin):
    list_display = (
        "isin",
        "codigo_if",
        "classe",
        "titulo",
        "data",
        "format_preco_minimo",
        "format_preco_maximo",
        "format_preco_ultimo",
        "quantidade",
        "num_negocios",
        "format_volume",
        "ambiente",
        "duration",
        "spread",
        "taxa",
    )
    list_filter = ("isin", "codigo_if", "classe", "ambiente")
    search_fields = ("isin", "codigo_if", "titulo")

    # ✅ Format helpers
    def format_preco_minimo(self, obj):
        return f"R$ {obj.preco_minimo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if obj.preco_minimo else "-"
    format_preco_minimo.short_description = "Preço (mínimo)"

    def format_preco_maximo(self, obj):
        return f"R$ {obj.preco_maximo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if obj.preco_maximo else "-"
    format_preco_maximo.short_description = "Preço (máximo)"

    def format_preco_ultimo(self, obj):
        return f"R$ {obj.preco_ultimo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if obj.preco_ultimo else "-"
    format_preco_ultimo.short_description = "Preço (último)"

    def format_volume(self, obj):
        if obj.volume:
            return f"R$ {obj.volume:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return "-"
    format_volume.short_description = "Volume"
    
    def quantidade_display(self, obj):
        return f"{obj.quantidade:.3f}" if obj.quantidade is not None else "-"
    quantidade_display.short_description = "Quantidade"