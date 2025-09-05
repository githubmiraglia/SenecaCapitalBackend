from django.db import models

class CRIOperacao(models.Model):
    # chave única escolhida
    codigo_if = models.CharField(max_length=64, unique=True)

    securitizadora = models.CharField(max_length=255, null=True, blank=True)
    operacao = models.CharField(max_length=255, null=True, blank=True)
    classe_titulo = models.CharField(max_length=255, null=True, blank=True)
    emissao = models.CharField(max_length=255, null=True, blank=True)
    serie = models.CharField(max_length=255, null=True, blank=True)
    data_emissao = models.DateField(null=True, blank=True)

    montante_emitido = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    remuneracao = models.CharField(max_length=255, null=True, blank=True)
    spread_aa = models.DecimalField(max_digits=7, decimal_places=3, null=True, blank=True)  # ex: 1.234 (% a.a.)
    prazo_meses = models.IntegerField(null=True, blank=True)

    ativo_lastro = models.CharField(max_length=255, null=True, blank=True)
    tipo_devedor = models.CharField(max_length=255, null=True, blank=True)
    agente_fiduciario = models.CharField(max_length=255, null=True, blank=True)
    tipo_oferta = models.CharField(max_length=255, null=True, blank=True)
    regime_fiduciario = models.CharField(max_length=255, null=True, blank=True)

    pulverizado = models.BooleanField(null=True, blank=True)
    qtd_emitida = models.IntegerField(null=True, blank=True)
    segmento_imobiliario = models.CharField(max_length=255, null=True, blank=True)

    certificacao_esg = models.BooleanField(null=True, blank=True)
    agencia_certificadora_esg = models.CharField(max_length=255, null=True, blank=True)

    contrato_lastro = models.CharField(max_length=255, null=True, blank=True)
    isin = models.CharField(max_length=64, null=True, blank=True)
    cedentes = models.CharField(max_length=255, null=True, blank=True)
    lider_distribuicao = models.CharField(max_length=255, null=True, blank=True)

    # NEW FIELDS
    carencia_principal_meses = models.IntegerField(
        null=True, blank=True,
        help_text="Período de carência em meses (0 = sem carência).",
    )

    frequencia_principal = models.CharField(
        max_length=30, null=True, blank=True,
        help_text="Frequência de amortização do principal.",
    )

    tabela_juros = models.CharField(
        max_length=30, null=True, blank=True,
        help_text="Tabela de juros (ex.: Integral).",
    )

    frequencia_juros = models.CharField(
        max_length=30, null=True, blank=True,
        help_text="Frequência de pagamento dos juros.",
    )

    metodo_principal = models.CharField(
        max_length=30, null=True, blank=True,
        help_text="Método de amortização do principal.",
    )

    periodo_integralizacao = models.CharField(
        max_length=30, null=True, blank=True,
        help_text="Período de integralização.",
    )

    frequencia_integralizacao = models.CharField(
        max_length=30, null=True, blank=True,
        help_text="Frequência de integralização.",
    )

    # housekeeping
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.codigo_if} - {self.operacao}"


class Indice(models.Model):
    """
    Guarda os pontos da curva (ETTJ) da ANBIMA.
    Unicidade por data_da_tabela + dias_uteis.
    Também mantemos composite_key para facilitar o upsert pela API.
    """
    data_da_tabela = models.DateField()
    dias_uteis = models.PositiveIntegerField()
    taxa_real = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    taxa_nominal = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    inflacao_implicita = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)

    # chave de upsert simples
    composite_key = models.CharField(max_length=40, unique=True, db_index=True)

    class Meta:
        unique_together = ("data_da_tabela", "dias_uteis")
        ordering = ("-data_da_tabela", "dias_uteis")

    def save(self, *args, **kwargs):
        if not self.composite_key:
            self.composite_key = f"{self.data_da_tabela.isoformat()}-{self.dias_uteis}"
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.data_da_tabela} / {self.dias_uteis}d"

class Investidor(models.Model):
    isin = models.CharField(max_length=20)
    codigo_if = models.CharField(max_length=50) 

    fii_investidor = models.CharField(max_length=255, null=True, blank=True)
    quantidade = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    valor_mercado = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    serie_investida = models.CharField(max_length=50, null=True, blank=True)
    classe_investida = models.CharField(max_length=50, null=True, blank=True)
    mes_referencia = models.CharField(max_length=50, null=True, blank=True)
    nome_operacao = models.CharField(max_length=255, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.fii_investidor} ({self.codigo_if})"

from django.db import models


class IPCADiario(models.Model):
    data = models.DateField(unique=True)
    index = models.DecimalField(max_digits=12, decimal_places=6)
    variacao_pct = models.DecimalField(max_digits=12, decimal_places=6)

    def __str__(self):
        return f"{self.data}: {self.index} ({self.variacao_pct}%)"


class Preco(models.Model):
    isin = models.CharField(max_length=20, null=True, blank=True)
    codigo_if = models.CharField(max_length=20, null=True, blank=True)

    classe = models.CharField(max_length=50, null=True, blank=True)
    titulo = models.CharField(max_length=255, null=True, blank=True)
    data = models.DateField()
    preco_minimo = models.DecimalField(max_digits=20, decimal_places=6, null=True, blank=True)
    preco_maximo = models.DecimalField(max_digits=20, decimal_places=6, null=True, blank=True)
    preco_ultimo = models.DecimalField(max_digits=20, decimal_places=6, null=True, blank=True)
    quantidade = models.DecimalField(max_digits=20, decimal_places=3, null=True, blank=True)
    num_negocios = models.BigIntegerField(null=True, blank=True)
    volume = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    ambiente = models.CharField(max_length=50, null=True, blank=True)

    class Meta:
        # ✅ removed unique_together and constraints
        ordering = ["-data"]

    def __str__(self):
        return f"{self.isin} - {self.codigo_if} - {self.data}"

