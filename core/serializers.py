# app/core/serializers.py
from rest_framework import serializers
from .models import CRIOperacao, Indice, Investidor, IPCADiario, Preco

class CRIOperacaoSerializer(serializers.ModelSerializer):
    class Meta:
        model = CRIOperacao
        fields = "__all__"


class IndiceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Indice
        fields = "__all__"

class InvestidorSerializer(serializers.ModelSerializer):
    class Meta:
        model = Investidor
        fields = "__all__"

class IPCADiarioSerializer(serializers.ModelSerializer):
    class Meta:
        model = IPCADiario
        fields = ["id", "data", "index", "variacao_pct"]

class PrecoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Preco
        fields = "__all__"