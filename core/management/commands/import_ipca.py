# core/management/commands/import_ipca.py
import pandas as pd
from django.core.management.base import BaseCommand
from core.models import IPCADiario   # ✅ use o nome correto do modelo
from datetime import datetime

class Command(BaseCommand):
    help = "Importa dados do IPCA diário de um CSV"

    def add_arguments(self, parser):
        parser.add_argument("csv_file", type=str, help="Caminho para o arquivo CSV")

    def handle(self, *args, **options):
        csv_file = options["csv_file"]

        self.stdout.write(f"Lendo arquivo: {csv_file}")
        df = pd.read_csv(csv_file)

        expected_cols = {"date", "ipca_index", "ipca_pct"}
        if not expected_cols.issubset(df.columns):
            raise ValueError(f"CSV deve conter as colunas: {expected_cols}")

        self.stdout.write(f"Iniciando importação de {len(df)} linhas...\n")

        for _, row in df.iterrows():
            try:
                data_ref = pd.to_datetime(row["date"]).date()

                obj, created = IPCADiario.objects.update_or_create(   # ✅ aqui
                    data=data_ref,
                    defaults={
                        "index": row["ipca_index"],
                        "variacao_pct": row["ipca_pct"],
                    },
                )

                status = "Criado" if created else "Atualizado"
                self.stdout.write(f"{status}: {data_ref} → índice={row['ipca_index']} pct={row['ipca_pct']}")

            except Exception as e:
                self.stderr.write(f"Erro na linha {row.to_dict()}: {e}")

        self.stdout.write(self.style.SUCCESS("✅ Importação concluída!"))
