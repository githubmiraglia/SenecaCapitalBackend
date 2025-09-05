import pandas as pd
from django.core.management.base import BaseCommand
from core.models import Indice

# reuse your working scraper
from playwright_scripts.fetch_indices import extract_anbima_data

class Command(BaseCommand):
    help = "Fetch ANBIMA ETTJ and upsert into the Indice table"

    def handle(self, *args, **options):
        df: pd.DataFrame = extract_anbima_data()

        created = updated = 0
        for _, row in df.iterrows():
            data = row["data_da_tabela"]
            du = int(row["dias_uteis"])
            defaults = {
                "data_da_tabela": data,
                "dias_uteis": du,
                "taxa_real": float(row["taxa_real"]),
                "taxa_nominal": float(row["taxa_nominal"]),
                "inflacao_implicita": float(row["inflacao_implicita"]),
                "composite_key": f"{data.isoformat()}-{du}",
            }
            obj, was_created = Indice.objects.update_or_create(
                composite_key=defaults["composite_key"],
                defaults=defaults,
            )
            if was_created:
                created += 1
            else:
                updated += 1

        self.stdout.write(self.style.SUCCESS(
            f"Done. created={created}, updated={updated} (rows={len(df)})"
        ))
