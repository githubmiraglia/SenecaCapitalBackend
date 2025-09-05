import pandas as pd
from django.core.management.base import BaseCommand
from core.models import Indice


class Command(BaseCommand):
    help = "Import historical yield curves CSV into Indice table (upsert)"

    def add_arguments(self, parser):
        parser.add_argument("csv_file", type=str, help="Path to CSV file")

    def handle(self, *args, **options):
        csv_file = options["csv_file"]

        # Read CSV
        df = pd.read_csv(csv_file)

        # Drop unwanted columns if they exist
        df = df.drop(columns=["NTNB_DATA", "ntnb_data_base"], errors="ignore")

        # Normalize headers (remove spaces + lowercase)
        df.columns = df.columns.str.strip().str.lower()

        # Rename to match Django model fields (handle multiple variants)
        df = df.rename(columns={
            "input_data": "data_da_tabela",
            "input_date": "data_da_tabela",   # your CSV uses this
            "du": "dias_uteis",
            "dias_uteis": "dias_uteis",
            "nominal_%": "taxa_nominal",
            "taxa_nominal": "taxa_nominal",
            "real_shifted_%": "taxa_real",
            "taxa_real": "taxa_real",
            "inflacao_%": "inflacao_implicita",
            "inflacao_implicita": "inflacao_implicita",
        })

        # Ensure proper datetime format
        if "data_da_tabela" in df.columns:
            df["data_da_tabela"] = pd.to_datetime(
                df["data_da_tabela"], format="%d/%m/%y", errors="coerce"
            ).dt.date

        # Helper to safely handle floats (NaN → None)
        def safe_float(x):
            try:
                return float(x) if pd.notna(x) else None
            except Exception:
                return None

        created = updated = 0
        total_rows = len(df)

        print(f"🚀 Starting import: {total_rows} rows to process...")

        for i, row in df.iterrows():
            data = row["data_da_tabela"]
            du = int(row["dias_uteis"])

            defaults = {
                "data_da_tabela": data,
                "dias_uteis": du,
                "taxa_real": safe_float(row["taxa_real"]),
                "taxa_nominal": safe_float(row["taxa_nominal"]),
                "inflacao_implicita": safe_float(row["inflacao_implicita"]),
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

            # Print progress every 100 rows
            if (i + 1) % 100 == 0 or (i + 1) == total_rows:
                print(f"   ✅ Processed {i+1}/{total_rows} rows "
                      f"(created={created}, updated={updated})")

        self.stdout.write(self.style.SUCCESS(
            f"🎉 Done importing {csv_file}. created={created}, updated={updated}, total_rows={len(df)}"
        ))
