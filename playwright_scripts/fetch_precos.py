import os
import pandas as pd
import requests
import unicodedata
from datetime import datetime
from playwright.sync_api import sync_playwright

# -------------------------------
# Helpers
# -------------------------------

def normalize_column(col):
    """Remove accents, lower-case, strip spaces."""
    return (
        unicodedata.normalize("NFKD", str(col))
        .encode("ASCII", "ignore")
        .decode("utf-8")
        .strip()
        .lower()
    )


def parse_number(value):
    """Convert strings like '20.750', 'R$ 20,96 mi' into float."""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    val = str(value).strip()

    # Remove R$, spaces
    val = val.replace("R$", "").replace(" ", "")

    # Handle millions ("mi")
    if "mi" in val.lower():
        val = val.lower().replace("mi", "")
        val = val.replace(".", "").replace(",", ".")
        return float(val) * 1_000_000

    # Handle normal thousand/decimal format
    val = val.replace(".", "").replace(",", ".")
    try:
        return float(val)
    except ValueError:
        return None


def parse_date(value):
    """Parse Brazilian DD/MM/YYYY into YYYY-MM-DD."""
    if not value or pd.isna(value):
        return None
    try:
        return datetime.strptime(str(value).strip(), "%d/%m/%Y").date().isoformat()
    except Exception:
        try:
            return pd.to_datetime(value, dayfirst=True).date().isoformat()
        except Exception:
            return None


def save_precos_to_django(csv_path, isin, codigo_if):
    """
    Reads the downloaded CSV, cleans it, and posts Precos to Django.
    """
    try:
        df = pd.read_csv(csv_path, sep=";")

        # Normalize headers
        df.columns = [normalize_column(c) for c in df.columns]
        print(f"Normalized columns: {list(df.columns)}")

        # Mapping between CSV headers (normalized) → Django fields
        COLUMN_MAPPING = {
            "classes": "classe",
            "titulo": "titulo",
            "data": "data",
            "preco (minimo)": "preco_minimo",
            "preco (maximo)": "preco_maximo",
            "preco (ultimo)": "preco_ultimo",
            "quantidade": "quantidade",
            "num neg.": "num_negocios",
            "volume": "volume",
            "ambiente": "ambiente",
        }

        records = []
        for _, row in df.iterrows():
            record = {
                "isin": isin,
                "codigo_if": codigo_if,
            }
            for csv_col, django_field in COLUMN_MAPPING.items():
                if csv_col in df.columns:
                    value = row.get(csv_col, "")

                    if django_field in ["preco_minimo", "preco_maximo", "preco_ultimo", "volume"]:
                        value = parse_number(value)
                        if value is not None:
                            value = round(value, 2)

                    elif django_field == "quantidade":
                        value = parse_number(value)
                        value = round(value, 3) if value is not None else None  # ✅ 3 decimals

                    elif django_field == "num_negocios":
                        try:
                            value = int(value) if value not in (None, "", " ") else 0
                        except Exception:
                            value = 0

                    elif django_field == "data":
                        value = parse_date(value)

                    elif django_field == "classe":
                        value = str(value).strip() if value else "Única"

                    record[django_field] = value

            print(" Parsed record:", record)  # 👈 DEBUG
            records.append(record)

        # 🔎 Debug: Show first few records
        print(f" Parsed {len(records)} rows from CSV {csv_path}")
        if records:
            print("First Preco record being sent:", records[0])

        url = "http://127.0.0.1:8000/api/precos/insertnew/"
        token = os.getenv("DJANGO_TOKEN")
        headers = {"Authorization": f"Bearer {token}"} if token else {}

        resp = requests.post(url, json=records, headers=headers)

        # 🔎 Debug response
        print("Response status:", resp.status_code)
        try:
            print("Response body:", resp.json())
        except Exception:
            print(" Raw response:", resp.text)

        resp.raise_for_status()
        print(f"Saved {len(records)} precos for {codigo_if}")

    except Exception as e:
        print(f"Failed to save CSV {csv_path} for {codigo_if}: {e}")


# -------------------------------
# Playwright Robot
# -------------------------------

def run_robot(headless=True, lista_codigos_isin_if=None):
    if not lista_codigos_isin_if:
        print("No Precos provided.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=500)
        page = browser.new_page()

        # Go to login
        page.goto("https://www.uqbar.com.br/plataforma")

        # ✅ Login
        page.wait_for_selector('//*[@id="mui-1"]', timeout=30000)
        page.fill('//*[@id="mui-1"]', os.getenv("UQBAR_EMAIL", "tommymv30@gmail.com"))
        page.fill('//*[@id="mui-2"]', os.getenv("UQBAR_PASSWORD", "Uqbar281173!!"))
        page.keyboard.press("Enter")

        # Wait for dashboard
        patliq_xpath = '//*[@id="root"]/section/div/div/div[2]/div/div[1]/div[2]/div[1]/p[2]'
        page.wait_for_selector(f'xpath={patliq_xpath}', timeout=20000)

        # Search input
        search_xpath = "xpath=//*[@id='pesquisa']"
        page.wait_for_selector(search_xpath, timeout=20000)
        search_box = page.locator(search_xpath)

        # Loop through ISIN / IF pairs
        for isin, codigo_if in lista_codigos_isin_if:
            print(f" Processing ISIN={isin}, IF={codigo_if}")

            # Search IF
            search_box.click()
            search_box.fill("")
            search_box.type(codigo_if)

            option_xpath = "xpath=//*[@id='pesquisa-option-0']"
            page.wait_for_selector(option_xpath, timeout=10000)
            page.click(option_xpath)

            page.wait_for_timeout(2000)

            # Secundário tab
            secundario_xpath = "xpath=//*[@id='tab-secundario']/span"
            page.wait_for_selector(secundario_xpath, timeout=10000)
            page.click(secundario_xpath)

            # Exportar button
            exportar_xpath = "//*[@id='tabpanel-secundario']/div/div/div[1]/div/div[6]/li/button/span[1]"
            page.wait_for_selector(exportar_xpath, timeout=10000)

            # ✅ Expect CSV download
            with page.expect_download() as download_info:
                page.click(exportar_xpath)
                
            download = download_info.value
            csv_path = download.path()
            print(f" Downloaded CSV: {csv_path}")

            # ✅ Save Precos to Django
            save_precos_to_django(csv_path, isin, codigo_if)

            # Pause between iterations
            page.wait_for_timeout(3000)

        print("Finished all Precos ISIN/IF pairs")
        browser.close()


# -------------------------------
# Entry Point
# -------------------------------

if __name__ == "__main__":
    lista_codigos_isin_if = [
        ["BRCASCCRI497", "24I2268708"],
        ["BRCASCCRI3R6", "24E2453531"],
    ]
    run_robot(headless=False, lista_codigos_isin_if=lista_codigos_isin_if)
