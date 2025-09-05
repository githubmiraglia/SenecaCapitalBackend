import os
import pandas as pd
import requests
import unicodedata
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


def save_investidores_to_django(csv_path, isin, codigo_if):

    print("SAVING TO DJANGO")

    """
    Reads the downloaded CSV, cleans it, and posts to Django.
    Ensures no duplicates by normalizing keys before sending.
    """
    try:
        df = pd.read_csv(csv_path, sep=";")

        # ✅ Normalize headers
        df.columns = [normalize_column(c) for c in df.columns]

        # Mapping between CSV headers → Django fields
        COLUMN_MAPPING = {
            "fii investidor": "fii_investidor",
            "quantidade": "quantidade",
            "valor de mercado": "valor_mercado",
            "serie investida": "serie_investida",
            "classe investida": "classe_investida",
            "mes de referencia": "mes_referencia",
            "nome da operacao": "nome_operacao",
        }

        records = []
        seen_keys = set()

        for _, row in df.iterrows():
            record = {
                "isin": f"IF{codigo_if}" if not isin else isin,  # ✅ ensure ISIN never blank
                "codigo_if": codigo_if,
            }

            # Map columns → Django fields
            for csv_col, django_field in COLUMN_MAPPING.items():
                value = row.get(csv_col, "")
                if django_field in ["quantidade", "valor_mercado"]:
                    value = parse_number(value)
                    if value is not None:
                        value = round(value, 2)  # ✅ force 2 decimals
                record[django_field] = value

            # ✅ Composite key to avoid duplicates in the same batch
            composite_key = (
                record["isin"],
                record["codigo_if"],
                str(record.get("fii_investidor", "")).strip(),
            )

            if composite_key not in seen_keys:
                seen_keys.add(composite_key)
                records.append(record)

        # 🔎 Debug: Show what we built
        print(f" Parsed {len(records)} rows from CSV {csv_path}")
        if records:
            print(" First record being sent:", records[0])

        url = "http://127.0.0.1:8000/api/investidores/insertnew/"
        token = os.getenv("DJANGO_TOKEN")
        headers = {"Authorization": f"Bearer {token}"} if token else {}

        # ✅ Send deduplicated batch to Django
        resp = requests.post(url, json=records, headers=headers)

        print(" Response status:", resp.status_code)
        try:
            print(" Response body:", resp.json())
        except Exception:
            print("Raw response:", resp.text)

        resp.raise_for_status()
        print(f"✅ Saved {len(records)} investidores for {codigo_if}")

    except Exception as e:
        print(f"❌ Failed to save CSV {csv_path} for {codigo_if}: {e}")


# -------------------------------
# Playwright Robot
# -------------------------------

def run_robot(headless=False, lista_codigos_isin_if=None):

    print("Starting Playwright robot...")

    if not lista_codigos_isin_if:
        print(" No ISIN/IF codes provided.")
        return

    from playwright.sync_api import sync_playwright

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
            try:
                codigo_if = str(codigo_if).strip().replace("\u200B", "")
                print(f" Processing ISIN={isin}, IF={codigo_if}")

                search_box.click()
                search_box.fill("")
                search_box.type(codigo_if)

                option_xpath = "xpath=//*[contains(@id,'pesquisa-option')]"
                try:
                    page.wait_for_selector(option_xpath, timeout=8000)
                    page.click(option_xpath)
                except:
                    print(f"⚠️ No dropdown option for {codigo_if}, pressing Enter instead.")
                    page.keyboard.press("Enter")

                page.wait_for_timeout(2000)

                # Investidores tab
                investidores_xpath = "xpath=//*[@id='tab-investidores']/span"
                page.wait_for_selector(investidores_xpath, timeout=10000)
                page.click(investidores_xpath)

                # ⚠️ Check if "sem investidores" message exists
                empty_xpath = "xpath=//*[contains(text(),'Não encontramos investidores')]"
                if page.locator(empty_xpath).is_visible():
                    print(f"⚠️ Nenhum investidor encontrado para {codigo_if}, pulando.")
                    continue  # skip this CRI

                # Exportar button
                exportar_xpath = "xpath=//*[@id='tabpanel-investidores']/div/div/div[1]/div[2]/div/div/div[1]/div[3]/div[1]/div/span/button/span[2]"
                page.wait_for_selector(exportar_xpath, timeout=30000)

                # ✅ Expect CSV download
                with page.expect_download() as download_info:
                    page.click(exportar_xpath)
                    csv_xpath = "xpath=//*[@id='simple-popover']/div[3]/div/span/li"
                    page.wait_for_selector(csv_xpath, timeout=10000)
                    page.click(csv_xpath)

                download = download_info.value
                csv_path = download.path()
                print(f"Downloaded CSV: {csv_path}")

                # ✅ Save to Django
                save_investidores_to_django(csv_path, isin, codigo_if)

                # Pause between iterations
                page.wait_for_timeout(3000)

            except Exception as e:
                print(f"❌ Failed for ISIN={isin}, IF={codigo_if}: {e}")
                continue

        print("Finished all ISIN/IF pairs")
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
