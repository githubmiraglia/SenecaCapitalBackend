# TestClickButton.py
import os
import re
import io
import csv
import json
import datetime as dt
from decimal import Decimal, InvalidOperation

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
import os
from playwright.sync_api import sync_playwright

def run():
    downloads_dir = os.path.expanduser("~/Downloads")
    os.makedirs(downloads_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        # Go to the platform page
        page.goto("https://www.uqbar.com.br/plataforma")

        # Email + password
        page.wait_for_selector('xpath=//*[@id="mui-1"]', timeout=10000)
        page.fill('xpath=//*[@id="mui-1"]', "tommymv30@gmail.com")
        page.fill('xpath=//*[@id="mui-2"]', "Uqbar281173!!")

        # Uncheck "Mantenha-me conectado" (your original locator)
        checkbox_xpath = '//*[@id="root"]/section/div/div[2]/form/div[2]/label/span[2]'
        page.wait_for_selector(f'xpath={checkbox_xpath}')
        locator = page.locator(f'xpath={checkbox_xpath}')
        try:
            # If Playwright detects it as checkable, do the safe thing
            if locator.is_checked():
                locator.click()
        except Exception:
            # If it's a span (not the input), just click to toggle off
            locator.click()

        # Login
        page.click('xpath=//*[@id="root"]/section/div/div[2]/form/div[1]/div[3]/button')

        # Wait for navbar
        page.wait_for_selector('xpath=//*[@id="root"]/div[1]/nav/div/div/ul', timeout=15000)
        # Click "Operações"
        operacoes_xpath = '//*[@id="root"]/div[1]/nav/div/div/ul/div[2]/div'
        page.click(f'xpath={operacoes_xpath}')
        # Click "CRI"
        cri_xpath = '//*[@id="root"]/div[1]/nav/div/div/ul/div[3]/div/div/div/a[2]/div/span'
        page.click(f'xpath={cri_xpath}')
        # Wait for the Export button to appear (removed stray ')' at end)
        export_xpath = '//*[@id="operations-content"]/div[3]/div/div/div[1]/span/button'
        page.wait_for_selector(f'xpath={export_xpath}', timeout=15000)
        # Click Visao Geral tab
        print("Clicking Visão Geral tab...")
        visao_geral_xpath = '//*[@id="operations-content"]/div[3]/div/div/div[2]/div[1]/div[3]/div[1]/span[1]/span[1]/input'
        page.click(f'xpath={visao_geral_xpath}')
        # Wait for Buttom Personalizar to ensure page is fully loaded and click it
        personalizar_xpath = '/html/body/div[1]/main/div/div/div/div[2]/div[2]/div[2]/div[3]/div/div/div[2]/div[1]/div[3]/div[2]/div[2]/div/button[1]/span'
        page.wait_for_selector(f'xpath={personalizar_xpath}', timeout=15000)
        print("Clicking Personalizar button...")
        page.click(f'xpath={personalizar_xpath}')
        # Click MOSTRAR TUDO button
        print("Clicking Mostrar Tudo button...")
        mostrar_tudo_xpath = '/html/body/div[3]/div[3]/ul/div/button[3]/span'
        page.wait_for_selector(f'xpath={mostrar_tudo_xpath}', timeout=15000)
        page.click(f'xpath={mostrar_tudo_xpath}')
        # Press ESC to close the Personalizar menu
        print("Pressing ESC to close Personalizar menu...")
        page.keyboard.press("Escape")
        # Wait a bit to ensure UI is ready
        page.wait_for_timeout(1000)
        page.keyboard.press("Escape")
        # Click Export button to open options
        export_xpath = '//*[@id="operations-content"]/div[3]/div/div/div[1]/div[2]/span/button'
        print("Clicking Export button to open options...")  
        page.click(f'xpath={export_xpath}')
        # Expect the download when selecting CSV option
        csv_option_xpath = '//*[@id="simple-popover"]/div[3]/div/span[1]/li'
        with page.expect_download() as download_info:
            print("Selecting CSV option...")
            page.click(f'xpath={csv_option_xpath}')
        download = download_info.value
        # Decide filename and save to ./downloads/
        suggested = download.suggested_filename or "export.csv"
        target_path = os.path.join(downloads_dir, suggested)
        download.save_as(target_path)
        print("Downloaded to:", target_path)

        while True:
            pass

if __name__ == "__main__":
    run()


