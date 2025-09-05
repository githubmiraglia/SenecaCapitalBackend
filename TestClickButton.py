# TestClickButton.py
import os
import re
import io
import csv
import argparse
import logging
import datetime as dt
import subprocess
import sys
from decimal import Decimal, InvalidOperation

import requests
from playwright.sync_api import sync_playwright

# =============================== Utilities ===============================

def _dec_br(x: str | None):
    if not x:
        return None
    s = re.sub(r"[^\d,.\-]", "", x).replace(".", "").replace(",", ".")
    try:
        return Decimal(s)
    except InvalidOperation:
        return None

def _int(x: str | None):
    if not x:
        return None
    s = re.sub(r"[^\d\-]", "", x)
    return int(s) if s else None

def _date(x: str | None):
    if not x:
        return None
    for f in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(x.strip(), f).date().isoformat()
        except ValueError:
            pass
    return None

def _bool(x: str | None):
    if x is None:
        return None
    v = (x or "").strip().lower()
    if v in {"sim", "yes", "true", "verdadeiro"}:
        return True
    if v in {"nao", "não", "no", "false", "falso"}:
        return False
    return None

def _to_jsonable(obj):
    if isinstance(obj, Decimal):
        return str(obj)  # keep string to avoid float rounding surprises
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_jsonable(x) for x in obj]
    return obj

# ---------- helpers to count rows & load more/pagination ----------

def try_set_rows_per_page(page, log):
    """
    Try to increase 'rows per page' if a combobox/button exists.
    """
    candidates = [
        re.compile(r"itens por página", re.I),
        re.compile(r"linhas por página", re.I),
        re.compile(r"resultados por página", re.I),
        re.compile(r"rows per page", re.I),
        re.compile(r"results per page", re.I),
        re.compile(r"per page", re.I),
    ]
    wanted = ["1000", "500", "200", "100"]

    for pat in candidates:
        # Combobox/select-like
        try:
            cmb = page.get_by_role("combobox", name=pat)
            if cmb.count():
                log.info("[ROWS] Found combobox; trying big page size...")
                cmb.first.click()
                for opt in wanted:
                    try:
                        cmb.first.select_option(opt)
                        log.info(f"[ROWS] Selected {opt}")
                        page.wait_for_timeout(800)
                        return True
                    except Exception:
                        try:
                            page.get_by_role("option", name=re.compile(rf"^{opt}$")).first.click(timeout=2000)
                            log.info(f"[ROWS] Clicked option {opt}")
                            page.wait_for_timeout(800)
                            return True
                        except Exception:
                            pass
        except Exception:
            pass

        # Button + menu
        try:
            btn = page.get_by_role("button", name=pat)
            if btn.count():
                log.info("[ROWS] Found page-size button; opening menu...")
                btn.first.click()
                for opt in wanted:
                    try:
                        page.get_by_role("menuitem", name=re.compile(rf"^{opt}$")).first.click(timeout=2000)
                        log.info(f"[ROWS] Picked page size {opt}")
                        page.wait_for_timeout(800)
                        return True
                    except Exception:
                        pass
        except Exception:
            pass

    log.info("[ROWS] Could not set rows-per-page (control not found).")
    return False

def click_show_more_next(page, log, max_clicks=2000):
    """
    Click buttons/links like 'Mostrar mais', 'Próxima', 'Next' repeatedly.
    """
    patterns = [
        re.compile(r"mostrar\s+mais", re.I),
        re.compile(r"carregar\s+mais", re.I),
        re.compile(r"próxima", re.I),
        re.compile(r"proxima", re.I),
        re.compile(r"next", re.I),
        re.compile(r"seguinte", re.I),
        re.compile(r"mais resultados", re.I),
    ]
    clicks = 0
    while clicks < max_clicks:
        found = False
        for pat in patterns:
            try:
                btn = page.get_by_role("button", name=pat)
                if btn.count():
                    txt = btn.first.inner_text()[:40]
                    log.info(f"[ROWS] Clicking button '{txt}...'")
                    btn.first.click()
                    page.wait_for_timeout(850)
                    clicks += 1
                    found = True
                    break
            except Exception:
                pass
        if found:
            continue
        for pat in patterns:
            try:
                lnk = page.get_by_role("link", name=pat)
                if lnk.count():
                    txt = lnk.first.inner_text()[:40]
                    log.info(f"[ROWS] Clicking link '{txt}...'")
                    lnk.first.click()
                    page.wait_for_timeout(850)
                    clicks += 1
                    found = True
                    break
            except Exception:
                pass
        if not found:
            break
    log.info(f"[ROWS] 'Show more/next' clicks: {clicks}")
    return clicks > 0

# ---------- SLOWER (but still efficient) jump-scroll ----------

def _detect_scroll_container(page):
    try:
        selector = page.evaluate("""
            () => {
              const els = Array.from(document.querySelectorAll('body, main, #operations-content, #root, div, section, article'));
              const scrollables = els.filter(el => {
                const style = getComputedStyle(el);
                const canScrollY = (el.scrollHeight || 0) > (el.clientHeight || 0);
                const oy = style.overflowY;
                return canScrollY && (oy === 'auto' || oy === 'scroll' || el === document.scrollingElement);
              });
              let best = null, bestScore = -1;
              for (const el of scrollables) {
                const score = (el.scrollHeight || 0) + (el.clientHeight || 0);
                if (score > bestScore) { best = el; bestScore = score; }
              }
              if (!best) return 'document';
              if (best === document.scrollingElement || best === document.documentElement || best === document.body) {
                return 'document';
              }
              if (best.id) return '#' + best.id;
              if (best.classList && best.classList.length) return '.' + Array.from(best.classList).join('.');
              return 'document';
            }
        """)
    except Exception:
        selector = "document"
    return selector or "document"

def _get_visible_row_count(page):
    try:
        return page.evaluate("""
            () => {
              const counts = [];
              counts.push(document.querySelectorAll('#operations-content [role="row"][data-rowindex], [role="row"].MuiDataGrid-row').length);
              counts.push(document.querySelectorAll('#operations-content table tbody tr').length);
              counts.push(document.querySelectorAll('table tbody tr').length);
              counts.push(document.querySelectorAll('#operations-content [data-index]').length);
              return Math.max(...counts.filter(n => Number.isFinite(n)), 0);
            }
        """)
    except Exception:
        return 0

def jump_scroll_until_stable(
    page, log, *, settle_checks=12, max_iters=3500, wait_ms=750, tickle_steps=3
):
    """
    Jump to ~80%, then to bottom with small waits; repeat until height and row count stop growing.
    This is intentionally slower than the previous 'fast' version to give the grid time to hydrate.
    """
    sel = _detect_scroll_container(page)
    log.info(f"[SCROLL] Using container: {sel}")

    def metrics():
        if sel == "document":
            return page.evaluate("""
                () => {
                  const el = document.scrollingElement || document.documentElement || document.body;
                  return { y: window.scrollY, h: el.scrollHeight, vh: window.innerHeight };
                }
            """)
        return page.evaluate(
            """(s) => {
                const el = document.querySelector(s);
                if (!el) return { y: 0, h: 0, vh: 0 };
                return { y: el.scrollTop, h: el.scrollHeight, vh: el.clientHeight };
            }""",
            sel,
        )

    def to_bottom():
        if sel == "document":
            page.evaluate("() => { const el=document.scrollingElement||document.documentElement||document.body; window.scrollTo(0, el.scrollHeight); }")
        else:
            page.evaluate("(s)=>{ const el=document.querySelector(s); if(el) el.scrollTop = el.scrollHeight; }", sel)

    def to_ratio(r: float):
        if sel == "document":
            page.evaluate("(r)=>{ const el=document.scrollingElement||document.documentElement||document.body; window.scrollTo(0, Math.floor(el.scrollHeight*r)); }", r)
        else:
            page.evaluate("(params)=>{ const el=document.querySelector(params.sel); if(el) el.scrollTop = Math.floor(el.scrollHeight*params.r); }", {"sel": sel, "r": r})

    stable = 0
    last_h = -1
    last_rows = -1

    for i in range(1, max_iters + 1):
        # tickle near 80% a few times
        for _ in range(tickle_steps):
            to_ratio(0.8)
            page.wait_for_timeout(wait_ms)

        # then jump to bottom
        to_bottom()
        page.wait_for_timeout(wait_ms)

        m = metrics()
        h = m.get("h", 0)
        y = m.get("y", 0)
        vh = m.get("vh", 0)
        rows = _get_visible_row_count(page)

        at_bottom = (y + vh) >= (h - 2)
        grew = (h > last_h) or (rows > last_rows)

        if i % 10 == 0:
            log.info(f"[SCROLL] iter={i} rows≈{rows} h={h} y={y} bottom={at_bottom}")

        if at_bottom and not grew:
            stable += 1
            if stable >= settle_checks:
                log.info(f"[SCROLL] Stable bottom after {i} passes. Final rows≈{rows}")
                break
        else:
            stable = 0

        last_h = max(last_h, h)
        last_rows = max(last_rows, rows)

    # final grace period
    page.wait_for_timeout(1200)

# ============================== Main Job ==============================

def run(headless: bool = False):
    log = logging.getLogger("uqbar_job")

    downloads_dir = os.path.expanduser("~/Downloads")
    os.makedirs(downloads_dir, exist_ok=True)

    # --- Django + parsing config INSIDE run() ---
    DJANGO_BASE = (os.getenv("DJANGO_BASE", "http://127.0.0.1:8000") or "").rstrip("/")
    DJANGO_USER = os.getenv("DJANGO_USER", "admin")
    DJANGO_PASS = os.getenv("DJANGO_PASS", "admin123")
    UNIQUE_KEY  = "codigo_if"

    HEADER_MAP = {
        "Securitizadora": "securitizadora",
        "Operação": "operacao",
        "Classe do Título": "classe_titulo",
        "Emissão": "emissao",
        "Série": "serie",
        "Data de Emissão": "data_emissao",
        "Montante Emitido": "montante_emitido",
        "Remuneração": "remuneracao",
        "Spread (% a.a.)": "spread_aa",
        "Prazo (em meses)": "prazo_meses",
        "Ativo-Lastro": "ativo_lastro",
        "Tipo de Devedor": "tipo_devedor",
        "Agente Fiduciário": "agente_fiduciario",
        "Tipo de Oferta": "tipo_oferta",
        "Regime Fiduciário": "regime_fiduciario",
        "Pulverizado?": "pulverizado",
        "Qtd. Emitida": "qtd_emitida",
        "Segmento Imobiliário": "segmento_imobiliario",
        "Certificação ESG": "certificacao_esg",
        "Agência Certificadora ESG": "agencia_certificadora_esg",
        "Contrato-lastro": "contrato_lastro",
        "Código IF": "codigo_if",
        "ISIN": "isin",
        "Cedente(s)": "cedentes",
        "Líder de Distribuição": "lider_distribuicao",
    }
    NUMERIC_FIELDS = {"montante_emitido", "spread_aa", "prazo_meses", "qtd_emitida"}
    DATE_FIELDS    = {"data_emissao"}
    BOOL_FIELDS    = {"pulverizado", "certificacao_esg"}

    def normalize_row(row: dict) -> dict:
        out = {}
        for pt, sk in HEADER_MAP.items():
            raw = row.get(pt)
            if sk in NUMERIC_FIELDS:
                out[sk] = _int(raw) if sk in {"prazo_meses", "qtd_emitida"} else _dec_br(raw)
            elif sk in DATE_FIELDS:
                out[sk] = _date(raw)
            elif sk in BOOL_FIELDS:
                out[sk] = _bool(raw)
            else:
                out[sk] = (raw or "").strip()
        return out

    def get_token() -> str:
        url = f"{DJANGO_BASE}/api/token/"
        resp = requests.post(url, json={"username": DJANGO_USER, "password": DJANGO_PASS}, timeout=30)
        resp.raise_for_status()
        return resp.json()["access"]

    def post_batch(rows: list[dict]) -> dict:
        url = f"{DJANGO_BASE}/api/cri-operacoes/upsert/"
        headers = {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}
        payload = {"unique_by": UNIQUE_KEY, "rows": _to_jsonable(rows)}
        r = requests.post(url, headers=headers, json=payload, timeout=240)
        r.raise_for_status()
        return r.json()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()

        # Go to the platform page
        page.goto("https://www.uqbar.com.br/plataforma")

        # Email + password
        page.wait_for_selector('xpath=//*[@id="mui-1"]', timeout=30000)
        page.fill('xpath=//*[@id="mui-1"]', os.getenv("UQBAR_EMAIL", "tommymv30@gmail.com"))
        page.fill('xpath=//*[@id="mui-2"]', os.getenv("UQBAR_PASSWORD", "Uqbar281173!!"))

        # Uncheck "Mantenha-me conectado"
        checkbox_xpath = '//*[@id="root"]/section/div/div[2]/form/div[2]/label/span[2]'
        try:
            page.wait_for_selector(f'xpath={checkbox_xpath}', timeout=8000)
            page.locator(f'xpath={checkbox_xpath}').click()
        except Exception:
            pass

        # Login
        page.click('xpath=//*[@id="root"]/section/div/div[2]/form/div[1]/div[3]/button')

        # Wait for navbar then go to CRI
        page.wait_for_selector('xpath=//*[@id="root"]/div[1]/nav/div/div/ul', timeout=30000)
        page.click('xpath=//*[@id="root"]/div[1]/nav/div/div/ul/div[2]/div')  # Operações
        page.click('xpath=//*[@id="root"]/div[1]/nav/div/div/ul/div[3]/div/div/div/a[2]/div/span')  # CRI

        # Wait for the CRI page
        page.wait_for_selector('xpath=//*[@id="operations-content"]', timeout=30000)

        # Click Visão Geral tab
        logging.info("Clicking Visão Geral tab...")
        try:
            visao_geral_xpath = '//*[@id="operations-content"]/div[3]/div/div/div[2]/div[1]/div[3]/div[1]/span[1]/span[1]/input'
            page.click(f'xpath={visao_geral_xpath}')
        except Exception:
            pass

        # Columns: Personalizar -> Mostrar tudo
        personalizar_xpath = '/html/body/div[1]/main/div/div/div/div[2]/div[2]/div[2]/div[3]/div/div/div[2]/div[1]/div[3]/div[2]/div[2]/div/button[1]/span'
        page.wait_for_selector(f'xpath={personalizar_xpath}', timeout=30000)
        logging.info("Clicking Personalizar button...")
        page.click(f'xpath={personalizar_xpath}')

        logging.info("Clicking Mostrar Tudo button...")
        page.wait_for_timeout(300)
        clicked = False
        try:
            page.get_by_role("menuitem", name=re.compile(r"mostrar\s*tudo", re.I)).click(timeout=8000)
            clicked = True
        except Exception:
            pass
        if not clicked:
            try:
                page.get_by_text(re.compile(r"Mostrar\s*tudo", re.I)).first.click(timeout=8000)
                clicked = True
            except Exception:
                pass
        if not clicked:
            try:
                page.click(
                    '//div[@role="menu"]//button[.//span[contains(translate(.,"ABCDEFGHIJKLMNOPQRSTUVWXYZÁÂÃÀÉÊÍÓÔÕÚÇ","abcdefghijklmnopqrstuvwxyzáâãàéêíóôõúç"),"mostrar")]]',
                    timeout=8000,
                )
                clicked = True
            except Exception:
                pass
        if not clicked:
            try:
                mostrar_tudo_xpath = '/html/body/div[3]/div[3]/ul/div/button[3]/span'
                page.wait_for_selector(f'xpath={mostrar_tudo_xpath}', timeout=8000)
                page.click(f'xpath={mostrar_tudo_xpath}')
                clicked = True
            except Exception:
                pass

        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(300)
        except Exception:
            pass

        # Load LOTS of rows first
        logging.info("Trying to set rows-per-page...")
        try_set_rows_per_page(page, logging)

        logging.info("Trying to click any 'show more/next' repeatedly...")
        click_show_more_next(page, logging, max_clicks=2500)

        logging.info("Slower jump-scroll to bottom until stable (tuned) ...")
        jump_scroll_until_stable(page, logging, settle_checks=12, max_iters=3500, wait_ms=750, tickle_steps=3)

        # Export → CSV
        export_xpath = '//*[@id="operations-content"]/div[3]/div/div/div[1]/div[2]/span/button'
        logging.info("Clicking Export button to open options...")
        page.click(f'xpath={export_xpath}')
        csv_option_xpath = '//*[@id="simple-popover"]/div[3]/div/span[1]/li'
        with page.expect_download() as download_info:
            logging.info("Selecting CSV option...")
            try:
                page.click(f'xpath={csv_option_xpath}')
            except Exception:
                page.get_by_text(re.compile(r"\bCSV\b", re.I)).first.click()
        download = download_info.value

        suggested = download.suggested_filename or "export.csv"
        target_path = os.path.join(os.path.expanduser("~/Downloads"), suggested)
        download.save_as(target_path)
        logging.info(f"Downloaded to: {target_path}")

        # Parse CSV & upsert to Django
        try:
            text = open(target_path, "r", encoding="utf-8-sig").read()
        except UnicodeDecodeError:
            text = open(target_path, "r", encoding="latin-1").read()

        sample = text[:4096]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,")
            delimiter = dialect.delimiter
        except Exception:
            delimiter = ";" if sample.count(";") >= sample.count(",") else ","
        logging.info(f"[DEBUG] Detected delimiter: {delimiter!r}")

        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        raw_headers = reader.fieldnames or []
        logging.info(f"[DEBUG] CSV headers (first 5): {raw_headers[:5]}")

        rows, dropped = [], 0
        for r in reader:
            nr = normalize_row(r)
            if not nr.get(UNIQUE_KEY):
                dropped += 1
                continue
            rows.append(nr)

        logging.info(f"[DEBUG] Parsed rows: {len(rows)} | Dropped (missing {UNIQUE_KEY}): {dropped}")

        CHUNK = 1000
        total_created = total_updated = 0
        for i in range(0, len(rows), CHUNK):
            part = rows[i:i + CHUNK]
            res = post_batch(part)
            total_created += res.get("created", 0)
            total_updated += res.get("updated", 0)
            logging.info(f"[DEBUG] Upsert {i}-{i+len(part)}: {res}")

        logging.info(f"Done! created={total_created}, updated={total_updated}")
        browser.close()

# ============================ Scheduler (Windows) ============================

def install_schedule(
    schedule_time: str = "22:00",
    task_name: str = "Uqbar CRI Upsert",
    log_path: str | None = None,
):
    """
    Create/update a .bat that sets env vars and runs this script headless,
    then register a Windows Scheduled Task (DAILY at schedule_time) to execute it.
    """
    root = os.path.dirname(os.path.abspath(__file__)) 
    logs_dir = os.path.join(root, "logs")
    os.makedirs(logs_dir, exist_ok=True)

    if not log_path:
        log_path = os.path.join(logs_dir, "uqbar_cron.log")

    py_exe = sys.executable
    script_path = os.path.abspath(__file__)
    bat_path = os.path.join(root, "run_uqbar.bat")

    # Write the .bat
    bat = f"""@echo off
setlocal
set DJANGO_BASE={os.getenv("DJANGO_BASE", "http://127.0.0.1:8000")}
set DJANGO_USER={os.getenv("DJANGO_USER", "admin")}
set DJANGO_PASS={os.getenv("DJANGO_PASS", "admin123")}
set UQBAR_EMAIL={os.getenv("UQBAR_EMAIL", "tommymv30@gmail.com")}
set UQBAR_PASSWORD={os.getenv("UQBAR_PASSWORD", "Uqbar281173!!")}
cd /d "{root}"
mkdir logs 2>NUL
"{py_exe}" "{script_path}" --headless --logfile "{log_path}"
endlocal
"""
    with open(bat_path, "w", encoding="ascii", errors="ignore") as f:
        f.write(bat)

    # Create/Update the scheduled task to run as SYSTEM (no password prompt)
    cmd = [
        "SCHTASKS",
        "/Create",
        "/SC", "DAILY",
        "/ST", schedule_time,
        "/TN", task_name,
        "/TR", bat_path,
        "/RU", "SYSTEM",
        "/RL", "HIGHEST",
        "/F",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, shell=False)
    if res.returncode != 0:
        print("Scheduler create error:", res.stdout or res.stderr)
    else:
        print(f"Scheduled task '{task_name}' set for daily {schedule_time}.")
        print(f"Batch: {bat_path}")
        print(f"Log:   {log_path}")

def remove_schedule(task_name: str = "Uqbar CRI Upsert"):
    cmd = ["SCHTASKS", "/Delete", "/TN", task_name, "/F"]
    res = subprocess.run(cmd, capture_output=True, text=True, shell=False)
    if res.returncode != 0:
        print("Scheduler delete error:", res.stdout or res.stderr)
    else:
        print(f"Scheduled task '{task_name}' removed.")

# ============================== Entrypoint ==============================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Uqbar CRI CSV and upsert into Django.")
    parser.add_argument("--headless", action="store_true", help="Run browser headless (no UI).")
    parser.add_argument("--logfile", type=str, default="", help="Optional path to log file.")
    parser.add_argument("--install-schedule", action="store_true", help="Create/Update Windows scheduled task (daily time).")
    parser.add_argument("--remove-schedule", action="store_true", help="Remove Windows scheduled task.")
    parser.add_argument("--time", type=str, default="22:00", help="Schedule time HH:MM for --install-schedule.")
    parser.add_argument("--task-name", type=str, default="Uqbar CRI Upsert", help="Scheduled task name.")

    args = parser.parse_args()

    if args.install_schedule:
        install_schedule(schedule_time=args.time, task_name=args.task_name, log_path=args.logfile or None)
        sys.exit(0)

    if args.remove_schedule:
        remove_schedule(task_name=args.task_name)
        sys.exit(0)

    handlers = [logging.StreamHandler()]
    if args.logfile:
        os.makedirs(os.path.dirname(args.logfile), exist_ok=True)
        handlers.append(logging.FileHandler(args.logfile, encoding="utf-8"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
    )

    # UI visible by default when you run it yourself:
    run(headless=False)
