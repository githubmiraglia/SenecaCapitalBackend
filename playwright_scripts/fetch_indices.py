# playwright_scripts/fetch_indices.py
import os, re, unicodedata, io, json
import pandas as pd
import requests
from playwright.sync_api import sync_playwright, Page, Frame

DEBUG = True
DUMP_DIR = os.path.abspath("anbima_frame_dumps")
os.makedirs(DUMP_DIR, exist_ok=True)

# ================= Django API config =================
DJANGO_BASE = (os.getenv("DJANGO_BASE", "http://127.0.0.1:8000") or "").rstrip("/")
DJANGO_USER = os.getenv("DJANGO_USER", "admin")
DJANGO_PASS = os.getenv("DJANGO_PASS", "admin123")

# ================= Utils =================
def log(msg: str):
    if DEBUG:
        print(msg)

def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def _int_thousands(s: str) -> int | None:
    if not s:
        return None
    s = s.replace(".", "")
    m = re.search(r"-?\d+", s)
    return int(m.group()) if m else None

def _float_br(s: str) -> float:
    if s is None:
        return 0.0
    s = re.sub(r"[^\d,.\-]", "", s)
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0

def _body_text(frame: Frame) -> str:
    try:
        return frame.evaluate("document.body ? document.body.innerText : ''")
    except Exception:
        try:
            return frame.content()
        except Exception:
            return ""

def _find_results_frame(page: Page) -> Frame:
    for fr in page.frames:
        if fr.url and "/CZ.asp" in fr.url:
            log(f"[INFO] Using results frame: {fr.url}")
            return fr
    for fr in page.frames:
        if fr.url and "est-termo" in fr.url:
            log(f"[INFO] Using fallback est-termo frame: {fr.url}")
            return fr
    raise RuntimeError("No est-termo frame found")

# ================= Django Posting =================
def get_django_token() -> str:
    url = f"{DJANGO_BASE}/api/token/"
    r = requests.post(url, json={"username": DJANGO_USER, "password": DJANGO_PASS}, timeout=30)
    r.raise_for_status()
    return r.json()["access"]

def post_indices(rows: list[dict]) -> dict:
    token = get_django_token()
    url = f"{DJANGO_BASE}/api/indices/insertnew/"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"rows": rows}
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=120)
    r.raise_for_status()
    return r.json()

# ================= Extraction =================
def extract_anbima_data() -> pd.DataFrame:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto("https://www.anbima.com.br/pt_br/informar/curvas-de-juros-fechamento.htm", wait_until="load")

        # Accept cookie if present
        #try:
        #    page.click('xpath=//*[@id="LGPD_ANBIMA_global_sites_text_btn"]', timeout=3000)
        #    log("[COOKIE] Accepted")
        #except Exception:
        #    pass

        outer = None
        for fr in page.frames:
            if fr.url and "est-termo" in fr.url:
                outer = fr
                break
        if not outer:
            iframe_el = page.wait_for_selector("iframe[src*='est-termo']", timeout=20000)
            outer = iframe_el.content_frame()
        log(f"[INFO] Found outer frame: {outer.url}")

        # Click "Consultar"
        try:
            outer.get_by_role("img", name=re.compile("consultar", re.I)).click(timeout=4000)
        except Exception:
            try:
                outer.locator("img[alt*='onsultar' i]").first.click(timeout=4000)
            except Exception:
                try:
                    xpath = '//*[@id="cinza50"]/form/div/table/tbody/tr/td/img'
                    outer.wait_for_selector(f"xpath={xpath}", timeout=8000)
                    outer.click(f"xpath={xpath}")
                except Exception:
                    outer.evaluate("(document.querySelector('form')||{}).submit && document.querySelector('form').submit()")

        page.wait_for_timeout(1500)
        res_frame = _find_results_frame(page)
        res_frame.wait_for_load_state("networkidle")
        page.wait_for_timeout(800)

        txt = _body_text(res_frame) or ""
        txt_path = os.path.join(DUMP_DIR, "anbima_body_text.txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(txt)

        m_date = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", txt)
        data_ref = pd.to_datetime(m_date.group(1), dayfirst=True).date() if m_date else pd.Timestamp.today().date()
        log(f"[INFO] Reference date: {data_ref}")

        m_start = re.search(r"ETTJ\s*/\s*INFLA\S*\s+IMPL\S*", txt, flags=re.IGNORECASE)
        if not m_start:
            m_start = re.search(r"ETTJ.*?V[ÉE]RTICES", txt, flags=re.IGNORECASE | re.S)
        if not m_start:
            browser.close()
            raise ValueError("Não encontrei o início do bloco ETTJ / Inflação implícita.")

        start_idx = m_start.end()
        m_end = re.search(r"(PREFIXADOS|ERRO\s+T[IÍ]TULO|CURVA\s+ZERO|BETA\s+1)", txt[start_idx:], flags=re.IGNORECASE)
        end_idx = start_idx + (m_end.start() if m_end else len(txt))
        block = txt[start_idx:end_idx]

        quad_pat = re.compile(
            r"(\d{1,4}(?:\.\d{3})?)\s+"
            r"(-?\d+,\d+)\s+"
            r"(-?\d+,\d+)\s+"
            r"(-?\d+,\d+)",
            flags=re.M,
        )
        rows = []
        for m in quad_pat.finditer(block):
            du_str, real_str, nom_str, imp_str = m.groups()
            du = _int_thousands(du_str)
            if du is None:
                continue
            real = _float_br(real_str)
            nom = _float_br(nom_str)
            imp = _float_br(imp_str)
            rows.append([data_ref, du, nom, real, imp])

        browser.close()
        df = pd.DataFrame(rows, columns=["data_da_tabela", "dias_uteis", "taxa_nominal", "taxa_real", "inflacao_implicita"])
        return df.sort_values("dias_uteis").reset_index(drop=True)

# ================= Posting wrapper (for scheduler reuse) =================
def post_indices_from_dataframe(df: pd.DataFrame):
    # make date JSON safe
    df["data_da_tabela"] = df["data_da_tabela"].astype(str)
    rows = df.to_dict(orient="records")

    CHUNK = 500
    results = []
    for i in range(0, len(rows), CHUNK):
        batch = rows[i:i+CHUNK]
        res = post_indices(batch)
        results.append(res)
        print(f"Inserted {len(batch)} indices : {res}")
    return results

# ================= Main =================
if __name__ == "__main__":
    df = extract_anbima_data()
    print("\nExtracted DataFrame (head):")
    print(df.head())

    results = post_indices_from_dataframe(df)
    print("Resumo final:", results)
