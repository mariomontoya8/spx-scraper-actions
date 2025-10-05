import os
import sys
from datetime import datetime
from pathlib import Path

import pytz
from tenacity import retry, stop_after_attempt, wait_fixed
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# Config
# =========================
TZ = os.getenv("TZ", "America/Monterrey")
LOCAL_TZ = pytz.timezone(TZ)

DEFAULT_LOGIN_URL = os.getenv("LOGIN_URL", "https://backtestingmarket.com/login")
DEFAULT_DASHBOARD_URL = os.getenv("DASHBOARD_URL", "https://backtestingmarket.com/backtestingIdea")

# Selectores (ajústalos si cambia la UI)
SELECTORS = {
    "email": 'input[name="email"], input#email',
    "password": 'input[name="password"], input#password',
    "login_button": "button:has-text('Log in'), button:has-text('Iniciar sesión'), button[type=submit]",
    "symbol_input": "input[placeholder='Symbol'], input[aria-label='Symbol']",

    # Riesgo / hora: intenta primero <select>, luego “dropdown custom”
    "risk_select": "select#risk, select[name='risk'], select[aria-label='Risk'], select:has(option)",
    "risk_dropdown": "[data-testid='risk-dropdown'], text=Risk, text=Riesgo",

    "time_select": "select#time, select[name='time'], select[aria-label='Time'], select:has(option)",
    "time_dropdown": "[data-testid='time-dropdown'], text=Time, text=Hora",

    # Botón/acción de descarga (más amplio)
    "download_btn": (
        "button:has-text('Download CSV'), text=Download CSV, "
        "button:has-text('Export CSV'), text=Export CSV, "
        "button:has-text('Exportar CSV'), text=Exportar CSV, "
        "a:has-text('CSV'), button:has-text('CSV'), text=CSV"
    ),
    # Enlaces directos a .csv como plan B
    "csv_link": "a[href$='.csv'], a[href*='.csv?']",
}

KNOWN_RISKS = ["Conservative", "Intermediate", "Aggressive", "Ultra Aggressive"]

RISK_VALUE_MAP = {
    "Conservative": ["conservador", "conservative"],
    "Intermediate": ["intermedio", "intermediate"],
    "Aggressive": ["agresivo", "aggressive"],
    "Ultra Aggressive": ["ultra agresivo", "ultra-aggressive", "ultra aggressive"],
}

# =========================
# Utils
# =========================
def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def normalize_risk(r: str) -> str:
    m = r.strip().lower()
    for k, vals in RISK_VALUE_MAP.items():
        if m == k.lower() or any(m == v for v in vals):
            return k
    return r.strip()

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fill_hard(page, selector: str, value: str):
    loc = page.locator(selector).first
    loc.wait_for(state="visible", timeout=8000)
    loc.fill("")
    loc.type(value)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def click_hard(page, selector_or_text: str, by_text: bool = False, timeout: int = 8000):
    if by_text:
        page.get_by_text(selector_or_text, exact=True).click(timeout=timeout)
    else:
        loc = page.locator(selector_or_text).first
        loc.scroll_into_view_if_needed(timeout=timeout)
        loc.click(timeout=timeout)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def click_by_text_ci(page, text: str, timeout: int = 12000):
    loc = page.get_by_text(text, exact=False).first
    loc.scroll_into_view_if_needed(timeout=timeout)
    loc.click(timeout=timeout)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def select_by_label_or_value(page, select_selector: str, wanted_label: str, alt_values: list[str]):
    sel = page.locator(select_selector).first
    try:
        sel.select_option(label=wanted_label)
        return True
    except Exception:
        pass
    try:
        for v in alt_values:
            found = sel.locator(f"option:has-text('{v}')")
            if found.count() > 0:
                opt_value = found.first.get_attribute("value")
                if opt_value:
                    sel.select_option(value=opt_value)
                else:
                    sel.select_option(label=v)
                return True
    except Exception:
        pass
    try:
        sel.click(timeout=6000)
    except Exception:
        pass
    try:
        click_by_text_ci(page, wanted_label)
        return True
    except Exception:
        for v in alt_values:
            try:
                click_by_text_ci(page, v)
                return True
            except Exception:
                continue
    return False

def download_csv_resilient(page, dest: Path) -> bool:
    """
    Intenta 1) expect_download con botones conocidos,
            2) click alternativo por texto,
            3) buscar <a href='...csv'> y descargarlo con fetch dentro de la página.
    """
    # 1) Botón conocido + expect_download
    btn = page.locator(SELECTORS["download_btn"]).first
    try:
        btn.scroll_into_view_if_needed(timeout=6000)
        with page.expect_download(timeout=20000) as dl_info:
            btn.click(timeout=12000)
        download = dl_info.value
        download.save_as(str(dest))
        return True
    except Exception:
        pass

    # 2) Alternativa: click por texto “CSV”
    try:
        with page.expect_download(timeout=20000) as dl_info:
            click_by_text_ci(page, "CSV", timeout=12000)
        download = dl_info.value
        download.save_as(str(dest))
        return True
    except Exception:
        pass

    # 3) Buscar enlace .csv y “descargar” vía fetch en el contexto de la página
    try:
        link = page.locator(SELECTORS["csv_link"]).first
        if link.count() > 0:
            href = link.get_attribute("href")
            if href:
                # Traemos el CSV desde la propia página (evita CORS en el runner)
                csv_text = page.evaluate(
                    """async (url) => {
                        const r = await fetch(url, { credentials: 'include' });
                        if (!r.ok) throw new Error('HTTP ' + r.status);
                        return await r.text();
                    }""",
                    href,
                )
                dest.write_text(csv_text, encoding="utf-8")
                return True
    except Exception:
        pass

    return False

# =========================
# Flujo principal
# =========================
def login(page, email: str, password: str, login_url: str):
    page.goto(login_url, wait_until="load", timeout=60000)
    fill_hard(page, SELECTORS["email"], email)
    fill_hard(page, SELECTORS["password"], password)
    click_hard(page, SELECTORS["login_button"])
    page.wait_for_timeout(1500)

def configure_symbol(page, symbol: str):
    try:
        fill_hard(page, SELECTORS["symbol_input"], symbol)
        page.keyboard.press("Enter")
        page.wait_for_timeout(600)
    except Exception:
        pass

def scrape_all(page, symbol: str, risks: list[str], horarios: list[str], dashboard_url: str, out_root: Path):
    page.goto(dashboard_url, wait_until="networkidle", timeout=60000)
    configure_symbol(page, symbol)

    today_str = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")
    base_dir = out_root / symbol

    for risk in risks:
        risk_norm = normalize_risk(risk)
        risk_dir = base_dir / risk_norm / today_str
        ensure_dir(risk_dir)

        # Seleccionar riesgo
        ok = select_by_label_or_value(page, SELECTORS["risk_select"], risk_norm, RISK_VALUE_MAP.get(risk_norm, []))
        if not ok:
            try:
                click_hard(page, SELECTORS["risk_dropdown"])
                click_by_text_ci(page, risk_norm)
            except Exception:
                for alias in RISK_VALUE_MAP.get(risk_norm, []):
                    try:
                        click_by_text_ci(page, alias)
                        break
                    except Exception:
                        continue

        for hhmm in horarios:
            # Seleccionar horario
            ok_t = select_by_label_or_value(page, SELECTORS["time_select"], hhmm, [hhmm])
            if not ok_t:
                try:
                    click_hard(page, SELECTORS["time_dropdown"])
                    click_by_text_ci(page, hhmm)
                except Exception:
                    pass

            # Pequeña espera para que la grilla/serie se regenere
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(600)

            filename = f"{symbol}_{risk_norm}_{hhmm.replace(':','')}.csv"
            dest = risk_dir / filename

            # Descargar
            try:
                ok_dl = download_csv_resilient(page, dest)
                if ok_dl:
                    print(f"✔︎ Guardado: {dest}")
                else:
                    print(f"* Error al descargar {symbol}/{risk_norm}/{hhmm}: no se encontró botón/enlace CSV")
            except Exception as e:
                print(f"* Error al descargar {symbol}/{risk_norm}/{hhmm}: {e}")

            page.wait_for_timeout(400)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scraper BTM CSVs con Playwright")
    parser.add_argument("--symbol", default=os.getenv("SYMBOL", "SPX"))
    parser.add_argument("--risks", default=os.getenv("RISKS", ",".join(KNOWN_RISKS)))
    parser.add_argument("--horarios", default=os.getenv("HORARIOS",
        "09:40,10:00,10:20,10:40,11:00,11:20,11:30,12:00,12:20,12:40,13:00,13:20,13:40,14:00,14:20,14:40,15:00"))
    parser.add_argument("--login-url", default=DEFAULT_LOGIN_URL)
    parser.add_argument("--dashboard-url", default=DEFAULT_DASHBOARD_URL)
    parser.add_argument("--out", default="data")
    args = parser.parse_args()

    email = os.getenv("BTM_EMAIL")
    password = os.getenv("BTM_PASSWORD")
    if not email or not password:
        print("Faltan credenciales BTM_EMAIL / BTM_PASSWORD")
        sys.exit(1)

    risks = [normalize_risk(r) for r in args.risks.split(",") if r.strip()]
    horarios = [h.strip() for h in args.horarios.split(",") if h.strip()]

    out_root = Path(args.out)
    ensure_dir(out_root)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True, timezone_id=TZ)
        page = context.new_page()

        login(page, email, password, args.login_url)
        scrape_all(page, args.symbol, risks, horarios, args.dashboard_url, out_root)

        context.close()
        browser.close()

if __name__ == "__main__":
    main()
