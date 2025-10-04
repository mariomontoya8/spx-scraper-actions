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
    "risk_dropdown": "[data-testid='risk-dropdown'], #risk, text=Risk, text=Riesgo",
    "time_dropdown": "[data-testid='time-dropdown'], #time, text=Time, text=Hora",
    "download_btn": "text=Download CSV, button:has-text('Download CSV'), text=Descargar CSV",
}

KNOWN_RISKS = ["Conservative", "Intermediate", "Aggressive", "Ultra Aggressive"]


# =========================
# Utils
# =========================
def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def normalize_risk(r: str) -> str:
    m = r.strip().lower()
    mapping = {
        "conservador": "Conservative",
        "conservative": "Conservative",
        "intermedio": "Intermediate",
        "intermediate": "Intermediate",
        "agresivo": "Aggressive",
        "aggressive": "Aggressive",
        "ultra_agresivo": "Ultra Aggressive",
        "ultra-aggressive": "Ultra Aggressive",
        "ultra aggressive": "Ultra Aggressive",
    }
    return mapping.get(m, r.strip())

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def click_hard(page, selector_or_text: str, by_text: bool = False):
    if by_text:
        page.get_by_text(selector_or_text, exact=True).click(timeout=8000)
    else:
        page.locator(selector_or_text).first.click(timeout=8000)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def click_by_text_ci(page, text: str):
    # Click por texto no exacto (case-insensitive)
    page.get_by_text(text, exact=False).first.click(timeout=12000)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fill_hard(page, selector: str, value: str):
    loc = page.locator(selector).first
    loc.wait_for(state="visible", timeout=8000)
    loc.fill("")
    loc.type(value)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def select_from_dropdown(page, dropdown_selector: str, option_text: str):
    # 1) Abre el dropdown
    page.locator(dropdown_selector).first.click(timeout=8000)
    # 2) Intenta como <select>
    try:
        page.locator(dropdown_selector).first.select_option(label=option_text)
        return
    except Exception:
        pass
    # 3) Intenta por texto visible (no exacto)
    page.get_by_text(option_text, exact=False).first.click(timeout=12000)


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
        page.wait_for_timeout(500)
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
        try:
            select_from_dropdown(page, SELECTORS["risk_dropdown"], risk_norm)
        except Exception:
            click_by_text_ci(page, risk_norm)

        for hhmm in horarios:
            # Seleccionar horario
            try:
                select_from_dropdown(page, SELECTORS["time_dropdown"], hhmm)
            except Exception:
                click_by_text_ci(page, hhmm)

            filename = f"{symbol}_{risk_norm}_{hhmm.replace(':','')}.csv"
            dest = risk_dir / filename

            try:
                with page.expect_download(timeout=30000) as download_info:
                    click_hard(page, SELECTORS["download_btn"])
                download = download_info.value
                download.save_as(str(dest))
                print(f"✔︎ Guardado: {dest}")
            except PWTimeout:
                print(f"✖︎ Timeout al descargar {symbol}/{risk_norm}/{hhmm}")
            except Exception as e:
                print(f"✖︎ Error al descargar {symbol}/{risk_norm}/{hhmm}: {e}")
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

    # Normaliza riesgos (acepta español/inglés)
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
