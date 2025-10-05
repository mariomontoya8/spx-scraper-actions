#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper BackTestingMarket (Backtesting Idea) sin Playwright.

- Login con CSRF.
- Detecta horas y riesgos desde la UI.
- Llama el endpoint JSON /backtestingIdea/get_backtesting_idea.
- Guarda CSVs en data/<SYMBOL>/<Strategy>/<risk>/table_...csv
- Escribe data/<SYMBOL>/<Strategy>/manifest.csv

Requiere secrets:
  BTM_EMAIL, BTM_PASSWORD
"""

from __future__ import annotations
import os, re, time, argparse
from pathlib import Path
from typing import List, Iterable, Dict, Any

import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = os.getenv("BTM_BASE_URL", "https://backtestingmarket.com").rstrip("/")
EMAIL = os.getenv("BTM_EMAIL")
PASSWORD = os.getenv("BTM_PASSWORD")

# Sesión HTTP
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Safari/537.36"
})

# ---------- Login (CSRF) ----------
def _get_csrf_from_login() -> str | None:
    r = session.get(urljoin(BASE, "/login"), timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    token = soup.find("input", {"name": "csrf_token"})
    return token.get("value") if token else None

def login(email: str, password: str) -> None:
    if not email or not password:
        raise RuntimeError("Faltan BTM_EMAIL/BTM_PASSWORD.")
    csrf = _get_csrf_from_login()
    if not csrf:
        raise RuntimeError("No se encontró csrf_token en /login")
    payload = {"email": email, "password": password, "csrf_token": csrf}
    r = session.post(urljoin(BASE, "/login"), data=payload, allow_redirects=True, timeout=60)
    r.raise_for_status()
    chk = session.get(urljoin(BASE, "/backtestingIdea"), timeout=30)
    chk.raise_for_status()
    print("✅ Login OK")

# ---------- Helpers ----------
def normalize_time_hour(hora: str) -> str:
    s = str(hora).strip().replace(":", "")
    m = re.search(r"(\d{1,2})\D?(\d{2})", s)
    return (m.group(1).zfill(2) + m.group(2)) if m else s.zfill(4)

def get_timehour_options() -> List[str]:
    r = session.get(urljoin(BASE, "/backtestingIdea"), timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    values: List[str] = []
    sel = soup.find(id="timeHour") or soup.find("select", {"name": "timeHour"})
    if sel:
        for opt in sel.find_all("option"):
            t = (opt.text or "").strip()
            if t and t.lower() != "selecciona una hora":
                values.append(t)
    # dedup
    seen, out = set(), []
    for v in values:
        if v not in seen:
            seen.add(v); out.append(v)
    return out

def get_risk_options() -> List[str]:
    r = session.get(urljoin(BASE, "/backtestingIdea"), timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    opts: List[str] = []
    sel = soup.find(id="risk") or soup.find("select", {"name": "risk"})
    if sel:
        for opt in sel.find_all("option"):
            val = (opt.get("value") or opt.text or "").strip()
            if val and val.lower() not in {"selecciona", "selecciona un riesgo"}:
                opts.append(val.strip().lower().replace(" ", "_"))
    seen, out = set(), []
    for v in opts:
        if v not in seen:
            seen.add(v); out.append(v)
    return out

def get_dates(symbol: str) -> List[str]:
    r = session.get(urljoin(BASE, "/get_dates"), params={"symbol": symbol}, timeout=30)
    r.raise_for_status()
    js = r.json()
    if isinstance(js, dict):
        js = js.get("data", js)
    return list(sorted(set(js)))

# ---------- Descarga de una tabla ----------
def fetch_table_csv(
    symbol: str, desde: str, hasta: str, time_hhmm: str, strategy: str, risk: str,
    out_dir: str, filename: str | None = None, clean_numeric: bool = True,
    also_return_df: bool = False,
):
    hora_norm = normalize_time_hour(time_hhmm)
    url = urljoin(BASE, "/backtestingIdea/get_backtesting_idea")
    params = {
        "desde": desde, "hasta": hasta, "symbol": symbol,
        "estrategia": strategy, "hora": hora_norm, "risk": risk,
    }
    r = session.get(url, params=params, timeout=120)
    r.raise_for_status()

    payload = r.json()
    rows = payload.get("data", payload if isinstance(payload, list) else [])
    if not isinstance(rows, list):
        raise RuntimeError(f"Respuesta inesperada: {type(rows)}")
    if not rows:
        return None if not also_return_df else pd.DataFrame()

    df = pd.DataFrame(rows)

    rename_map = {
        "fecha": "Date", "date": "Date",
        "hora": "Time", "time": "Time",
        "strikes": "Strikes", "type": "Type",
        "credit": "Credit", "price": "Price", "close": "Close",
        "result": "Result", "strike_distance": "Strike Distance", "moneyness": "Moneyness",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if clean_numeric:
        def clean_money(x):
            return pd.to_numeric(str(x).replace("$", "").replace(",", "").strip(), errors="coerce")
        for c in ["Credit", "Price", "Close", "Result", "Strike Distance", "Moneyness"]:
            if c in df.columns:
                df[c] = df[c].map(clean_money)

    out_path = Path(out_dir); out_path.mkdir(parents=True, exist_ok=True)
    if filename is None:
        safe_risk = re.sub(r"\s+", "", str(risk))
        safe_strategy = re.sub(r"\s+", "", str(strategy))
        hora_norm = normalize_time_hour(time_hhmm)
        filename = f"table_{symbol}_{safe_strategy}_{safe_risk}_{hora_norm}_{desde}_{hasta}.csv"

    csv_file = out_path / filename
    df.to_csv(csv_file, index=False, encoding="utf-8")
    print(f"💾 CSV guardado: {csv_file} | Filas: {len(df)}")
    return df if also_return_df else None

# ---------- Descarga masiva ----------
def bulk_download_tables(
    symbol: str, strategy: str, desde: str | None, hasta: str | None,
    hours: Iterable[str] | None, risks: Iterable[str] | None,
    out_base: str = "data", pause_s: float = 0.1, overwrite: bool = False,
) -> pd.DataFrame:
    if not desde or not hasta:
        dates = get_dates(symbol)
        if not dates:
            raise RuntimeError("No se pudieron obtener fechas con /get_dates.")
        desde, hasta = dates[0], dates[-1]
        print(f"🗓️  Rango auto: {desde} → {hasta}")

    if hours is None or (isinstance(hours, str) and hours.strip().lower() in {"", "auto", "all"}):
        hours = get_timehour_options()
    if risks is None or (isinstance(risks, str) and risks.strip().lower() in {"", "auto", "all"}):
        risks = get_risk_options()

    hours = list(hours)
    risks = [str(r).strip().lower().replace(" ", "_") for r in risks]

    base = Path(out_base) / symbol / strategy
    base.mkdir(parents=True, exist_ok=True)

    manifest: List[Dict[str, Any]] = []
    total_done = total_empty = total_skipped = 0

    for risk in risks:
        risk_dir = base / risk
        risk_dir.mkdir(parents=True, exist_ok=True)

        for hour in hours:
            hhmm = normalize_time_hour(hour)
            fname = f"table_{symbol}_{strategy}_{risk}_{hhmm}_{desde}_{hasta}.csv"
            fpath = risk_dir / fname

            if fpath.exists() and not overwrite:
                total_skipped += 1
                print(f"⏭️  Ya existe, salto: {fpath.name}")
                manifest.append({"risk": risk, "hour": hhmm, "file": str(fpath), "rows": None, "status": "skipped"})
                continue

            try:
                df = fetch_table_csv(
                    symbol=symbol, desde=desde, hasta=hasta,
                    time_hhmm=hhmm, strategy=strategy, risk=risk,
                    out_dir=str(risk_dir), filename=fname, also_return_df=True
                )
                rows = 0 if df is None else int(len(df))
                if df is None or rows == 0:
                    total_empty += 1
                    print(f"⚠️  Vacío: {risk:<16} {hhmm} -> {fname}")
                    manifest.append({"risk": risk, "hour": hhmm, "file": str(fpath), "rows": 0, "status": "empty"})
                else:
                    total_done += 1
                    manifest.append({"risk": risk, "hour": hhmm, "file": str(fpath), "rows": rows, "status": "ok"})
                    print(f"✅ {risk:<16} {hhmm}: {rows:>4} filas -> {fname}")
            except Exception as e:
                print(f"❌ Error {risk} {hhmm}: {e}")
                manifest.append({"risk": risk, "hour": hhmm, "file": str(fpath), "rows": 0, "status": f"error:{e}"})
            finally:
                time.sleep(pause_s)

    man_df = pd.DataFrame(manifest)
    man_df.to_csv(base / "manifest.csv", index=False)
    print("\nResumen:")
    print(f"  OK: {total_done}  | Vacíos: {total_empty}  | Saltados: {total_skipped}")
    print(f"  Manifest: {base / 'manifest.csv'}")
    return man_df

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="Descarga masiva Backtesting Idea (sin navegador)")
    p.add_argument("--symbol", default=os.getenv("SYMBOL", "SPX"))
    p.add_argument("--strategy", default=os.getenv("STRATEGY", "Vertical"))
    p.add_argument("--desde", default=os.getenv("DESDE", ""))
    p.add_argument("--hasta", default=os.getenv("HASTA", ""))
    p.add_argument("--risks", default=os.getenv("RISKS", "auto"),
                   help="CSV: conservador,intermedio,agresivo,ultra_agresivo | 'auto'")
    p.add_argument("--hours", default=os.getenv("HORARIOS", "auto"),
                   help="CSV: 09:40,10:00,... | 'auto'")
    p.add_argument("--out-base", default=os.getenv("OUT_BASE", "data"))
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--pause", type=float, default=float(os.getenv("PAUSE_S", "0.10")))
    return p.parse_args()

def main():
    args = parse_args()
    print(f"▶️  Run SYMBOL={args.symbol}  STRATEGY={args.strategy}")
    login(EMAIL, PASSWORD)

    hours = None if args.hours.strip().lower() in {"", "auto", "all"} else [h.strip() for h in args.hours.split(",") if h.strip()]
    risks = None if args.risks.strip().lower() in {"", "auto", "all"} else [r.strip() for r in args.risks.split(",") if r.strip()]

    bulk_download_tables(
        symbol=args.symbol, strategy=args.strategy,
        desde=args.desde or None, hasta=args.hasta or None,
        hours=hours, risks=risks, out_base=args.out_base,
        pause_s=args.pause, overwrite=args.overwrite
    )

if __name__ == "__main__":
    main()
