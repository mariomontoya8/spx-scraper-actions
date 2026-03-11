#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper BackTestingMarket (Backtesting Idea) sin Playwright.

Flujo:
- Login con CSRF
- Detecta horas y riesgos desde la UI clásica /backtestingIdea
- Lanza tarea en /backtestingIdea2/get_backtesting_idea
- Consulta resultado en /backtestingIdea2/task_result/<task_id>
- Reintenta hasta que el resultado esté listo
- Guarda CSVs en data/<SYMBOL>/<Strategy>/<risk>/table_...csv
- Escribe data/<SYMBOL>/<Strategy>/manifest.csv

Requiere secrets:
  BTM_EMAIL, BTM_PASSWORD
"""

from __future__ import annotations
import os
import re
import time
import argparse
from pathlib import Path
from typing import List, Iterable, Dict, Any, Tuple, Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter


BASE = os.getenv("BTM_BASE_URL", "https://backtestingmarket.com").rstrip("/")
EMAIL = os.getenv("BTM_EMAIL")
PASSWORD = os.getenv("BTM_PASSWORD")

# Sesión HTTP
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
})
adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
session.mount("https://", adapter)
session.mount("http://", adapter)

# Cache simple para no pedir /backtestingIdea dos veces
_BACKTESTING_IDEA_HTML: Optional[str] = None


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

    payload = {
        "email": email,
        "password": password,
        "csrf_token": csrf,
    }

    r = session.post(
        urljoin(BASE, "/login"),
        data=payload,
        allow_redirects=True,
        timeout=60
    )
    r.raise_for_status()

    chk = session.get(urljoin(BASE, "/backtestingIdea"), timeout=30)
    chk.raise_for_status()

    global _BACKTESTING_IDEA_HTML
    _BACKTESTING_IDEA_HTML = chk.text

    print("✅ Login OK")


# ---------- Helpers ----------
def normalize_time_hour(hora: str) -> str:
    s = str(hora).strip().replace(":", "")
    m = re.search(r"(\d{1,2})\D?(\d{2})", s)
    return (m.group(1).zfill(2) + m.group(2)) if m else s.zfill(4)


def _get_backtestingidea_html() -> str:
    global _BACKTESTING_IDEA_HTML
    if _BACKTESTING_IDEA_HTML is None:
        r = session.get(urljoin(BASE, "/backtestingIdea"), timeout=30)
        r.raise_for_status()
        _BACKTESTING_IDEA_HTML = r.text
    return _BACKTESTING_IDEA_HTML


def _parse_ui_options() -> Tuple[List[str], List[str]]:
    html = _get_backtestingidea_html()
    soup = BeautifulSoup(html, "html.parser")

    hours: List[str] = []
    risks: List[str] = []

    sel_hour = soup.find(id="timeHour") or soup.find("select", {"name": "timeHour"})
    if sel_hour:
        for opt in sel_hour.find_all("option"):
            t = (opt.text or "").strip()
            if t and t.lower() != "selecciona una hora":
                hours.append(t)

    sel_risk = soup.find(id="risk") or soup.find("select", {"name": "risk"})
    if sel_risk:
        for opt in sel_risk.find_all("option"):
            val = (opt.get("value") or opt.text or "").strip()
            low = val.lower()
            if val and low not in {"selecciona", "selecciona un riesgo"}:
                risks.append(low.replace(" ", "_"))

    def dedupe(items: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in items:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    return dedupe(hours), dedupe(risks)


def get_timehour_options() -> List[str]:
    hours, _ = _parse_ui_options()
    return hours


def get_risk_options() -> List[str]:
    _, risks = _parse_ui_options()
    return risks


def get_dates(symbol: str) -> List[str]:
    r = session.get(urljoin(BASE, "/get_dates"), params={"symbol": symbol}, timeout=30)
    r.raise_for_status()
    js = r.json()
    if isinstance(js, dict):
        js = js.get("data", js)
    return list(sorted(set(js)))


def _extract_rows_from_payload(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data", [])
    return data if isinstance(data, list) else []


def _poll_task_result(task_id: str, max_tries: int = 18, sleep_s: float = 0.6) -> Dict[str, Any]:
    """
    Consulta SIEMPRE el endpoint final /task_result/<task_id>
    hasta que regrese data o termine el número de intentos.
    """
    task_url = urljoin(BASE, f"/backtestingIdea2/task_result/{task_id}")
    last_payload: Dict[str, Any] = {}

    for _ in range(max_tries):
        rr = session.get(task_url, timeout=120)
        rr.raise_for_status()

        task_payload = rr.json()
        if isinstance(task_payload, dict):
            last_payload = task_payload
        else:
            last_payload = {"raw": task_payload}

        rows = _extract_rows_from_payload(last_payload)
        if rows:
            return last_payload

        if isinstance(last_payload, dict):
            result_ready = last_payload.get("result_ready")
            state = str(last_payload.get("state", "")).upper()

            if state in {"FAILURE", "FAILED", "ERROR"}:
                raise RuntimeError(f"Tarea falló: {last_payload}")

            if result_ready is False or state in {"STARTED", "PENDING", "PROCESSING", "PROGRESS", ""}:
                time.sleep(sleep_s)
                continue

            if result_ready is True:
                return last_payload

        time.sleep(sleep_s)

    return last_payload


# ---------- Descarga de una tabla ----------
def fetch_table_csv(
    symbol: str,
    desde: str,
    hasta: str,
    time_hhmm: str,
    strategy: str,
    risk: str,
    out_dir: str,
    filename: str | None = None,
    clean_numeric: bool = True,
    also_return_df: bool = False,
):
    hora_norm = normalize_time_hour(time_hhmm)

    url = urljoin(BASE, "/backtestingIdea2/get_backtesting_idea")
    params = {
        "desde": desde,
        "hasta": hasta,
        "symbol": symbol,
        "estrategia": strategy,
        "hora": hora_norm,
        "risk": risk,
    }

    r = session.get(url, params=params, timeout=120)
    r.raise_for_status()
    payload = r.json()

    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        final_payload = payload
    elif isinstance(payload, dict) and payload.get("task_id"):
        task_id = payload["task_id"]
        final_payload = _poll_task_result(task_id=task_id, max_tries=18, sleep_s=0.6)
    else:
        raise RuntimeError(f"Respuesta inicial inesperada: {str(payload)[:300]}")

    rows = final_payload.get("data", []) if isinstance(final_payload, dict) else []
    if not isinstance(rows, list):
        raise RuntimeError(
            f"Respuesta final inesperada: {type(rows)} | body={str(final_payload)[:300]}"
        )

    if not rows:
        return None if not also_return_df else pd.DataFrame()

    df = pd.DataFrame(rows)

    # --- FORZAR NOMBRES EXACTOS DE COLUMNAS ---
    rename_map = {
        "Close": "Close",
        "close": "Close",

        "Credit": "Credit",
        "credit": "Credit",

        "Day": "Day",
        "Date": "Day",
        "fecha": "Day",
        "date": "Day",

        "Diff": "Diff",
        "diff": "Diff",

        "Option": "Option",
        "Type": "Option",
        "type": "Option",

        "P/L": "P/L",
        "Result": "P/L",
        "result": "P/L",

        "Price": "Price",
        "price": "Price",

        "Strikes": "Strikes",
        "strikes": "Strikes",

        "Time": "Time",
        "time": "Time",
        "hora": "Time",

        "itmOtm": "itmOtm",
        "ITM/OTM": "itmOtm",
        "itm_otm": "itmOtm",

        "movimiento_esperado": "movimiento_esperado",
        "Expected Move": "movimiento_esperado",
        "expected_move": "movimiento_esperado",

        "score_30min": "score_30min",
        "Score 30min": "score_30min",
        "score30min": "score_30min",
    }

    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Ordenar columnas si existen
    desired_order = [
        "Close",
        "Credit",
        "Day",
        "Diff",
        "Option",
        "P/L",
        "Price",
        "Strikes",
        "Time",
        "itmOtm",
        "movimiento_esperado",
        "score_30min",
    ]
    ordered_existing = [c for c in desired_order if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered_existing]
    df = df[ordered_existing + remaining]

    if clean_numeric:
        def clean_money(x):
            return pd.to_numeric(
                str(x).replace("$", "").replace(",", "").strip(),
                errors="coerce"
            )

        for c in ["Close", "Credit", "Diff", "P/L", "Price", "movimiento_esperado", "score_30min"]:
            if c in df.columns:
                df[c] = df[c].map(clean_money)

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    if filename is None:
        safe_risk = re.sub(r"\s+", "", str(risk))
        safe_strategy = re.sub(r"\s+", "", str(strategy))
        filename = f"table_{symbol}_{safe_strategy}_{safe_risk}_{hora_norm}_{desde}_{hasta}.csv"

    csv_file = out_path / filename
    df.to_csv(csv_file, index=False, encoding="utf-8")
    print(f"💾 CSV guardado: {csv_file} | Filas: {len(df)}")

    return df if also_return_df else None


# ---------- Descarga masiva ----------
def bulk_download_tables(
    symbol: str,
    strategy: str,
    desde: str | None,
    hasta: str | None,
    hours: Iterable[str] | None,
    risks: Iterable[str] | None,
    out_base: str = "data",
    pause_s: float = 0.1,
    overwrite: bool = False,
) -> pd.DataFrame:
    if not desde or not hasta:
        dates = get_dates(symbol)
        if not dates:
            raise RuntimeError("No se pudieron obtener fechas con /get_dates.")
        desde, hasta = dates[0], dates[-1]
        print(f"🗓️ Rango auto: {desde} → {hasta}")

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
                print(f"⏭️ Ya existe, salto: {fpath.name}")
                manifest.append({
                    "risk": risk,
                    "hour": hhmm,
                    "file": str(fpath),
                    "rows": None,
                    "status": "skipped"
                })
                continue

            try:
                df = fetch_table_csv(
                    symbol=symbol,
                    desde=desde,
                    hasta=hasta,
                    time_hhmm=hhmm,
                    strategy=strategy,
                    risk=risk,
                    out_dir=str(risk_dir),
                    filename=fname,
                    also_return_df=True,
                )

                rows = 0 if df is None else int(len(df))

                if df is None or rows == 0:
                    total_empty += 1
                    print(f"⚠️ Vacío: {risk:<16} {hhmm} -> {fname}")
                    manifest.append({
                        "risk": risk,
                        "hour": hhmm,
                        "file": str(fpath),
                        "rows": 0,
                        "status": "empty"
                    })
                else:
                    total_done += 1
                    manifest.append({
                        "risk": risk,
                        "hour": hhmm,
                        "file": str(fpath),
                        "rows": rows,
                        "status": "ok"
                    })
                    print(f"✅ {risk:<16} {hhmm}: {rows:>4} filas -> {fname}")

            except Exception as e:
                print(f"❌ Error {risk} {hhmm}: {e}")
                manifest.append({
                    "risk": risk,
                    "hour": hhmm,
                    "file": str(fpath),
                    "rows": 0,
                    "status": f"error:{e}"
                })
            finally:
                time.sleep(pause_s)

    man_df = pd.DataFrame(manifest)
    man_df.to_csv(base / "manifest.csv", index=False)

    print("\nResumen:")
    print(f"  OK: {total_done} | Vacíos: {total_empty} | Saltados: {total_skipped}")
    print(f"  Manifest: {base / 'manifest.csv'}")

    return man_df


# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="Descarga masiva Backtesting Idea (sin navegador)")
    p.add_argument("--symbol", default=os.getenv("SYMBOL", "SPX"))
    p.add_argument("--strategy", default=os.getenv("STRATEGY", "Vertical"))
    p.add_argument("--desde", default=os.getenv("DESDE", ""))
    p.add_argument("--hasta", default=os.getenv("HASTA", ""))
    p.add_argument(
        "--risks",
        default=os.getenv("RISKS", "auto"),
        help="CSV: conservador,intermedio,agresivo,ultra_agresivo | 'auto'"
    )
    p.add_argument(
        "--hours",
        default=os.getenv("HORARIOS", "auto"),
        help="CSV: 09:40,10:00,... | 'auto'"
    )
    p.add_argument("--out-base", default=os.getenv("OUT_BASE", "data"))
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--pause", type=float, default=float(os.getenv("PAUSE_S", "0.10")))
    return p.parse_args()


def main():
    args = parse_args()
    print(f"▶️ Run SYMBOL={args.symbol} STRATEGY={args.strategy}")

    login(EMAIL, PASSWORD)

    hours = None if args.hours.strip().lower() in {"", "auto", "all"} else [
        h.strip() for h in args.hours.split(",") if h.strip()
    ]
    risks = None if args.risks.strip().lower() in {"", "auto", "all"} else [
        r.strip() for r in args.risks.split(",") if r.strip()
    ]

    bulk_download_tables(
        symbol=args.symbol,
        strategy=args.strategy,
        desde=args.desde or None,
        hasta=args.hasta or None,
        hours=hours,
        risks=risks,
        out_base=args.out_base,
        pause_s=args.pause,
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    main()
