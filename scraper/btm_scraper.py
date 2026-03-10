#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import csv
import json
import time
import argparse
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


BASE = "https://www.backtestingmarket.com"
LOGIN_URL = "https://www.backtestingmarket.com/login"
IDEA_URL = "https://www.backtestingmarket.com/backtesting-idea"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
}


# =========================
# Helpers
# =========================

def clean_text(x) -> str:
    if x is None:
        return ""
    return str(x).strip()


def first_not_empty(*vals):
    for v in vals:
        if v is not None and str(v).strip() != "":
            return v
    return None


def extract_csrf(html: str) -> Tuple[Optional[str], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    for inp in soup.select("input[type='hidden']"):
        name = inp.get("name")
        value = inp.get("value")
        if name and value and (
            "csrf" in name.lower()
            or "_token" in name.lower()
            or "token" == name.lower()
        ):
            return name, value
    return None, None


def discover_api_endpoint(html: str) -> Optional[str]:
    patterns = [
        r'["\'](\/backtestingIdea\/get_backtesting_[^"\']+)["\']',
        r'["\'](\/backtesting-idea\/get_backtesting_[^"\']+)["\']',
        r'["\'](\/[^"\']*get_backtesting[^"\']*)["\']',
    ]
    for pat in patterns:
        m = re.search(pat, html, flags=re.I)
        if m:
            return urljoin(BASE, m.group(1))
    return None


def parse_select_options(html: str) -> Tuple[List[str], List[str]]:
    soup = BeautifulSoup(html, "html.parser")

    hours = []
    risks = []

    # Buscar selects por id/name/class/texto
    for sel in soup.select("select"):
        meta = " ".join(
            filter(
                None,
                [
                    sel.get("id", ""),
                    sel.get("name", ""),
                    " ".join(sel.get("class", [])),
                ],
            )
        ).lower()

        vals = [
            clean_text(opt.get("value")) or clean_text(opt.text)
            for opt in sel.select("option")
        ]
        vals = [v for v in vals if v]

        if not vals:
            continue

        if any(k in meta for k in ["hour", "time", "entry"]):
            hours.extend(vals)
        elif any(k in meta for k in ["risk", "profile"]):
            risks.extend(vals)

    # Fallback por regex en html/js
    if not hours:
        hours = sorted(set(re.findall(r"\b\d{2}-\d{2}\b", html)))

    if not risks:
        candidate_risks = re.findall(
            r"(conservador|intermedio|agresivo|ultra\s*agresivo)",
            html,
            flags=re.I,
        )
        risks = sorted(set(x.lower().replace(" ", "_") for x in candidate_risks))

    # limpiar duplicados preservando orden
    def dedupe(seq):
        seen = set()
        out = []
        for x in seq:
            x = clean_text(x)
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    return dedupe(hours), dedupe(risks)


def try_json(resp: requests.Response):
    try:
        return resp.json()
    except Exception:
        return None


def normalize_rows(payload, date_str: str, hour: str, risk: str) -> List[Dict]:
    rows = []

    if isinstance(payload, dict):
        # buscar listas comunes
        for key in ["data", "results", "rows", "table", "response"]:
            if isinstance(payload.get(key), list):
                payload = payload[key]
                break

    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                row = {"date": date_str, "hour": hour, "risk": risk}
                row.update(item)
                rows.append(row)
    elif isinstance(payload, dict):
        row = {"date": date_str, "hour": hour, "risk": risk}
        row.update(payload)
        rows.append(row)

    return rows


# =========================
# Core
# =========================

class BTMClient:
    def __init__(self, email: str, password: str, timeout: int = 30):
        self.email = email
        self.password = password
        self.timeout = timeout
        self.s = requests.Session()
        self.s.headers.update(HEADERS)

    def login(self):
        r = self.s.get(LOGIN_URL, timeout=self.timeout)
        r.raise_for_status()

        token_name, token_value = extract_csrf(r.text)

        payload = {
            "email": self.email,
            "password": self.password,
        }
        if token_name and token_value:
            payload[token_name] = token_value

        r = self.s.post(LOGIN_URL, data=payload, timeout=self.timeout, allow_redirects=True)
        r.raise_for_status()

        text = r.text.lower()
        if "login" in r.url.lower() and "logout" not in text and "dashboard" not in text:
            raise RuntimeError("Login no exitoso. Revisa usuario/contraseña o cambios en el form.")

    def load_idea_page(self) -> Tuple[str, List[str], List[str], str]:
        r = self.s.get(IDEA_URL, timeout=self.timeout)
        r.raise_for_status()

        html = r.text
        api_url = discover_api_endpoint(html)
        if not api_url:
            raise RuntimeError("No pude detectar el endpoint API. Revisa el HTML actual de la página.")

        hours, risks = parse_select_options(html)
        if not hours:
            raise RuntimeError("No pude detectar horarios.")
        if not risks:
            raise RuntimeError("No pude detectar perfiles de riesgo.")

        return html, hours, risks, api_url

    def fetch_one(self, api_url: str, date_str: str, hour: str, risk: str) -> List[Dict]:
        """
        Intenta varios nombres de parámetros porque BTM cambia entre pantallas.
        """
        candidate_payloads = [
            {"date": date_str, "hour": hour, "risk": risk},
            {"day": date_str, "hour": hour, "risk": risk},
            {"date": date_str, "entryHour": hour, "risk": risk},
            {"date": date_str, "entry_hour": hour, "risk": risk},
            {"date": date_str, "time": hour, "risk": risk},
            {"date": date_str, "hour": hour, "riskProfile": risk},
            {"date": date_str, "hour": hour, "risk_profile": risk},
        ]

        last_error = None

        for payload in candidate_payloads:
            try:
                r = self.s.post(api_url, data=payload, timeout=self.timeout)
                if r.status_code == 404:
                    continue
                r.raise_for_status()

                data = try_json(r)
                if data is None:
                    # a veces responde HTML/texto con json embebido
                    txt = r.text.strip()
                    if txt.startswith("{") or txt.startswith("["):
                        data = json.loads(txt)
                    else:
                        continue

                rows = normalize_rows(data, date_str, hour, risk)
                if rows:
                    return rows

            except Exception as e:
                last_error = e
                continue

        if last_error:
            print(f"[WARN] {date_str} | {hour} | {risk} -> {last_error}", flush=True)

        return []


# =========================
# Output
# =========================

def write_csv(rows: List[Dict], output_file: str):
    if not rows:
        print("No hubo registros para guardar.")
        return

    keys = set()
    for r in rows:
        keys.update(r.keys())

    preferred = ["date", "hour", "risk"]
    rest = sorted(k for k in keys if k not in preferred)
    fieldnames = preferred + rest

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV generado: {output_file} ({len(rows)} filas)")


# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Fecha YYYY-MM-DD")
    parser.add_argument("--output", default="btm_output.csv")
    parser.add_argument("--hours", default="", help="Ej: 10-05,10-15,10-30")
    parser.add_argument("--risks", default="", help="Ej: conservador,intermedio")
    parser.add_argument("--sleep", type=float, default=0.0, help="Pausa entre requests")
    args = parser.parse_args()

    email = os.getenv("BTM_EMAIL", "").strip()
    password = os.getenv("BTM_PASSWORD", "").strip()

    if not email or not password:
        print("Faltan variables de entorno BTM_EMAIL y BTM_PASSWORD", file=sys.stderr)
        sys.exit(1)

    client = BTMClient(email, password)

    print("Login...")
    client.login()

    print("Cargando página...")
    _, detected_hours, detected_risks, api_url = client.load_idea_page()

    selected_hours = [x.strip() for x in args.hours.split(",") if x.strip()] or detected_hours
    selected_risks = [x.strip() for x in args.risks.split(",") if x.strip()] or detected_risks

    print(f"API detectada: {api_url}")
    print(f"Hours: {selected_hours}")
    print(f"Risks: {selected_risks}")

    all_rows = []

    for risk in selected_risks:
        for hour in selected_hours:
            print(f"Consultando {args.date} | {hour} | {risk} ...", flush=True)
            rows = client.fetch_one(api_url, args.date, hour, risk)
            if rows:
                all_rows.extend(rows)
            if args.sleep > 0:
                time.sleep(args.sleep)

    write_csv(all_rows, args.output)


if __name__ == "__main__":
    main()
