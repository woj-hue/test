#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json
from collections import defaultdict
from googleapiclient.discovery import build
from google.oauth2 import service_account

SHEET_ID = os.environ["SHEET_ID"]
RANGE_DANE = os.environ.get("RANGE_DANE","Dane!A2:K")
RANGE_POZ  = os.environ.get("RANGE_POZ","Pozycje!A2:J")

REQUIRED_DANE_COLS = {"invoice_id":0, "supplier_name":1, "supplier_tax_id":2, "issue_date":3, "currency":5, "total_net":6, "total_vat":7, "total_gross":8}

def get_svc():
    key = os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"]
    info = json.loads(key)
    creds = service_account.Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    return build("sheets","v4",credentials=creds)

def fetch_values(svc, rng):
    return svc.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=rng).execute().get("values",[])

def as_float(x):
    try:
        return float(str(x).replace(" ","").replace(",","."))
    except:
        return None

def main():
    svc = get_svc()
    dane = fetch_values(svc, RANGE_DANE)
    poz  = fetch_values(svc, RANGE_POZ)

    missing = []
    for r, row in enumerate(dane, start=2):
        for colname, idx in REQUIRED_DANE_COLS.items():
            if idx >= len(row) or row[idx] in ("", None):
                missing.append(f"Dane!{r}:{colname} puste")

    sumy = defaultdict(lambda: {"net":0.0,"vat":0.0,"gross":0.0})
    for row in poz:
        if not row: continue
        inv = row[0] if len(row)>0 else ""
        net = as_float(row[6]) if len(row)>6 else 0.0
        vat = as_float(row[8]) if len(row)>8 else None
        gross = as_float(row[9]) if len(row)>9 else None
        if net is not None: sumy[inv]["net"] += net
        if vat is not None: sumy[inv]["vat"] += vat
        if gross is not None: sumy[inv]["gross"] += gross

    tol = 0.02
    mismatch = []
    for row in dane:
        if not row or len(row) < 9: continue
        inv = row[0]
        tnet = as_float(row[6]); tvat = as_float(row[7]); tgr = as_float(row[8])
        if inv in sumy:
            s = sumy[inv]
            if tnet is not None and abs(s["net"] - tnet) > tol:
                mismatch.append(f"{inv}: sum_net {s['net']:.2f} != total_net {tnet:.2f}")
            if tvat is not None and abs(s["vat"] - tvat) > tol:
                mismatch.append(f"{inv}: sum_vat {s['vat']:.2f} != total_vat {tvat:.2f}")
            if tgr is not None and abs(s["gross"] - tgr) > tol:
                mismatch.append(f"{inv}: sum_gross {s['gross']:.2f} != total_gross {tgr:.2f}")

    if missing or mismatch:
        print("VALIDATION FAILED")
        if missing:
            print("Missing fields:\n - " + "\n - ".join(missing))
        if mismatch:
            print("Totals mismatch:\n - " + "\n - ".join(mismatch))
        sys.exit(1)
    else:
        print("OK: Google Sheet dane kompletne i spójne")

if __name__ == "__main__":
    main()
