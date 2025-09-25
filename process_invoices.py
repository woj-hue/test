# -*- coding: utf-8 -*-
"""
process_invoices.py
Stabilna podstawa przetwarzania faktur z trybem DRY_RUN, walidacją sum
i zapisem do Excela (3 arkusze). Przygotowane haki pod integracje Google.

Uruchomienia:
  python process_invoices.py --once
  DRY_RUN=true python process_invoices.py --once
  python process_invoices.py               # pętla co 5 min

Wymagania (lokalny zapis Excela):
  pip install openpyxl
"""

from __future__ import annotations

import os
import sys
import json
import time
import logging
import argparse
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

# =============== KONFIG ===============

INBOX_DIR = Path(os.getenv("INBOX_DIR", "Skopiowane_faktury")).resolve()
OUT_XLSX = Path(os.getenv("OUT_XLSX", "Szablon_Faktury_AI_v4.xlsx")).resolve()
LOOP_SLEEP_SEC = int(os.getenv("LOOP_SLEEP_SEC", "300"))  # 5 min
DRY_RUN = os.getenv("DRY_RUN", "").lower() in {"1", "true", "yes"}
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Placeholders pod Google – używamy tylko jeśli są dostępne
SHEET_ID = os.getenv("SHEET_ID")
GOOGLE_APPLICATION_CREDENTIALS_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
DOC_AI_PROJECT = os.getenv("DOC_AI_PROJECT")
DOC_AI_LOCATION = os.getenv("DOC_AI_LOCATION", "eu")
DOC_AI_PROCESSOR = os.getenv("DOC_AI_PROCESSOR")

# =============== LOGGING ===============

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("process_invoices")

# =============== DANE ===============


@dataclass
class LineItem:
    description: str
    quantity: float
    unit_price: float
    net: float
    vat_rate: float
    vat: float
    gross: float


@dataclass
class Invoice:
    number: str
    issue_date: str  # ISO yyyy-mm-dd
    seller: str
    buyer: str
    currency: str
    total_net: float
    total_vat: float
    total_gross: float
    line_items: List[LineItem]

    def as_header_row(self) -> Dict[str, Any]:
        return {
            "number": self.number,
            "issue_date": self.issue_date,
            "seller": self.seller,
            "buyer": self.buyer,
            "currency": self.currency,
            "total_net": self.total_net,
            "total_vat": self.total_vat,
            "total_gross": self.total_gross,
        }


# =============== WALIDACJA ===============


def validate_totals(data: Dict[str, Any], tol: float = 0.01) -> Tuple[bool, List[str]]:
    """
    Minimalna walidacja sum pozycji vs. sumy całkowite.
    Zwraca (ok, errors). Zgodne z testami w repo.
    """
    total_net = float(data.get("total_net", 0) or 0)
    total_vat = float(data.get("total_vat", 0) or 0)
    total_gross = float(data.get("total_gross", 0) or 0)

    items = data.get("line_items", []) or []
    net_sum = sum(float(i.get("net", 0) or 0) for i in items)
    vat_sum = sum(float(i.get("vat", 0) or 0) for i in items)
    gross_sum = sum(float(i.get("gross", 0) or 0) for i in items)

    errors: List[str] = []
    if abs(net_sum - total_net) > tol:
        errors.append(f"Mismatch net: {net_sum:.2f} vs {total_net:.2f}")
    if abs(vat_sum - total_vat) > tol:
        errors.append(f"Mismatch vat: {vat_sum:.2f} vs {total_vat:.2f}")
    if abs(gross_sum - total_gross) > tol:
        errors.append(f"Mismatch gross: {gross_sum:.2f} vs {total_gross:.2f}")

    return len(errors) == 0, errors


# =============== IO: PARSING (UPROSZCZONY) ===============

SUPPORTED_EXT = {".pdf", ".jpg", ".jpeg", ".png"}


def find_new_files(folder: Path) -> List[Path]:
    folder.mkdir(parents=True, exist_ok=True)
    files = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXT]
    return sorted(files)


def parse_invoice_stub(file_path: Path) -> Invoice:
    """
    Uproszczony parser – w realnym wdrożeniu tu podpinamy Google Document AI.
    Na razie używamy „fikcyjnych” danych, żeby przepływ działał end-to-end.
    """
    # Prosty przykład: 2 pozycje, 23% VAT
    items = [
        LineItem(description=f"Pozycja 1 z {file_path.name}", quantity=1.0, unit_price=100.0,
                 net=100.0, vat_rate=23.0, vat=23.0, gross=123.0),
        LineItem(description=f"Pozycja 2 z {file_path.name}", quantity=2.0, unit_price=50.0,
                 net=100.0, vat_rate=23.0, vat=23.0, gross=123.0),
    ]
    total_net = sum(i.net for i in items)
    total_vat = sum(i.vat for i in items)
    total_gross = sum(i.gross for i in items)

    inv = Invoice(
        number=f"INV-{file_path.stem}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        issue_date=datetime.now().date().isoformat(),
        seller="Acme Sp. z o.o.",
        buyer="Twoja Firma Sp. z o.o.",
        currency="PLN",
        total_net=total_net,
        total_vat=total_vat,
        total_gross=total_gross,
        line_items=items,
    )
    return inv


# =============== ZAPIS DO EXCELA ===============

def ensure_openpyxl():
    try:
        import openpyxl  # noqa: F401
    except Exception as e:
        raise RuntimeError(
            "Brak pakietu 'openpyxl'. Zainstaluj: pip install openpyxl"
        ) from e


def write_to_excel(invoices: List[Invoice], out_xlsx: Path) -> None:
    """
    Zapis do pliku XLSX w 3 arkuszach:
      - Dane (nagłówki),
      - Pozycje (linie),
      - Koszty_surowcow (puste – gotowe pod późniejsze reguły).
    """
    ensure_openpyxl()
    from openpyxl import Workbook
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    # Arkusz 1: Dane
    ws1 = wb.active
    ws1.title = "Dane"
    headers1 = ["number", "issue_date", "seller", "buyer", "currency", "total_net", "total_vat", "total_gross"]
    ws1.append(headers1)
    for inv in invoices:
        row = [inv.number, inv.issue_date, inv.seller, inv.buyer, inv.currency,
               inv.total_net, inv.total_vat, inv.total_gross]
        ws1.append(row)

    # Arkusz 2: Pozycje
    ws2 = wb.create_sheet("Pozycje")
    headers2 = ["invoice_number", "description", "quantity", "unit_price", "net", "vat_rate", "vat", "gross"]
    ws2.append(headers2)
    for inv in invoices:
        for li in inv.line_items:
            ws2.append([inv.number, li.description, li.quantity, li.unit_price, li.net, li.vat_rate, li.vat, li.gross])

    # Arkusz 3: Koszty_surowcow (na razie puste nagłówki)
    ws3 = wb.create_sheet("Koszty_surowcow")
    ws3.append(["invoice_number", "category", "amount", "note"])

    # Proste auto-szerokości
    for ws in (ws1, ws2, ws3):
        for col_idx, _ in enumerate(ws.iter_cols(min_row=1, max_row=1), start=1):
            ws.column_dimensions[get_column_letter(col_idx)].width = 18

    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    wb.save(str(out_xlsx))
    log.info("Zapisano Excel: %s", out_xlsx)


# =============== GOOGLE – HAKI (bez crashowania) ===============

def have_google_creds() -> bool:
    return bool(SHEET_ID and GOOGLE_APPLICATION_CREDENTIALS_JSON)


def try_init_google() -> None:
    """
    Opcjonalna inicjalizacja – nie blokuje działania, jeśli brak sekretów.
    """
    if not have_google_creds():
        log.info("Google credentials/secrets nie ustawione – pomijam integracje Google.")
        return
    try:
        # Tu można dodać realne inicjacje klientów (Sheets/Drive/DocAI).
        log.info("Google credentials dostępne – integracje mogą zostać podłączone w kolejnych iteracjach.")
    except Exception as e:
        log.warning("Nie udało się zainicjalizować Google klienta: %s", e)


# =============== GŁÓWNY PRZEPŁYW ===============

def process_one_file(file_path: Path) -> Optional[Invoice]:
    """
    Przetwarza pojedynczy plik na obiekt Invoice. Tu podpinamy OCR/DocAI.
    """
    try:
        inv = parse_invoice_stub(file_path)
        ok, errors = validate_totals({
            "total_net": inv.total_net,
            "total_vat": inv.total_vat,
            "total_gross": inv.total_gross,
            "line_items": [asdict(li) for li in inv.line_items],
        })
        if not ok:
            log.warning("Walidacja sum NIE przeszła dla %s: %s", file_path.name, errors)
        else:
            log.info("Walidacja sum OK dla %s", file_path.name)
        return inv
    except Exception as e:
        log.exception("Błąd podczas przetwarzania %s: %s", file_path, e)
        return None


def run_once() -> None:
    try_init_google()
    files = find_new_files(INBOX_DIR)
    if not files:
        log.info("Brak nowych plików w %s", INBOX_DIR)
        return

    log.info("Znaleziono %d plik(ów) do przetworzenia.", len(files))
    invoices: List[Invoice] = []
    for f in files:
        inv = process_one_file(f)
        if inv:
            invoices.append(inv)

    if not invoices:
        log.info("Nie powstały żadne faktury do zapisu.")
        return

    if DRY_RUN:
        log.info("[DRY_RUN] Pomijam zapis do Excela. Podgląd danych:")
        for inv in invoices:
            log.info("HEADER: %s", json.dumps(inv.as_header_row(), ensure_ascii=False))
            for li in inv.line_items:
                log.info("LINE: %s", json.dumps(asdict(li), ensure_ascii=False))
        return

    write_to_excel(invoices, OUT_XLSX)


def run_loop() -> None:
    log.info("Start pętli – co %d s", LOOP_SLEEP_SEC)
    while True:
        run_once()
        time.sleep(LOOP_SLEEP_SEC)


# =============== CLI ===============

def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Przetwarzanie faktur (podstawa, DRY_RUN, Excel).")
    p.add_argument("--once", action="store_true", help="Wykonaj tylko jednorazowy przebieg i zakończ.")
    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    log.info("Konfiguracja: INBOX_DIR=%s, OUT_XLSX=%s, DRY_RUN=%s", INBOX_DIR, OUT_XLSX, DRY_RUN)
    INBOX_DIR.mkdir(parents=True, exist_ok=True)

    try:
        if args.once:
            run_once()
        else:
            run_loop()
        return 0
    except KeyboardInterrupt:
        log.info("Przerwano przez użytkownika.")
        return 0
    except Exception as e:
        log.exception("Błąd krytyczny: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
