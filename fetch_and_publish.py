#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Primevo / BigBuy → Public Feed (HTTPS)
- Downloadt geselecteerde BigBuy XML's (uit products_list.txt) via FTP
- Parse + filtert server-side (voor Sheets)
- Publiceert naar /public:
    catalog_preview.csv  (volledige velden + image_preview formule)
    catalog_light.csv    (lichte versie voor Google Sheets)
    feed.xml             (eenvoudige XML)
    last_run.txt, _ping.txt

Secrets (GitHub Actions → Repository secrets):
  BB_HOST=www.dropshippers.com.es
  BB_USER=...
  BB_PASS=...

Optionele regelbestanden (repo root; 1 item per regel):
  allow_categories.txt   # BigBuy category ID’s om toe te laten (bv. 2662)
  deny_keywords.txt      # te blokkeren keywords (case-insensitive)
  allow_brands.txt       # whitelabel brands (id/naam); leeg = alles
"""

import os
import sys
import io
import csv
import ftplib
import xml.etree.ElementTree as ET
from datetime import datetime

# --------- ENV / PAD ---------
HOST = os.environ.get("BB_HOST")
USER = os.environ.get("BB_USER")
PASS = os.environ.get("BB_PASS")

INPUT_LIST = "products_list.txt"
OUT_DIR = "public"
os.makedirs(OUT_DIR, exist_ok=True)

# --------- VELDEN UIT XML ---------
FIELDS = [
    "id", "name", "description", "price", "pvd", "ean13", "stock", "image1",
    "category", "brand", "date_upd", "width", "height", "depth", "weight"
]

# --------- FILTER INSTELLINGEN ---------
MIN_PRICE     = 10.0
MAX_PRICE     = 80.0
MIN_STOCK     = 1
MAX_WEIGHT    = 8.0          # kg; genegeerd als leeg
RECENT_MONTHS = 18           # genegeerd als date_upd ontbreekt
MAX_PRODUCTS  = 5000         # harde cap na sorteren

# --------- REGELBESTANDEN (optioneel) ---------
def _load_list(path: str):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]

ALLOW_CATS   = set(_load_list("allow_categories.txt"))           # bv. {"2662","2609"}
DENY_WORDS   = set(w.lower() for w in _load_list("deny_keywords.txt"))
ALLOW_BRANDS = set(_load_list("allow_brands.txt"))

# --------- HELPERS ---------
def _pfloat(x, default=0.0):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return default

def _pint(x, default=0):
    try:
        return int(float(x))
    except Exception:
        return default

def _is_recent(iso: str) -> bool:
    if not iso:
        return True
    try:
        dt = datetime.strptime(iso.strip(), "%Y-%m-%d %H:%M:%S")
        return (datetime.now() - dt).days <= RECENT_MONTHS * 30
    except Exception:
        return True

def _has_allowed_category(cat_field: str) -> bool:
    if not ALLOW_CATS:
        return True
    parts = [c.strip() for c in (cat_field or "").split(",") if c.strip()]
    return any(p in ALLOW_CATS for p in parts)

def _has_bad_word(name: str, desc: str) -> bool:
    if not DENY_WORDS:
        return False
    t = f"{name or ''} {desc or ''}".lower()
    return any(bad in t for bad in DENY_WORDS)

def _keep_row(r: dict) -> bool:
    # categorie-filter
    if not _has_allowed_category(r.get("category", "")):
        return False

    # basisfilters
    if _pint(r.get("stock")) < MIN_STOCK:
        return False

    price = _pfloat(r.get("price"))
    if not (MIN_PRICE <= price <= MAX_PRICE):
        return False

    if not r.get("ean13"):
        return False
    if not r.get("image1"):
        return False

    # gewicht (optioneel)
    w = _pfloat(r.get("weight"))
    if w and w > MAX_WEIGHT:
        return False

    # actualiteit (optioneel)
    if r.get("date_upd") and not _is_recent(r.get("date_upd")):
        return False

    # brand whitelist (optioneel)
    if ALLOW_BRANDS and (r.get("brand", "") not in ALLOW_BRANDS):
        return False

    # keywords blokkeren
    if _has_bad_word(r.get("name", ""), r.get("description", "")):
        return False

    return True

def _ftp_fetch(ftp: ftplib.FTP, filename: str) -> bytes:
    path = f"/files/products/xml/standard/{filename}"
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {path}", buf.write)
    return buf.getvalue()

def _parse_xml(xml_bytes: bytes):
    out = []
    root = ET.fromstring(xml_bytes)
    for p in root.findall(".//product"):
        row = {}
        for f in FIELDS:
            el = p.find(f)
            row[f] = (el.text.strip() if (el is not None and el.text) else "")
        out.append(row)
    return out

# --------- MAIN ---------
def main():
    with open(INPUT_LIST, "r", encoding="utf-8") as f:
        file_names = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]

    all_rows = []
    files_done = 0

    with ftplib.FTP(HOST, timeout=60) as ftp:
        ftp.set_pasv(True)
        ftp.login(USER, PASS)

        for name in file_names:
            try:
                raw = _ftp_fetch(ftp, name)
                rows = _parse_xml(raw)
                files_done += 1

                kept = [r for r in rows if _keep_row(r)]
                all_rows.extend(kept)
                print(f"[ok] {name}: {len(rows)} → kept {len(kept)}")

            except Exception as e:
                print(f"[warn] {name}: {e}", file=sys.stderr)

    # sorteer: hoogste stock eerst, dan laagste prijs
    all_rows.sort(key=lambda r: (-_pint(r.get("stock")), _pfloat(r.get("price"))))
    if MAX_PRODUCTS and len(all_rows) > MAX_PRODUCTS:
        all_rows = all_rows[:MAX_PRODUCTS]

    # ---- CSV: preview (vol) ----
    csv_preview = os.path.join(OUT_DIR, "catalog_preview.csv")
    with open(csv_preview, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Keep?"] + FIELDS + ["image_preview"])
        w.writeheader()
        for r in all_rows:
            row = {"Keep?": ""}
            row.update(r)
            row["image_preview"] = f'=IMAGE("{r.get("image1","")}")' if r.get("image1") else ""
            w.writerow(row)

    # ---- CSV: light (sneller voor Sheets) ----
    light_fields = ["id", "name", "price", "pvd", "ean13", "stock", "image1"]
    csv_light = os.path.join(OUT_DIR, "catalog_light.csv")
    with open(csv_light, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Keep?"] + light_fields)
        w.writeheader()
        for r in all_rows:
            row = {"Keep?": ""}
            for k in light_fields:
                row[k] = r.get(k, "")
            w.writerow(row)

    # ---- XML eenvoudig ----
    def esc(t: str) -> str:
        return (t or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    xml_path = os.path.join(OUT_DIR, "feed.xml")
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n<catalog>')
        for r in all_rows:
            f.write("<product>")
            for k in FIELDS:
                if k in {"name", "description"}:
                    f.write(f"<{k}><![CDATA[{r.get(k,'')}]]></{k}>")
                else:
                    f.write(f"<{k}>{esc(r.get(k))}</{k}>")
            f.write("</product>")
        f.write("</catalog>")

    # markers
    open(os.path.join(OUT_DIR, "last_run.txt"), "w", encoding="utf-8").write(datetime.utcnow().isoformat())
    open(os.path.join(OUT_DIR, "_ping.txt"), "w", encoding="utf-8").write("ok")

    print(f"Done. files={files_done}, kept={len(all_rows)}")
    print(f"Preview CSV: {csv_preview}")
    print(f"Light   CSV: {csv_light}")
    print(f"XML        : {xml_path}")

if __name__ == "__main__":
    main()
