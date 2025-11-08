#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Primevo / BigBuy → Public Feed (HTTPS)
- Download geselecteerde BigBuy XML's (uit products_list.txt) via FTP
- Parse + filter server-side
- Publiceert naar /public:
    catalog_preview.csv  (volledige velden + image_preview)
    catalog_light.csv    (lichte versie voor Google Sheets)
    feed.xml             (eenvoudige XML)
    last_run.txt, _ping.txt

ENV (GitHub Actions → secrets/variables):
  BB_HOST, BB_USER, BB_PASS
  MIN_PRICE, MAX_PRICE, MIN_STOCK, MAX_WEIGHT, RECENT_MONTHS, MAX_PRODUCTS
  VAT_PCT, SHIP_COST_EUR, BOL_FEE_PCT, BOL_FEE_FIXED, MIN_PROFIT_EUR, MIN_MARGIN_PCT

Terminologie:
  - pvd  = DP (Dropship Price)  → inkoop excl. btw (jouw kost)
  - price = AVP incl. btw       → adviesprijs consument (referentie)
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

# --------- FILTER INSTELLINGEN / PRIJS-PARAMS (ENV met defaults) ---------
def _getf(name, default): 
    try: 
        return float(os.environ.get(name, str(default)))
    except Exception:
        return float(default)

def _geti(name, default): 
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return int(default)

# filters
MIN_PRICE     = _getf("MIN_PRICE", 10.0)
MAX_PRICE     = _getf("MAX_PRICE", 80.0)
MIN_STOCK     = _geti("MIN_STOCK", 1)
MAX_WEIGHT    = _getf("MAX_WEIGHT", 8.0)
RECENT_MONTHS = _geti("RECENT_MONTHS", 18)
MAX_PRODUCTS  = _geti("MAX_PRODUCTS", 5000)

# prijsmodel
VAT_PCT         = _getf("VAT_PCT", 21.0)       # btw %
SHIP_COST_EUR   = _getf("SHIP_COST_EUR", 0.0)  # € (0 indien BigBuy verzendt)
BOL_FEE_PCT     = _getf("BOL_FEE_PCT", 12.0)   # %
BOL_FEE_FIXED   = _getf("BOL_FEE_FIXED", 0.3)  # €
MIN_PROFIT_EUR  = _getf("MIN_PROFIT_EUR", 3.0) # €
MIN_MARGIN_PCT  = _getf("MIN_MARGIN_PCT", 15.0)# %

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

    price = _pfloat(r.get("price"))  # AVP incl. btw als band voor filtering
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

# --------- PRIJSBEREKENING (DP=pvd, AVP=price incl. btw) ---------
def _round_up_to_euro(x: float) -> float:
    import math
    return math.ceil(x)

def _calc_fields(pvd: float, avp_inc: float) -> dict:
    vat   = VAT_PCT/100.0
    f_pct = BOL_FEE_PCT/100.0
    f_fix = BOL_FEE_FIXED
    ship  = SHIP_COST_EUR
    minP  = MIN_PRICE
    maxP  = MAX_PRICE
    minProf = MIN_PROFIT_EUR
    minMarg = MIN_MARGIN_PCT/100.0

    # ondergrenzen verkoopprijs
    denom_profit = max(1e-9, 1 - vat - f_pct)
    denom_margin = max(1e-9, 1 - vat - f_pct - minMarg)
    s1 = (minProf + f_fix + ship + pvd) / denom_profit
    s2 = (f_fix + ship + pvd) / denom_margin

    # nooit onder min_price of onder AVP incl. btw
    s_base = max(minP, s1, s2, avp_inc or 0.0)

    # afronden naar hele euro omhoog en .01 eraf → *.99*
    s = min(maxP, _round_up_to_euro(s_base) - 0.01)

    profit = s*(1 - vat - f_pct) - (f_fix + ship + pvd)
    margin = (profit/s) if s > 0 else 0.0
    ok = (profit >= minProf) and (margin >= minMarg) and (s >= minP) and (s <= maxP)

    avp_ex = avp_inc/(1+vat) if avp_inc else 0.0
    diff   = (s/avp_inc - 1.0) if avp_inc else 0.0
    ge_avp = (s >= avp_inc) if avp_inc else True

    return {
        "sell_price": round(s, 2),
        "profit_eur": round(profit, 2),
        "margin_pct": round(margin, 4),  # 0.1578 = 15.78%
        "ok": "TRUE" if ok else "FALSE",
        "avp_inc": round(avp_inc, 2),
        "avp_excl": round(avp_ex, 2),
        "diff_vs_avp": round(diff, 4),
        "ge_avp": "TRUE" if ge_avp else "FALSE",
    }

# --------- FTP / XML ---------
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

    # sorteer: hoogste stock eerst, dan laagste prijs (AVP)
    all_rows.sort(key=lambda r: (-_pint(r.get("stock")), _pfloat(r.get("price"))))
    if MAX_PRODUCTS and len(all_rows) > MAX_PRODUCTS:
        all_rows = all_rows[:MAX_PRODUCTS]

    # ---- CSV: preview (vol) ----
    csv_preview = os.path.join(OUT_DIR, "catalog_preview.csv")
    with open(csv_preview, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["Keep?"] + FIELDS +
                      ["sell_price","profit_eur","margin_pct","ok",
                       "avp_inc","avp_excl","diff_vs_avp","ge_avp",
                       "image_preview"]
        )
        w.writeheader()
        for r in all_rows:
            row = {"Keep?": ""}
            row.update(r)
            # prijsvelden op basis van pvd (DP) en price (AVP incl.)
            pvd_val = _pfloat(r.get("pvd"))
            avp_inc = _pfloat(r.get("price"))
            row.update(_calc_fields(pvd_val, avp_inc))
            row["image_preview"] = f'=IMAGE("{r.get("image1","")}")' if r.get("image1") else ""
            w.writerow(row)

    # ---- CSV: light (sneller voor Sheets) ----
    light_fields = [
        "id","name","price","pvd","ean13","stock","image1","brand",
        "sell_price","profit_eur","margin_pct","ok",
        "avp_inc","avp_excl","diff_vs_avp","ge_avp"
    ]
    csv_light = os.path.join(OUT_DIR, "catalog_light.csv")
    with open(csv_light, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Keep?"] + light_fields)
        w.writeheader()
        for r in all_rows:
            row = {"Keep?": ""}
            # basisvelden
            for k in ["id","name","price","pvd","ean13","stock","image1","brand"]:
                row[k] = r.get(k, "")
            # berekende velden
            pvd_val = _pfloat(r.get("pvd"))
            avp_inc = _pfloat(r.get("price"))
            row.update(_calc_fields(pvd_val, avp_inc))
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
