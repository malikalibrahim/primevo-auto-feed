#!/usr/bin/env python3
import os, sys, csv, ftplib, io, xml.etree.ElementTree as ET
from datetime import datetime, timezone

HOST = os.environ.get("BB_HOST")
USER = os.environ.get("BB_USER")
PASS = os.environ.get("BB_PASS")

INPUT_LIST = "products_list.txt"
OUT_DIR = "public"
os.makedirs(OUT_DIR, exist_ok=True)

FIELDS = ["id","name","description","price","pvd","ean13","stock","image1"]

def fetch_file(ftp, name):
    path = f"/files/products/xml/standard/{name}"
    bio = io.BytesIO()
    ftp.retrbinary(f"RETR {path}", bio.write)
    return bio.getvalue()

def parse_xml(xml_bytes):
    out = []
    root = ET.fromstring(xml_bytes)
    for p in root.findall(".//product"):
        row = {}
        for f in FIELDS:
            el = p.find(f)
            row[f] = (el.text.strip() if (el is not None and el.text) else "")
        out.append(row)
    return out

def main():
    all_rows = []
    with open(INPUT_LIST, "r", encoding="utf-8") as f:
        names = [line.strip() for line in f if line.strip()]
    with ftplib.FTP(HOST) as ftp:
        ftp.login(USER, PASS)
        for name in names:
            try:
                data = fetch_file(ftp, name)
                all_rows.extend(parse_xml(data))
                print(f"[ok] {name}")
            except Exception as e:
                print(f"[warn] {name}: {e}", file=sys.stderr)
    # CSV
    with open(os.path.join(OUT_DIR,"catalog_preview.csv"),"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Keep?"]+FIELDS+["image_preview"])
        w.writeheader()
        for r in all_rows:
            r2 = {"Keep?": ""}
            r2.update(r)
            r2["image_preview"] = f'=IMAGE("{r.get("image1","")}")' if r.get("image1") else ""
            w.writerow(r2)
    # XML
    with open(os.path.join(OUT_DIR,"feed.xml"),"w",encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\\n<catalog>')
        for r in all_rows:
            def esc(t): 
                return (t or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
            f.write("<product>")
            f.write(f"<id>{esc(r.get('id'))}</id>")
            f.write(f"<name><![CDATA[{r.get('name','')}]]></name>")
            f.write(f"<description><![CDATA[{r.get('description','')}]]></description>")
            f.write(f"<price>{esc(r.get('price'))}</price>")
            f.write(f"<pvd>{esc(r.get('pvd'))}</pvd>")
            f.write(f"<ean13>{esc(r.get('ean13'))}</ean13>")
            f.write(f"<stock>{esc(r.get('stock'))}</stock>")
            f.write(f"<image1>{esc(r.get('image1'))}</image1>")
            f.write("</product>")
        f.write("</catalog>")
    # Timestamp
    with open(os.path.join(OUT_DIR,"last_run.txt"),"w") as f:
        f.write(datetime.now(timezone.utc).isoformat())

if __name__ == "__main__":
    main()
