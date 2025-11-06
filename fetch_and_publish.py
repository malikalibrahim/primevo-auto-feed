#!/usr/bin/env python3
import os, sys, csv, ftplib, io, xml.etree.ElementTree as ET
from datetime import datetime, timezone

HOST = os.environ.get("BB_HOST")
USER = os.environ.get("BB_USER")
PASS = os.environ.get("BB_PASS")

INPUT_LIST = "products_list.txt"
OUT_DIR = "public"
os.makedirs(OUT_DIR, exist_ok=True)

# نضيف حقول مفيدة للفلترة + الإخراج
FIELDS = [
    "id","name","description","price","pvd","ean13","stock","image1",
    "category","brand","date_upd","width","height","depth","weight"
]

BAD_WORDS = {"adult","sex","erot","porno","condom","dildo","vibrator","bdsm"}
MIN_PRICE = 10.0
MAX_PRICE = 80.0
MAX_WEIGHT = 8.0     # كغ، تجاهل لو غير متوفر
MAX_PRODUCTS = 5000  # سقف نهائي للعدد
RECENT_MONTHS = 18   # تجاهل لو ما في date_upd

def parse_float(x, default=0.0):
    try:
        return float(x.replace(",", "."))
    except:
        return default

def parse_int(x, default=0):
    try:
        return int(float(x))
    except:
        return default

def is_recent(iso):
    # "YYYY-MM-DD HH:MM:SS"
    try:
        dt = datetime.strptime(iso.strip(), "%Y-%m-%d %H:%M:%S")
        age_days = (datetime.now() - dt).days
        return age_days <= RECENT_MONTHS * 30
    except:
        return True  # لو ما قدرنا نقرأها ما نرفض بسببها

def keep_row(r):
    # شروط أساسية
    stock = parse_int(r.get("stock","0"))
    if stock <= 0: return False

    price = parse_float(r.get("price","0"))
    if not (MIN_PRICE <= price <= MAX_PRICE): return False

    if not r.get("ean13"): return False
    if not r.get("image1"): return False

    # فلترة كلمات مزعجة
    text = (r.get("name","") + " " + r.get("description","")).lower()
    if any(bad in text for bad in BAD_WORDS): return False

    # أوزان/أبعاد لو متوفرة
    w_kg = parse_float(r.get("weight","0"))
    if w_kg and w_kg > MAX_WEIGHT: return False

    # حداثة التحديث (اختياري)
    du = r.get("date_upd","").strip()
    if du and not is_recent(du): return False

    return True

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
    files_done = 0

    with open(INPUT_LIST, "r", encoding="utf-8") as f:
        names = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

    with ftplib.FTP(HOST, timeout=60) as ftp:
        ftp.set_pasv(True)
        ftp.login(USER, PASS)
        for name in names:
            try:
                data = fetch_file(ftp, name)
                rows = parse_xml(data)
                files_done += 1
                # فلترة
                for r in rows:
                    if keep_row(r):
                        all_rows.append(r)
                print(f"[ok] {name}: {len(rows)} rows → kept {sum(1 for _ in rows if keep_row(_))}")
            except Exception as e:
                print(f"[warn] {name}: {e}", file=sys.stderr)

    # ترتيب حسب المخزون نزولاً ثم السعر صعوداً
    all_rows.sort(key=lambda r: (-parse_int(r.get("stock","0")), parse_float(r.get("price","0"))))
    # سقف العدد
    if MAX_PRODUCTS and len(all_rows) > MAX_PRODUCTS:
        all_rows = all_rows[:MAX_PRODUCTS]

    # CSV
    csv_path = os.path.join(OUT_DIR,"catalog_preview.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Keep?"]+FIELDS+["image_preview"])
        w.writeheader()
        for r in all_rows:
            r2 = {"Keep?": ""}
            r2.update(r)
            r2["image_preview"] = f'=IMAGE("{r.get("image1","")}")' if r.get("image1") else ""
            w.writerow(r2)

    # XML مصغّر
    xml_path = os.path.join(OUT_DIR,"feed.xml")
    def esc(t): 
        return (t or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n<catalog>')
        for r in all_rows:
            f.write("<product>")
            for k in FIELDS:
                if k in {"name","description"}:
                    f.write(f"<{k}><![CDATA[{r.get(k,'')}]]></{k}>")
                else:
                    f.write(f"<{k}>{esc(r.get(k))}</{k}>")
            f.write("</product>")
        f.write("</catalog>")

    # لمحة و Ping
    open(os.path.join(OUT_DIR,"last_run.txt"),"w").write(datetime.now(timezone.utc).isoformat())
    open(os.path.join(OUT_DIR,"_ping.txt"),"w").write("ok")

    print(f"Done. files={files_done}, kept={len(all_rows)}")
if __name__ == "__main__":
    main()
