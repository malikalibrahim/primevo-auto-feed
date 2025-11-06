# Primevo Auto-Update Feed (BigBuy FTP → HTTPS)

This repo turns BigBuy FTP XML files into a **public HTTPS feed** that auto-refreshes (via GitHub Actions).
It downloads your BigBuy FTP XMLs, extracts fields, and publishes:
- `catalog_preview.csv`
- `feed.xml`
- `last_run.txt`

## Quick Setup
1) Create a new **public** GitHub repo (e.g. `primevo-auto-feed`).
2) Upload these files keeping the same structure.
3) Add repo secrets in **Settings → Secrets → Actions**:
   - `BB_HOST` = `www.dropshippers.com.es`
   - `BB_USER` = your FTP username
   - `BB_PASS` = your FTP password
4) Enable **Pages** with **GitHub Actions**.
5) Commit & push. It runs every 6 hours by default.
6) Use the URLs:
   - `https://<user>.github.io/<repo>/catalog_preview.csv`
   - `https://<user>.github.io/<repo>/feed.xml`
