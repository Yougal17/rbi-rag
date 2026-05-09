import os
import json
import time
import random
import shutil
from pathlib import Path
from playwright.sync_api import sync_playwright

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

METADATA_FILE = "data/processed/metadata.json"
RAW_DIR       = "data/raw"
DOWNLOAD_DIR  = Path(RAW_DIR)

MIN_DELAY = 3.0
MAX_DELAY = 6.0

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def polite_sleep():
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    print(f"  ⏳ Waiting {delay:.1f}s...")
    time.sleep(delay)


def is_real_pdf(filepath):
    try:
        with open(filepath, "rb") as f:
            header = f.read(4)
        return header == b"%PDF"
    except:
        return False


def load_metadata():
    with open(METADATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_metadata(records):
    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)


# ─────────────────────────────────────────────
# MAIN DOWNLOADER
# ─────────────────────────────────────────────

def run_downloader():
    print("=" * 60)
    print("RBI PDF Downloader — Browser Mode (Playwright)")
    print("=" * 60)

    records = load_metadata()
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    to_download = []
    for r in records:
        if not r.get("pdf_url"):
            continue
        filepath = DOWNLOAD_DIR / r["pdf_filename"]
        if filepath.exists() and is_real_pdf(str(filepath)):
            continue
        to_download.append(r)

    print(f"\n📋 Need to download: {len(to_download)} real PDFs")
    print(f"⏭️  Already have:    {len(records) - len(to_download)} real PDFs\n")

    if not to_download:
        print("✅ Nothing to download — all PDFs are real.")
        return

    total_success = 0
    total_failed  = 0

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            accept_downloads=True,
        )

        # ── Response interceptor ─────────────────────
        intercepted = {}

        def handle_response(response):
            content_type = response.headers.get("content-type", "")
            url = response.url
            if "pdf" in content_type.lower() or url.lower().endswith(".pdf"):
                try:
                    body = response.body()
                    if body[:4] == b"%PDF":
                        intercepted["bytes"] = body
                        print(f"  📡 Intercepted: {len(body):,} bytes")
                except Exception:
                    # Body not readable — will be caught by download handler
                    pass

        page = context.new_page()
        page.on("response", handle_response)

        # ── Establish session ────────────────────────
        print("🌐 Establishing session...")
        page.goto("https://www.rbi.org.in", timeout=30000)
        page.wait_for_load_state("networkidle")
        polite_sleep()

        page.goto(
            "https://www.rbi.org.in/Scripts/BS_CircularIndexDisplay.aspx?Id=12500",
            timeout=30000
        )
        page.wait_for_load_state("networkidle")
        polite_sleep()
        print("  ✅ Session established\n")

        # ── Download loop ────────────────────────────
        for i, record in enumerate(to_download):
            pdf_url      = record["pdf_url"]
            pdf_filename = record["pdf_filename"]
            circ_num     = record["circular_number"]
            filepath     = DOWNLOAD_DIR / pdf_filename

            print(f"[{i+1}/{len(to_download)}] {circ_num}")
            print(f"  🔗 {pdf_url[:80]}...")

            intercepted.clear()
            saved = False

            try:
                # Try interception + automatic download simultaneously
                with page.expect_download(timeout=15000) as download_info:
                    try:
                        page.goto(pdf_url, timeout=15000, wait_until="commit")
                    except Exception:
                        pass  # "Download is starting" — expected

                    # Give interceptor time to capture bytes
                    time.sleep(2)

                    # Channel 1: intercepted response bytes
                    if "bytes" in intercepted:
                        with open(str(filepath), "wb") as f:
                            f.write(intercepted["bytes"])
                        if is_real_pdf(str(filepath)):
                            size_kb = filepath.stat().st_size / 1024
                            print(f"  ✅ Saved via intercept: {size_kb:.1f} KB")
                            total_success += 1
                            saved = True

            except Exception:
                # expect_download timed out — no automatic download triggered
                # Check if interception still caught something
                if "bytes" in intercepted and not saved:
                    with open(str(filepath), "wb") as f:
                        f.write(intercepted["bytes"])
                    if is_real_pdf(str(filepath)):
                        size_kb = filepath.stat().st_size / 1024
                        print(f"  ✅ Saved via intercept (no download): {size_kb:.1f} KB")
                        total_success += 1
                        saved = True

            # Channel 2: automatic browser download
            if not saved:
                try:
                    download = download_info.value
                    tmp_path = download.path()
                    if tmp_path and os.path.exists(tmp_path):
                        shutil.copy(tmp_path, str(filepath))
                        if is_real_pdf(str(filepath)):
                            size_kb = filepath.stat().st_size / 1024
                            print(f"  ✅ Saved via download: {size_kb:.1f} KB")
                            total_success += 1
                            saved = True
                except Exception:
                    pass

            if not saved:
                print(f"  ❌ Both channels failed")
                total_failed += 1

            polite_sleep()

            # Save progress every 10 downloads
            if (i + 1) % 10 == 0:
                save_metadata(records)
                print(f"  💾 Progress saved ({i+1}/{len(to_download)})")

            # Refresh session every 50 downloads
            if (i + 1) % 50 == 0:
                print("\n  🔄 Refreshing session...")
                intercepted.clear()
                page.goto("https://www.rbi.org.in", timeout=30000)
                page.wait_for_load_state("networkidle")
                polite_sleep()
                print("  ✅ Session refreshed\n")

        context.close()
        browser.close()

    # ── Final summary ────────────────────────────
    print("\n" + "=" * 60)
    print("DOWNLOAD COMPLETE")
    print(f"  ✅ Successfully downloaded: {total_success}")
    print(f"  ❌ Failed:                 {total_failed}")
    print("=" * 60)

    real_pdfs = sum(
        1 for r in records
        if r.get("pdf_filename") and
        (DOWNLOAD_DIR / r["pdf_filename"]).exists() and
        is_real_pdf(str(DOWNLOAD_DIR / r["pdf_filename"]))
    )
    print(f"\n📂 Real PDFs on disk: {real_pdfs} / {len(records)}")


if __name__ == "__main__":
    run_downloader()