import os
import re
import json
import asyncio
import hashlib
from urllib.parse import urljoin, urlparse, parse_qs, unquote
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

BASE_URL = "https://www.sebi.gov.in"
LEGAL_URL = "https://www.sebi.gov.in/legal.html"

BASE_DIR = "D:\Data\SEBI_LEGAL"
OUTPUT_JSON = "D:\Data\SEBI_LEGAL\sebi_version_db.json"
    
os.makedirs(BASE_DIR, exist_ok=True)

# ======================================================
# Utility Functions
# ======================================================

def safe_filename(text):
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"\s+", "_", text).strip("_")[:200]


def extract_year(text):
    match = re.search(r"(19|20)\d{2}", text)
    return match.group() if match else "Unknown"


def classify_type(title):
    title = title.lower()
    if "amend" in title:
        return "Amendment"
    if "corrigendum" in title:
        return "Corrigendum"
    return "Original"

def calculate_hash(content):
    return hashlib.sha256(content).hexdigest()

def normalize_pdf_url(detail_url, raw):
    full = urljoin(detail_url, raw)

    if "/web/?" in full and "file=" in full:
        qs = parse_qs(urlparse(full).query)
        real = qs.get("file", [None])[0]
        if real:
            return unquote(real)

    return full


# ✅ STRICT FILTER – ONLY LEGAL PDFs
def is_valid_pdf(url):
    if not url:
        return False

    url = url.lower()

    return (
        url.endswith(".pdf")
        and "sebi.gov.in" in url
        and (
            "/legal/" in url
            or "/sebi_data/" in url
        )
    )


def load_db():
    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_db(db):
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=4, ensure_ascii=False)


# ======================================================
# SAFE NAVIGATION
# ======================================================

async def safe_goto(page, url, retries=3):
    for attempt in range(retries):
        try:
            await page.goto(url, timeout=120000)
            return True
        except:
            print(f"Retry {attempt+1} failed for {url}")
            await asyncio.sleep(5)
    return False


# ======================================================
# SAFE PDF DOWNLOAD (Handles Aborted Large PDFs)
# ======================================================

async def safe_pdf_download(context, pdf_url, retries=3):

    for attempt in range(retries):
        try:
            response = await context.request.get(
                pdf_url,
                timeout=300000   # increased timeout for large PDFs
            )

            if not response.ok:
                continue

            body = await response.body()

            # Ensure valid PDF header
            if body[:4] == b"%PDF":
                return body

        except Exception as e:
            print(f"Download retry {attempt+1} for {pdf_url}")
            await asyncio.sleep(5)

    return None


# ======================================================
# Extract Legal Sections Only
# ======================================================

async def get_legal_sections(page):

    allowed_sections = [
        "Acts",
        "Rules",
        "Regulations",
        "General Orders",
        "Guidelines",
        "Master Circulars",
        "Advisory/Guidance",
        "Circulars",
        "Gazette Notification",
        "Guidance Notes"
    ]

    sections = []
    anchors = page.locator("a")
    count = await anchors.count()

    for i in range(count):
        text = (await anchors.nth(i).inner_text()).strip()
        href = await anchors.nth(i).get_attribute("href")

        if not text or not href:
            continue

        if text in allowed_sections and "sid=1" in href:
            sections.append({
                "name": text,
                "url": urljoin(BASE_URL, href)
            })

    unique = {s["name"]: s for s in sections}
    return list(unique.values())


# ======================================================
# Process Detail Page (LOGIC UNCHANGED)
# ======================================================

async def process_detail(context, db, section, date, title, detail_url):

    page = await context.new_page()

    ok = await safe_goto(page, detail_url)
    if not ok:
        await page.close()
        return

    soup = BeautifulSoup(await page.content(), "html.parser")

    pdf_links = []

    iframe = await page.query_selector("iframe")
    if iframe:
        raw = await iframe.get_attribute("src")
        if raw:
            pdf_url = normalize_pdf_url(detail_url, raw)
            if is_valid_pdf(pdf_url):
                pdf_links.append(pdf_url)

    for a in soup.select("a[href$='.pdf']"):
        pdf_url = urljoin(BASE_URL, a["href"])
        if is_valid_pdf(pdf_url):
            pdf_links.append(pdf_url)

    await page.close()

    if not pdf_links:
        return

    year = extract_year(date)
    base_title = safe_filename(title)
    doc_id = f"{section}_{base_title}"

    if doc_id not in db:
        db[doc_id] = {
            "section": section,
            "base_title": title,
            "versions": []
        }

    for pdf_url in pdf_links:

        content = await safe_pdf_download(context, pdf_url)
        if not content:
            print("Failed to download:", pdf_url)
            continue

        file_hash = calculate_hash(content)

        existing_hashes = [v["hash"] for v in db[doc_id]["versions"]]
        if file_hash in existing_hashes:
            continue

        version_number = len(db[doc_id]["versions"]) + 1

        folder = os.path.join(BASE_DIR, section, year)
        os.makedirs(folder, exist_ok=True)

        filename = f"{base_title}_v{version_number}.pdf"
        save_path = os.path.join(folder, filename)

        with open(save_path, "wb") as f:
            f.write(content)

        db[doc_id]["versions"].append({
            "version": version_number,
            "date": date,
            "year": year,
            "type": classify_type(title),
            "pdf_link": pdf_url,
            "source_page": detail_url,
            "file_path": save_path,
            "hash": file_hash
        })

        save_db(db)

        print("Saved:", filename)

        await asyncio.sleep(3)


# ======================================================
# Scrape Section (UNCHANGED LOGIC)
# ======================================================

async def scrape_section(context, db, section):

    print(f"\n🚀 Processing Section: {section['name']}")

    page = await context.new_page()

    ok = await safe_goto(page, section["url"])
    if not ok:
        await page.close()
        return

    hist = page.locator("a:has-text('Historical Data')")
    if await hist.count() > 0:
        await hist.first.click()
        await page.wait_for_timeout(3000)

    await page.wait_for_selector("table tbody tr", timeout=120000)

    while True:

        soup = BeautifulSoup(await page.content(), "html.parser")
        rows = soup.select("table tbody tr")

        if not rows:
            break

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue

            date = cols[0].get_text(strip=True)
            year = extract_year(date)
            if year == "Unknown" or int(year) < 2026:
                continue
            a = cols[1].find("a", href=True)
            if not a:
                continue

            title = a.get_text(strip=True)
            detail_url = urljoin(BASE_URL, a["href"])

            await process_detail(context, db,
                                 section["name"],
                                 date,
                                 title,
                                 detail_url)

        next_btn = page.locator("a:has-text('Next')")
        if await next_btn.count() == 0:
            break

        await next_btn.first.click()
        await page.wait_for_timeout(4000)

    await page.close()


# ======================================================
# MAIN
# ======================================================

async def main():

    db = load_db()

    async with async_playwright() as p:

        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        page = await context.new_page()
        await safe_goto(page, LEGAL_URL)

        sections = await get_legal_sections(page)

        print("Detected Legal Sections:",
              [s["name"] for s in sections])

        for sec in sections:
            await scrape_section(context, db, sec)

        await browser.close()

    print("\n🎉 Completed Successfully")
    print("Total Documents:", len(db))


asyncio.run(main())
