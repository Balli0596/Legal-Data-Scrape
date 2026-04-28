import os
import requests
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://indiacode.nic.in"
START_URL = BASE_URL + "/handle/123456789/1362/browse?type=actyear&order=ASC&rpp=20"
DOWNLOAD_DIR = "D:\Data\IndiaCode_pdfs"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

headers = {
    "User-Agent": "Mozilla/5.0"
}
import json
from datetime import datetime

TRACK_FILE = "D:\Data\pdf_metadata.json"


def load_data():
    if os.path.exists(TRACK_FILE):
        with open(TRACK_FILE, "r") as f:
            data = json.load(f)
            return data.get("pdfs", [])
    return []


def save_data(pdf_list):
    with open(TRACK_FILE, "w") as f:
        json.dump({"pdfs": pdf_list}, f, indent=4)
def get_existing_urls(pdf_list):
    return set(item["url"] for item in pdf_list)
# ---------------------------------------------------
# STEP 1: GET ALL YEAR LINKS
# ---------------------------------------------------
def get_year_links():
    year_links = set()
    offset = 0
    previous_first_year = None

    while True:
        url = START_URL + f"&offset={offset}"
        print("Fetching Year Page:", url)

        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        years = soup.select("ul.list-group li.list-group-item a")

        if not years:
            break

        current_first_year = years[0].text.strip()

        if current_first_year == previous_first_year:
            break

        previous_first_year = current_first_year

        for a in years:
            text = a.text.strip()
            href = a["href"]

            # Only keep 2026
            if text == "2025":
                year_links.add(urljoin(BASE_URL, href))

        offset += 20
        time.sleep(1)

    print("Total Years Found:", len(year_links))
    return sorted(year_links)


# ---------------------------------------------------
# STEP 2: GET HANDLE LINKS
# ---------------------------------------------------
def get_handles_from_year(year_url):
    handles = []
    offset = 0
    previous_first_row = None

    while True:
        url = year_url + f"&offset={offset}"
        print("  Processing:", url)

        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table")
        if not table:
            break

        rows = table.find_all("tr")
        if len(rows) <= 1:
            break

        first_row_text = rows[1].text.strip()

        if first_row_text == previous_first_row:
            break

        previous_first_row = first_row_text

        for row in rows:
            view_link = row.find("a", href=lambda x: x and "view_type=browse" in x)
            if view_link:
                handles.append(urljoin(BASE_URL, view_link["href"]))

        offset += 20
        time.sleep(1)

    print("    Handles found:", len(handles))
    return handles


# ---------------------------------------------------
# STEP 3: GET ALL ENGLISH PDFs FROM HANDLE
# ---------------------------------------------------
def get_pdf_from_handle(handle_url):
    pdfs = []

    response = requests.get(handle_url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract Act Name (usually page title)
    title_tag = soup.find("h2")
    act_name = title_tag.text.strip() if title_tag else "Unknown Act"

    for a in soup.find_all("a", href=True):
        href = a["href"]

        if "/bitstream/" in href and href.endswith(".pdf"):
            filename = href.split("/")[-1]

            if filename.lower().startswith("a"):
                pdfs.append({
                    "url": urljoin(BASE_URL, href),
                    "filename": filename,
                    "act_name": act_name,
                    "source_page": handle_url
                })

    return pdfs

import time

def fetch_with_retry(url, headers, retries=5, delay=3):
    for attempt in range(retries):
        try:
            print(f"Attempt {attempt+1}: Fetching {url}")
            
            response = requests.get(url, headers=headers, timeout=10)

            # ✅ Check success
            if response.status_code == 200 and response.content:
                return response

            print("⚠️ Invalid response, retrying...")

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Error: {e}")

        # ⏳ wait before retry
        time.sleep(delay)

    print("❌ Failed after retries:", url)
    return None
# ---------------------------------------------------
# STEP 4: DOWNLOAD PDF
# ---------------------------------------------------
def download_pdf(pdf_info, existing_urls, pdf_list):
    url = pdf_info["url"]
    filename = pdf_info["filename"]

    filepath = os.path.join(DOWNLOAD_DIR, filename)

    # 🚫 Skip if already in JSON
    if url in existing_urls:
        print("[SKIP - JSON]", filename)
        return

    # 🚫 Skip if file exists
    if os.path.exists(filepath):
        print("[SKIP - FILE]", filename)
    else:
        print("[DOWNLOADING]", filename)

        try:
            response = fetch_with_retry(url, headers)

            if response is None:
                print("[FAILED]", filename)
                return

            with open(filepath, "wb") as f:
                f.write(response.content)

            time.sleep(2)

        except Exception as e:
            print("Error:", e)
            return

    # ✅ Add metadata
    pdf_info["downloaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pdf_info["year"] = "2026"

    pdf_list.append(pdf_info)
    existing_urls.add(url)


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------
def main():
    year_links = get_year_links()

    # Load previous data
    pdf_list = load_data()
    existing_urls = get_existing_urls(pdf_list)

    download_count = 0   # ✅ Track only NEW downloads

    for year_url in year_links:
        print("\nProcessing Year:", year_url)

        handles = get_handles_from_year(year_url)

        for handle_url in handles:
            pdfs = get_pdf_from_handle(handle_url)

            for pdf_info in pdfs:
                before = len(existing_urls)

                download_pdf(pdf_info, existing_urls, pdf_list)

                after = len(existing_urls)

                # ✅ Count only if new PDF added
                if after > before:
                    download_count += 1

                    # ⏳ Delay after every 2 downloads
                    if download_count % 2 == 0:
                        print("⏳ Waiting 5 seconds after 2 downloads...")
                        time.sleep(5)

    # Save updated JSON
    save_data(pdf_list)

    print("\n✅ Scraping completed successfully!")


if __name__ == "__main__":
    main()
