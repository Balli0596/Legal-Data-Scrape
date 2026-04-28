import requests
from bs4 import BeautifulSoup
import os
import time
import json
import datetime
BASE = "https://www.rbi.org.in"
BASE_FOLDER = "D:\Data\RBI"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

PDF_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept": "application/pdf",
    "Referer": "https://www.rbi.org.in/",
    "Connection": "close"
}

# =====================================================
# UTILITIES
# =====================================================

def safe_request(session, method, url, **kwargs):
    for attempt in range(5):
        try:
            response = session.request(method, url, timeout=40, **kwargs)
            response.raise_for_status()
            return response
        except Exception as e:
            wait = 5 * (attempt + 1)
            print(f"Retry {attempt+1} → waiting {wait}s", e)
            time.sleep(wait)
    raise Exception("Max retries reached.")


def extract_pdf_links(html):
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "rbidocs.rbi.org.in" in href and href.lower().endswith(".pdf"):
            links.append(href)

    return list(dict.fromkeys(links))

#sk_8pfijv0p_jUjmazd6vK5b4GO5MqbplZSg
def load_existing(filepath):
    if not os.path.exists(filepath):
        return set()

    with open(filepath, "r") as f:
        return set(line.strip() for line in f if line.startswith("http"))


def save_all(filepath, data):
    with open(filepath, "w") as f:
        for year in data:
            f.write(f"\n========== {year} ==========\n")
            for link in data[year]:
                f.write(link + "\n")


def save_new(filepath, new_links):
    if not new_links:
        print("No new PDFs detected.")
        return

    with open(filepath, "w") as f:
        for link in new_links:
            f.write(link + "\n")

    print("New links saved:", filepath)


def read_links_grouped(file_path):
    grouped = {}
    current_year = None

    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()

            if line.startswith("=========="):
                current_year = line.replace("=", "").strip()
                grouped[current_year] = []

            elif line.startswith("http") and current_year:
                grouped[current_year].append(line)

    return grouped


def download_pdf(session, url, save_folder):
    filename = url.split("/")[-1]
    filepath = os.path.join(save_folder, filename)

    if os.path.exists(filepath):
        print("Already exists:", filename)
        return

    try:
        print("⬇️ Downloading:", filename)
        r = session.get(url, timeout=60)

        if r.status_code == 200 and r.content.startswith(b"%PDF"):
            with open(filepath, "wb") as f:
                f.write(r.content)
            print("✅ Saved:", filename)
        else:
            print("❌ Not valid PDF:", filename)

    except Exception as e:
        print("❌ Error:", filename, e)


# =====================================================
# SCRAPER ENGINE (Reusable)
# =====================================================


def scrape_section(section_name, url, year_selector, payload_builder):

    print(f"\n===== SCRAPING {section_name.upper()} =====\n")

    folder = os.path.join(BASE_FOLDER, section_name)
    os.makedirs(folder, exist_ok=True)

    json_file = os.path.join(folder, "all_links.json")

    # 🔹 Load existing JSON (if exists)
    if os.path.exists(json_file):
        with open(json_file, "r") as f:
            data = json.load(f)
    else:
        data = {}

    session = requests.Session()
    session.headers.update(HEADERS)

    response = safe_request(session, "GET", url)
    soup = BeautifulSoup(response.text, "html.parser")

    years = year_selector(soup)
    years = sorted(set(years), reverse=True)

    print("Years detected:", years)

    new_links_count = 0

    for year in years:
        if year == "2024":
            break   # ✅ stops older years

        if year not in data:
            data[year] = {}

        payload = payload_builder(soup, year)

        time.sleep(2)
        r = safe_request(session, "POST", url, data=payload)
        pdfs = extract_pdf_links(r.text)

        print(year, "→", len(pdfs))

        for link in pdfs:
            # 🔥 ONLY ADD NEW LINKS
            if link not in data[year]:
                data[year][link] = {
                    "downloaded": False,
                    "timestamp": datetime.datetime.now().isoformat()
                }
                new_links_count += 1

        time.sleep(3)

    print("New PDFs detected:", new_links_count)

    # 🔹 Save updated JSON
    with open(json_file, "w") as f:
        json.dump(data, f, indent=2)


def download_section(section_name):

    print(f"\n===== DOWNLOADING {section_name.upper()} =====")

    section_path = os.path.join(BASE_FOLDER, section_name)
    json_file = os.path.join(section_path, "all_links.json")

    if not os.path.exists(json_file):
        print("No all_links.json found.")
        return

    # 🔹 Load JSON data
    with open(json_file, "r") as f:
        data = json.load(f)

    session = requests.Session()
    session.headers.update(PDF_HEADERS)

    updated = False  # track changes

    for year in data:

        print(f"\nProcessing Year → {year}")
        year_folder = os.path.join(section_path, year)
        os.makedirs(year_folder, exist_ok=True)

        for link in data[year]:

            # 🔥 SKIP already downloaded
            if data[year][link]["downloaded"]:
                continue

            download_pdf(session, link, year_folder)

            # ✅ mark as downloaded
            data[year][link]["downloaded"] = True
            updated = True

            time.sleep(2)

        print("Cooling down...")
        time.sleep(5)

    # 🔹 Save updated JSON
    if updated:
        with open(json_file, "w") as f:
            json.dump(data, f, indent=2)

        print("✅ JSON updated with download status")

# =====================================================
# MAIN PIPELINE
# =====================================================

if __name__ == "__main__":

    # 1️⃣ Notifications
    scrape_section(
        "Notifications",
        BASE + "/Scripts/NotificationUser.aspx",
        lambda soup: [div.get("id") for div in soup.find_all("div", class_="accordionContent month") if div.get("id") and div.get("id").isdigit()],
        lambda soup, year: {
            "__VIEWSTATE": soup.find(id="__VIEWSTATE")["value"],
            "__VIEWSTATEGENERATOR": soup.find(id="__VIEWSTATEGENERATOR")["value"],
            "__EVENTVALIDATION": soup.find(id="__EVENTVALIDATION")["value"],
            "hdnYear": year,
            "hdnMonth": "0",
        }
    )

    download_section("Notifications")

    # 2️⃣ Master Directions
    scrape_section(
        "MasterDirections",
        BASE + "/Scripts/BS_ViewMasterDirections.aspx",
        lambda soup: [a.get("id") for a in soup.find_all("a", class_="year") if a.get("id") and a.get("id").isdigit()],
        lambda soup, year: {
            "__VIEWSTATE": soup.find(id="__VIEWSTATE")["value"],
            "__VIEWSTATEGENERATOR": soup.find(id="__VIEWSTATEGENERATOR")["value"],
            "__EVENTVALIDATION": soup.find(id="__EVENTVALIDATION")["value"],
            "hdnYear": year,
        }
    )

    download_section("MasterDirections")

    # 3️⃣ Master Circulars
    scrape_section(
        "MasterCirculars",
        BASE + "/Scripts/BS_ViewMasterCirculardetails.aspx",
        lambda soup: [a.get("id") for a in soup.find_all("a", class_="year_tree") if a.get("id") and a.get("id").isdigit()],
        lambda soup, year: {
            "__VIEWSTATE": soup.find(id="__VIEWSTATE")["value"],
            "__VIEWSTATEGENERATOR": soup.find(id="__VIEWSTATEGENERATOR")["value"],
            "__EVENTVALIDATION": soup.find(id="__EVENTVALIDATION")["value"],
            "hdnYear": year,
        }
    )

    download_section("MasterCirculars")

    print("\n🎉 FULL RBI PIPELINE COMPLETED\n")
