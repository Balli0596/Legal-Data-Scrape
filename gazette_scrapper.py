import asyncio
import nest_asyncio
from playwright.async_api import async_playwright
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
nest_asyncio.apply()

BASE_URL = "https://egazette.gov.in"
TOTAL_PAGES = 5

CATEGORIES = {
    1: "Bills_Acts",
    2: "Election_Bye_Election",
    3: "Land_Acquisition",
    4: "Delhi_Master_Plan",
    5: "Recruitment_Rules"
}

BASE_DIR = "D:\Data\gazettes"
TRACKER_DIR = "D:\Data\gazettes\gazette_trackers"

os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(TRACKER_DIR, exist_ok=True)


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")




def load_tracker(category_name):
    tracker_file = os.path.join(TRACKER_DIR, f"{category_name}.json")

    if os.path.exists(tracker_file):
        with open(tracker_file, "r") as f:
            data = json.load(f)

            # return set of URLs for fast lookup
            return data, set(item["pdf_url"] for item in data)

    return [], set()
def save_to_tracker(category_name, entry):
    tracker_file = os.path.join(TRACKER_DIR, f"{category_name}.json")

    if os.path.exists(tracker_file):
        with open(tracker_file, "r") as f:
            data = json.load(f)
    else:
        data = []

    data.append(entry)

    with open(tracker_file, "w") as f:
        json.dump(data, f, indent=4)

async def scrape():

    log("Starting multi-category scraping...")

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu"
            ]
        )

        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_timeout(120000)

        session = requests.Session()

        # ===============================
        # CATEGORY LOOP
        # ===============================
        for category_id, category_name in CATEGORIES.items():

            log(f"\nProcessing Category: {category_name}")

            tracker_data, downloaded_links = load_tracker(category_name)

            # 🔥 Reset to homepage
            await page.goto(BASE_URL + "/default.aspx",
                            wait_until="domcontentloaded")
            await page.wait_for_timeout(4000)

            await page.click(f"a[href*='Category={category_id}']")
            await page.wait_for_selector("#gvGazetteList")
            await page.wait_for_timeout(5000)

            category_folder = os.path.join(BASE_DIR, category_name)
            os.makedirs(category_folder, exist_ok=True)

            # Sync cookies
            cookies = await context.cookies()
            session.cookies.clear()
            for cookie in cookies:
                session.cookies.set(cookie["name"], cookie["value"])

            # ===============================
            # PAGINATION LOOP
            # ===============================
            for page_number in range(1, TOTAL_PAGES + 1):

                log(f"  Page {page_number}")

                await page.wait_for_function(
                    """() => {
                        return document.querySelectorAll(
                            "#gvGazetteList input[id*='imgbtndownload']"
                        ).length > 0;
                    }"""
                )

                buttons = page.locator(
                    "#gvGazetteList input[id*='imgbtndownload']"
                )

                count = await buttons.count()
                log(f"  Records found: {count}")
                stop_pagination = False
                for i in range(count):

                    try:
                        # 🔥 Extract Publish Date
                        publish_selector = f"#gvGazetteList_lbl_PublishDate_{i}"
                        publish_date = await page.locator(
                            publish_selector
                        ).inner_text()

                        import re

                        match = re.search(r"\d{4}", publish_date)
                        if not match:
                            continue

                        year = int(match.group())

                        # 🔥 ONLY allow 2026
                        if year < 2026:
                            log(f"    Older year {year} reached → stopping pagination")
                            stop_pagination = True
                            break
                        
                        year_folder = os.path.join(category_folder, str(year))
                        os.makedirs(year_folder, exist_ok=True)

                        btn = buttons.nth(i)

                        async with page.expect_popup() as popup_info:
                            await btn.click()

                        popup = await popup_info.value
                        await popup.wait_for_load_state("load")

                        html = await popup.content()
                        soup = BeautifulSoup(html, "html.parser")

                        iframe = soup.find("iframe",
                                           id="framePDFDisplay")

                        if not iframe:
                            await popup.close()
                            continue

                        pdf_path = iframe.get("src").replace("..", "")
                        pdf_url = BASE_URL + pdf_path

                        # 🔥 Skip if already downloaded in this category
                        if pdf_url in downloaded_links:
                            log("    Already downloaded. Skipping.")
                            await popup.close()
                            break

                        r = session.get(pdf_url)

                        if not r.content.startswith(b"%PDF"):
                            log("    Invalid PDF. Skipping.")
                            await popup.close()
                            continue

                        file_name = pdf_url.split("/")[-1]
                        file_path = os.path.join(year_folder, file_name)

                        with open(file_path, "wb") as f:
                            f.write(r.content)

                        # Save to tracker file
                        entry = {
                            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "category": category_name,
                            "section": year,
                            "pdf_name": file_name,
                            "pdf_url": pdf_url
                        }

                        save_to_tracker(category_name, entry)
                        downloaded_links.add(pdf_url)

                        log(f"    Saved: {category_name}/{year}/{file_name}")

                        await popup.close()
                        await page.wait_for_timeout(500)

                    except Exception as e:
                        log(f"    Error: {e}")
                        continue
                if stop_pagination:
                    break           
                # ===============================
                # ASP.NET PAGINATION
                # ===============================
                if page_number < TOTAL_PAGES:

                    next_page_number = page_number + 1
                    log(f"  Moving to Page {next_page_number}")

                    first_row_before = await page.locator(
                        "#gvGazetteList tr:nth-child(2)"
                    ).inner_text()

                    await page.evaluate(
                        f"__doPostBack('gvGazetteList','Page${next_page_number}')"
                    )

                    await page.wait_for_function(
                        """(prevText) => {
                            const row = document.querySelector('#gvGazetteList tr:nth-child(2)');
                            return row && row.innerText !== prevText;
                        }""",
                        arg=first_row_before
                    )

                    await page.wait_for_timeout(3000)

            log(f"Finished Category: {category_name}")

        await browser.close()

    log("All categories scraping completed successfully.")


# ===============================
# RUN
# ===============================
asyncio.run(scrape())
