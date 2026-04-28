# 📄 Legal Data Scraper

A Python-based web scraping project designed to extract legal documents from multiple sources such as Gazette and India Code. This project automates the process of collecting, organizing, and storing legal data efficiently.

---

## 🚀 Features

* 🔍 Scrapes legal data from:

  * Gazette websites
  * India Code portals
* 📂 Structured data extraction (PDFs, metadata, links)
* ⚡ Automated workflow for continuous scraping
* 🧠 Handles duplicate detection and avoids re-downloading
* 📦 Scalable design for adding new sources

---

## 🛠️ Tech Stack

* **Language:** Python 3.x
* **Libraries:**

  * requests
  * BeautifulSoup
  * asyncio (if used)
  * other scraping utilities

---

## 📁 Project Structure

```
Legal-Data-Scrape/
│
├── gazette_scrapper.py       # Scrapes Gazette data
├── indiacode_scraper.py      # Scrapes India Code data
├── requirements.txt          # Dependencies
├── README.md                 # Project documentation
└── data/                     # (Optional) Stored output files
```

---

## ⚙️ Installation

1. Clone the repository:

```bash
git clone https://github.com/Balli0596/Legal-Data-Scrape.git
cd Legal-Data-Scrape
```

2. Create virtual environment:

```bash
python -m venv myenv
```

3. Activate environment:

* Windows:

```bash
myenv\Scripts\activate
```

* Mac/Linux:

```bash
source myenv/bin/activate
```

4. Install dependencies:

```bash
pip install -r requirements.txt
```

---

## ▶️ Usage

Run individual scrapers:

```bash
python gazette_scrapper.py
```

```bash
python indiacode_scraper.py
```

---

## 📊 Output

* Extracted PDFs
* Metadata (title, date, links)
* JSON or structured storage (based on implementation)

---

## 🔄 Workflow

1. Fetch source URLs
2. Parse HTML content
3. Extract required data
4. Download/store documents
5. Avoid duplicates

---

## ⚠️ Notes

* Ensure stable internet connection while scraping
* Some websites may block frequent requests → consider adding delays
* Follow website terms of use before scraping

---

## 📌 Future Improvements

* Add scheduling (cron/automation)
* Database integration (MongoDB/PostgreSQL)
* API layer for serving data
* Logging and monitoring system

---

## 👤 Author

**Bahnishankar Maharana**

* GitHub: https://github.com/Balli0596

---

## 📜 License

This project is for educational and research purposes.

---
