#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


BASE_URL = "https://courts.mt.gov/external/orders/dailyorders"

COURT_NAME = "supreme_court"

ROOT = Path(__file__).resolve().parent
DOWNLOAD_ROOT = ROOT / "downloads" / COURT_NAME
CSV_DIR = ROOT / "downloads" / COURT_NAME / "CSV"
LOG_DIR = ROOT / "Log"

CSV_FILE = CSV_DIR / "supreme_court_daily_orders.csv"


CSV_COLUMNS = [
    "document_description",
    "file_date",
    "case_number",
    "title",
    "case_info_url",
    "inner_case_number",
    "original_court",
    "case_type",
    "short_title",
    "full_title",
    "case_filing_date",
    "origination_court_case_number",
    "pdf_url",
    "pdf_local_path",
]


def ensure_dirs():
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)


def setup_logger():
    ensure_dirs()

    log_file = LOG_DIR / f"montana_orders_{datetime.now().strftime('%Y-%m-%d')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )


def normalize(text):
    if text is None:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def sanitize_filename(text):
    text = normalize(text)
    text = re.sub(r'[<>:"/\\|?*]', "", text)
    return text.strip()


def ensure_csv():
    if not CSV_FILE.exists():
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()


def load_existing_keys():

    ensure_csv()

    keys = set()

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for r in reader:
            key = r["case_number"] + r["title"]
            keys.add(key)

    return keys


def append_row(row):

    ensure_csv()

    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow(row)


def make_driver(headless):

    options = Options()

    if headless == 1:
        options.add_argument("--headless=new")

    options.add_argument("--start-maximized")

    service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=options)

    return driver


def build_pdf_folder(row):

    date = normalize(row["file_date"])

    year = date.split("-")[0]

    title_folder = sanitize_filename(row["case_number"])

    folder = DOWNLOAD_ROOT / year / title_folder

    folder.mkdir(parents=True, exist_ok=True)

    return folder


def download_pdf(row):

    pdf_url = row["pdf_url"]

    if not pdf_url:
        return ""

    folder = build_pdf_folder(row)

    filename = sanitize_filename(row["case_number"]) + ".pdf"

    pdf_path = folder / filename

    if pdf_path.exists():
        return str(pdf_path)

    try:

        r = requests.get(pdf_url, timeout=60)

        with open(pdf_path, "wb") as f:
            f.write(r.content)

        logging.info("Downloaded PDF %s", pdf_path)

        return str(pdf_path)

    except Exception as e:

        logging.error("PDF download failed %s", e)

        return ""


def parse_case_inner(driver, url):

    data = {
        "inner_case_number": "",
        "original_court": "",
        "case_type": "",
        "short_title": "",
        "full_title": "",
        "case_filing_date": "",
        "origination_court_case_number": "",
    }

    if not url:
        return data

    main = driver.current_window_handle

    driver.execute_script("window.open(arguments[0]);", url)

    driver.switch_to.window(driver.window_handles[-1])

    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    table = soup.find("table")

    if table:

        rows = table.find_all("tr")

        if len(rows) >= 2:

            headers = [normalize(x.text).lower() for x in rows[0].find_all(["td", "th"])]
            values = [normalize(x.text) for x in rows[1].find_all(["td", "th"])]

            mapping = dict(zip(headers, values))

            data["inner_case_number"] = mapping.get("case number", "")
            data["original_court"] = mapping.get("original court", "")
            data["case_type"] = mapping.get("case type", "")
            data["short_title"] = mapping.get("short title", "")
            data["full_title"] = mapping.get("full title", "")
            data["case_filing_date"] = mapping.get("case filling date", "")
            data["origination_court_case_number"] = mapping.get(
                "origination court case number", ""
            )

    driver.close()

    driver.switch_to.window(main)

    return data


def scrape(driver, existing_keys):

    driver.get(BASE_URL)

    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.TAG_NAME, "table"))
    )

    soup = BeautifulSoup(driver.page_source, "html.parser")

    table = soup.find("table")

    rows = table.find_all("tr")[1:]

    for r in rows:

        tds = r.find_all("td")

        if len(tds) < 4:
            continue

        desc = normalize(tds[0].text)
        file_date = normalize(tds[1].text)
        case_number = normalize(tds[2].text)
        title = normalize(tds[3].text)

        desc_link = tds[0].find("a")
        case_link = tds[2].find("a")

        pdf_url = urljoin(BASE_URL, desc_link["href"]) if desc_link else ""
        case_info_url = urljoin(BASE_URL, case_link["href"]) if case_link else ""

        key = case_number + title

        if key in existing_keys:
            logging.info("Skipping duplicate %s", case_number)
            continue

        row = {
            "document_description": desc,
            "file_date": file_date,
            "case_number": case_number,
            "title": title,
            "case_info_url": case_info_url,
            "pdf_url": pdf_url,
        }

        inner = parse_case_inner(driver, case_info_url)

        row.update(inner)

        row["pdf_local_path"] = download_pdf(row)

        append_row(row)

        existing_keys.add(key)

        logging.info("Saved %s", case_number)


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--headless", type=int, default=1)

    args = parser.parse_args()

    setup_logger()

    existing = load_existing_keys()

    driver = make_driver(args.headless)

    try:

        scrape(driver, existing)

    finally:

        driver.quit()


if __name__ == "__main__":
    main()