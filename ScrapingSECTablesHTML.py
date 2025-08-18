#!/usr/bin/env python
# coding: utf-8

# In[2]:


import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import pandas as pd
import unicodedata
import os
import warnings

import logging

import argparse

# silence warnings when html parsed as xml
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

#ignore junk tables i.e table of contents
# usually if row is less than 2 cells in length then there is no data
MIN_ROWS = 2
# usually if column has less than 3 cells of length there is no data
MIN_COLS = 3

# build SEC url
BASE = "https://data.sec.gov/submissions/"

#reusable header for everywhere so website allows you to pass without seeming a bot
HEADERS_URL = {
    "User-Agent": "MyResearchBot/1.0 (contact: myemail@example.com)",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive"}

# map ticker to CIK
TICKER_JSON = "https://www.sec.gov/files/company_tickers.json"

# Configure logging
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

#run script as ScrapingSECTablesHTMLImproved.py --ticker AAPL --year 2023 --keyword revenue
def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch and extract SEC 10-K tables into Excel"
    )
    parser.add_argument("--ticker", help="Company ticker (e.g. AAPL)", default=None)
    parser.add_argument("--year", help="Filing year (e.g. 2024)", default=None)
    parser.add_argument("--keyword", help="Keyword to match table text", default=None)
    parser.add_argument("--out-dir", default="tables_output", help="Output folder")

    # Accept unknown args
    args, _ = parser.parse_known_args()

    # Prompt user interactvely if missing arguments
    if not args.ticker:
        args.ticker = input("Enter company ticker (e.g. AAPL): ").strip()
    if not args.year:
        args.year = input("Enter filing year (e.g. 2024): ").strip()
    if not args.keyword:
        args.keyword = input("Enter keyword to match table text: ").strip()

    return args

# go from company ticker to CIK
def get_cik_from_ticker(ticker: str) -> str:
    url = TICKER_JSON
    data = requests.get(url, headers=HEADERS_URL).json()
    for entry in data.values():
        # get json mapping from SEC and search of company ticker
        if entry['ticker'].lower() == ticker.lower():
            # fetch CIK
            cik = str(entry['cik_str']).zfill(10)
            print(f"Found CIK {cik} for ticker {ticker}")
            return cik
    raise ValueError(f"Ticker {ticker} not found in SEC database")

# get SEC submisson JSON using CIK
def get_10k_url(ticker: str, year: str) -> str:
    cik = get_cik_from_ticker(ticker)
    json_link = f"{BASE}CIK{cik}.json"
    print(json_link)
    headers = HEADERS_URL
    resp = requests.get(json_link, headers=headers)
    resp.raise_for_status()
    if "application/json" not in resp.headers.get("Content-Type", ""):
        print("Unexpected content:", resp.text[:200])
        raise RuntimeError(f"Did not receive JSON from SEC for {url}")
    data = resp.json()
    forms = data['filings']['recent']

    filings = data.get("filings", {}).get("recent", {})
    forms = filings.get("form", [])
    dates = filings.get("filingDate", [])
    accessions = filings.get("accessionNumber", [])
    documents = filings.get("primaryDocument", [])

    filings_data = list(zip(forms, dates, accessions, documents))
    # Sort by date descending
    filings_data.sort(key=lambda x: x[1], reverse=True)

    # loop through filings and find first 10-K for year
    for form, date, acc, doc in zip(forms, dates, accessions, documents):
        #Check form type and date
        if form.startswith("10-K") and date.startswith(year): #and date == year:
            # construct URL
            acc_no_dashes = acc.replace("-", "")
            filing_url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{int(cik)}/{acc_no_dashes}/{doc}"
            )
            print(f"Found 10-K filing for {ticker} {year}: {filing_url}")
            return filing_url  # return immediately

    # if no 10-K found at all
    raise ValueError(f"No 10-K filing found for {ticker} in {year}")

# extract table and parse with Beautiful soup
def extract_table(table) -> pd.DataFrame | None:
    # parse table extracting row and cells
    rows = []
    for row in table.find_all("tr"):
        cells = []
        for cell in row.find_all(["td", "th"]):
            # normalize unicode
            txt = unicodedata.normalize("NFKC", cell.get_text(" ", strip=True))
            # trim trailing commas
            if txt.endswith(","): txt = txt.rstrip(",")
            # fix non-breaking spaces
            txt = txt.replace(u'\xa0', ' ')
            #skip lone % and $
            if txt not in ["$", "%"]:
                # convert negtaive values in paranthesis into numbers with "-" sign prefix
                if txt.startswith("(") and txt.endswith(")"):
                    txt = "-" + txt[1:-1]
                cells.append(txt)
        if cells:
            cleaned = []
            for c in cells:
                if not cleaned or c != cleaned[-1]:
                    cleaned.append(c)
            rows.append(cleaned)
    # rows should be greater than 3 cells in length
    if not rows or len(rows) < MIN_ROWS:
        return None

    header = [c.strip().lower() for c in rows[0]]
    first_col = [r[0].strip().lower() for r in rows]
    # filtering tables with headers/first column with "page" or "index"
    # these would be table of content data
    if any(k in header + first_col for k in ("page", "index")):
        return None
    # padding row ton same length
    max_cols = max(len(r) for r in rows)
    rows = [r + ['']*(max_cols-len(r)) for r in rows]
    df = pd.DataFrame(rows)
    # reject tables with less than 3 columns after padding
    if df.shape[1] < MIN_COLS:
        return None
    return df

# main workflow
def main():
    # call parser
    args = parse_args()
    try:
        url = get_10k_url(args.ticker, args.year)
    except Exception as e:
        log.error(f"Failed to retrieve filing: {e}")
        return

    # get HTML doc
    response = requests.get(url, headers=HEADERS_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")

    # search table for key word given by user
    all_tables = soup.find_all("table")
    target_tables = [
        t for t in all_tables
        if args.keyword.lower() in t.get_text(" ", strip=True).lower()
    ]

    log.info(f"Found {len(target_tables)} matching tables for keyword '{args.keyword}'")
    if not target_tables:
        log.warning("No matching tables found.")
        return

    output_dir = "tables_output"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{args.ticker}_{args.year}_tables.xlsx")

    # save valid tables into excel file (1 per sheet)
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for i, table_tag in enumerate(target_tables):
            df = extract_table(table_tag)
            if df is None:
                continue
            df = df.drop_duplicates().fillna("")
            df = df.loc[:, ~df.T.duplicated()]
            sheet_name = f"table_{i}"[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            log.info(f"Saved table {i} to sheet '{sheet_name}'")

    log.info(f"All valid tables saved to {output_file}")
if __name__ == "__main__":
    main()


# In[ ]:




