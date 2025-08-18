#!/usr/bin/env python
# coding: utf-8

# In[46]:


import requests
from bs4 import BeautifulSoup
import pandas as pd

import unicodedata

# URL of Google's 10-K
url = "https://www.sec.gov/Archives/edgar/data/1652044/000165204421000010/goog-20201231.htm"

# SEC requires a descriptive User-Agent
headers = {
    "User-Agent": "DataResearchBot/1.0 (contact: daria@example.com)",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov"
}

# 1: Load & parse HTML
resp = requests.get(url, headers=headers)
resp.raise_for_status()
soup = BeautifulSoup(resp.text, "lxml")

# 2: Find income statement tables
all_tables = soup.find_all("table")
target_tables = []
for table in all_tables:
    text = table.get_text(" ", strip=True).lower()
    if "consolidated statements" in text:
        target_tables.append(table)

print(f"Found {len(target_tables)} matching tables.")

# Helper to clean rows
def extract_table(table):
    rows = []
    for row in table.find_all("tr"):
        cells = []
        for cell in row.find_all(["td", "th"]):
            txt = cell.get_text(" ", strip=True)
            if txt.endswith(","): txt=txt.rstrip(",")
            txt = txt.replace(u'\xa0', ' ')       # fix &nbsp;
            txt = unicodedata.normalize("NFKC", txt)
            if txt != "$":  # skip lone '$'
                cells.append(txt)
        if cells:
            # collapse duplicates: ["Revenue","Revenue","90,272"] -> ["Revenue","90,272"]
            cleaned = []
            for c in cells:
                if not cleaned or c != cleaned[-1]:
                    cleaned.append(c)
            rows.append(cleaned)
    return pd.DataFrame(rows) if rows else None

# Step 3: Take first target table and extract
if not target_tables:
    print("No matching tables found.")
# now, need to recursively extract and place each table found on a new csv
# should first count how many tables and make new worksheets in existing workbook (made for first table) per extracted table 

else:

    raw_df = extract_table(target_tables[1])

    # 4: Save raw table to CSV
    csv_file = "google_income_statement.csv"
    raw_df.to_csv(csv_file, index=False)
    print(f"Raw table saved to {csv_file}")

    # 5: Reload table from CSV
    df = pd.read_csv(csv_file, dtype=str).fillna("")

    # 6: Remove duplicate rows (exact match)
    df = df.drop_duplicates()

    # 7: Remove duplicate columns (exact match)
    df = df.loc[:, ~df.T.duplicated()]

    # 8: Save cleaned table back to CSV
    df.to_csv(csv_file, index=False)
    print(f"Cleaned table saved to {csv_file}")

