#!/usr/bin/env python
# coding: utf-8

# In[1]:


import csv
import pprint
import pathlib
import collections
# API for parsing and creating XML data
import xml.etree.ElementTree as ET
import lxml.etree as ETL
import requests
import argparse
import time
from bs4 import BeautifulSoup

import math
import decimal as Decimal
import re

# map ticker to CIK
TICKER_JSON = "https://www.sec.gov/files/company_tickers.json"

# get accession number from cik
SUB_URL = "https://data.sec.gov/submissions/"

BASE = "https://www.sec.gov/Archives/edgar/data/"

#reusable header for sec.gov, complying with standards so website allows you to pass without seeming a bot
HEADERS_URL = {
    "User-Agent": "MyResearchBot/1.0 (contact: myemail@example.com)",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive"}

# keys used to parse the document for desired data
parse = ['label', 'labelLink', 'labelArc', 'loc', 'definitionLink', 'definitionArc', 'calculationArc', 'presentationLink', 'presentationArc', 'presentation']

# Max CIK matches count
MAX_COUNT = 100

# parser
def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch SEC XMLs"
    )
    parser.add_argument("--ticker", help="Company ticker", default=None)
    parser.add_argument("--date", help="Date", default=None)
    # Accept unknown args
    args, _ = parser.parse_known_args()

    # user interactvely if missing arguments
    if not args.ticker:
        args.ticker = input("Company ticker")
        args.date = input("Date")
    return args

def parse_numeric_text_to_float(text, decimals=None):

    if text is None:
        return None

    s = str(text).strip()
    if s == "" or s.lower() in ("null", "n/a"):
        return None

    # Remove thousands separators
    s = s.replace(",", "").strip()

    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1].strip()

    # Extract a numeric substring (handles e/E scientific notation)
    m = re.search(r"[-+]?\d+(\.\d+)?([eE][-+]?\d+)?", s)
    if not m:
        return None

    try:
        val = Decimal(m.group(0))
    except Exception:
        return None

    # XBRL uses decimals=-3 indicates thousands
    if decimals is not None:
        try:
            d = int(decimals)
            # scaled = val * (10 ** (-d))
            val = val * (Decimal(10) ** (-d))
        except Exception:
            # ignore malformed
            pass

    if negative:
        val = -val

    try:
        f = float(val)
        if math.isfinite(f):
            return f
        return None
    except Exception:
        return None


def extract_root_nsmap_and_prefixes(file_path):
    try:
        root = ETL.parse(str(file_path)).getroot()
        nsmap = root.nsmap or {}
        uri_to_prefix = {}
        for prefix, uri in nsmap.items():
            if uri is None:
                continue
            if prefix is None:
                uri_to_prefix[uri] = ""  # default namespace => empty prefix
            else:
                uri_to_prefix[uri] = prefix
        return uri_to_prefix
    except Exception:
        return {}


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

# creates a mapping for xml documents and their urls
def get_url(cik, ticker, year: str) -> str:
    # cik = get_cik_from_ticker(ticker)
    json_link = f"{SUB_URL}CIK{cik}.json"
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
    # by date descending
    filings_data.sort(key=lambda x: x[1], reverse=True)

    for form, date, acc, doc in filings_data:
        acc_no_dashes = acc.replace("-", "")
        # base URL
        base_url = f"{BASE}{int(cik)}/{acc_no_dashes}/"

        # filing's index.json (listing all files)
        index_url = f"{base_url}index.json"
        print(index_url)
        try:
            r = requests.get(index_url, headers=headers)
            r.raise_for_status()
            idx_data = r.json()

            # relevant XML files found here
            found_files = {}
            for file_entry in idx_data.get("directory", {}).get("item", []):
                name = file_entry.get("name", "")
                # check if file is one of the XML types you want
                if any(suffix in name for suffix in ["_def.xml", "_htm.xml", "_lab.xml", "_cal.xml", "_pre.xml"]):
                    # keys without extensions for clarity or keep names as is
                    found_files[name] = base_url + name

            # found at least the instance document (_htm.xml), consider success
            if any("_htm.xml" in fname for fname in found_files):
                print(f"Found filing with required XMLs at: {index_url}")
                print(found_files)
                # scans directory items for the XML files you want (_def.xml, _htm.xml, _lab.xml, _cal.xml
                # returns dict with found required elements of fmap (some may be missing)
                return found_files  # return dictionary of files with full URLs

        except requests.RequestException as e:
            print(f"Error fetching {index_url}: {e}")
            continue  # Try next filing

    # If none found
    raise ValueError(f"No filing found with required XML files for CIK {cik} and year {year}")

#can loop through each file
# P3
# del if breaks
def parse_linkbases(files_list, parse_labels):
    storage_list = []
    storage_values = {}
    storage_gaap = {}

    for file in files_list:
        # expect FilingTuple(file_path, namespace_element, namespace_label)
        if not getattr(file, "file_path", None):
            print(f"Warning: File for {getattr(file, 'namespace_label', '?')} not found, skipping.")
            continue

        try:
            tree = ET.parse(str(file.file_path))
        except Exception as e:
            print(f"Error parsing {file.file_path}: {e}")
            continue

        root = tree.getroot()
        elements = root.findall(file.namespace_element)
        for element in elements:
            for child_element in element.iter():
                tag = child_element.tag if isinstance(child_element.tag, str) else ""
                # safe localname
                local = tag.split("}")[-1] if "}" in tag else tag
                if local in parse_labels:
                    element_type_label = f"{file.namespace_label}_{local}"

                    dict_storage = {"item_type": element_type_label}
                    # transfer attributes; strip namespace from attrib keys that contain '}'
                    for key, val in child_element.attrib.items():
                        if "}" in key:
                            new_key = key.split("}", 1)[1]
                        else:
                            new_key = key
                        dict_storage[new_key] = val

                    # handle label-specific mapping (your original logic)
                    if element_type_label == "label_label" and "label" in dict_storage:
                        key_store = dict_storage["label"]
                        master_key = key_store.replace("lab_", "")
                        label_split = master_key.split("_")
                        if len(label_split) >= 2:
                            gaap_id = f"{label_split[0]};{label_split[1]}"
                        else:
                            gaap_id = master_key

                        storage_values.setdefault(master_key, {})
                        storage_values[master_key].update({
                            "label_id": key_store,
                            "location_id": key_store.replace("lab_", "loc_"),
                            "us_gaap_id": gaap_id,
                            "us_gaap_values": None,
                        })
                        storage_values[master_key][element_type_label] = dict_storage

                        storage_gaap.setdefault(gaap_id, {})
                        storage_gaap[gaap_id].update({
                            "id": gaap_id,
                            "master_id": master_key
                        })
                    else:
                        # store other linkbase entries keyed by their item label
                        # make unique key
                        storage_list.append([file.namespace_label, dict_storage])

    return storage_list, storage_values, storage_gaap


def parse_instance_doc(file_htm, storage_values, storage_list, storage_gaap):
    storage_facts = []
    uri_to_prefix = extract_root_nsmap_and_prefixes(file_htm)

    try:
        tree = ET.parse(str(file_htm))
    except Exception as e:
        print(f"Error parsing instance document {file_htm}: {e}")
        return

    root = tree.getroot()

    #context and unit maps
    contexts = {}
    units = {}
    for el in root.iter():
        tag = el.tag if isinstance(el.tag, str) else ""
        local = tag.split("}")[-1] if "}" in tag else tag
        if local == "context":
            cid = el.attrib.get("id")
            if cid:
                # entity.identifier and period
                entity_id = None
                period_start = None
                period_end = None
                instant = None
                for sub in el:
                    sub_local = sub.tag.split("}")[-1] if "}" in sub.tag else sub.tag
                    if sub_local == "entity":
                        id_el = sub.find(".//{*}identifier")
                        if id_el is not None:
                            entity_id = id_el.text
                    elif sub_local == "period":
                        start_el = sub.find(".//{*}startDate")
                        end_el = sub.find(".//{*}endDate")
                        instant_el = sub.find(".//{*}instant")
                        if start_el is not None:
                            period_start = start_el.text
                        if end_el is not None:
                            period_end = end_el.text
                        if instant_el is not None:
                            instant = instant_el.text
                contexts[cid] = {
                    "entity_identifier": entity_id,
                    "period_start": period_start,
                    "period_end": period_end,
                    "instant": instant
                }
        elif local == "unit":
            uid = el.attrib.get("id")
            if uid:
                # store unit element text or measures found
                measures = [m.text for m in el.findall(".//{*}measure")]
                units[uid] = {"measures": measures}

    # attach contexts/units to storage_values for CSV output
    storage_values["_contexts"] = contexts
    storage_values["_units"] = units

    # iterate facts
    for element in root.iter():
        tag = element.tag if isinstance(element.tag, str) else ""
        local = tag.split("}")[-1] if "}" in tag else tag

        #nonNumeric/nonFractional specially
        if "nonNumeric" in tag or "nonFractional" in tag:
            attr_name = element.attrib.get("name")
            if not attr_name:
                continue
            if attr_name in storage_gaap:
                g = storage_gaap[attr_name]
                g["context_ref"] = element.attrib.get("contextRef")
                g["context_id"] = element.attrib.get("id")
                g["continued_at"] = element.attrib.get("continuedAt", "null")
                g["escape"] = element.attrib.get("escape", "null")
                g["format"] = element.attrib.get("format", "null")
                g["unit_ref"] = element.attrib.get("unitRef", "null")
                g["decimals"] = element.attrib.get("decimals", "null")
                g["scale"] = element.attrib.get("scale", "null")
                g["value"] = element.text.strip() if element.text else "null"

                gaap_key = attr_name
                if gaap_key in storage_gaap:
                    master = storage_gaap[gaap_key].get("master_id")
                    if master:
                        storage_values.setdefault(master, {})["us_gaap_value"] = storage_gaap[attr_name]
                else:
                    storage_values.setdefault("_unmapped_nonNumeric", {})[attr_name] = storage_gaap[attr_name]
            else:
                # store unmapped nonNumeric if desired
                storage_values.setdefault("_unmapped_nonNumeric", {})[attr_name] = {
                    "context_ref": element.attrib.get("contextRef"),
                    "value": element.text.strip() if element.text else None
                }
            # continue to next element (nonNumeric handled)
            continue

        # skip things that are not facts (no contextRef)
        ctx = element.attrib.get("contextRef")
        if ctx is None:
            continue

        # build gaap_candidate using namespace mapping
        if "}" in tag:
            ns_uri = tag.split("}")[0].lstrip("{")
            localname = tag.split("}")[1]
        else:
            ns_uri = None
            localname = tag

        prefix = uri_to_prefix.get(ns_uri) if ns_uri else None
        if prefix in (None, ""):
            gaap_candidate = localname
        else:
            gaap_candidate = f"{prefix};{localname}"

        unit_ref = element.attrib.get("unitRef")
        decimals = element.attrib.get("decimals")


        value_raw = element.text.strip() if element.text else None
        value_numeric = parse_numeric_text_to_float(value_raw, decimals=decimals)


        fact = {
            "tag_local": localname,
            "tag_prefix": prefix,
            "gaap_id_candidate": gaap_candidate,
            "contextRef": ctx,
            "unitRef": unit_ref,
            "decimals": decimals,
            "value_raw": value_raw,
            "value_numeric": value_numeric
        }
        storage_facts.append(fact)

        if gaap_candidate in storage_gaap:
            master_key = storage_gaap[gaap_candidate]["master_id"]
            storage_values.setdefault(master_key, {}).setdefault("facts", []).append(fact)
        else:
            storage_values.setdefault("_unmapped_facts", []).append(fact)

    storage_values["_facts_list"] = storage_facts


def write_csv(storage_list, storage_values):
    file_name = "sec_xbrl_scrape_content.csv"
    with open(file_name, mode="w", newline="", encoding="utf-8") as sec_file:
        writer = csv.writer(sec_file)
        writer.writerow(["FILE", "LABEL", "VALUE"])
        for entry in storage_list:
            ns_label = entry[0]
            data = entry[1]
            if isinstance(data, dict):
                for k, v in data.items():
                    writer.writerow([ns_label, k, v])
            else:
                writer.writerow([ns_label, str(data)])

    file_name = "sec_xbrl_scrape_values.csv"
    with open(file_name, mode="w", newline="", encoding="utf-8") as sec_file:
        writer = csv.writer(sec_file)
        writer.writerow(["ID", "CATEGORY", "LABEL", "VALUE"])
        for id_key, id_val in storage_values.items():
            if isinstance(id_val, dict):
                for cat_key, cat_val in id_val.items():
                    if isinstance(cat_val, dict):
                        for label, value in cat_val.items():
                            writer.writerow([id_key, cat_key, label, value])
                    else:
                        # cat_val is a scalar or list
                        writer.writerow([id_key, cat_key, "", cat_val])
            else:
                writer.writerow([id_key, "", "", id_val])

    # Facts CSV
    facts = storage_values.get("_facts_list", [])
    if facts:
        with open("sec_xbrl_facts.csv", "w", newline="", encoding="utf-8") as ff:
            w = csv.writer(ff)
            w.writerow(["tag_prefix", "tag_local", "gaap_candidate", "contextRef", "unitRef", "decimals", "value_raw"])
            for f in facts:
                w.writerow([
                    f.get("tag_prefix"),
                    f.get("tag_local"),
                    f.get("gaap_id_candidate"),
                    f.get("contextRef"),
                    f.get("unitRef"),
                    f.get("decimals"),
                    f.get("value_raw"),

                ])

    contexts = storage_values.get("_contexts", {})
    if contexts:
        with open("sec_xbrl_contexts.csv", "w", newline="", encoding="utf-8") as cf:
            w = csv.writer(cf)
            w.writerow(["contextRef", "entity_identifier", "period_start", "period_end", "instant"])
            for k, v in contexts.items():
                w.writerow([k, v.get("entity_identifier"), v.get("period_start"), v.get("period_end"), v.get("instant")])



# main workflow
def main():

    # call parser
    args = parse_args()

    cik = get_cik_from_ticker(args.ticker)

    # populate fmap
    fmap = {}

    fmap = get_url(cik, args.ticker, args.date)

    sec_directory = pathlib.Path.cwd().joinpath("folder_to_store_xml_docs")
    sec_directory.mkdir(parents=True, exist_ok=True)

    file_htm = None
    file_cal = None
    file_def = None
    file_lab = None
    file_pre = None

    for fname, url in fmap.items():
        fpath = sec_directory / fname
        if fname.endswith("_htm.xml"):
            file_htm = sec_directory / fname
        if fname.endswith("_cal.xml"):
            file_cal = sec_directory / fname
        if fname.endswith("_def.xml"):
            file_def = sec_directory / fname
        if fname.endswith("_lab.xml"):
            file_lab = sec_directory / fname
        if fname.endswith("_pre.xml"):
            file_pre = sec_directory / fname
        if not fpath.exists():
            try:
                response = requests.get(url, headers=HEADERS_URL)
                time.sleep(0.2) 
                #response = requests.get(f"{BASE}/{tail}", headers = HEADERS_URL)
                response.raise_for_status()
                fpath.write_bytes(response.content)
                print(f"Downloaded: {fname}")
            except requests.exceptions.RequestException as e:
                print(f"Error downloading {fname}: {e}")
        else:
            print(f"Already exists: {fname}")


    # create constructor for named tuple object type
    FilingTuple = collections.namedtuple("FilingTuple", ["file_path", "namespace_element", "namespace_label"])

    files_list = [
        FilingTuple(file_cal, '{http://www.xbrl.org/2003/linkbase}calculationLink', 'calculation'),
        FilingTuple(file_def, '{http://www.xbrl.org/2003/linkbase}definitionLink', 'definition'),
        FilingTuple(file_lab, '{http://www.xbrl.org/2003/linkbase}labelLink', 'label'),
        FilingTuple(file_pre, '{http://www.xbrl.org/2003/linkbase}presentationLink', 'presentation')
    ]

    # label categories
    # labelArc points to next element you want

    storage_list, storage_values, storage_gaap = parse_linkbases(files_list, parse)
    parse_instance_doc(file_htm, storage_values, storage_list, storage_gaap)
    write_csv(storage_list, storage_values)

    unmapped = storage_values.get("_unmapped_facts", [])
    if unmapped:
        print(f"NOTE: {len(unmapped)} unmapped facts found. Inspect 'sec_xbrl_facts.csv' and storage_values['_unmapped_facts']")
    
    print("Created: sec_xbrl_scrape_content.csv, sec_xbrl_scrape_values.csv, sec_xbrl_contexts.csv, sec_xbrl_facts.csv")
    print("Written to csv successfully")

    csv_list = ["sec_xbrl_scrape_content.csv", "sec_xbrl_scrape_values.csv", "sec_xbrl_contexts.csv", "sec_xbrl_facts.csv"]
    # print(csv_list)
    return csv_list



if __name__ == "__main__":
    main()         

