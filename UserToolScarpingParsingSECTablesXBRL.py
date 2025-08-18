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
parse = ['label', 'labelLink', 'labelArc', 'loc', 'definitionLink', 'definitionArc', 'calculationArc']

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

    # Prompt user interactvely if missing arguments
    if not args.ticker:
        args.ticker = input("Company ticker")
        args.date = input("Date")
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
    # Sort by date descending
    filings_data.sort(key=lambda x: x[1], reverse=True)

    for form, date, acc, doc in filings_data:
        acc_no_dashes = acc.replace("-", "")
        # Build base URL
        base_url = f"{BASE}{int(cik)}/{acc_no_dashes}/"

        # Fetch filing's index.json (listing all files)
        index_url = f"{base_url}index.json"
        print(index_url)
        try:
            r = requests.get(index_url, headers=headers)
            r.raise_for_status()
            idx_data = r.json()

            # Collect the relevant XML files found here
            found_files = {}
            for file_entry in idx_data.get("directory", {}).get("item", []):
                name = file_entry.get("name", "")
                # Check if file is one of the XML types you want
                if any(suffix in name for suffix in ["_def.xml", "_htm.xml", "_lab.xml", "_cal.xml"]):
                    # Use keys without extensions for clarity or keep names as is
                    found_files[name] = base_url + name

            # If found at least the instance document (_htm.xml), consider success
            if any("_htm.xml" in fname for fname in found_files):
                print(f"Found filing with required XMLs at: {index_url}")
                print(found_files)
                # Scans the directory items for the XML files you want (_def.xml, _htm.xml, _lab.xml, _cal.xml
                # returns dict with found required elements of fmap (some may be missing)
                return found_files  # Return dictionary of files with full URLs

        except requests.RequestException as e:
            print(f"Error fetching {index_url}: {e}")
            continue  # Try next filing

    # If none found
    raise ValueError(f"No filing found with required XML files for CIK {cik} and year {year}")

#can loop through each file
# P3
# del if breaks
def parse_linkbases(files_list, parse):

    # list
    storage_list = []

    # dictionary 
    storage_values= {}

    # another dictionary
    storage_gaap = {}

    dict_storage = {}


    for file in files_list:

        #print(file)
        # that returns first item in file tuple, which is a list
        # we want to access file so we do files_list[0]
        # i.e file_cal for instance
        if file[0] is None:
            print(f"Warning: File for {file.namespace_label} not found or missing, skipping.")
            continue

        # parse file
        tree = ET.parse(file.file_path)
        # create element tree
        # print(tree)

        # grab all namespace_elements in tree
        elements = tree.findall(file.namespace_element)
        # will return all elements that match this: 
        # http://www.xbrl.org/2003/linkbase)calculationLink namespace
        #print(elements)

        # loop through each element
        # loop through child elements
        # P4
        for element in elements:
            # create iterator
            # loop through child element of each element
            for child_element in element.iter():

                #print(child_element)
                # get elements and their children from document
                # next is getting attributes of elements
                element_split_label = child_element.tag.split("}")
                # print(element_split_label)
                # want to remove the redundant prefix on label:
                # {http://www.xbrl.org/2003/linkbase}
                # get parts of label
                namespace = element_split_label[0]
                label = element_split_label[1]
                # is this label we want?
                # wanted labels in parse
                if label in parse:
                    element_type_label = file.namespace_label + "_" + label
                    #print(element_type_label)

                    # define dictionary
                    dict_storage = {}
                    dict_storage["item_type"] = element_type_label

                    # get attribute keys
                    cal_keys = child_element.keys()
                    # print(cal_keys)

                    for key in cal_keys:
                        if "}" in key:
                            new_key = key.split("}")[1]
                            dict_storage[new_key] = child_element.attrib[key]
                        else:
                            dict_storage[key] = child_element.attrib[key]
                    #print(dict_storage)

                    # choosing master key to be the label document
                    # could choose anything else - experimental
                    if element_type_label == "label_label":
                        key_store = dict_storage["label"]

                        # create master key
                        master_key = key_store.replace("lab_", "")

                        # split master key
                        label_split = master_key.split("_")

                        #a
                        # create gaap id
                        gaap_id =  label_split[0] + ";" + label_split[1]
                        #print(label_split)
                        # there are duplicates
                        # thats why we put it in a dicionary - unique key to value
                        # dict for xml files
                        storage_values[master_key] = {}
                        # dictionary storage values is created with the master key

                        storage_values[master_key]["label_id"] = key_store
                        storage_values[master_key]["location_id"] = key_store.replace("lab_", "loc_")
                        storage_values[master_key]["us_gaap_id"] = gaap_id
                        storage_values[master_key]["us_gaap_values"] = None
                        storage_values[master_key][element_type_label] = dict_storage
                        #b is a subdictiory of a
                        # dict for only values related to GAAP
                        storage_gaap[gaap_id] = {}
                        storage_gaap[gaap_id]["id"] = gaap_id
                        storage_gaap[gaap_id]["master_id"] = master_key
                        # a and b should be merged
                        # master keys created in big dictiory
                        # master key associated with smaller dictiory for GAAP stuff exclusively, organized as in b
            # add to dict
            storage_list.append([file.namespace_label, dict_storage])
            # parsing the html file with nonNumeric and nonFractional stuff
            # parse 10Q file
            # load file_htm
    # del if breaks
    return storage_list, storage_values, storage_gaap

def parse_instance_doc(file_htm, storage_values, storage_list, storage_gaap):        
        tree = ET.parse(file_htm)
        # Process nonNumeric elements
        for element in tree.iter():
            #print(element.attrib)
            if "nonNumeric" in element.tag or "nonFractional" in element.tag:
                # get attribute name and master id
                attr_name = element.attrib.get("name")
                if not attr_name or attr_name not in storage_gaap: 
                    continue
                storage_gaap[attr_name]["context_ref"] = element.attrib["contextRef"]
                storage_gaap[attr_name]["context_id"] = element.attrib["id"]
                storage_gaap[attr_name]["continued_at"] = element.attrib.get("continuedAt", "null")
                storage_gaap[attr_name]["escape"] = element.attrib.get("escape", "null")
                storage_gaap[attr_name]["format"] = element.attrib.get("format", "null")
                storage_gaap[attr_name]["unit_ref"] = element.attrib.get("unitRef", "null")
                storage_gaap[attr_name]["decimals"] = element.attrib.get("decimals", "null")
                storage_gaap[attr_name]["scale"] = element.attrib.get("scale", "null")
                storage_gaap[attr_name]["format"] = element.attrib.get("format", "null")
                storage_gaap[attr_name]["value"] = element.text.strip() if element.text else "null"

                if gaap_id in storage_values:
                    storage_values[gaap_id]["us_gaap_value"] = storage_gaap[attr_name]     


def write_csv(storage_list, storage_values):
    # create csv
    file_name = "sec_xbrl_scrape_content.csv"

    with open(file_name, mode = "w", newline = "") as sec_file:
        #create writer
        writer = csv.writer(sec_file)
        # write the header
        # pass to the row writer the list of things to go into the header
        writer.writerow(["FILE", "LABEL", "VALUE"])
        # dump dict into csv
        for dict_cont in storage_list:
            # write row by row the things stored inside the storage list
            # the first is the namespace label
            # the second item is the actual dict
            for item in dict_cont[1].items():
                # second item is list of lists
                # grab items per each item
                writer.writerow([dict_cont[0]] + list(item))

    # create csv
    file_name = "sec_xbrl_scrape_values.csv"

    with open(file_name, mode = "w", newline = "") as sec_file:
        writer = csv.writer(sec_file)
        writer.writerow(["ID", "CATEGORY", "LABEL", "VALUE"])
        for storage1 in storage_values:
            # storage1 are keys to the values extracted from the second level dict
            # the .items() call enumerates values in dict
            for storage2 in storage_values[storage1].items():
                # extract by key the value
                # the value might be another dict because elements can have child elements
                if isinstance(storage2[1], dict): # check if it is
                    for storage3 in storage2[1].items():
                        # write to csv
                        writer.writerow([storage1] + [storage2[0]] + list(storage3))
                else:
                    if storage2[1] != None:
                        #write to csv, if storage2 is not a dictionry (we dont go to storage3)
                        writer.writerow([storage1] + [storage2] + ["None"])
# main workflow
def main():

    # call parser
    args = parse_args()

    # results are args.ticker and args.date
    # check if date is present

    # if user doesnt provide date, we automate to grab most recent
    # if user does provide date then we search for that

    # get cik
    #cik = get_cik_from_ticker(args.ticker)

   # time.sleep(0.5)  # Half a second between SEC requests

    # get accession number
    #accession_num = get_accession_for_date(cik, args.date)

    #time.sleep(0.5)  # Half a second between SEC requests

    # build url
    #base_url = get_base_url(cik, accession_num)

    #time.sleep(0.5)  # Half a second between SEC requests

    # here we go look for xml files
    # either for specified date or for most recent
    # file for htm.xml stored in htm
    # file for cal.xml stored in cal
    # file for def.xml stored in defi

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
        FilingTuple(file_lab, '{http://www.xbrl.org/2003/linkbase}labelLink', 'label')
    ]

    # label categories
    # labelArc points to next element you want

    storage_list, storage_values, storage_gaap = parse_linkbases(files_list, parse)
    parse_instance_doc(file_htm, storage_values, storage_list, storage_gaap)
    write_csv(storage_list, storage_values)
    print("Written to csv successfully")


if __name__ == "__main__":
    main()         


# In[ ]:





# In[ ]:





# In[ ]:




