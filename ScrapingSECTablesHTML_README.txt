ScrapingSECTablesHTML 

(first testing with TestScrapingTableHTML)
 
The script, written in python, does a basic automation of extracting and parsing structured financial data from SEC EDGAR 10‑K filing.

At run time prompts the user to enter a stock ticker and year, then searches for  the correct EDGAR filing URL through SEC’s public JSON endpoints.

Raw filing documents (10‑K) are scraped, without use of APIs HTML and parsed with BeautifulSoup. This is not an ideal way of scraping because the structure of the tables are parsed in a way that may cause slight misalignment in the data.

The script automatically detects <table> elements, and searches data for user-specified financial terms (e.g., "income tax", "margins", "tax provisions"), then exports only the relevant data.

Built in Python, using requests, BeautifulSoup, and pandas. The project is a preliminary level investigation into web scarping, with some data cleaning and normalization. Exporting data into a file "tables_output", containing the workbook "financial statements".

This project is the first in a series of data scraping projects. 