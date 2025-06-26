# 🕸️ ISO Standard Document Crawler & Metadata Extractor  
*Automated crawling, downloading, and metadata extraction of freely available regulatory standards*

> This project strictly complies with downloading only **freely accessible documents**. It does not scrape or store any **copyrighted or login-restricted content**.

---

## 📋 Project Overview

This project aims to build an automated system to:
- Crawl open-access repositories for ISO/regulatory standard documents
- Download PDF/XML files
- Extract metadata: title, document ID, publisher, publication date, abstract
- Store extracted data in structured format (SQLite/JSON/CSV)
- Index metadata for search and filtering
- Support future AI/LLM training or semantic search

---

## 🧰 Tools & Technologies

| Component | Tool |
|----------|------|
| Language | Python 3.x |
| Libraries | `requests`, `beautifulsoup4`, `PyMuPDF`, `pdfminer.six`, `lxml`, `sqlite3`, `pandas` |
| Version Control | GitHub |
| Storage Format | SQLite / JSON / CSV |
| Documentation | Markdown + inline comments |

---

## 🛠️ Setup Instructions
1. 📦 Clone the Repository
git clone https://github.com/yourname/iso_document_indexer.git 
cd iso_document_indexer

*Replace yourname with your GitHub username or private repo path.* 

2. 🔧 Create and Activate Virtual Environment
On Windows:
python -m venv env
.\env\Scripts\activate


On Linux/macOS:
python3 -m venv env
source env/bin/activate

3. 📥 Install Required Dependencies
Make sure you have this file in your root folder:

## Requirements.txt
requests
beautifulsoup4
PyMuPDF
pdfminer.six
lxml
sqlite3
pandas
redis

Then run:

pip install -r requirements.txt

## 4. 📄 Prepare URL List
Create a file named urls_to_crawl.txt in the root folder:
urls_to_crawl.txt 

https://public.resource.org/DOE/NREL/U.S.+Department+of+Energy+Buildings+Energy+Data+Book/ 
https://public.resource.org/scribd/2556263.pdf 
https://ieeexplore.ieee.org/document/IEEE-802.15.4-2020/ 
https://www.iso.org/home.html 
https://www.scc.ca/en/standardsdb 
https://www.regulations.gov/ 
https://www.etsi.org/standards-search 

⚠️ Only include freely downloadable URLs — avoid login walls or paywalled content 


## 🚀 Usage

### Step 1: Configure Seed URLs
Edit urls_to_crawl.txt with valid public document repositories:
https://public.resource.org/standards 
https://www.iso.org/obp/ui
          
**OR**

### Step 1: Start Redis
redis-server

### Step 2: Crawl and download files
python scripts/download.py

### Step 3: Extract metadata
python scripts/extract_pdf_metadata.py
python scripts/parse_xml_metadata.py

### Step 4: Index into database
python scripts/index_documents.py index

### Step 5: Search documents
python scripts/index_documents.py search -k "information security" -d "public.resource.org" -e "security_standards.csv"