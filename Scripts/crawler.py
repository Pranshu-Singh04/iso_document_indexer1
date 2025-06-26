import os
import re
import json
import time
import asyncio
import random
import logging
import requests
from datetime import datetime
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse, urljoin
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import redis
import fitz  # PyMuPDF
import xml.etree.ElementTree as ET
import hashlib
import urllib
import ssl


# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
USER_AGENTS = [  
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36"
]
OUTPUT_DIR = os.path.join(os.getcwd(), 'data', 'raw')
SUPPORTED_TYPES = ['application/pdf', 'text/xml', 'application/xml', 'text/html']
API_SOURCES = {
    'regulations.gov': 'https://api.regulations.gov/v4/documents', 
}
LOG_FILE = "crawler.log"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class EnhancedCrawler:
    def __init__(self):
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        self.session = requests.Session()
        self.ua = UserAgent()
        self.playwright = None
        self.browser = None
        self.allowed_domains = set()
        logging.info("Enhanced crawler initialized")

    def get_domain(self, url):
        parsed = urlparse(url)
        netloc = parsed.netloc.replace('www.', '').split(':')[0]
        parts = netloc.split('.')
        if len(parts) >= 2:
            return '.'.join(parts[-2:])
        return netloc

    def can_fetch(self, url):
        domain = self.get_domain(url)
        robots_url = self.get_robots_txt_url(domain)
        rp = RobotFileParser()
        try:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            response = urllib.request.urlopen(robots_url, timeout=10, context=context)
            rp.parse(response.read().decode())
            return rp.can_fetch("*", url)
        except Exception as e:
                        logging.warning(f"Robots.txt check failed for {domain}: {str(e)}")

    async def init_browser(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--blink-settings=imagesEnabled=false",
                "--disable-blink-features=AutomationControlled"
            ]
        )

    def check_copyright(self, content):
        copyright_keywords = ['copyright', '©', 'all rights reserved']
        return any(keyword in content.lower() for keyword in copyright_keywords)

    def extract_year_from_pdf(self, pdf_path):
        try:
            doc = fitz.open(pdf_path)
            if doc.is_encrypted:
                return None
            metadata = doc.metadata
            for key in ['creationDate', 'modDate']:
                if metadata.get(key):
                    year_match = re.search(r'D:?(19|20)\d{2}', metadata[key])
                    if year_match:
                        return year_match.group(1) + year_match.group(2)
            return None
        except Exception as e:
            logging.warning(f"Metadata extraction failed: {str(e)}")
            return None

    def extract_year_from_url(self, url):
        path = urlparse(url).path
        year_match = re.search(r'/(?:19|20)\d{2}/', path)
        if year_match:
            return year_match.group(0).strip('/')
        version_match = re.search(r'(?:v|rev|version|ver)(?:\d{4}|(?:19|20)\d{2}-\d{2}-\d{2})', path, re.IGNORECASE)
        if version_match:
            version = version_match.group(0)
            year_match = re.search(r'(19|20)\d{2}', version)
            if year_match:
                return year_match.group(0)
        return None

    def extract_year_from_content(self, pdf_path):
        try:
            doc = fitz.open(pdf_path)
            text = ""
            for page in doc[:min(3, len(doc))]:
                text += page.get_text()
            year_match = re.search(r'\b(19|20)\d{2}\b', text)
            if year_match:
                return year_match.group(0)
            date_match = re.search(r'(?:19|20)\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])', text)
            if date_match:
                return date_match.group(0).split('-')[0]
            return None
        except Exception as e:
            logging.warning(f"Content year extraction failed: {str(e)}")
            return None

    def extract_year_from_filename(self, filename):
        if not filename:
            return None
        year_match = re.search(r'\b(19|20)\d{2}\b', filename)
        if year_match:
            return year_match.group(0)
        version_match = re.search(r'(?:v|rev|version|ver)(?:\d{4}|(?:19|20)\d{2}-\d{2}-\d{2})', filename, re.IGNORECASE)
        if version_match:
            version = version_match.group(0)
            year_match = re.search(r'(19|20)\d{2}', version)
            if year_match:
                return year_match.group(0)
        return None

    def log_download(self, url, domain, year, file_path):
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'url': url,
            'domain': domain,
            'year': year,
            'file_path': file_path
        }
        logging.info(f"Log entry: {json.dumps(log_entry)}")
        self.redis_client.lpush('downloaded_files', json.dumps(log_entry))

    def save_file(self, content, domain, url, content_type):
        try:
            domain_dir = os.path.join(OUTPUT_DIR, domain)
            os.makedirs(domain_dir, exist_ok=True)
            parsed = urlparse(url)
            filename = os.path.basename(parsed.path)
            if not filename:
                filename = hashlib.md5(url.encode()).hexdigest()
            if not re.search(r'\.\w+$', filename):
                if 'pdf' in content_type:
                    filename += '.pdf'
                elif 'xml' in content_type:
                    filename += '.xml'
                else:
                    ext = os.path.splitext(parsed.path)[1]
                    filename += ext or '.bin'
            temp_path = os.path.join(domain_dir, 'temp_download')
            with open(temp_path, 'wb') as f:
                f.write(content)
            file_size = os.path.getsize(temp_path)
            if file_size < 50:
                logging.warning(f"File too small ({file_size}B): {url}")
                os.remove(temp_path)
                return None, None
            file_size = os.path.getsize(temp_path)
            year_metadata = self.extract_year_from_pdf(temp_path)
            year_content = self.extract_year_from_content(temp_path)
            year_url = self.extract_year_from_url(url)
            year_filename = self.extract_year_from_filename(filename)
            year_candidates = [y for y in [year_metadata, year_content, year_url, year_filename] if y is not None]
            year = min(year_candidates) if year_candidates else datetime.now().strftime('%Y')
            year_dir = os.path.join(domain_dir, year)
            os.makedirs(year_dir, exist_ok=True)
            final_path = os.path.join(year_dir, filename)
            os.rename(temp_path, final_path)
            logging.info(f"Saved file: {final_path} ({file_size}B)")
            return final_path, year
        except Exception as e:
            logging.error(f"File save failed: {str(e)}")
            return None, None

    async def handle_javascript_content(self, url):
        try:
            # Use a real browser profile
            page = await self.browser.new_page(user_agent=self.ua.random)
            await page.goto(url, wait_until='networkidle', timeout=120000)
            # Wait for critical elements (e.g., PDF viewer or download button)
            try:
                await page.wait_for_selector("button.preview-button", timeout=10000)
                await page.click("button.preview-button")
            except:
                pass
            await page.wait_for_timeout(5000)  # Allow content to load
            content = await page.content()
            await page.close()
            return content
        except Exception as e:
            logging.error(f"JS rendering error {url}: {str(e)}")
            return None

    def detect_content_type(self, content):
        if content.startswith(b'%PDF-'):
            return 'application/pdf'
        elif content.startswith(b'<?xml') or content.startswith(b'<xml'):
            return 'application/xml'
        elif content.startswith(b'<html') or b'<HTML' in content[:100]:
            return 'text/html'
        return 'unknown'

    def should_download(self, url, content_type, content):
        if content_type == 'application/pdf':
            try:
                doc = fitz.open(stream=content, filetype="pdf")
                if doc.is_encrypted:
                    return False
                text = ""
                for page in doc[:min(3, len(doc))]:
                    text += page.get_text()
                if self.check_copyright(text):
                    return False
            except Exception as e:
                logging.warning(f"PDF check error: {str(e)}")
                return False
        return content_type in SUPPORTED_TYPES

    def is_open_access_ieee(self, html_content):
        keywords = ["open access", "Creative Commons", "CC BY", "free to read"]
        return any(keyword.lower() in html_content.lower() for keyword in keywords)

    async def handle_iso_preview(self, url):
        try:
            page = await self.browser.new_page(user_agent=self.ua.random)
            await page.goto(url, wait_until='networkidle', timeout=60000)
            await page.wait_for_selector("button.preview-button")
            await page.click("button.preview-button")
            await page.wait_for_timeout(3000)
            content = await page.content()
            await page.close()
            return content
        except Exception as e:
            logging.error(f"Preview rendering failed for ISO: {str(e)}")
            return None

    def generate_etsi_pdf_link(self, standard_type, number_range, number, version, format="en"):
        return f"https://www.etsi.org/deliver/etsi_{standard_type}/{number_range}/{number}/{version}/{format}/en_{number}v{version}p.pdf" 

    def extract_links(self, soup, base_url):
        """Extract all links from a page, not just document patterns"""
        links = []
        
        for a_tag in soup.find_all('a', href=True):
            link = urljoin(base_url, a_tag['href'])
            parsed = urlparse(link)
            # Skip non-http(s) schemes and mailto:
            if not parsed.scheme.startswith(('http', 'https')):
                continue

            if parsed.netloc == '' or parsed.scheme in ['mailto', 'javascript']:
                continue
            link_domain = self.get_domain(link)
            # Optional: Stay within same domain
            if link_domain not in self.allowed_domains:
                logging.debug(f"Skipped (domain not allowed): {link}")
                continue
            links.append(link)
        logging.info(f"Found {len(links)} valid links total links")
        return links
    
    async def check_for_embedded_documents(self, content, domain, url):
        if isinstance(content, bytes):
            content_str = content.decode('utf-8', errors='ignore')
        else:
            content_str = content

        soup = BeautifulSoup(content_str, 'html.parser')
        embedded_links = []

        # Search for embedded PDFs/XMLs
        for tag in soup.find_all(['iframe', 'object', 'embed', 'source']):
            src = tag.get('src')
            if src and any(ext in src.lower() for ext in ['.pdf', '.xml']):
                embedded_links.append(urljoin(url, src))

        # Also check for <a> tags that point directly to PDF/XML
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if any(ext in href.lower() for ext in ['.pdf', '.xml']):
                embedded_links.append(urljoin(url, href))

        # Download embedded documents
        for doc_url in embedded_links:
            if not self.redis_client.sismember('seen_urls', doc_url):
                success = await self.process_url(doc_url)
                if success:
                    logging.info(f"Downloaded embedded document: {doc_url}")

    async def process_url(self, url):
        try:
            domain = self.get_domain(url)
            logging.info(f"Processing: {url}")
            delay = max(2, random.gauss(3, 1))
            logging.info(f"Sleeping {delay:.1f}s before downloading {url}")
            time.sleep(delay)

            # Direct PDF/XML detection
            if url.lower().endswith(('.pdf', '.xml')):
                success = await self.download_pdf_direct(url, domain)
                return success

            # Always attempt JS rendering for unknown domains
            html = await self.handle_javascript_content(url)
            if not html:
                # Fallback to requests if JS rendering fails
                headers = {'User-Agent': self.ua.random}
                response = self.session.get(url, headers=headers, timeout=30, allow_redirects=True)
                if response.status_code != 200:
                    logging.warning(f"HTTP {response.status_code}: {url}")
                    return False
                soup = BeautifulSoup(response.text, 'html.parser')
            else:
                soup = BeautifulSoup(html, 'html.parser')

            # Extract and enqueue all links
            all_links = self.extract_links(soup, url)
            for link in all_links:
                if not self.redis_client.sismember('seen_urls', link):
                    self.redis_client.rpush('url_queue', link)
                    self.redis_client.sadd('seen_urls', link)
                    logging.info(f"Added to queue: {link}")

            # Check for embedded documents
            await self.check_for_embedded_documents(html, domain, url)
            return True
        except Exception as e:
            logging.error(f"Processing error {url}: {str(e)}")
            return False

    async def download_pdf_direct(self, url, domain):
        headers = {'User-Agent': self.ua.random}
        response = self.session.get(url, headers=headers, timeout=30, allow_redirects=True)
        if response.status_code != 200:
            logging.warning(f"HTTP {response.status_code}: {url}")
            return False
        content_type = response.headers.get('Content-Type', '')
        if self.should_download(url, content_type, response.content):
            file_path, year = self.save_file(response.content, domain, url, content_type)
            self.log_download(url, domain, year, file_path)
            return True
        return False

    async def process_api_source(self, api_endpoint):
        try:
            params = {
                'api_key': os.getenv('REGULATIONS_GOV_API_KEY'),
                'sort': '-publicationDate',
                'size': 100
            }
            response = self.session.get(api_endpoint, params=params, timeout=30)
            if response.status_code != 200:
                return False
            data = response.json()
            for item in data.get('data', []):
                attributes = item.get('attributes', {})
                pdf_url = attributes.get('attachments', [{}])[0].get('fileUrl')
                if pdf_url and not self.redis_client.sismember('seen_urls', pdf_url):
                    await self.process_url(pdf_url)
            return True
        except Exception as e:
            logging.error(f"API processing error: {str(e)}")
            return False

    async def run(self):
        logging.info("Initializing browser...")
        await self.init_browser()
        try:
            self.redis_client.delete('url_queue')
            self.redis_client.delete('seen_urls')
            with open('urls_to_crawl.txt', 'r', encoding='utf-8') as f:
                urls = [url.strip() for url in f.readlines()]
                random.shuffle(urls)
            # Populate allowed domains from seed URLs
            self.allowed_domains = {self.get_domain(url) for url in urls if url}
            logging.info(f"Allowed domains: {self.allowed_domains}")
            
            for url in urls:
                if not url or self.redis_client.sismember('seen_urls', url):
                    continue
                if url.lower().endswith('.pdf'):
                    self.redis_client.lpush('url_queue', url)
                else:
                    self.redis_client.rpush('url_queue', url)
                self.redis_client.sadd('seen_urls', url)
                logging.debug(f"Seeded URL: {url}")
            logging.info("Starting crawl loop...")
            while True:
                url = self.redis_client.lpop('url_queue')
                if not url:
                    logging.info("URL queue is empty. Waiting before checking again...")
                    time.sleep(10)
                    continue
                url = url.strip()
                logging.info(f"Processing: {url}")
                success = await self.process_url(url)
                if not success:
                    logging.warning(f"Failed to process: {url}")
        finally:
            await self.cleanup()

    async def cleanup(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

if __name__ == "__main__":
    logging.info("Starting Enhanced Crawler")
    crawler = EnhancedCrawler()
    try:
        asyncio.run(crawler.run())
    except KeyboardInterrupt:
        logging.info("Crawler stopped manually")
    finally:
        logging.info("Crawler stopped")