import os
import re
import sys
import json
import signal
import datetime
from urllib.parse import urlparse, urljoin
import requests
import boto3
from playwright.sync_api import sync_playwright
import warnings
from urllib3.exceptions import InsecureRequestWarning
import concurrent.futures
from pymongo import MongoClient

print("lknkj")
# Suppress InsecureRequestWarning globally
warnings.simplefilter('ignore', InsecureRequestWarning)

S3_CLIENT = boto3.client(
    "s3",
    aws_access_key_id="",
    aws_secret_access_key="",
    region_name="eu-north-1"
)

REPORT_KEYWORDS = [
    'annual report', 'annual-report', 'form 10-k', '10k', '10-k', 'financial report',
    'financial statement', 'financial results', 'consolidated financial', 'fiscal year',
    'year end', 'annual review', 'yearly report', 'annual financial',
    "annual report and accounts", "annual integrated report", "annual accounts", 
    "earnings report", "stockholder report", "shareholder report","report","reports"
]

STRONG_INDICATORS = ['annual', 'financial', 'report', 'statement', 'fiscal', '10-k', 'consolidated',"report","reports"]
WEAK_KEYWORDS = ['download', 'data', 'q', 'our', 'information']
REPORT_EXTENSIONS = ['.pdf', '.xls', '.xlsx', '.doc', '.docx']
skip_current_website = False
no_reports=[]

# Initialize a list to store problematic URLs
problematic_urls = []

def log(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def extract_file_name(url):
    log(f"Extracting file name from URL: {url}")
    parsed_url = urlparse(url)
    path = parsed_url.path
    filename = os.path.basename(path)
    if not filename or '.' not in filename:
        filename = f"report_{parsed_url.netloc}_{path.replace('/', '_')}.pdf"
    return filename

def upload_directly_to_s3(url, bucket_name, s3_key):
    log(f"Uploading file from URL: {url} to S3 bucket: {bucket_name} with key: {s3_key}")
    return "Need to upload"

def check_is_website(url):
    log(f"Checking if URL is a website: {url}")
    lower_url = url.lower()
    return not any(lower_url.endswith(ext) for ext in ['.pdf', '.xls', '.xlsx', '.doc', '.docx', '.csv'])

def process_financial_report(url, bucket_name, link_text=None):
    try:
        filename = extract_file_name(url)
        filename = re.sub(r'[^a-zA-Z0-9._-]', '_', filename)
        
        if not filename.lower().endswith('.pdf'):
            filename += '.pdf'
            
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        s3_key = f"financial_reports/{timestamp}_{filename}"
        
        retry_attempts = 3
        response = None
        for attempt in range(retry_attempts):
            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                content_type = response.headers.get('Content-Type', '').lower()
                
                if 'pdf' not in content_type:
                    log(f"Warning: File might not be a PDF. Content-Type: {content_type}")
                    
                    if 'html' in content_type:
                        log(f"Skipping HTML file: {url}")
                        return None
                break
            except requests.exceptions.RequestException as e:
                log(f"Error downloading file after : {e}")
                return None
        
        if not response:
            log(f"Failed to download file from URL: {url}")
            return None
        
        log(f"Uploading {url} to S3 as {s3_key}")
        s3_url = upload_directly_to_s3(url, bucket_name, s3_key)
        if not s3_url:
            log(f"Failed to upload file to S3: {url}")
            return None
        
        if link_text and len(link_text) > 5:
            title = link_text
        else:
            title = filename
        
        sheet_names = []  # Placeholder for future use
        
        metadata = {
            "filename": filename,
            "title": title,
            "sheets": sheet_names,
            "source_url": url,
            "s3_bucket": bucket_name,
            "s3_key": s3_key,
            "s3_url": s3_url
        }
        
        log(f"Successfully processed report: {metadata}")
        return metadata
    except Exception as e:
        log(f"Error processing report: {e}")
        import traceback
        traceback.print_exc()
        return None

def is_website_accessible(url, max_retries=2):
    log(f"Checking website accessibility for URL: {url}")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0")
            page = context.new_page()
            for _ in range(max_retries):
                try:
                    response = page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    if response and response.status < 400:
                        browser.close()
                        log(f"Website is accessible: {url}")
                        return True
                except Exception:
                    print(Exception)
                    log(f"Retrying accessibility check for URL: {url}")
            browser.close()
            log(f"Website is not accessible: {url}")
            return False
    except Exception as e:
        log(f"Error checking website accessibility for URL: {url}. Error: {e}")
        return False

def find_report_links(url):
    """
    Find financial report links by checking anchor tag type attribute and text content,
    not just the URL extension. Handles dynamic pagination and year tabs.
    """
    from urllib.parse import urljoin
    from playwright.sync_api import sync_playwright
    import traceback

    report_links = []
    visited_urls = set()

    doc_keywords = ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'aspx',"download","annual","Annual"]

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page = context.new_page()
            print(f"🌐 Navigating to {url}...")
            response = page.goto(url, wait_until="domcontentloaded", timeout=80000)

            if not response or not response.ok:
                print("❌ Failed to load page.")
                return []

            page_number = 1
            done_years = []

            while True:
                print(f"🧭 Scraping page {page_number}...")
                page.wait_for_timeout(3000)

                links_data = page.evaluate("""
                    () => Array.from(document.querySelectorAll('a')).map(link => ({
                        href: link.href || '',
                        text: (link.innerText || link.textContent || '').trim(),
                        type: link.getAttribute('type') || '',
                        download: link.getAttribute('download') || '',
                        dataType: link.getAttribute('data-type') || '',
                        target:link.getAttribute('target') || ''
                    }))
                """)

                base_url = page.url

                for link in links_data:
                    href = link.get("href", "").strip()
                    text = link.get("text", "").lower()
                    type_attr = link.get("type", "").lower()
                    download = link.get("download", "").lower()
                    data_type = link.get("dataType", "").lower()
                    target=link.get("target","").lower()

                    if href and href not in visited_urls:
                        if any(keyword in (type_attr + download + data_type + text + href) for keyword in doc_keywords) or href.endswith(".pdf") or ".pdf" in href or "pdf" in href or target=="_blank":
                            full_url = urljoin(base_url, href)
                            report_links.append((full_url, link.get("text", "").strip()))
                            visited_urls.add(full_url)

                pagination_keywords = ["Next", "Load More", "Show More"]
                year_keywords = [str(y) for y in range(2024, 1997, -1)]
                found_clickable = False

                for keyword in pagination_keywords + year_keywords:
                    try:
                        selector = f"text={keyword}"
                        clickable = page.query_selector(selector)

                        if clickable and clickable.is_enabled() and keyword not in done_years:
                            print(f"🔘 Clicking '{keyword}'...")
                            clickable.scroll_into_view_if_needed()
                            clickable.click()
                            page.wait_for_timeout(3000)
                            page_number += 1
                            done_years.append(keyword)
                            found_clickable = True
                            break
                    except Exception as e:
                        print(f"⚠️ Failed to click '{keyword}': {e}")
                        done_years.append(keyword)

                if not found_clickable:
                    print("✅ No more pagination or year elements found.")
                    break

            browser.close()
            print(f"📌 Found {len(report_links)} document link(s) on {url}")
            return report_links

    except Exception as e:
        print(f"❗ Error scraping website {url}: {e}")
        traceback.print_exc()
        return report_links

# Initialize MongoDB client
MONGO_URI = "mongodb+srv://mohanavamsi14:vamsi@cluster.74mis.mongodb.net/?retryWrites=true&w=majority&appName=Cluster/"
client = MongoClient(MONGO_URI)
db = client["financial_reports"]
collection = db["reports"]

def save_problematic_urls_to_file(file_path="problematic_urls.json"):
    """
    Save the problematic URLs to a JSON file.
    """
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(problematic_urls, f, indent=4)
        log(f"Problematic URLs saved to {file_path}")
    except Exception as e:
        log(f"Error saving problematic URLs to file. Error: {e}")

def process_website(url, bucket_name, sector, industry, country):
    print("\n")
    log("="*10+f"Processing website: {url}"+"="*10)
    print("\n")
    global skip_current_website
    skip_current_website = False
    if not is_website_accessible(url):
        log(f"Website is not accessible: {url}")
        problematic_urls.append({"url": url, "reason": "Not accessible"})
        return []
    try:
        report_links = find_report_links(url)
        if not report_links or skip_current_website:
            log(f"No report links found or website skipped: {url}")
            problematic_urls.append({"url": url, "reason": "No reports found or skipped"})
            return []
        results = []
        for link_url, link_text in report_links:
            if skip_current_website:
                log(f"Skipping remaining reports for website: {url}")
                break
            metadata = process_financial_report(link_url, bucket_name, link_text)
            if metadata:
                metadata["source_website"] = url
                metadata["sector"] = sector
                metadata["industry"] = industry
                metadata["country"] = country

                results.append(metadata)
            else:
                problematic_urls.append({"url": link_url, "reason": "Error processing report"})
        log(f"Completed processing website: {url}. Found {len(results)} reports.")
        collection.insert_many(results)

        return results
    except Exception as e:
        log(f"Error processing website: {url}. Error: {e}")
        problematic_urls.append({"url": url, "reason": f"Error: {e}"})
        return []

def process_input(websites, bucket_name="companiesannualreports", country="unknown"):
    log(f"Starting processing for country: {country}")
    
    processed_urls = set()  # Track processed URLs

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        
        for website in websites:
            url= website.get("link")
            sector=website.get("sector")
            industry=website.get("Industry")
            country=website.get("Country")
            if collection.find_one({"source_website": url}):
                log(f"Skipping already processed URL: {url}")
                continue
            if url in processed_urls:
                log(f"Skipping already processed URL: {url}")
                continue
            
            futures.append(executor.submit(process_website, url, bucket_name,sector,industry,country))

        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    # collection.insert_many(result)
                    print("done")
            except Exception as e:
                log(f"Error processing a website: {e}")
    
    save_problematic_urls_to_file(f"{country}_problematic_urls.json")
    
    log(f"Completed processing for country: {country}. Results saved to MongoDB.")
    return list(collection.find({"country": country}))

def get_data():
    coll=db["companies"]
    return coll.find()
    
if __name__ == "__main__":
    log("Starting main execution for main stock list.....")
    bucket_name = "companiesfinancialreports"

    def process_country():
        websites = get_data()
        return process_input(websites, bucket_name)
    try:
        result = process_country()
        log(f"Completed processing for country.")
    except Exception as e:
        log(f"Error processing country Error: {e}")

    log("Main execution completed")
port = int(os.environ.get("PORT", 5000))
app.run(host="0.0.0.0", port=port)

