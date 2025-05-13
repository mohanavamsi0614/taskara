from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor

# MongoDB setup
MONGO_URI = "mongodb+srv://mohanavamsi14:vamsi@cluster.74mis.mongodb.net/?retryWrites=true&w=majority&appName=Cluster/"
client = MongoClient(MONGO_URI)
db = client["financial_reports"]
collection = db["all_stock_list"]

def serp(company):
    with sync_playwright() as p:
        print(f"Started process for {company['Name']}...")
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
        )
        page = context.new_page()
        query = f"{company.get('Name', '')} official website {company.get('Country',"")}"
        query2 = f"{company.get('Name', '')} investor relations page {company.get('Country',"")}"

        try:
            page.goto(f"https://duckduckgo.com/?q={query.replace(' ', '+')}", timeout=80000)
            print("Page loaded...")
            page.wait_for_selector("a", timeout=65000)
            data = page.content()
            soup = BeautifulSoup(data, "html.parser")
            print("Resources located...")
            search_results = soup.select("a.eVNpHGjtxRBq_gLOfGDr")[0]
            link = search_results["href"]
            print("LINK FOUND 😎", link, company.get("Name"))

            page.goto(f"https://duckduckgo.com/?q={query2.replace(' ', '+')}", timeout=80000)
            print("Page loaded...")
            page.wait_for_selector("a", timeout=65000)
            data = page.content()
            soup = BeautifulSoup(data, "html.parser")
            print("Resources located...")
            search_results = soup.select("a.eVNpHGjtxRBq_gLOfGDr")[0]
            link2 = search_results["href"]
            print("LINK FOUND 😎", link2, company.get("Name"))

            if link and link2:
                result = {
                    "name": company["Name"],
                    "link": link,
                    "ir": link2,
                    "country": company.get("country",""),
                    "symbol": company["Symbol"]
                }
                collection.update_one(
                    {"name": company["Name"]},
                    {"$set": result},
                    upsert=True
                )
                print("✅ Data saved successfully!", company["Name"])
            else:
                print("❌ No valid links found for", company["Name"])

        except Exception as e:
            print(f"⚠️ Error during Playwright operation for {company['Name']}: {e}")
        finally:
            browser.close()

def main():
    print(f"started ......")

    # Fetch companies from MongoDB
    companies = list(db["all_exchanege_stocks"].find({}))

    processed_names = set(
        doc["symbol"] for doc in collection.find({})
    )

    def process_company(company):
        if company["Symbol"] in processed_names:
            print(f"Skipping... {company['Name']}")
            return None
        return serp(company)

    with ThreadPoolExecutor(max_workers=50) as executor:
        executor.map(process_company, companies)

    print("✅ Data saved successfully!")

main()
