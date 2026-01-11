import asyncio
import json
import random
import os
import boto3
from datetime import datetime
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async
from src.utils.url_builder import UrlBuilder

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"

class DetailedScraper:
    def __init__(self, event):
        self.url_builder = UrlBuilder()
        self.sqs = boto3.client('sqs')
        self.records = event.get('Records', [])

        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(os.environ['DDB_TABLE'])

        self.browser = None
        self.context = None

    async def start_browser(self):
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context(
            user_agent=USER_AGENT,
            viewport={'width': 1280, 'height': 800}
        )
        
        await self.context.route("**/*", lambda route: 
            route.abort() if route.request.resource_type in ["image", "media", "font"] 
            else route.continue_()
        )

    async def process_all(self):

        if not self.records:
            print("Brak rekordów do przetworzenia.")
            return

        await self.start_browser()
        
        tasks = []
        for record in self.records:
            # SQS body jest stringiem, parsujemy go
            item = json.loads(record['body'])
            tasks.append(self.scrape_detailed_data(item))
        
        results = await asyncio.gather(*tasks)
        
        await self.browser.close()
        return results

    async def scrape_detailed_data(self, item):
        page = await self.context.new_page()
        await stealth_async(page) # Ukrywamy ślady bota
        
        url = self.url_builder.build_product_url(item.get('url'))
        print(f"Scrapuję szczegóły: {url}")

        try:
            
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

           
            desc_loc = page.locator(".css-19duwlz")
            if await desc_loc.count() > 0:
                item["description"] = (await desc_loc.inner_text(timeout=5000)).replace("\n", " ")

            
            params_container = page.locator('[data-testid="ad-parameters-container"]')
            if await params_container.count() > 0:
                params = await params_container.locator('p.css-13x8d99').all()
                for p in params:
                    text = await p.inner_text()
                    if ":" in text:
                        k, v = text.split(":", 1)
                        item[k.strip().lower().replace(" ", "_")] = v.strip()

            item["scraped_at_detailed"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            print(f"Zapisuję do DynamoDB: {item.get('url')}")
            self.table.put_item(Item=item)

            return item

        except Exception as e:
            print(f"Błąd przy {url}: {e}")
            return None
        finally:
            await page.close()

def handler(event, context):
    scraper = DetailedScraper(event)
    asyncio.run(scraper.process_all())
    
    return {
        "statusCode": 200,
        "body": "Scrapowanie szczegółów zakończone."
    }