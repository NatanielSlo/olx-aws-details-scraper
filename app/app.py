import asyncio
import json
import random
import os
import boto3
from datetime import datetime
from playwright.async_api import async_playwright
from src.utils.url_builder import UrlBuilder

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"

class DetailedScraper:
    def __init__(self, event):
        self.semaphore = asyncio.Semaphore(5)
        self.url_builder = UrlBuilder()
        self.sqs = boto3.client('sqs')
        self.records = event.get('Records', [])
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(os.environ['DDB_TABLE'])
        self.browser = None
        self.context = None

    async def start_browser(self):
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            headless=True, 
            args=[
                "--single-process",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-zygote"
            ]
        )
        self.context = await self.browser.new_context(
            user_agent=USER_AGENT,
            viewport={'width': 1280, 'height': 800}
        )
        
        await self.context.route("**/*", lambda route: 
            route.abort() if route.request.resource_type in ["image", "media", "font"] 
            else route.continue_()
        )
        print("Browser Started")

    async def process_all(self):
        if not self.records:
            print("Brak rekordów do przetworzenia.")
            return

        await self.start_browser()
        
        tasks = []
        for record in self.records:
            item = json.loads(record['body'])
            tasks.append(self.scrape_detailed_data(item))
        
        # return_exceptions=True pozwala nam obsłużyć błędy po zakończeniu wszystkich zadań
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        await self.browser.close()
        
        # Sprawdzamy, czy którykolwiek task rzucił błędem. 
        # Jeśli tak, rzucamy wyjątek na poziomie handlera, żeby SQS ponowił próbę.
        for res in results:
            if isinstance(res, Exception):
                print(f"Krytyczny błąd w jednym z zadań: {res}")
                raise res
                
        return results

    async def scrape_detailed_data(self, item):
        async with self.semaphore:
            page = await self.context.new_page()
            url = self.url_builder.build_product_url(item.get('url'))
            print(f"Rozpoczynam: {url}")

            try:
                # Czekamy na domcontentloaded, ale potem i tak musimy czekać na selektor
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                # Czekamy aż opis będzie widoczny
                desc_loc = page.locator(".css-19duwlz")
                try:
                    await desc_loc.wait_for(state="visible", timeout=8000)
                except Exception:
                    # DEBUG: Jeśli nie ma opisu, zrzuć fragment HTML
                    content = await page.content()
                    print(f"DEBUG HTML (pierwsze 1000 znaków): {content[:1000]}")
                    raise Exception(f"Timeout: Nie znaleziono opisu (.css-19duwlz) na {url}")

                # Pobieramy opis
                item["description"] = (await desc_loc.inner_text()).replace("\n", " ")

                # Pobieramy parametry
                params_container = page.locator('[data-testid="ad-parameters-container"]')
                if await params_container.count() > 0:
                    params = await params_container.locator('p.css-13x8d99').all()
                    for p in params:
                        text = await p.inner_text()
                        if ":" in text:
                            k, v = text.split(":", 1)
                            item[k.strip().lower().replace(" ", "_")] = v.strip()

                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                item["scraped_at_detailed"] = now
                item["last_seen"] = now
                
                print(f"Zapisuję do DynamoDB: {item.get('url')}")
                self.table.put_item(Item=item)

                return item
            
            except Exception as e:
                print(f"Błąd przy {url}: {str(e)}")
                # Rzucamy błąd dalej, żeby proces_all wiedział o porażce
                raise e 
            finally:
                await page.close()

def handler(event, context):
    scraper = DetailedScraper(event)
    
    # Używamy asyncio.run dla czystszego zarządzania pętlą w AWS Lambda
    try:
        asyncio.run(scraper.process_all())
    except Exception as e:
        print(f"Lambda zakończona błędem (SQS ponowi próbę): {e}")
        # Ponowne rzucenie błędu informuje SQS o niepowodzeniu
        raise e
    
    return {
        "statusCode": 200,
        "body": "Scrapowanie szczegółów zakończone sukcesem."
    }