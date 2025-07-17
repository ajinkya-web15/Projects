import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
import pandas as pd
import sqlite3
import schedule
import time
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import csv
import os
from dataclasses import dataclass
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ScrapeConfig:
    """Configuration for a scraping job"""
    name: str
    url: str
    scrape_type: str  # 'static' or 'dynamic'
    selectors: Dict[str, str]
    schedule_interval: str  # '5min', '1hour', '1day', etc.
    output_format: str  # 'csv', 'json', 'database'
    max_retries: int = 3
    timeout: int = 30
    headers: Optional[Dict[str, str]] = None
    wait_for_element: Optional[str] = None  # CSS selector to wait for (dynamic scraping)


class BaseScraper(ABC):
    """Base class for scrapers"""

    def __init__(self, config: ScrapeConfig):
        self.config = config
        self.session = requests.Session()
        if config.headers:
            self.session.headers.update(config.headers)

    @abstractmethod
    def scrape(self) -> List[Dict[str, Any]]:
        """Abstract method to scrape data"""
        pass

    def retry_scrape(self) -> List[Dict[str, Any]]:
        """Retry scraping with exponential backoff"""
        for attempt in range(self.config.max_retries):
            try:
                return self.scrape()
            except Exception as e:
                logger.warning(f"Scraping attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise


class StaticScraper(BaseScraper):
    """Scraper for static websites using BeautifulSoup"""

    def scrape(self) -> List[Dict[str, Any]]:
        logger.info(f"Starting static scrape of {self.config.url}")

        response = self.session.get(self.config.url, timeout=self.config.timeout)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        data = []

        # Find all elements that match the first selector (usually container elements)
        container_selector = list(self.config.selectors.keys())[0]
        containers = soup.select(container_selector)

        for container in containers:
            item = {}
            item['scraped_at'] = datetime.now().isoformat()
            item['url'] = self.config.url

            # Extract data using remaining selectors
            for field, selector in self.config.selectors.items():
                if field == container_selector:
                    continue

                element = container.select_one(selector)
                if element:
                    # Try to get text, href, or src attribute
                    if element.get('href'):
                        item[field] = element.get('href')
                    elif element.get('src'):
                        item[field] = element.get('src')
                    else:
                        item[field] = element.get_text(strip=True)
                else:
                    item[field] = None

            data.append(item)

        logger.info(f"Static scrape completed. Found {len(data)} items")
        return data


class DynamicScraper(BaseScraper):
    """Scraper for dynamic websites using Selenium"""

    def __init__(self, config: ScrapeConfig):
        super().__init__(config)
        self.driver = None

    def _setup_driver(self):
        """Setup Chrome driver with options"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')

        # Add user agent if specified in headers
        if self.config.headers and 'User-Agent' in self.config.headers:
            chrome_options.add_argument(f'--user-agent={self.config.headers["User-Agent"]}')

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(10)

    def scrape(self) -> List[Dict[str, Any]]:
        logger.info(f"Starting dynamic scrape of {self.config.url}")

        try:
            self._setup_driver()
            self.driver.get(self.config.url)

            # Wait for specific element if configured
            if self.config.wait_for_element:
                WebDriverWait(self.driver, self.config.timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, self.config.wait_for_element))
                )

            # Allow time for dynamic content to load
            time.sleep(2)

            data = []

            # Find all elements that match the first selector
            container_selector = list(self.config.selectors.keys())[0]
            containers = self.driver.find_elements(By.CSS_SELECTOR, container_selector)

            for container in containers:
                item = {}
                item['scraped_at'] = datetime.now().isoformat()
                item['url'] = self.config.url

                # Extract data using remaining selectors
                for field, selector in self.config.selectors.items():
                    if field == container_selector:
                        continue

                    try:
                        element = container.find_element(By.CSS_SELECTOR, selector)

                        # Try to get text, href, or src attribute
                        if element.get_attribute('href'):
                            item[field] = element.get_attribute('href')
                        elif element.get_attribute('src'):
                            item[field] = element.get_attribute('src')
                        else:
                            item[field] = element.text.strip()
                    except Exception:
                        item[field] = None

                data.append(item)

            logger.info(f"Dynamic scrape completed. Found {len(data)} items")
            return data

        finally:
            if self.driver:
                self.driver.quit()


class DataExporter:
    """Handle data export to various formats"""

    @staticmethod
    def to_csv(data: List[Dict[str, Any]], filename: str):
        """Export data to CSV"""
        if not data:
            logger.warning("No data to export to CSV")
            return

        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        logger.info(f"Data exported to CSV: {filename}")

    @staticmethod
    def to_json(data: List[Dict[str, Any]], filename: str):
        """Export data to JSON"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Data exported to JSON: {filename}")

    @staticmethod
    def to_database(data: List[Dict[str, Any]], db_name: str, table_name: str):
        """Export data to SQLite database"""
        if not data:
            logger.warning("No data to export to database")
            return

        conn = sqlite3.connect(db_name)
        df = pd.DataFrame(data)
        df.to_sql(table_name, conn, if_exists='append', index=False)
        conn.close()
        logger.info(f"Data exported to database: {db_name}, table: {table_name}")


class WebScraperScheduler:
    """Main scheduler class for web scraping jobs"""

    def __init__(self):
        self.jobs: Dict[str, ScrapeConfig] = {}
        self.running = False

    def add_job(self, config: ScrapeConfig):
        """Add a scraping job to the scheduler"""
        self.jobs[config.name] = config

        # Schedule the job based on interval
        if config.schedule_interval.endswith('min'):
            minutes = int(config.schedule_interval[:-3])
            schedule.every(minutes).minutes.do(self._run_job, config.name)
        elif config.schedule_interval.endswith('hour'):
            hours = int(config.schedule_interval[:-4])
            schedule.every(hours).hours.do(self._run_job, config.name)
        elif config.schedule_interval.endswith('day'):
            days = int(config.schedule_interval[:-3])
            schedule.every(days).days.do(self._run_job, config.name)
        else:
            logger.error(f"Invalid schedule interval: {config.schedule_interval}")

        logger.info(f"Job '{config.name}' added with interval: {config.schedule_interval}")

    def _run_job(self, job_name: str):
        """Run a specific scraping job"""
        config = self.jobs[job_name]
        logger.info(f"Running job: {job_name}")

        try:
            # Choose scraper based on type
            if config.scrape_type == 'static':
                scraper = StaticScraper(config)
            else:
                scraper = DynamicScraper(config)

            # Scrape data
            data = scraper.retry_scrape()

            # Export data
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            if config.output_format == 'csv':
                filename = f"{job_name}_{timestamp}.csv"
                DataExporter.to_csv(data, filename)
            elif config.output_format == 'json':
                filename = f"{job_name}_{timestamp}.json"
                DataExporter.to_json(data, filename)
            elif config.output_format == 'database':
                db_name = f"{job_name}.db"
                DataExporter.to_database(data, db_name, job_name)

            logger.info(f"Job '{job_name}' completed successfully")

        except Exception as e:
            logger.error(f"Job '{job_name}' failed: {str(e)}")

    def run_job_now(self, job_name: str):
        """Run a specific job immediately"""
        if job_name in self.jobs:
            self._run_job(job_name)
        else:
            logger.error(f"Job '{job_name}' not found")

    def start_scheduler(self):
        """Start the scheduler"""
        self.running = True
        logger.info("Scheduler started")

        while self.running:
            schedule.run_pending()
            time.sleep(1)

    def stop_scheduler(self):
        """Stop the scheduler"""
        self.running = False
        logger.info("Scheduler stopped")

    def list_jobs(self):
        """List all configured jobs"""
        for name, config in self.jobs.items():
            print(f"Job: {name}")
            print(f"  URL: {config.url}")
            print(f"  Type: {config.scrape_type}")
            print(f"  Schedule: {config.schedule_interval}")
            print(f"  Output: {config.output_format}")
            print()


# Example usage and configuration
def create_example_configs():
    """Create example scraping configurations"""

    # Example 1: Static scraping of a news website
    news_config = ScrapeConfig(
        name="news_scraper",
        url="https://example-news.com",
        scrape_type="static",
        selectors={
            "article": "article.news-item",  # Container selector
            "title": "h2.title",
            "summary": "p.summary",
            "link": "a.read-more",
            "date": "span.date"
        },
        schedule_interval="1hour",
        output_format="csv",
        headers={"User-Agent": "Mozilla/5.0 (compatible; WebScraper/1.0)"}
    )

    # Example 2: Dynamic scraping of a product listing
    product_config = ScrapeConfig(
        name="product_scraper",
        url="https://example-store.com/products",
        scrape_type="dynamic",
        selectors={
            "product": "div.product-card",  # Container selector
            "name": "h3.product-name",
            "price": "span.price",
            "image": "img.product-image",
            "rating": "div.rating"
        },
        schedule_interval="30min",
        output_format="database",
        wait_for_element="div.product-card",
        timeout=30
    )

    return [news_config, product_config]


def main():
    """Main function to demonstrate the scraper"""

    # Create scheduler
    scheduler = WebScraperScheduler()

    # Add example jobs
    configs = create_example_configs()
    for config in configs:
        scheduler.add_job(config)

    # List all jobs
    print("Configured Jobs:")
    scheduler.list_jobs()

    # Run a job immediately for testing
    print("Running news_scraper job now...")
    scheduler.run_job_now("news_scraper")

    # Start the scheduler (commented out for demo)
    # scheduler.start_scheduler()


if __name__ == "__main__":
    main()