"""
Extract module for Fashion Studio ETL Pipeline
Scrapes product data from Fashion Studio website
"""

import requests
from bs4 import BeautifulSoup
import time
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://fashion-studio.dicoding.dev"


def extract_product_from_card(card) -> dict:
    """
    Extract product data from a single product card HTML element.
    
    Args:
        card: BeautifulSoup element representing a product card
        
    Returns:
        dict: Product data with keys: title, price, rating, colors, size, gender
    """
    try:
        # Extract title from h3.product-title
        title_elem = card.select_one("h3.product-title")
        title = title_elem.get_text(strip=True) if title_elem else "Unknown Product"
        
        # Extract price from span.price
        price_elem = card.select_one("span.price")
        price = price_elem.get_text(strip=True) if price_elem else "Price Unavailable"
        
        # Extract other details from <p> tags
        p_tags = card.select("p")
        
        rating = ""
        colors = ""
        size = ""
        gender = ""
        
        for p in p_tags:
            text = p.get_text(strip=True)
            text_lower = text.lower()
            if "rating" in text_lower or "⭐" in text:
                rating = text
            elif "color" in text_lower:
                colors = text
            elif "size" in text_lower:
                size = text
            elif "gender" in text_lower:
                gender = text
        
        return {
            "title": title,
            "price": price,
            "rating": rating,
            "colors": colors,
            "size": size,
            "gender": gender
        }
    except Exception as e:
        logger.error(f"Error extracting product from card: {e}")
        return None


def extract_products_from_page(html_content: str) -> list:
    """
    Extract all products from a page's HTML content.
    
    Args:
        html_content: Raw HTML string of the page
        
    Returns:
        list: List of product dictionaries
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    products = []
    
    # Find all product cards using the correct class
    cards = soup.select("div.collection-card")
    
    for card in cards:
        product = extract_product_from_card(card)
        if product:
            products.append(product)
    
    return products


def fetch_page(url: str, retries: int = 3, timeout: int = 10) -> str:
    """
    Fetch a page with retry logic and error handling.
    
    Args:
        url: URL to fetch
        retries: Number of retry attempts
        timeout: Request timeout in seconds
        
    Returns:
        str: HTML content of the page
        
    Raises:
        Exception: If all retries fail
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt + 1} for {url}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise Exception(f"Failed to fetch {url} after {retries} attempts: Timeout")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error on attempt {attempt + 1} for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise Exception(f"Failed to fetch {url} after {retries} attempts: {e}")
    
    return ""


def extract_all_pages(base_url: str = BASE_URL, max_pages: int = 50) -> list:
    """
    Extract products from all pages of the Fashion Studio website.
    
    Args:
        base_url: Base URL of the website
        max_pages: Maximum number of pages to scrape
        
    Returns:
        list: List of all product dictionaries from all pages
    """
    all_products = []
    
    for page_num in range(1, max_pages + 1):
        try:
            # Construct URL
            if page_num == 1:
                url = base_url
            else:
                url = f"{base_url}/page{page_num}"
            
            logger.info(f"Scraping page {page_num}: {url}")
            
            # Fetch and parse page
            html_content = fetch_page(url)
            products = extract_products_from_page(html_content)
            
            all_products.extend(products)
            logger.info(f"Extracted {len(products)} products from page {page_num}")
            
            # Be polite to the server
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {e}")
            continue
    
    logger.info(f"Total products extracted: {len(all_products)}")
    return all_products


if __name__ == "__main__":
    # Test extraction
    products = extract_all_pages(max_pages=2)
    for p in products[:5]:
        print(p)
