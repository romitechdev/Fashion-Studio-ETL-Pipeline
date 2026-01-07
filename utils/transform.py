"""
Transform module for Fashion Studio ETL Pipeline
Cleans and transforms raw product data
"""

import pandas as pd
import re
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Currency conversion rate
USD_TO_IDR = 16000


def clean_price(price_str: str) -> float:
    """
    Clean price string and convert from USD to IDR.
    
    Args:
        price_str: Raw price string (e.g., "$123.45" or "Price Unavailable")
        
    Returns:
        float: Price in IDR, or None if invalid
    """
    try:
        if not price_str or "unavailable" in price_str.lower():
            return None
        
        # Remove currency symbols and extract number
        price_match = re.search(r'[\d,]+\.?\d*', price_str.replace(',', ''))
        if price_match:
            price_usd = float(price_match.group())
            return price_usd * USD_TO_IDR
        return None
    except Exception as e:
        logger.warning(f"Error cleaning price '{price_str}': {e}")
        return None


def clean_rating(rating_str: str) -> float:
    """
    Clean rating string and extract float value.
    
    Args:
        rating_str: Raw rating string (e.g., "⭐ 4.5 / 5" or "Invalid Rating")
        
    Returns:
        float: Rating value, or None if invalid
    """
    try:
        if not rating_str or "invalid" in rating_str.lower():
            return None
        
        # Extract number from rating string
        rating_match = re.search(r'(\d+\.?\d*)\s*/\s*5', rating_str)
        if rating_match:
            return float(rating_match.group(1))
        
        # Try to find any float number
        rating_match = re.search(r'(\d+\.?\d*)', rating_str)
        if rating_match:
            rating = float(rating_match.group(1))
            if 0 <= rating <= 5:
                return rating
        return None
    except Exception as e:
        logger.warning(f"Error cleaning rating '{rating_str}': {e}")
        return None


def clean_colors(colors_str: str) -> int:
    """
    Extract number of colors from colors string.
    
    Args:
        colors_str: Raw colors string (e.g., "3 Colors")
        
    Returns:
        int: Number of colors, or None if invalid
    """
    try:
        if not colors_str:
            return None
        
        # Extract number
        colors_match = re.search(r'(\d+)', colors_str)
        if colors_match:
            return int(colors_match.group(1))
        return None
    except Exception as e:
        logger.warning(f"Error cleaning colors '{colors_str}': {e}")
        return None


def clean_size(size_str: str) -> str:
    """
    Clean size string by removing "Size: " prefix.
    
    Args:
        size_str: Raw size string (e.g., "Size: M")
        
    Returns:
        str: Clean size value
    """
    try:
        if not size_str:
            return None
        
        # Remove "Size: " prefix
        cleaned = re.sub(r'^Size:\s*', '', size_str, flags=re.IGNORECASE)
        return cleaned.strip() if cleaned.strip() else None
    except Exception as e:
        logger.warning(f"Error cleaning size '{size_str}': {e}")
        return None


def clean_gender(gender_str: str) -> str:
    """
    Clean gender string by removing "Gender: " prefix.
    
    Args:
        gender_str: Raw gender string (e.g., "Gender: Men")
        
    Returns:
        str: Clean gender value
    """
    try:
        if not gender_str:
            return None
        
        # Remove "Gender: " prefix
        cleaned = re.sub(r'^Gender:\s*', '', gender_str, flags=re.IGNORECASE)
        return cleaned.strip() if cleaned.strip() else None
    except Exception as e:
        logger.warning(f"Error cleaning gender '{gender_str}': {e}")
        return None


def transform_data(raw_data: list) -> pd.DataFrame:
    """
    Transform raw product data into a clean DataFrame.
    
    Args:
        raw_data: List of dictionaries with raw product data
        
    Returns:
        pd.DataFrame: Cleaned and transformed data
    """
    try:
        if not raw_data:
            logger.warning("No data to transform")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(raw_data)
        logger.info(f"Initial data: {len(df)} rows")
        
        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates()
        logger.info(f"After removing duplicates: {len(df)} rows (removed {initial_count - len(df)})")
        
        # Filter out "Unknown Product"
        initial_count = len(df)
        df = df[df['title'].str.lower() != 'unknown product']
        logger.info(f"After filtering Unknown Product: {len(df)} rows (removed {initial_count - len(df)})")
        
        # Clean price (convert to IDR)
        df['price'] = df['price'].apply(clean_price)
        
        # Clean rating
        df['rating'] = df['rating'].apply(clean_rating)
        
        # Clean colors
        df['colors'] = df['colors'].apply(clean_colors)
        
        # Clean size
        df['size'] = df['size'].apply(clean_size)
        
        # Clean gender
        df['gender'] = df['gender'].apply(clean_gender)
        
        # Remove rows with null values
        initial_count = len(df)
        df = df.dropna()
        logger.info(f"After removing null values: {len(df)} rows (removed {initial_count - len(df)})")
        
        # Add timestamp column
        df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Reset index
        df = df.reset_index(drop=True)
        
        logger.info(f"Final transformed data: {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise


if __name__ == "__main__":
    # Test transformation with sample data
    sample_data = [
        {
            "title": "T-shirt 1",
            "price": "$50.00",
            "rating": "⭐ 4.5 / 5",
            "colors": "3 Colors",
            "size": "Size: M",
            "gender": "Gender: Men"
        },
        {
            "title": "Unknown Product",
            "price": "Price Unavailable",
            "rating": "Invalid Rating",
            "colors": "5 Colors",
            "size": "Size: L",
            "gender": "Gender: Women"
        }
    ]
    
    df = transform_data(sample_data)
    print(df)
