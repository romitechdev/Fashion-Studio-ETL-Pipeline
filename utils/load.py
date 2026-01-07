"""
Load module for Fashion Studio ETL Pipeline
Saves transformed data to CSV
"""

import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_to_csv(df: pd.DataFrame, filepath: str = "products.csv") -> bool:
    """
    Save DataFrame to CSV file.
    
    Args:
        df: DataFrame to save
        filepath: Path to output CSV file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if df.empty:
            logger.warning("Cannot save empty DataFrame to CSV")
            return False
        
        df.to_csv(filepath, index=False)
        logger.info(f"Data saved to CSV: {filepath} ({len(df)} rows)")
        return True
        
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        return False


if __name__ == "__main__":
    # Test with sample data
    sample_df = pd.DataFrame({
        'title': ['T-shirt 1', 'Hoodie 2'],
        'price': [800000.0, 1200000.0],
        'rating': [4.5, 4.2],
        'colors': [3, 5],
        'size': ['M', 'L'],
        'gender': ['Men', 'Women'],
        'timestamp': ['2024-01-01 12:00:00', '2024-01-01 12:00:00']
    })
    
    # Test CSV
    load_to_csv(sample_df, "test_products.csv")
