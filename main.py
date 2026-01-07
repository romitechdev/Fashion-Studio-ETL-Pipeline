"""
Fashion Studio ETL Pipeline
Main orchestrator for Extract, Transform, Load operations
"""

import logging
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.extract import extract_all_pages
from utils.transform import transform_data
from utils.load import load_to_csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://fashion-studio.dicoding.dev"
MAX_PAGES = 50
CSV_OUTPUT = "products.csv"


def run_etl_pipeline():
    """
    Run the complete ETL pipeline.
    
    Steps:
    1. Extract: Scrape data from Fashion Studio website
    2. Transform: Clean and transform the data
    3. Load: Save to CSV
    """
    logger.info("=" * 50)
    logger.info("Starting Fashion Studio ETL Pipeline")
    logger.info("=" * 50)
    
    # Step 1: Extract
    logger.info("\n[STEP 1] EXTRACT - Scraping data from website...")
    try:
        raw_data = extract_all_pages(BASE_URL, MAX_PAGES)
        logger.info(f"Extraction complete: {len(raw_data)} products extracted")
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        return False
    
    if not raw_data:
        logger.error("No data extracted. Aborting pipeline.")
        return False
    
    # Step 2: Transform
    logger.info("\n[STEP 2] TRANSFORM - Cleaning and transforming data...")
    try:
        df = transform_data(raw_data)
        logger.info(f"Transformation complete: {len(df)} products after cleaning")
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        return False
    
    if df.empty:
        logger.error("No data after transformation. Aborting pipeline.")
        return False
    
    # Step 3: Load to CSV
    logger.info("\n[STEP 3] LOAD - Saving data to CSV...")
    csv_success = load_to_csv(df, CSV_OUTPUT)
    if csv_success:
        logger.info(f"✓ CSV: Data saved to {CSV_OUTPUT}")
    else:
        logger.error("✗ CSV: Failed to save")
        return False
    
    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("ETL Pipeline completed!")
    logger.info(f"Total products processed: {len(df)}")
    logger.info(f"Output file: {CSV_OUTPUT}")
    logger.info("=" * 50)
    
    return True


if __name__ == "__main__":
    success = run_etl_pipeline()
    sys.exit(0 if success else 1)
