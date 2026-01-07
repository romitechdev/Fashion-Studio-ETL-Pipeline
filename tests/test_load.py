"""
Unit tests for Load module
"""

import pytest
import pandas as pd
import os
import tempfile
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.load import load_to_csv


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing"""
    return pd.DataFrame({
        'title': ['T-shirt 1', 'Hoodie 2'],
        'price': [800000.0, 1200000.0],
        'rating': [4.5, 4.2],
        'colors': [3, 5],
        'size': ['M', 'L'],
        'gender': ['Men', 'Women'],
        'timestamp': ['2024-01-01 12:00:00', '2024-01-01 12:00:00']
    })


@pytest.fixture
def empty_df():
    """Create an empty DataFrame for testing"""
    return pd.DataFrame()


class TestLoadToCsv:
    """Tests for load_to_csv function"""
    
    def test_save_to_csv_success(self, sample_df):
        """Test successful CSV save"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            filepath = f.name
        
        try:
            result = load_to_csv(sample_df, filepath)
            
            assert result is True
            assert os.path.exists(filepath)
            
            # Verify content
            saved_df = pd.read_csv(filepath)
            assert len(saved_df) == 2
            assert 'title' in saved_df.columns
        finally:
            if os.path.exists(filepath):
                os.unlink(filepath)
    
    def test_save_empty_df_to_csv(self, empty_df):
        """Test saving empty DataFrame to CSV"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            filepath = f.name
        
        try:
            result = load_to_csv(empty_df, filepath)
            assert result is False
        finally:
            if os.path.exists(filepath):
                os.unlink(filepath)
    
    def test_csv_contains_all_columns(self, sample_df):
        """Test that CSV contains all expected columns"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            filepath = f.name
        
        try:
            load_to_csv(sample_df, filepath)
            saved_df = pd.read_csv(filepath)
            
            expected_columns = ['title', 'price', 'rating', 'colors', 'size', 'gender', 'timestamp']
            for col in expected_columns:
                assert col in saved_df.columns
        finally:
            if os.path.exists(filepath):
                os.unlink(filepath)
    
    def test_csv_data_integrity(self, sample_df):
        """Test that data integrity is maintained in CSV save/load cycle"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            filepath = f.name
        
        try:
            load_to_csv(sample_df, filepath)
            loaded_df = pd.read_csv(filepath)
            
            # Check data integrity
            assert loaded_df['title'].tolist() == sample_df['title'].tolist()
            assert loaded_df['price'].tolist() == sample_df['price'].tolist()
            assert loaded_df['rating'].tolist() == sample_df['rating'].tolist()
        finally:
            if os.path.exists(filepath):
                os.unlink(filepath)
    
    def test_csv_price_values(self, sample_df):
        """Test that price values are correctly saved"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            filepath = f.name
        
        try:
            load_to_csv(sample_df, filepath)
            loaded_df = pd.read_csv(filepath)
            
            # Verify price is in IDR (large numbers)
            assert all(loaded_df['price'] > 10000)
        finally:
            if os.path.exists(filepath):
                os.unlink(filepath)
