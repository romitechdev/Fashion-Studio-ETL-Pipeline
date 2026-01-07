"""
Unit tests for Transform module
"""

import pytest
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.transform import (
    clean_price,
    clean_rating,
    clean_colors,
    clean_size,
    clean_gender,
    transform_data
)


class TestCleanPrice:
    """Tests for clean_price function"""
    
    def test_valid_price(self):
        """Test cleaning valid price string"""
        result = clean_price("$50.00")
        assert result == 800000.0  # 50 * 16000
    
    def test_price_with_comma(self):
        """Test cleaning price with comma separator"""
        result = clean_price("$1,000.00")
        assert result == 16000000.0  # 1000 * 16000
    
    def test_unavailable_price(self):
        """Test handling unavailable price"""
        result = clean_price("Price Unavailable")
        assert result is None
    
    def test_empty_price(self):
        """Test handling empty price"""
        result = clean_price("")
        assert result is None
    
    def test_none_price(self):
        """Test handling None price"""
        result = clean_price(None)
        assert result is None


class TestCleanRating:
    """Tests for clean_rating function"""
    
    def test_valid_rating(self):
        """Test cleaning valid rating string"""
        result = clean_rating("⭐ 4.5 / 5")
        assert result == 4.5
    
    def test_rating_integer(self):
        """Test cleaning integer rating"""
        result = clean_rating("⭐ 4 / 5")
        assert result == 4.0
    
    def test_invalid_rating(self):
        """Test handling invalid rating"""
        result = clean_rating("Invalid Rating")
        assert result is None
    
    def test_empty_rating(self):
        """Test handling empty rating"""
        result = clean_rating("")
        assert result is None
    
    def test_none_rating(self):
        """Test handling None rating"""
        result = clean_rating(None)
        assert result is None


class TestCleanColors:
    """Tests for clean_colors function"""
    
    def test_valid_colors(self):
        """Test cleaning valid colors string"""
        result = clean_colors("3 Colors")
        assert result == 3
    
    def test_colors_various_formats(self):
        """Test cleaning colors with different formats"""
        assert clean_colors("5 Colors") == 5
        assert clean_colors("10 colors") == 10
    
    def test_empty_colors(self):
        """Test handling empty colors"""
        result = clean_colors("")
        assert result is None
    
    def test_none_colors(self):
        """Test handling None colors"""
        result = clean_colors(None)
        assert result is None


class TestCleanSize:
    """Tests for clean_size function"""
    
    def test_valid_size(self):
        """Test cleaning valid size string"""
        result = clean_size("Size: M")
        assert result == "M"
    
    def test_size_various_values(self):
        """Test cleaning various size values"""
        assert clean_size("Size: S") == "S"
        assert clean_size("Size: L") == "L"
        assert clean_size("Size: XL") == "XL"
        assert clean_size("Size: XXL") == "XXL"
    
    def test_size_without_prefix(self):
        """Test size without prefix"""
        result = clean_size("M")
        assert result == "M"
    
    def test_empty_size(self):
        """Test handling empty size"""
        result = clean_size("")
        assert result is None
    
    def test_none_size(self):
        """Test handling None size"""
        result = clean_size(None)
        assert result is None


class TestCleanGender:
    """Tests for clean_gender function"""
    
    def test_valid_gender(self):
        """Test cleaning valid gender string"""
        result = clean_gender("Gender: Men")
        assert result == "Men"
    
    def test_gender_various_values(self):
        """Test cleaning various gender values"""
        assert clean_gender("Gender: Women") == "Women"
        assert clean_gender("Gender: Unisex") == "Unisex"
    
    def test_gender_without_prefix(self):
        """Test gender without prefix"""
        result = clean_gender("Women")
        assert result == "Women"
    
    def test_empty_gender(self):
        """Test handling empty gender"""
        result = clean_gender("")
        assert result is None
    
    def test_none_gender(self):
        """Test handling None gender"""
        result = clean_gender(None)
        assert result is None


class TestTransformData:
    """Tests for transform_data function"""
    
    def test_transform_valid_data(self):
        """Test transforming valid data"""
        raw_data = [
            {
                "title": "T-shirt 1",
                "price": "$50.00",
                "rating": "⭐ 4.5 / 5",
                "colors": "3 Colors",
                "size": "Size: M",
                "gender": "Gender: Men"
            }
        ]
        
        df = transform_data(raw_data)
        
        assert len(df) == 1
        assert df.iloc[0]['title'] == 'T-shirt 1'
        assert df.iloc[0]['price'] == 800000.0
        assert df.iloc[0]['rating'] == 4.5
        assert df.iloc[0]['colors'] == 3
        assert df.iloc[0]['size'] == 'M'
        assert df.iloc[0]['gender'] == 'Men'
        assert 'timestamp' in df.columns
    
    def test_filter_unknown_product(self):
        """Test that Unknown Product is filtered out"""
        raw_data = [
            {
                "title": "Unknown Product",
                "price": "$50.00",
                "rating": "⭐ 4.5 / 5",
                "colors": "3 Colors",
                "size": "Size: M",
                "gender": "Gender: Men"
            },
            {
                "title": "T-shirt 1",
                "price": "$50.00",
                "rating": "⭐ 4.5 / 5",
                "colors": "3 Colors",
                "size": "Size: M",
                "gender": "Gender: Men"
            }
        ]
        
        df = transform_data(raw_data)
        
        assert len(df) == 1
        assert df.iloc[0]['title'] == 'T-shirt 1'
    
    def test_remove_duplicates(self):
        """Test that duplicates are removed"""
        raw_data = [
            {
                "title": "T-shirt 1",
                "price": "$50.00",
                "rating": "⭐ 4.5 / 5",
                "colors": "3 Colors",
                "size": "Size: M",
                "gender": "Gender: Men"
            },
            {
                "title": "T-shirt 1",
                "price": "$50.00",
                "rating": "⭐ 4.5 / 5",
                "colors": "3 Colors",
                "size": "Size: M",
                "gender": "Gender: Men"
            }
        ]
        
        df = transform_data(raw_data)
        
        assert len(df) == 1
    
    def test_remove_null_values(self):
        """Test that rows with null values are removed"""
        raw_data = [
            {
                "title": "T-shirt 1",
                "price": "Price Unavailable",  # Will become None
                "rating": "⭐ 4.5 / 5",
                "colors": "3 Colors",
                "size": "Size: M",
                "gender": "Gender: Men"
            },
            {
                "title": "T-shirt 2",
                "price": "$50.00",
                "rating": "⭐ 4.5 / 5",
                "colors": "3 Colors",
                "size": "Size: M",
                "gender": "Gender: Men"
            }
        ]
        
        df = transform_data(raw_data)
        
        assert len(df) == 1
        assert df.iloc[0]['title'] == 'T-shirt 2'
    
    def test_transform_empty_data(self):
        """Test transforming empty data"""
        df = transform_data([])
        
        assert df.empty
    
    def test_timestamp_added(self):
        """Test that timestamp column is added"""
        raw_data = [
            {
                "title": "T-shirt 1",
                "price": "$50.00",
                "rating": "⭐ 4.5 / 5",
                "colors": "3 Colors",
                "size": "Size: M",
                "gender": "Gender: Men"
            }
        ]
        
        df = transform_data(raw_data)
        
        assert 'timestamp' in df.columns
        assert df.iloc[0]['timestamp'] is not None


class TestEdgeCases:
    """Additional edge case tests for better coverage"""
    
    def test_clean_price_no_match(self):
        """Test cleaning price with no numeric value"""
        result = clean_price("No price here")
        assert result is None
    
    def test_clean_rating_out_of_range(self):
        """Test cleaning rating that is out of 0-5 range without / 5 format"""
        result = clean_rating("Rating: 10")
        assert result is None
    
    def test_clean_rating_just_number(self):
        """Test cleaning rating with just a number in valid range"""
        result = clean_rating("4.2")
        assert result == 4.2
    
    def test_clean_colors_no_number(self):
        """Test cleaning colors with no number"""
        result = clean_colors("Many Colors")
        assert result is None
    
    def test_clean_size_only_spaces(self):
        """Test cleaning size with only spaces"""
        result = clean_size("   ")
        assert result is None
    
    def test_clean_gender_only_spaces(self):
        """Test cleaning gender with only spaces"""
        result = clean_gender("   ")
        assert result is None
    
    def test_transform_filter_invalid_rating(self):
        """Test that Invalid Rating products are removed after cleaning"""
        raw_data = [
            {
                "title": "Product 1",
                "price": "$50.00",
                "rating": "Invalid Rating",  # Will become None
                "colors": "3 Colors",
                "size": "Size: M",
                "gender": "Gender: Men"
            },
            {
                "title": "Product 2",
                "price": "$50.00",
                "rating": "⭐ 4.5 / 5",
                "colors": "3 Colors",
                "size": "Size: M",
                "gender": "Gender: Men"
            }
        ]
        
        df = transform_data(raw_data)
        
        assert len(df) == 1
        assert df.iloc[0]['title'] == 'Product 2'
    
    def test_transform_multiple_invalid_fields(self):
        """Test transform with multiple invalid fields"""
        raw_data = [
            {
                "title": "Product 1",
                "price": "",
                "rating": "",
                "colors": "",
                "size": "",
                "gender": ""
            }
        ]
        
        df = transform_data(raw_data)
        
        assert len(df) == 0  # Should be empty after removing nulls
    
    def test_clean_price_only_cents(self):
        """Test price with only cents value"""
        result = clean_price("$0.99")
        assert result == 0.99 * 16000
