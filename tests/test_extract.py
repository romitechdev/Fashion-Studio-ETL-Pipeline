"""
Unit tests for Extract module
"""

import pytest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.extract import (
    extract_product_from_card,
    extract_products_from_page,
    fetch_page,
    extract_all_pages
)


class TestExtractProductFromCard:
    """Tests for extract_product_from_card function"""
    
    def test_extract_valid_product(self):
        """Test extraction from a valid product card"""
        from bs4 import BeautifulSoup
        
        # Mock HTML matching actual Fashion Studio structure
        html = '''
        <div class="collection-card">
            <h3 class="product-title">T-shirt 1</h3>
            <span class="price">$50.00</span>
            <p>Rating: ⭐ 4.5 / 5</p>
            <p>3 Colors</p>
            <p>Size: M</p>
            <p>Gender: Men</p>
        </div>
        '''
        soup = BeautifulSoup(html, 'html.parser')
        card = soup.find('div', class_='collection-card')
        
        result = extract_product_from_card(card)
        
        assert result is not None
        assert result['title'] == 'T-shirt 1'
        assert result['price'] == '$50.00'
        assert '4.5' in result['rating']
        assert '3' in result['colors']
        assert 'M' in result['size']
        assert 'Men' in result['gender']
    
    def test_extract_missing_elements(self):
        """Test extraction when some elements are missing"""
        from bs4 import BeautifulSoup
        
        html = '''
        <div class="collection-card">
            <h3 class="product-title">Hoodie 2</h3>
        </div>
        '''
        soup = BeautifulSoup(html, 'html.parser')
        card = soup.find('div', class_='collection-card')
        
        result = extract_product_from_card(card)
        
        assert result is not None
        assert result['title'] == 'Hoodie 2'


class TestExtractProductsFromPage:
    """Tests for extract_products_from_page function"""
    
    def test_extract_multiple_products(self):
        """Test extraction of multiple products from a page"""
        html = '''
        <html>
        <body>
            <div class="collection-card">
                <h3 class="product-title">Product 1</h3>
                <span class="price">$10.00</span>
                <p>Rating: ⭐ 4.0 / 5</p>
                <p>3 Colors</p>
                <p>Size: M</p>
                <p>Gender: Men</p>
            </div>
            <div class="collection-card">
                <h3 class="product-title">Product 2</h3>
                <span class="price">$20.00</span>
                <p>Rating: ⭐ 4.5 / 5</p>
                <p>5 Colors</p>
                <p>Size: L</p>
                <p>Gender: Women</p>
            </div>
        </body>
        </html>
        '''
        
        products = extract_products_from_page(html)
        
        assert len(products) == 2
        assert products[0]['title'] == 'Product 1'
        assert products[1]['title'] == 'Product 2'
    
    def test_extract_empty_page(self):
        """Test extraction from empty page"""
        html = '<html><body></body></html>'
        
        products = extract_products_from_page(html)
        
        assert isinstance(products, list)
        assert len(products) == 0


class TestFetchPage:
    """Tests for fetch_page function"""
    
    @patch('utils.extract.requests.get')
    def test_fetch_success(self, mock_get):
        """Test successful page fetch"""
        mock_response = MagicMock()
        mock_response.text = '<html>Test</html>'
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response
        
        result = fetch_page('http://example.com')
        
        assert result == '<html>Test</html>'
        mock_get.assert_called_once()
    
    @patch('utils.extract.requests.get')
    def test_fetch_with_retry(self, mock_get):
        """Test fetch with retry on failure"""
        import requests
        
        # First call fails, second succeeds
        mock_response = MagicMock()
        mock_response.text = '<html>Success</html>'
        mock_response.raise_for_status = MagicMock()
        
        mock_get.side_effect = [
            requests.exceptions.Timeout(),
            mock_response
        ]
        
        result = fetch_page('http://example.com', retries=3)
        
        assert result == '<html>Success</html>'
        assert mock_get.call_count == 2
    
    @patch('utils.extract.requests.get')
    def test_fetch_all_retries_fail(self, mock_get):
        """Test fetch when all retries fail"""
        import requests
        
        mock_get.side_effect = requests.exceptions.Timeout()
        
        with pytest.raises(Exception) as excinfo:
            fetch_page('http://example.com', retries=2)
        
        assert 'Failed to fetch' in str(excinfo.value)


class TestExtractAllPages:
    """Tests for extract_all_pages function"""
    
    @patch('utils.extract.fetch_page')
    @patch('utils.extract.extract_products_from_page')
    def test_extract_single_page(self, mock_extract, mock_fetch):
        """Test extraction from a single page"""
        mock_fetch.return_value = '<html>Test</html>'
        mock_extract.return_value = [{'title': 'Product 1'}]
        
        # Use low timeout to speed up test
        with patch('utils.extract.time.sleep'):
            products = extract_all_pages(max_pages=1)
        
        assert len(products) == 1
        assert products[0]['title'] == 'Product 1'
    
    @patch('utils.extract.fetch_page')
    @patch('utils.extract.extract_products_from_page')
    def test_extract_multiple_pages(self, mock_extract, mock_fetch):
        """Test extraction from multiple pages"""
        mock_fetch.return_value = '<html>Test</html>'
        mock_extract.return_value = [{'title': 'Product'}]
        
        with patch('utils.extract.time.sleep'):
            products = extract_all_pages(max_pages=3)
        
        assert len(products) == 3
    
    @patch('utils.extract.fetch_page')
    def test_extract_handles_page_error(self, mock_fetch):
        """Test that extraction continues when a page fails"""
        mock_fetch.side_effect = Exception("Network error")
        
        with patch('utils.extract.time.sleep'):
            products = extract_all_pages(max_pages=2)
        
        assert products == []  # Should return empty list, not crash
