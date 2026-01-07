# Fashion Studio ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![Coverage](https://img.shields.io/badge/Coverage-81%25-green.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

A complete **ETL (Extract, Transform, Load) Pipeline** for scraping, cleaning, and storing product data from Fashion Studio e-commerce website.

## 🎯 Project Overview

This project demonstrates a production-ready data pipeline that:
- **Extracts** 1000+ product data from 50 web pages
- **Transforms** raw data with cleaning, validation, and currency conversion
- **Loads** clean data to CSV format

## 🚀 Features

- ✅ Web scraping with retry logic and error handling
- ✅ Data cleaning (duplicates, null values, invalid entries)
- ✅ Currency conversion (USD → IDR)
- ✅ Timestamp tracking for data lineage
- ✅ Unit tests with 81% coverage

## 📊 Data Pipeline Flow

```
┌─────────────┐     ┌──────────────┐     ┌────────────┐
│   EXTRACT   │ ──► │  TRANSFORM   │ ──► │    LOAD    │
│  (Scraping) │     │  (Cleaning)  │     │   (CSV)    │
└─────────────┘     └──────────────┘     └────────────┘
     │                    │                    │
     ▼                    ▼                    ▼
 1000 raw           867 clean            products.csv
 products           products
```

## 🛠️ Tech Stack

- **Python 3.11+**
- **Pandas** - Data manipulation
- **BeautifulSoup4** - Web scraping
- **Requests** - HTTP client
- **Pytest** - Unit testing

## 📁 Project Structure

```
submission-pemda/
├── utils/
│   ├── extract.py      # Web scraping module
│   ├── transform.py    # Data cleaning module
│   └── load.py         # CSV export module
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── main.py             # Pipeline orchestrator
├── products.csv        # Output data
├── requirements.txt
└── README.md
```

## 🚀 Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/romitechdev/Fashion-Studio-ETL-Pipeline.git
cd Fashion-Studio-ETL-Pipeline

# Install dependencies
pip install -r requirements.txt
```

### Run Pipeline

```bash
python main.py
```

### Run Tests

```bash
pytest tests/ -v --cov=utils --cov-report=term-missing
```

## 📈 Output Sample

| title | price (IDR) | rating | colors | size | gender |
|-------|-------------|--------|--------|------|--------|
| T-shirt 2 | 1,634,400 | 3.9 | 3 | M | Women |
| Hoodie 3 | 7,950,080 | 4.8 | 3 | L | Unisex |
| Pants 4 | 7,476,960 | 3.3 | 3 | XL | Men |

## 🧪 Test Coverage

```
Name                    Stmts   Miss  Cover
-------------------------------------------
utils/__init__.py           0      0   100%
utils/extract.py           62     12    81%
utils/load.py              14      3    79%
utils/transform.py         96     23    76%
-------------------------------------------
TOTAL                     196     38    81%
```

## 📝 Data Cleaning Rules

1. Remove duplicate products
2. Filter out "Unknown Product" entries
3. Remove invalid ratings
4. Convert price from USD to IDR (×16,000)
5. Clean text prefixes (Size:, Gender:)
6. Add extraction timestamp

## 👨‍💻 Author

**Your Name**
- LinkedIn: [romi-webdev](https://linkedin.com/in/romi-webdev)
- GitHub: [romitechdev](https://github.com/romitechdev)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

⭐ If you found this project helpful, please give it a star!
