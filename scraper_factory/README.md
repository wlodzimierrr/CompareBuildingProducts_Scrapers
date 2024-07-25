scraper_factory/
│
├── src/                          # Source files
│   ├── __init__.py               # Makes src a Python package
│   ├── .env                      # Environment variables
│   ├── main.py                   # Main application entry point
│   ├── config.py                 # Centralized configuration management
│   ├── email_utils.py            # Email functionalities
│   ├── db_utils.py               # Database interaction utilities
│   ├── agolia_utils.py           # Algolia search utilities
│   ├── lambda_ec2_stop.py        # AWS Lambda function to stop EC2
│   ├── tradepoint/               # Tradepoint specific module
│   │   ├── __init__.py           # Makes tradepoint a Python package
│   │   ├── tradepoint.py         # Tradepoint specific functions
│   │   └── cookies_headers.py    # Cookies and headers for Tradepoint
│   │
│   ├── screwfix/                 # Screwfix specific module
│   │   ├── __init__.py           # Makes screwfix a Python package
│   │   ├── screwfix.py           # Screwfix specific functions
│   │   └── cookies_headers.py    # Cookies and headers for Screwfix
│   │
│   └── screwfix/                 # Wickes specific module
│       ├── __init__.py           # Makes wickes a Python package
│       └── wickes.py             # Wickes specific functions
│ 
│
├── tests/                        # Test modules (maybe coming soon)
│   ├── __init__.py
│   ├── test_tradepoint.py        # Tests for Tradepoint
│   └── test_screwfix.py          # Tests for Screwfix
│
│
├── Dockerfile                    # Docker configuration
└── requirements.txt              # Project dependencies
