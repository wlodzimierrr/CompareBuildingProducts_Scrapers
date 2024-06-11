scraper_factory/
│
├── src/                          # Source files
│   ├── __init__.py               # Makes src a Python package
|   |__ .env                      # env 
│   ├── main.py                   # Main application entry point
│   ├── config.py                 # Centralized configuration management
│   ├── email_utils.py            # Email functionalities
│   ├── db_utils.py               # Database interaction utilities
│   ├── api_utils.py              # API handling utilities
│   ├── tradepoint/               # Tradepoint specific module
│   │   ├── __init__.py           # Makes tradepoint a Python package
│   │   ├── tradepoint.py         # Tradepoint specific functions
│   │   └── cookies_headers.py    # Cookies and headers for Tradepoint
│   │
│   └── screwfix/                 # Screwfix specific module
│       ├── __init__.py           # Makes screwfix a Python package
│       ├── screwfix.py           # Screwfix specific functions
│       └── cookies_headers.py    # Cookies and headers for Screwfix
│
├── tests/                        # Test modules
│   ├── __init__.py
│   ├── test_tradepoint.py        # Tests for Tradepoint
│   └── test_screwfix.py          # Tests for Screwfix
│
├── docs/                         # Documentation
│   └── ...
│
├── scripts/                      # Scripts for maintenance and setup
│   └── ...
│
├── requirements.txt              # Project dependencies
├── README.md                     # Project overview
└── LICENSE                       # Licensing information