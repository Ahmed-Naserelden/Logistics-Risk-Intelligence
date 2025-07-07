#!/bin/bash

# this cron job script for vessels web scrapping

echo "Current directory: $(pwd)"

echo "Running Logistic Risk Intelligence Web Scrapping scripts..."
echo "$VESSELS_SCRAPPER_CODE"
VESSELS_SCRAPPER_CODE="/extraction/scripts/vessels_scrapper/scraper/vessel-scrapper.py"
/usr/local/bin/python3 "$VESSELS_SCRAPPER_CODE"

echo "Web scrapping completed."

