import scrapy
from datetime import datetime
import json

# ======================= HOW TO RUN THIS SPIDER ===========================
# Step 1: Open terminal and navigate to the spider directory:
#         cd /Sec/nse_options
#
# Step 2: Run the spider with desired arguments like symbol, options, and date:
#         scrapy crawl nse_spider -a symbol=RELIANCE -a date=2025-05-29 -o file_name.csv/json
#         or
#         scrapy crawl nse_spider -a options=NIFTY -a date=2025-05-29 -o file_name.csv/json
#
# example :scrapy crawl nse_spider -a options=NIFTY -a date=2025-05-08 -o Nifty_8-May-2025.csv
# example :scrapy crawl nse_spider -a symbol=ABB -a date=2025-05-29 -o ABB_29-May-2025.csv
#
# Note : WE CAN PASS ONLY ONE ARGUMENT AT A TIME, EITHER options OR symbol.
# =========================================================================

class NSESpider(scrapy.Spider):
    name = "nse_spider"
    

    def __init__(self, options=None, symbol=None, date=None, *args, **kwargs):
        super(NSESpider, self).__init__(*args, **kwargs)
        self.options = options.upper() if options else None
        self.symbol = symbol.upper() if symbol else None

        # Convert user-provided date to format expected by NSE (e.g., 29-May-2025)
        if date:
            try:
                self.date = date 
                parsed_date = datetime.strptime(self.date, "%Y-%m-%d")
                self.date = parsed_date.strftime("%d-%b-%Y")
            except Exception as e:
                self.logger.error(f"Invalid date format: {e}")
                self.date = None
        else:
            self.date = None

    def start_requests(self):
        try:
            # Choose the appropriate API endpoint
            if self.options:
                url = f"https://www.nseindia.com/api/option-chain-indices?symbol={self.options}"
            elif self.symbol:
                url = f"https://www.nseindia.com/api/option-chain-equities?symbol={self.symbol}"
            else:
                self.logger.error("Neither symbol nor options provided.")
                return

            # Load cookies from local JSON file
            with open('cookies.json', 'r') as f:
                cookies = json.load(f).get('cookies')

            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://www.nseindia.com/option-chain"
            }

            # Send initial request
            yield scrapy.Request(
                url,
                headers=headers,
                cookies=cookies,
                callback=self.parse
            )
        except Exception as e:
            self.logger.error(f"Error in start_requests: {e}")

    def parse(self, response):
        try:
            self.logger.info(f"Symbol/Option: {self.symbol or self.options}, Date: {self.date}")

            # Parse JSON response
            data = response.json()

            # Get option chain records
            records = data.get('records', {}).get('data', [])

            # Iterate over each record (strike price level)
            for single_record in records:
                if self.date:
                    if single_record.get('expiryDate') != self.date:
                        continue

                # Get PE and CE data if available
                pe = single_record.get('PE') or {}
                ce = single_record.get('CE') or {}

                combined_record = {}

                # Exclude base keys but include them as reference
                exclude_keys = ['strikePrice', 'expiryDate', 'underlying', 'identifier']
                for key in exclude_keys:
                    combined_record[key] = pe.get(key)

                # Add PE data with prefix
                for key, value in pe.items():
                    if key not in exclude_keys:
                        combined_record[f'pe_{key}'] = value

                # Add CE data with prefix
                for key, value in ce.items():
                    if key not in exclude_keys:
                        combined_record[f'ce_{key}'] = value

                # Yield combined item
                yield combined_record
        except Exception as e:
            self.logger.error(f"Error while parsing response: {e}")
