import requests
import json
from datetime import datetime
import pandas as pd

def fetch_dividend_information(isin, x_client_traceid, client_date):
    """
    Fetches dividend information from the Börse Frankfurt API for a given ISIN.
    """
    url = "https://api.boerse-frankfurt.de/v1/data/dividend_information"

    params = {
        "isin": isin,
        "limit": 5
    }

    headers = {
        "client-date": client_date,
        "x-client-traceid": x_client_traceid
    }

    try:
        response = requests.get(url, params=params, headers=headers)

        if response.status_code == 200:
            try:
                data = response.json() if response.text else {}
            except json.JSONDecodeError:
                print("Failed to parse JSON response.")
                return response.status_code, None

            print(f"Status Code: {response.status_code}")

            if not data:
                print("Response received with status 200 but no data (empty response)")
            else:
                print(f"Response received with Data")

            return response.status_code, data
        else:
            print(f"Request failed with status code: {response.status_code}")
            return response.status_code, None

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None, None


def process_dividend_data(data):
    """
    Processes the dividend data and saves it to a CSV file.
    """
    try:
        records = data.get('data')
        dividend_data = []

        for single_record in records:
            try:
                dividend_data.append(
                    {
                        "Last_dividend_payment": single_record.get('dividendLastPayment'),
                        "Dividend_cycle": single_record.get('dividendCycle', {}).get('translations', {}).get('en'),
                        "Value": f"{single_record.get('dividendValue')} {single_record.get('dividendCurrency')}",
                        "ISIN": single_record.get('dividendIsin')
                    }
                )
            except Exception as e:
                print(f"Error processing record: {e}")
                continue

        if dividend_data:
            df = pd.DataFrame(dividend_data)
            df.to_csv("dividend_data.csv", index=False)
            print("Dividend data saved to 'dividend_data.csv'")
        else:
            print("No dividend data found to save.")
    except Exception as e:
        print(f"Error processing dividend data: {e}")


# Main execution
if __name__ == "__main__":
    # Example usage
    status_code, data = fetch_dividend_information(
        "DE000A1EWWW0",
        "cbc69252e555cafbfea9501408258299",
        "2025-05-04T15:01:53.823Z"
    )

    print("\nSummary:")
    print(f"- Status code: {status_code}")

    if data == {} or data is None:
        print("Please provide provide cookies data.")
    else:
        # Call the function to process and save the dividend data
        process_dividend_data(data)
