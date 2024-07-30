import json
from urllib.parse import urlparse, parse_qs
import csv
import logging
from tqdm import tqdm

# Path to your HAR file
har_file_path = "paths_data_scrapers/tradepoint/data/network_logs_all.har"
csv_file = 'paths_data_scrapers/tradepoint/data/all_tradepoint_category_codes.csv'

def har_parser():
    logging.info("Starting the HAR parser")

    all_codes = set()  # Use a set to automatically handle duplicates

    try:
        # Read the HAR File and parse it using JSON
        with open(har_file_path, "r", encoding="utf-8") as f:
            logs = json.load(f)
        logging.info("Successfully loaded HAR file")

        # Open the CSV file and write the header row if needed
        with open(csv_file, mode='w', newline='') as file:
            writer = csv.writer(file)
        logging.info("CSV file initialized with header row")

        # Iterate through the logs and extract the desired URLs and parameters
        for log in tqdm(logs, desc="Processing logs"):
            network_logs = log['log']['entries']
            for entry in network_logs:
                try:
                    url = entry['request']['url']
                    parsed_url = urlparse(url)
                    query_params = parse_qs(parsed_url.query)

                    # Find the entry with 'filter[category]' parameter
                    if 'filter[category]' in query_params:
                        value_param = query_params['filter[category]'][0]
                        logging.debug(f"URL: {url}, Value of 'filter[category]': {value_param}")
                        all_codes.add(value_param)

                except KeyError:
                    logging.warning(f"Missing expected keys in entry: {entry}")
                except Exception as e:
                    logging.error(f"An error occurred while processing entry: {e}")

        # Write the unique category codes to the CSV file
        with open(csv_file, mode='a', newline='') as file:
            writer = csv.writer(file)
            for code in tqdm(all_codes, desc="Writing codes to CSV"):
                writer.writerow([code])
        logging.info(f"Successfully wrote {len(all_codes)} unique category codes to the CSV file")

    except FileNotFoundError:
        logging.error(f"Error: HAR file '{har_file_path}' not found.")
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    har_parser()
