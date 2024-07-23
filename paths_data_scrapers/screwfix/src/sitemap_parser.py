import requests
import xml.etree.ElementTree as ET
import csv
import logging

# URL of the XML sitemap
sitemap_url = 'https://www.screwfix.com/sitemap-en-gb.xml'

def sitemap_parser():
    try:
        logging.info("Starting the sitemap parser")

        # Download the XML file
        response = requests.get(sitemap_url)
        response.raise_for_status()  # Check if the request was successful
        logging.info("Downloaded the sitemap XML successfully")

        # Parse the XML content
        tree = ET.ElementTree(ET.fromstring(response.content))
        root = tree.getroot()

        # Extract all <loc> URLs and clean them
        # The namespace might need to be handled if present
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        urls = [url.text.strip().replace('"', '') for url in root.findall('.//ns:loc', namespace)]

        # Save cleaned URLs to a CSV file
        csv_file = '/home/wlodzimierrr/Desktop/code/paths_data_scrapers/screwfix/data/sitemap_urls.csv'
        with open(csv_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            for url in urls:
                writer.writerow([url])
        logging.info(f"Saved {len(urls)} cleaned URLs to {csv_file}")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
    except ET.ParseError as e:
        logging.error(f"Failed to parse XML: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    sitemap_parser()
