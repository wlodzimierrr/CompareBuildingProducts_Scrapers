import json
import csv
import time
import logging
from selenium import webdriver 
from browsermobproxy import Server
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from chromedriver_py import binary_path 
from tqdm import tqdm

# Function to wait for an element to be present
def wait_for_element_presence(driver, by, selector, timeout=10):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, selector))
    )

def network_log_collector():
    logging.info("Starting the network log collector")

    # Main Function 
    path_to_browsermobproxy = "/home/wlodzimierrr/tools/browsermob-proxy-2.1.4/bin/"
    server = Server(path_to_browsermobproxy + "browsermob-proxy", options={'port': 8090})
    server.start()
    logging.info("BrowserMob Proxy server started")

    proxy = server.create_proxy(params={"trustAllServers": "true"})

    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--proxy-server={0}".format(proxy.proxy))

    service = Service(executable_path=binary_path)
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # Initialize an empty list to store HAR entries
        network_logs = []

        # Read URLs from CSV file
        with open('/home/wlodzimierrr/Desktop/code/paths_data_scrapers/tradepoint/data/sitemap_urls.csv', 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            urls = [row[0] for row in reader]

        logging.info(f"Found {len(urls)} URLs in the sitemap CSV file")

        # Iterate over each URL from the sitemap with tqdm progress bar
        for url in tqdm(urls, desc="Processing URLs"):
            time.sleep(10)  # Sleep for 10 seconds between each URL to mimic user behavior
            logging.info(f"Navigating to: {url}")

            # Start capturing HAR data for the current URL
            proxy.new_har(url)

            # Load the URL in the browser
            driver.get(url)

            # Check if the "Load More" button exists
            try:
                load_more_element = wait_for_element_presence(driver, By.CSS_SELECTOR, '[data-test-id="load-more-products"]')

                # Scroll into view if necessary
                actions = ActionChains(driver)
                actions.move_to_element(load_more_element).perform()
                time.sleep(1)

                # Click the "Load More" button using JavaScript to bypass interception
                driver.execute_script("arguments[0].click();", load_more_element)

                # Wait for additional content to load
                time.sleep(5)
                logging.info("'Load More' button clicked and waiting for content to load")

            except TimeoutException:
                # Handle the case where "Load More" button doesn't exist
                logging.info("No 'Load More' button found. Proceeding with the current page content.")

            # Capture the HAR data after the page has fully loaded
            har_entry = proxy.har

            # Append the HAR entry to the list
            network_logs.append(har_entry)

        # Write the list of HAR entries to a file
        with open("/home/wlodzimierrr/Desktop/code/paths_data_scrapers/tradepoint/data/network_logs_all.har", "w", encoding="utf-8") as f:
            json.dump(network_logs, f, ensure_ascii=False)
        logging.info("Network logs saved successfully")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

    finally:
        driver.quit()
        server.stop()
        logging.info("Browser and BrowserMob Proxy server stopped")

if __name__ == "__main__":
    network_log_collector()
