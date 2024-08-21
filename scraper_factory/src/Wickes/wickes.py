import requests
import json
import queue
import os
import sys
import logging
from bs4 import BeautifulSoup
from tqdm import tqdm
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_utils import paths_db_connection, storage_db_connection

class WickesScraper:
    def __init__(self, prometheus_metrics=None):
        self.error_log = []
        self.prometheus_metrics = prometheus_metrics

    def get_scraping_target_data(self):
        """Retrieve scraping target data from paths database."""
        try:
            conn = paths_db_connection()
            logging.info("Paths database connection successful")
            cursor = conn.cursor()
            cursor.execute("SELECT shop_id, page_url FROM wickes LIMIT 10")  
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return [{"shop_id": row[0], "page_url": row[1]} for row in data]
        except Exception as e:
            logging.error(f"Error connecting to the database: {e}")
            raise e

    def insert_scraped_data(self, product):
        """Insert scraped data into the storage database."""
        try:
            conn = storage_db_connection()
            logging.info("Storage database connection successful")
            cursor = conn.cursor()

            product_name = product['product_name']
            category = product['category']
            subcategory = product['subcategory']
            image_url = product['image_url']
            page_url = product['page_url']
            product_description = product['description']
            features = product.get('features', '')
            rating = product['rating']
            rating_count = product['rating_count']
            price = product['price']
            brand = product['brand_name']
            shop_id = product['shop_id']
            
            try:
                cursor.execute(
                    """
                    INSERT INTO products (
                        shop_id, category, subcategory, product_name, page_url, features, image_url, product_description, rating, rating_count, price, brand, created_at, last_checked_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (page_url) DO UPDATE SET
                        shop_id = EXCLUDED.shop_id,
                        category = EXCLUDED.category,
                        subcategory = EXCLUDED.subcategory,
                        product_name = EXCLUDED.product_name,
                        features = EXCLUDED.features,
                        image_url = EXCLUDED.image_url,
                        product_description = EXCLUDED.product_description,
                        rating = EXCLUDED.rating,
                        rating_count = EXCLUDED.rating_count,
                        price = EXCLUDED.price,
                        brand = EXCLUDED.brand,
                        last_checked_at = CURRENT_TIMESTAMP;
                    """,
                    (shop_id, category, subcategory, product_name, page_url, features, image_url, product_description, rating, rating_count, price, brand)
                )
            except Exception as e:
                logging.error(f"Error inserting product {page_url}: {e}")
                self.error_log.append({
                    "Page url": page_url,
                    "error": 'Inserting error'
                })
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error inserting data into the target database: {e}")
            raise e

    def scraping_process(self, task_queue, total_jobs, progress_bar):
        """Main scraping process."""
        while not task_queue.empty():
            product_path_data = task_queue.get()
            product_path = product_path_data['page_url']
            shop_id = product_path_data['shop_id']
            
            try:
                logging.info(f"Navigating to: {product_path}")
                response = requests.get(product_path)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')

                # Extract product name
                product_name_tag = soup.find('h1', class_='pdp__heading')
                product_name = product_name_tag.get_text(strip=True) if product_name_tag else 'No heading found'

                # Extract category and subcategory
                breadcrumbs_tag = soup.find_all('li', class_='breadcrumbs__item')
                breadcrumb_links = [
                    (breadcrumb.find('a').get_text(strip=True), breadcrumb.find('a')['href'])
                    for breadcrumb in breadcrumbs_tag if breadcrumb.find('a')
                ]
                category = breadcrumb_links[2][0] if len(breadcrumb_links) > 2 else 'Category not found'
                subcategory = breadcrumb_links[3][0] if len(breadcrumb_links) > 3 else 'Subcategory not found'

                # Extract product image
                img_tag = soup.find('img')
                img_src = img_tag['src'] if img_tag else 'No image found'

                # Extract bullet point information (features & benefits)
                features_tag = soup.find('div', class_='info__features')
                features = [li.text.strip() for li in features_tag.find_all('li')] if features_tag else []
                
                # Extract the description
                description_tag = soup.find('div', class_='product-main-info__description')
                description = description_tag.text.strip() if description_tag else 'Description not found'

                # Extract the brand name
                brand_name_tag = soup.find('strong', string='Brand Name:')
                brand_name = brand_name_tag.find_next('span').text.strip() if brand_name_tag else 'Brand Name not found'

                # # Extract Cloudinary product identifiers
                # cloudinary_script = soup.find('script', src="https://product-gallery.cloudinary.com/all.js")
                # if cloudinary_script:
                #     onload_attr = cloudinary_script.get('onload')
                #     if onload_attr:
                #         # Use regex to extract the list of product identifiers
                #         identifiers_str = re.search(r'\[(.*?)\]', onload_attr).group(1)
                #         product_identifiers = [id.strip() for id in identifiers_str.split(',')]  # Convert to list
                # # Extract all images after gallery initialization
                # image_urls = []
                # # Try to directly access images from the Cloudinary gallery if possible
                # if product_identifiers:
                #     for identifier in product_identifiers:
                #         image_url = f"https://res.cloudinary.com/dj6kabpgi/image/upload/{identifier}.jpg"
                #         image_urls.append(image_url)
                # else:
                #     # Fallback: extract images from the page if product identifiers are not available
                #     for img_tag in soup.select('.pdp__gallery img'):
                #         image_url = img_tag.get('src')
                #         if image_url:
                #             image_urls.append(image_url)

                # Extract product details from JSON-LD script
                script_tags = soup.find_all('script', type='application/ld+json')
                product_data = None

                for script_tag in script_tags:
                    json_data = json.loads(script_tag.string)
                    if json_data.get('@type') == 'Product':
                        product_data = json_data
                        break

                if product_data:
                    aggregate_rating = product_data.get('aggregateRating', {})
                    rating_count = aggregate_rating.get('reviewCount', 0)
                    rating = aggregate_rating.get('ratingValue', 0.0)
                    offers = product_data.get('offers', {})
                    price = offers.get('price', 'Price not found')
                    brand = product_data.get('brand', 'Brand not found')
                    brand_name = brand if brand else brand_name

                # Fallback price extraction if JSON-LD data is not available
                if not product_data or price == 'Price not found':
                    price_tag = soup.find('div', class_='main-price__value pdp-price__new-price')
                    price = price_tag.get_text(strip=True).replace('£', '') if price_tag else 'Price not found'

                product_info = {
                    'product_name': product_name,
                    'page_url': product_path,
                    'image_url': f"https:{img_src}",
                    'description': description,
                    'features': features,
                    'rating': rating,
                    'rating_count': rating_count,
                    'price': price,
                    'brand_name': brand_name,
                    'category': category,
                    'subcategory': subcategory,
                    'shop_id': shop_id,
                }

                logging.info(f"Finished scraping {product_path}")
                if product_info:
                    self.insert_scraped_data(product_info)

            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching URL {product_path}: {e}")
                self.error_log.append({
                    "Product_path": str(product_path),
                    "error": str(e)
                })
            except json.JSONDecodeError as e:
                logging.info(f"Error parsing JSON from {product_path}: {e}")
                self.error_log.append({
                    "Product_path": str(product_path),
                    "error": str(e)
                })
            except Exception as e:
                logging.error(f"Error processing {product_path}: {e}")
                self.error_log.append({
                    "Product_path": str(product_path),
                    "error": str(e)
                })
            finally:
                task_queue.task_done()
                progress_bar.update(1)
                jobs_left = task_queue.qsize()

                if self.prometheus_metrics:
                    progress = (jobs_left / total_jobs) * 100
                    self.prometheus_metrics.update_progress('wickes', progress)
                    self.prometheus_metrics.update_jobs_remaining('wickes', jobs_left)
                logging.info(f"Jobs left: {jobs_left}/{total_jobs}")
                time.sleep(2)

    def run(self):
        """Main function to run the scraping process."""
        try:
            category_paths = self.get_scraping_target_data()
            task_queue = queue.Queue()

            for path in category_paths:
                task_queue.put(path)

            total_jobs = task_queue.qsize()
            logging.info(f'Total jobs to scrape: {total_jobs}')

            if self.prometheus_metrics:
                self.prometheus_metrics.set_total_jobs('wickes', total_jobs)

            with tqdm(total=total_jobs, desc="Wickes scraping progress") as progress_bar:
                self.scraping_process(task_queue, total_jobs, progress_bar)

            if self.prometheus_metrics:
                self.prometheus_metrics.update_progress('wickes', 100)
                self.prometheus_metrics.update_jobs_remaining('wickes', 0)

            return {"status": "success", "error_log": self.error_log, "total_jobs": total_jobs}
        except Exception as e:
            logging.error(f"Error in run_wickes: {str(e)}")
            if self.prometheus_metrics:
                self.prometheus_metrics.update_progress('wickes', 0)
            return {"status": "failed", "error": str(e)}

if __name__ == "__main__":
    scraper = WickesScraper()
    scraper.run()
