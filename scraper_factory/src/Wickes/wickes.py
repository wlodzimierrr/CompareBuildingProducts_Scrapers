import requests
import json
import queue
import os
import sys
import logging
from bs4 import BeautifulSoup
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_utils import conn_to_pathsdb, conn_to_storagedb

error_log = []

def get_scraping_initial_target_data():
    """Retrieve scraping target data from paths database."""
    try:
        conn = conn_to_pathsdb()
        logging.info("Paths database connection successful")
        cursor = conn.cursor()
        cursor.execute("SELECT shop_id, category_id, subcategory_id, paths FROM wickes")  
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return [{"shop_id": row[0], "category_id": row[1], "subcategory_id": row[2], "paths": row[3]} for row in data]
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise e


def get_scraping_final_target_data():
    """Retrieve scraping target data from paths database."""
    try:
        conn = conn_to_storagedb()
        logging.info("Products database connection successful")
        cursor = conn.cursor()
        cursor.execute("SELECT page_url FROM products WHERE shop_id = '4' ")  
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return [{"page_url": row[0]} for row in data]
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise e
    
def initial_insert_scraped_data(products, error_log):
    """Insert initial scraped data into the storage database."""
    try:
        conn = conn_to_storagedb()
        logging.info("Storage database connection successful")
        cursor = conn.cursor()
        
        for product in products:
            shop_id = product['shop_id']
            category_id = product['category_id']
            product_name = product['product_name']
            page_url = product['page_url']
            image_url = product['image_url']
            subcategory_id = product['subcategory_id']
            price = product['price']
    
            try:
                cursor.execute(
                    """
                    INSERT INTO products (
                        shop_id, category_id, subcategory_id, product_name, page_url, image_url, price, created_at, updated_at, last_checked_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (page_url) DO UPDATE SET
                        shop_id = EXCLUDED.shop_id,
                        category_id = EXCLUDED.category_id,
                        subcategory_id = EXCLUDED.subcategory_id,
                        product_name = EXCLUDED.product_name,
                        image_url = EXCLUDED.image_url,
                        price = EXCLUDED.price,
                        updated_at = CASE
                            WHEN products.price <> EXCLUDED.price
                                OR products.product_name <> EXCLUDED.product_name
                                OR products.image_url <> EXCLUDED.image_url
                                OR products.category_id <> EXCLUDED.category_id
                                OR products.subcategory_id <> EXCLUDED.subcategory_id
                            THEN CURRENT_TIMESTAMP
                            ELSE products.updated_at
                        END,
                        last_checked_at = CURRENT_TIMESTAMP;
                    """,
                    (shop_id, category_id, subcategory_id, product_name, page_url, image_url, price)
                )
            except Exception as e:
                logging.error(f"Error inserting product {product_name}: {e}")
                error_log.append({
                    "Product name": product_name,
                    "Page url": page_url,
                    "error": 'Inserting error'
                })
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error inserting data into the target database: {e}")
        raise e

def final_insert_scraped_data(product, error_log):
    """Insert final scraped data into the storage database."""
    try:
        conn = conn_to_storagedb()
        logging.info("Storage database connection successful")
        cursor = conn.cursor()
    
        page_url = product['page_url']
        description = product['description']
        features = product.get('features', '')
        rating = product['rating']
        rating_count = product['rating_count']
        price = product['price']
        
        try:
            cursor.execute(
                """
                UPDATE products
                SET
                    product_description = %s,
                    features = %s,
                    rating = %s,
                    rating_count = %s,
                    price = %s,
                    updated_at = CASE
                        WHEN products.price <> %s
                            OR products.rating <> %s
                            OR products.features <> %s
                            OR products.rating_count <> %s
                            OR products.product_description <> %s
                        THEN CURRENT_TIMESTAMP
                        ELSE products.updated_at
                    END,
                    last_checked_at = CURRENT_TIMESTAMP
                WHERE page_url = %s
                """,
                (description ,features, rating, rating_count, price, price, rating, features, rating_count, description, page_url)
            )

        except Exception as e:
            logging.error(f"Error inserting product {page_url}: {e}")
            error_log.append({
                "Page url": page_url,
                "error": 'Inserting error'
            })
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error inserting data into the target database: {e}")
        raise e

def initial_scraping_process(task_queue, total_jobs, error_log):
    """Make initial scraping."""
    while not task_queue.empty():
        category_path_data = task_queue.get()
        shop_id = category_path_data['shop_id']
        category_id = category_path_data['category_id']
        subcategory_id = category_path_data['subcategory_id']
        category_path = category_path_data['paths']
        
        try:
            logging.info(f"Navigating to: {category_path}")
            page_number = 0
            products_info = []  # List to collect products
            
            while True:
                logging.info(f"Requesting page {page_number + 1}")
                response = requests.get(f"https://www.wickes.co.uk{category_path}?q=%3Arelevance&page={page_number}&perPage=120")
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
        
                product_cards = soup.find_all('div', class_='card product-card')
        
                # Iterate through each product card
                for card in product_cards:
                    try:
                        # Extract title and href
                        title_tag = card.find('a', class_='product-card__title')
                        title = title_tag.get('title')
                        href = title_tag.get('href')
        
                        # Extract image source
                        img_tag = card.find('img', class_='card__img-v2')
                        img_src = img_tag.get('src')
        
                        # Extract rating
                        rating_overlay = card.find('div', class_='rating-overlay')
                        rating = rating_overlay.get('data-rating')
        
                        # Extract rating count
                        rating_count_tag = card.find('div', class_='product-card__reviews')
                        rating_count = rating_count_tag.text.strip().strip('()')

                        # Extract price
                        price_tag = card.find('div', class_='main-price__value')
                        price = price_tag.text.strip() if price_tag else None
                        price = price.replace('£', '').replace(',','').replace('.','')
                        price = float(price.replace('From\n\t\t\t\t\t\t', '').strip())  
        
                        # Store the product info in a dictionary
                        product_info = {
                            'product_name': title,
                            'page_url': f"https://www.wickes.co.uk{href}",
                            'image_url': f"https:{img_src}",
                            'rating': rating,
                            'rating_count': rating_count,
                            'price': price,
                            'shop_id': shop_id,
                            'category_id': category_id,
                            'subcategory_id': subcategory_id
                        }
          
                        # Append the product info to the list
                        products_info.append(product_info)
        
                    except AttributeError as e:
                        logging.error(f"Error extracting product info: {e}")
        
                # Check if we need to navigate to the next page or URL
                if len(product_cards) < 120:
                    break  # Less than 120 products means we've reached the end of this URL's results
        
                page_number += 1  # Move to the next page
            
            # After all pages are scraped, insert collected products into the database
            logging.info(f"Total products collected: {len(products_info)}")
            if products_info:
                initial_insert_scraped_data(products_info, error_log)
            
            logging.info(f"Finished scraping {category_path}")
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {category_path}: {e}")
            error_log.append({
                "Category_code": str(category_path),
                "error": str(e)
            })  
        except Exception as e:
            logging.error(f"An error occurred while scraping {category_path}: {e}")
            error_log.append({
                "Category_code": str(category_path),
                "error": str(e)
            })  
        finally:
            task_queue.task_done()
            jobs_left = task_queue.qsize()
            logging.info(f"Jobs left: {jobs_left}/{total_jobs}")


def final_scraping_process(task_queue, total_jobs, error_log):
    """Make final scraping."""
    while not task_queue.empty():
        product_path_data = task_queue.get()
        product_path = product_path_data['page_url']
        
        try:
            logging.info(f"Navigating to: {product_path}")
            response = requests.get(product_path)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract bullet point information (features & benefits)
            features_tag = soup.find('div', class_='info__features')
            features = [li.text.strip() for li in features_tag.find_all('li')] if features_tag else 'Features not found'
            features_arr = []
            for feature in features:
                features_arr.append(feature)

            # Extract the description
            description_tag = soup.find('div', class_='product-main-info__description')
            description = description_tag.text.strip() if description_tag else 'Description not found'

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

            # Find all script tags with type="application/ld+json"
            script_tags = soup.find_all('script', type='application/ld+json')

            # Initialize variables to store extracted data
            product_data = None

            # Loop through all found script tags
            for script_tag in script_tags:
                # Parse JSON content
                json_data = json.loads(script_tag.string)
                
                # Check if the JSON data corresponds to a Product schema
                if json_data.get('@type') == 'Product':
                    product_data = json_data
                    break  # Stop searching further if found

            # Extracting information from product_data
            if product_data:
                aggregate_rating = product_data.get('aggregateRating', {})
                rating_count = aggregate_rating.get('reviewCount') or aggregate_rating.get('reviewcount')
                rating = aggregate_rating.get('ratingValue')

                offers = product_data.get('offers', {})
                price = offers.get('price')
            else:
                logging.error("No script tag with JSON data found.")

            # Store the product info in a dictionary
            product_info = {
                # 'media': image_urls,
                'description' : description,
                'page_url': product_path,
                'features': features_arr,
                'rating': rating,
                'rating_count': rating_count,
                'price': price,
            }

            logging.info(f"Finished scraping {product_path}")
            if product_info:
                final_insert_scraped_data(product_info, error_log)
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching URL {product_path}: {e}")
            error_log.append({
                "Product_path": str(product_path),
                "error": str(e)
            })  
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing JSON from {product_path}: {e}")
            error_log.append({
                "Product_path": str(product_path),
                "error": str(e)
            })  
        except Exception as e:
            logging.error(f"Error processing {product_path}: {e}")
            error_log.append({
                "Product_path": str(product_path),
                "error": str(e)
            }) 

        finally:
            task_queue.task_done()
            jobs_left = task_queue.qsize()
            logging.info(f"Jobs left: {jobs_left}/{total_jobs}")
            time.sleep(2)

def run_initial_wickes():
    error_log = []
    try:
        category_paths = get_scraping_initial_target_data()
  
        task_queue = queue.Queue()

        for path in category_paths:
            task_queue.put(path)

        total_jobs = task_queue.qsize()
        logging.info(f'Total jobs to scrape: {total_jobs}')

        initial_scraping_process(task_queue, total_jobs, error_log)

    except Exception as e:
        logging.error(f"Error in run_initial_wickes: {str(e)}")
        return {"status": "failed", "error": str(e)}
    
def run_final_wickes():
    error_log = []
    try:
        category_paths = get_scraping_final_target_data()
        task_queue = queue.Queue()

        for path in category_paths:
            task_queue.put(path)

        total_jobs = task_queue.qsize()
        logging.info(f'Total jobs to scrape: {total_jobs}')

        final_scraping_process(task_queue, total_jobs, error_log)

    except Exception as e:
        logging.error(f"Error in run_final_wickes: {str(e)}")
        return {"status": "failed", "error": str(e)}

def run_wickes():
    """Main function to run the scraping process."""
    try:
        run_initial_wickes()
        time.sleep(10)
        run_final_wickes()

        return {"status": "success", "error_log": error_log}
    except Exception as e:
        logging.error(f"Error in run_wickes: {str(e)}")
        return {"status": "failed", "error": str(e)}

if __name__ == "__main__":
    run_wickes()