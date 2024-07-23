import pandas as pd
from datetime import datetime
import sys
import os
from tqdm import tqdm
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from db_utils import conn_to_pathsdb 

def main_wickes():
    try:
        csv_path = 'paths_data_scrapers/wickes/data/sitemap_urls.csv'
        df = pd.read_csv(csv_path)
        product_pattern = r'/p/\d+'
        df_products = df[df['page_url'].str.contains(product_pattern, regex=True)]

        df_products['shop_id'] = 4
        logging.info(f"Total number of products: {len(df_products)}")
        conn = conn_to_pathsdb()
        logging.info("Connected to the database")
        cur = conn.cursor()
        try:
            for index, row in tqdm(df_products.iterrows()):
                cur.execute("INSERT INTO wickes (page_url, shop_id, created_at) VALUES (%s, %s, CURRENT_TIMESTAMP)", (row['page_url'], row['shop_id']))
            conn.commit()
            logging.info("Data inserted successfully")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error inserting data: {e}")
        finally:
            cur.close()
            conn.close()
    except FileNotFoundError:
        logging.error(f"CSV file not found: {csv_path}")
    except pd.errors.EmptyDataError:
        logging.error(f"CSV file is empty: {csv_path}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    main_wickes()