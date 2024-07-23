import pandas as pd
from datetime import datetime

from db_utils import conn_to_pathsdb 

# Placeholder for the path to the CSV file
df_products_path = f'/home/wlodzimierrr/Desktop/code/paths_data_scrapers/wickes/data/filtered_products_urls_{datetime.now().strftime("%d-%m-%y")}.csv'
csv_path = '/home/wlodzimierrr/Desktop/code/paths_data_scrapers/wickes/data/sitemap_urls.csv'

df = pd.read_csv(csv_path)

# Define the regex pattern for product URLs ending with /p/product-id
product_pattern = r'/p/\d+'

# Filter the DataFrame for URLs matching the product pattern
df_products = df[df['page_url'].str.contains(product_pattern, regex=True)]

# Add shop_id column with value 4
df_products['shop_id'] = 4

# Save the filtered product URLs to a new CSV
df_products.to_csv(df_products_path, index=False)