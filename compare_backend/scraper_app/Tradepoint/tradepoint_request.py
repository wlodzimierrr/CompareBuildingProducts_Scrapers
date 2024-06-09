import psycopg2
import requests
import json
import csv
import random
import time

def initial_request(category_path):
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8,pl;q=0.7,nl;q=0.6',
        'Authorization': 'Atmosphere atmosphere_app_id=kingfisher-LTbGHXKinHaJSV86nEjf0KnO70UOVE6UcYAswLuC',
        'Connection': 'keep-alive',
        'Origin': 'https://www.trade-point.co.uk',
        'Referer': 'https://www.trade-point.co.uk/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'x-dtc': 'sn="v_4_srv_-2D20_sn_3DVN1BO5L1CBBVKHKCI1EI1OPMMJFPCD", pc="-20$258663744_944h18vFEUORRPPOFPAHCRUJPDPSRRHACKKKQVA-0e0", v="17112790819981S31G3BNLBCJ3II45KG52PONU7NP0H1A", app="d09643a9157f0c88", r="https://www.trade-point.co.uk/departments/building-supplies/TPW762928.cat"',
        'x-tenant': 'TPUK',
    }
    try:
        time.sleep(random.uniform(5, 10))
        response = requests.get(
            f'https://api.kingfisher.com/v2/mobile/products/TPUK?filter[category]={category_id}&include=content&page[size]=1',
            headers=headers,
        )
        response.raise_for_status()  
        return response
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print("The request timed out:", timeout_err)
    except requests.exceptions.RequestException as err:
        print("An error occurred while handling your request:", err)

    return None  

def total_page_count(response):
    if response and response.status_code == 200:
        try:
            data = response.json()
            meta = data.get('meta', {})
            paging = meta.get('paging', None)
            if paging is None:
                print("Paging data is missing in the response.")
                return 0, False
            total_results = paging.get('totalResults', 0)
            return total_results, True
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
    else:
        print("Failed to fetch data or no response")
    return 0, False

def final_request(category_id, total_results):
     
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8,pl;q=0.7,nl;q=0.6',
        'Authorization': 'Atmosphere atmosphere_app_id=kingfisher-LTbGHXKinHaJSV86nEjf0KnO70UOVE6UcYAswLuC',
        'Connection': 'keep-alive',
        'Origin': 'https://www.trade-point.co.uk',
        'Referer': 'https://www.trade-point.co.uk/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'x-dtc': 'sn="v_4_srv_-2D20_sn_3DVN1BO5L1CBBVKHKCI1EI1OPMMJFPCD", pc="-20$258663744_944h18vFEUORRPPOFPAHCRUJPDPSRRHACKKKQVA-0e0", v="17112790819981S31G3BNLBCJ3II45KG52PONU7NP0H1A", app="d09643a9157f0c88", r="https://www.trade-point.co.uk/departments/building-supplies/TPW762928.cat"',
        'x-tenant': 'TPUK',
    }
    
    max_items_per_page = 200
    num_pages = (total_results // max_items_per_page) + (1 if total_results % max_items_per_page > 0 else 0)

    all_responses = []
    for page in range(1, num_pages + 1):
        time.sleep(random.uniform(10, 30))
        try:
            response = requests.get(
                f'https://api.kingfisher.com/v2/mobile/products/TPUK?filter[category]={category_id}&include=content&page[number]={page}&page[size]={max_items_per_page}',
                headers=headers,
            )
            response.raise_for_status()
            all_responses.append(response.json()) 
        except requests.exceptions.RequestException as e:
            print(f"An error occurred during the API request on page {page}: {e}")
            continue
    return all_responses 

missing_paging = []
conn = psycopg2.connect(

        )
cur = conn.cursor()



def insert_database(data, conn, cur):
    try:
        for product in data['data']:
            attributes = product.get('attributes', {})
            ean = attributes.get('ean', None)
            name = attributes.get('name', 'No Name Provided')
            description = attributes.get('description', '')
            short_name = attributes.get('shortName', '')
            primary_category_id = attributes.get('primaryCategoryId', None)
            product_status = attributes.get('productStatus', 'Unavailable')
            purchasable = attributes.get('purchasable', False)

            category_names = attributes.get('categoryNames')
            
      
                
            if isinstance(category_id, list) or isinstance(category_id, tuple):
                if category_id:
                    category_id = category_id[0]  
                else:
                    category_id = None
            
            cur.execute("""
                INSERT INTO products (
                    ean, 
                    name, 
                    description, 
                    short_name, 
                    primary_category_id, 
                    product_status, 
                    purchasable, 
                    category_name,  
                    category_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING id;
                """, 
                (ean, name, description, short_name, primary_category_id, product_status, purchasable, category_names[0], category_id)
            )
            product_id = cur.fetchone()
            if product_id:
                product_id = product_id[0]
                print(f"Inserted Product ID: {product_id}")

        
            for feature in product['attributes'].get('features', []):
                cur.execute("""
                    INSERT INTO features (product_id, feature)
                    VALUES (%s, %s)
                    """,
                    (product_id, feature)
                )

            for category in product['attributes'].get('breadcrumbList', []):
                cur.execute("""
                    INSERT INTO categories (category_id, name, parent_id, seo_url)
                    VALUES (%s, %s, %s, %s) ON CONFLICT (category_id) DO NOTHING
                    """,
                    (category['categoryId'], category['name'], category.get('parentId'), category.get('seoUrl'))
                )

            for media in product['attributes'].get('mediaObjects', []):
                cur.execute("""
                    INSERT INTO media_objects (product_id, url, alt_text, description, merchandised_url)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (product_id, media.get('url'), media.get('altText'), media.get('description'), media.get('merchandisedUrl'))
                )

            pricing = product['attributes'].get('pricing', {})
            if pricing:
                cur.execute("""
                    INSERT INTO pricing (product_id, currency_code, amount_ex_tax, amount_inc_tax, unit_of_measure)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (product_id, pricing.get('currencyCode'), pricing['currentPrice'].get('amountExTax'), pricing['currentPrice'].get('amountIncTax'), pricing.get('unitOfMeasure'))
                )

            for spec in product['attributes'].get('technicalSpecifications', []):
                cur.execute("""
                    INSERT INTO technical_specifications (product_id, name, value)
                    VALUES (%s, %s, %s)
                    """,
                    (product_id, spec['name'], spec['value'])
                    
                )
                
            for bullet in product['attributes'].get('infoBullets', []):
                cur.execute("""
                INSERT INTO info_bullets (product_id, info)
                VALUES (%s, %s)
                """,
                (product_id, bullet)
            )
                
            if 'averageRating' in attributes:
                rating = attributes['averageRating']
                rating_count = rating.get('count', 0)
                rating_value = rating.get('value', 0.0)
                cur.execute("""
                    INSERT INTO average_rating (product_id, rating_count, rating_value)
                    VALUES (%s, %s, %s);
                    """, (product_id, rating_count, rating_value))

            for f_type, f_data in product['attributes'].get('fulfilmentOptions', {}).items():
                if isinstance(f_data, dict): 
                    for key, value in f_data.items():
                        if isinstance(value, dict):  
                            cur.execute("""
                                INSERT INTO fulfillment_options (product_id, type, availability, message)
                                VALUES (%s, %s, %s, %s)
                                """,
                                (product_id, f"{f_type}_{key}", value.get('availability', 'Unavailable'), value.get('shortMessage', 'No message provided'))
                            )


        conn.commit()
        print("All data committed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()

        
with open('sorted_categories_codes.csv', newline='', encoding='utf-8') as csvfile:
    url_reader = csv.reader(csvfile)
    next(url_reader, None)
    for row in url_reader:
        category_id = row[0]
        response = initial_request(category_id)
        total_results, has_paging = total_page_count(response)
        
        if not has_paging:
            missing_paging.append(category_id)
        else:
            all_responses = final_request(category_id, total_results)
            if all_responses:
                for response_data in all_responses:
                    try:  
                        insert_database(response_data, conn, cur)  
                    except json.JSONDecodeError as e:
                        print(f"Failed to decode JSON from response: {e}")
                    except Exception as e:
                        print(f"An error occurred while processing the response: {e}")
            else:
                print("Failed to retrieve or process detailed data.")

with open('missing_paging_categories.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    for cat_id in missing_paging:
        writer.writerow([cat_id])

cur.close()
conn.close()

print(f"Categories without paging data saved to 'missing_paging_categories.csv'")