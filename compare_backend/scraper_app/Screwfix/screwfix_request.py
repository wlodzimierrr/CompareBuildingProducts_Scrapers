import psycopg2
import requests
import json
import csv
import random
import time

def initial_request(category_path):        
    
    cookies = {
        'rxVisitor': '1705589577116OD0KTOF69LFC8NAD4O12DU3K4CTKAMBA',
        'isPIIMasked': 'true',
        'notice_behavior': 'expressed,eu',
        'TAconsentID': '006740bd-0659-450f-975f-429480d5ba43',
        'notice_gdpr_prefs': '0,1,2,3,4:',
        'cmapi_cookie_privacy': 'permit 1,2,3,4,5',
        'cmapi_gtm_bl': '',
        'notice_preferences': '4:',
        '_cs_c': '0',
        'optimizelyEndUserId': 'oeu1711225018302r0.1009099587559974',
        'BVBRANDID': 'ed9f143e-934e-4810-8d01-655a4775489f',
        'BVImplmain_site': '22465',
        'JSESSIONID': 'TabpD9i5A3eNU0tkisjgcJX_k5QEp89cfd83VWSB415Vo4b3Fvkf!-1321055760',
        '__Host-fusionx.sid': 's%3AWy-DyzrvvjbbK3y4dsydJwnew1Ti1FMs.qzn%2BJwTFKLBIyfB5ET%2BWKFBBRhdqhA73YOtIL%2B9HBUw',
        'unsupported_browser': 'false',
        'atgUserId': '18907693414',
        '_cs_mk': '0.1558066765509667_1713307114562',
        'TAsessionID': '8b438043-e648-4acf-996e-6a3ec64c981b|EXISTING',
        '_gid': 'GA1.2.1673564778.1713307117',
        'prevPageName': 'lister Page',
        'dtSa': '-',
        '_br_uid_2': 'uid%3D5914070484133%3Av%3D13.0%3Ats%3D1710278604438%3Ahc%3D414',
        '_ga': 'GA1.2.1943236586.1710278621',
        '_gat_gtag_UA_4391415_14': '1',
        '_cs_id': 'f7f58951-d343-a689-ca67-d423b1c1b465.1711541657.40.1713307245.1713307114.1.1745705657090.1',
        '_cs_s': '4.5.0.1713309045457',
        'prevPageURL': 'https://www.screwfix.com/c/tools/drills/cat830704?page_size=100',
        'utag_main': 'v_id:018e348c6ec900196288939ee9a40506f004106700bd0$_sn:59$_se:30$_ss:0$_st:1713309046406$dc_visit:57$ses_id:1713307113783%3Bexp-session$_pn:2%3Bexp-session$dc_event:4%3Bexp-session$dc_region:eu-west-1%3Bexp-session',
        '_ga_74MS9KFBCG': 'GS1.1.1713307116.57.1.1713307246.59.0.0',
        'dtCookie': 'v_4_srv_8_sn_F4356087715791C09B76809FE262F704_perc_100000_ol_0_mul_1_app-3Ac4f0aebc3d79082a_1',
        'BIGipServersfx.com_HTTPS_Pool': '!LFkG1F89HWfE5f8fkx1a+sXyK5MWD8F0LOZvZEdfBC0QOIKN3l6dxzTDkLkyM4Um3LTmYTNpYDsJ',
        'rxvt': '1713309080668|1713307113721',
        'dtPC': '8$307124389_768h41vICRGLVOFQKAUPWRHFUABAHNVHKCRERNR-0e0',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en;q=0.9,en-US;q=0.8,pl;q=0.7,nl;q=0.6',
        'authorization': 'eyJvcmciOiI2MGFlMTA0ZGVjM2M1ZjAwMDFkMjYxYTkiLCJpZCI6IjU3OTZiYWJkMmUwMDQ0Zjc4ODJjZDgwYWM3YWY5ZmMxIiwiaCI6Im11cm11cjEyOCJ9',
        # 'cookie': 'rxVisitor=1705589577116OD0KTOF69LFC8NAD4O12DU3K4CTKAMBA; isPIIMasked=true; notice_behavior=expressed,eu; TAconsentID=006740bd-0659-450f-975f-429480d5ba43; notice_gdpr_prefs=0,1,2,3,4:; cmapi_cookie_privacy=permit 1,2,3,4,5; cmapi_gtm_bl=; notice_preferences=4:; _cs_c=0; optimizelyEndUserId=oeu1711225018302r0.1009099587559974; BVBRANDID=ed9f143e-934e-4810-8d01-655a4775489f; BVImplmain_site=22465; JSESSIONID=TabpD9i5A3eNU0tkisjgcJX_k5QEp89cfd83VWSB415Vo4b3Fvkf!-1321055760; __Host-fusionx.sid=s%3AWy-DyzrvvjbbK3y4dsydJwnew1Ti1FMs.qzn%2BJwTFKLBIyfB5ET%2BWKFBBRhdqhA73YOtIL%2B9HBUw; unsupported_browser=false; atgUserId=18907693414; _cs_mk=0.1558066765509667_1713307114562; TAsessionID=8b438043-e648-4acf-996e-6a3ec64c981b|EXISTING; _gid=GA1.2.1673564778.1713307117; prevPageName=lister Page; dtSa=-; _br_uid_2=uid%3D5914070484133%3Av%3D13.0%3Ats%3D1710278604438%3Ahc%3D414; _ga=GA1.2.1943236586.1710278621; _gat_gtag_UA_4391415_14=1; _cs_id=f7f58951-d343-a689-ca67-d423b1c1b465.1711541657.40.1713307245.1713307114.1.1745705657090.1; _cs_s=4.5.0.1713309045457; prevPageURL=https://www.screwfix.com/c/tools/drills/cat830704?page_size=100; utag_main=v_id:018e348c6ec900196288939ee9a40506f004106700bd0$_sn:59$_se:30$_ss:0$_st:1713309046406$dc_visit:57$ses_id:1713307113783%3Bexp-session$_pn:2%3Bexp-session$dc_event:4%3Bexp-session$dc_region:eu-west-1%3Bexp-session; _ga_74MS9KFBCG=GS1.1.1713307116.57.1.1713307246.59.0.0; dtCookie=v_4_srv_8_sn_F4356087715791C09B76809FE262F704_perc_100000_ol_0_mul_1_app-3Ac4f0aebc3d79082a_1; BIGipServersfx.com_HTTPS_Pool=!LFkG1F89HWfE5f8fkx1a+sXyK5MWD8F0LOZvZEdfBC0QOIKN3l6dxzTDkLkyM4Um3LTmYTNpYDsJ; rxvt=1713309080668|1713307113721; dtPC=8$307124389_768h41vICRGLVOFQKAUPWRHFUABAHNVHKCRERNR-0e0',
        'referer': 'https://www.screwfix.com/c/safety-workwear/safety-trainer-boots/cat4690016',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'traceparent': '00-06fb7a1377d2401e9319e75261da1195-00f067aa0ba902b7-00',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'x-dtpc': '8$307124389_768h41vICRGLVOFQKAUPWRHFUABAHNVHKCRERNR-0e0',
    }

    params = {
        'page_size': '1',
        'page_start': '0',
    }

    try:
        time.sleep(random.uniform(5, 10))
        response = requests.get(
            f'https://www.screwfix.com/prod/ffx-browse-bff/v1/SFXUK/data{category_path}',
            params=params,
            cookies=cookies,
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
            # Check if the expected keys exist
            if 'enrichedData' in data and 'totalProducts' in data['enrichedData']:
                meta = data.get('enrichedData', {})
                total_results = meta.get('totalProducts', None)
                return total_results, True
            else:
                print("Expected data not found in response")
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
    else:
        print("Failed to fetch data or no response")
    return 0, False


def final_request(category_path, total_results):        
    cookies = {
        'rxVisitor': '1705589577116OD0KTOF69LFC8NAD4O12DU3K4CTKAMBA',
        'isPIIMasked': 'true',
        'notice_behavior': 'expressed,eu',
        'TAconsentID': '006740bd-0659-450f-975f-429480d5ba43',
        'notice_gdpr_prefs': '0,1,2,3,4:',
        'cmapi_cookie_privacy': 'permit 1,2,3,4,5',
        'cmapi_gtm_bl': '',
        'notice_preferences': '4:',
        '_cs_c': '0',
        'optimizelyEndUserId': 'oeu1711225018302r0.1009099587559974',
        'BVBRANDID': 'ed9f143e-934e-4810-8d01-655a4775489f',
        'BVImplmain_site': '22465',
        'JSESSIONID': 'TabpD9i5A3eNU0tkisjgcJX_k5QEp89cfd83VWSB415Vo4b3Fvkf!-1321055760',
        '__Host-fusionx.sid': 's%3AWy-DyzrvvjbbK3y4dsydJwnew1Ti1FMs.qzn%2BJwTFKLBIyfB5ET%2BWKFBBRhdqhA73YOtIL%2B9HBUw',
        'unsupported_browser': 'false',
        'atgUserId': '18907693414',
        '_cs_mk': '0.1558066765509667_1713307114562',
        'TAsessionID': '8b438043-e648-4acf-996e-6a3ec64c981b|EXISTING',
        '_gid': 'GA1.2.1673564778.1713307117',
        'prevPageName': 'lister Page',
        'dtSa': '-',
        '_br_uid_2': 'uid%3D5914070484133%3Av%3D13.0%3Ats%3D1710278604438%3Ahc%3D414',
        '_ga': 'GA1.2.1943236586.1710278621',
        '_gat_gtag_UA_4391415_14': '1',
        '_cs_id': 'f7f58951-d343-a689-ca67-d423b1c1b465.1711541657.40.1713307245.1713307114.1.1745705657090.1',
        '_cs_s': '4.5.0.1713309045457',
        'prevPageURL': 'https://www.screwfix.com/c/tools/drills/cat830704?page_size=100',
        'utag_main': 'v_id:018e348c6ec900196288939ee9a40506f004106700bd0$_sn:59$_se:30$_ss:0$_st:1713309046406$dc_visit:57$ses_id:1713307113783%3Bexp-session$_pn:2%3Bexp-session$dc_event:4%3Bexp-session$dc_region:eu-west-1%3Bexp-session',
        '_ga_74MS9KFBCG': 'GS1.1.1713307116.57.1.1713307246.59.0.0',
        'dtCookie': 'v_4_srv_8_sn_F4356087715791C09B76809FE262F704_perc_100000_ol_0_mul_1_app-3Ac4f0aebc3d79082a_1',
        'BIGipServersfx.com_HTTPS_Pool': '!LFkG1F89HWfE5f8fkx1a+sXyK5MWD8F0LOZvZEdfBC0QOIKN3l6dxzTDkLkyM4Um3LTmYTNpYDsJ',
        'rxvt': '1713309080668|1713307113721',
        'dtPC': '8$307124389_768h41vICRGLVOFQKAUPWRHFUABAHNVHKCRERNR-0e0',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en;q=0.9,en-US;q=0.8,pl;q=0.7,nl;q=0.6',
        'authorization': 'eyJvcmciOiI2MGFlMTA0ZGVjM2M1ZjAwMDFkMjYxYTkiLCJpZCI6IjU3OTZiYWJkMmUwMDQ0Zjc4ODJjZDgwYWM3YWY5ZmMxIiwiaCI6Im11cm11cjEyOCJ9',
        # 'cookie': 'rxVisitor=1705589577116OD0KTOF69LFC8NAD4O12DU3K4CTKAMBA; isPIIMasked=true; notice_behavior=expressed,eu; TAconsentID=006740bd-0659-450f-975f-429480d5ba43; notice_gdpr_prefs=0,1,2,3,4:; cmapi_cookie_privacy=permit 1,2,3,4,5; cmapi_gtm_bl=; notice_preferences=4:; _cs_c=0; optimizelyEndUserId=oeu1711225018302r0.1009099587559974; BVBRANDID=ed9f143e-934e-4810-8d01-655a4775489f; BVImplmain_site=22465; JSESSIONID=TabpD9i5A3eNU0tkisjgcJX_k5QEp89cfd83VWSB415Vo4b3Fvkf!-1321055760; __Host-fusionx.sid=s%3AWy-DyzrvvjbbK3y4dsydJwnew1Ti1FMs.qzn%2BJwTFKLBIyfB5ET%2BWKFBBRhdqhA73YOtIL%2B9HBUw; unsupported_browser=false; atgUserId=18907693414; _cs_mk=0.1558066765509667_1713307114562; TAsessionID=8b438043-e648-4acf-996e-6a3ec64c981b|EXISTING; _gid=GA1.2.1673564778.1713307117; prevPageName=lister Page; dtSa=-; _br_uid_2=uid%3D5914070484133%3Av%3D13.0%3Ats%3D1710278604438%3Ahc%3D414; _ga=GA1.2.1943236586.1710278621; _gat_gtag_UA_4391415_14=1; _cs_id=f7f58951-d343-a689-ca67-d423b1c1b465.1711541657.40.1713307245.1713307114.1.1745705657090.1; _cs_s=4.5.0.1713309045457; prevPageURL=https://www.screwfix.com/c/tools/drills/cat830704?page_size=100; utag_main=v_id:018e348c6ec900196288939ee9a40506f004106700bd0$_sn:59$_se:30$_ss:0$_st:1713309046406$dc_visit:57$ses_id:1713307113783%3Bexp-session$_pn:2%3Bexp-session$dc_event:4%3Bexp-session$dc_region:eu-west-1%3Bexp-session; _ga_74MS9KFBCG=GS1.1.1713307116.57.1.1713307246.59.0.0; dtCookie=v_4_srv_8_sn_F4356087715791C09B76809FE262F704_perc_100000_ol_0_mul_1_app-3Ac4f0aebc3d79082a_1; BIGipServersfx.com_HTTPS_Pool=!LFkG1F89HWfE5f8fkx1a+sXyK5MWD8F0LOZvZEdfBC0QOIKN3l6dxzTDkLkyM4Um3LTmYTNpYDsJ; rxvt=1713309080668|1713307113721; dtPC=8$307124389_768h41vICRGLVOFQKAUPWRHFUABAHNVHKCRERNR-0e0',
        'referer': 'https://www.screwfix.com/c/safety-workwear/safety-trainer-boots/cat4690016',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'traceparent': '00-06fb7a1377d2401e9319e75261da1195-00f067aa0ba902b7-00',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'x-dtpc': '8$307124389_768h41vICRGLVOFQKAUPWRHFUABAHNVHKCRERNR-0e0',
    }

    params = {
        'page_size': '100',
        'page_start': '0',
    }
   
    page_size = int(params['page_size'])
    num_pages = (total_results // page_size) + (1 if total_results % page_size > 0 else 0)

    all_responses = []
    for page in range(num_pages):
        params['page_start'] = str(page * page_size)
        time.sleep(random.uniform(10, 20))
        try:
            response = requests.get(
                f'https://www.screwfix.com/prod/ffx-browse-bff/v1/SFXUK/data{category_path}',
                params=params,
                cookies=cookies,
                headers=headers,
            )
            response.raise_for_status()
            all_responses.append(response.json()) 
        except requests.exceptions.RequestException as e:
            print(f"An error occurred during the API request on page {page + 1}: {e}")
            continue
    return all_responses 

missing_paging = []
conn = psycopg2.connect(
 
        )
cur = conn.cursor()


def insert_database(data, shop, category, subcategory, conn, cur):
    try:
        enriched_data = data['enrichedData']

        category_set = {(enriched_data['categoryInfo']['id'], enriched_data['categoryInfo']['name'], enriched_data['categoryInfo']['parentCategoryName'])}
        
        for product in enriched_data['products']:
            category_id = product['category']['id']
            category_name = product['category']['name']
            category_set.add((category_id, category_name, None))

        for cat_id, name, parent_name in category_set:
            cur.execute("""
                INSERT INTO categories (id, name, parent_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (cat_id, name, parent_name))

        brands = {product['brand'] for product in enriched_data['products']}
        for brand in brands:
            cur.execute("""
                INSERT INTO brands (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING;
            """, (brand,))

        for product in enriched_data['products']:
            product_data = (
                product['skuId'],
                product['brand'],
                product['category']['id'],
                product['longDescription'],
                product['detailPageUrl'],
                product['imageUrl'], 
                product.get('starRating', 'No rating'), 
                product['numberOfReviews'],
                product['fulfilmentAvailability']['canBeCollected'],
                product['fulfilmentAvailability']['canBeDelivered'],
                product['priceInformation']['currentPriceExVat']['amount'],
                product['priceInformation']['currentPriceIncVat']['amount']
            )
            cur.execute("""
                INSERT INTO products (sku_id, name, category_id, long_description, detail_page_url, image_url, star_rating, number_of_reviews, can_be_collected, can_be_delivered, price_ex_vat, price_inc_vat)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sku_id) DO UPDATE SET
                name = EXCLUDED.name,
                category_id = EXCLUDED.category_id,
                long_description = EXCLUDED.long_description,
                detail_page_url = EXCLUDED.detail_page_url,
                image_url = EXCLUDED.image_url,
                star_rating = EXCLUDED.star_rating,
                number_of_reviews = EXCLUDED.number_of_reviews,
                can_be_collected = EXCLUDED.can_be_collected,
                can_be_delivered = EXCLUDED.can_be_delivered,
                price_ex_vat = EXCLUDED.price_ex_vat,
                price_inc_vat = EXCLUDED.price_inc_vat;
            """,  product_data)
            
        product_features = []
        for product in enriched_data['products']:
            sku_id = product['skuId']
            if 'bullets' in product:
                for bullet in product['bullets']:
                    product_features.append((sku_id, bullet))

        cur.executemany("""
            INSERT INTO product_features (product_id, feature)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        """, product_features)


        promotions = []
        for product in enriched_data['products']:
            if 'promotions' in product:  
                for promotion in product['promotions']:
                    promotions.append((product['skuId'], promotion['title']))
        
        cur.executemany("""
            INSERT INTO promotions (product_id, promotion_title)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        """, promotions)
    
        filters = set()
        for filter_group in enriched_data['filters']:
            filters.add((filter_group['filterIdentifier'], filter_group['filterLabel'], filter_group['filterType']))

       
        cur.executemany("""
            INSERT INTO filters (id, label, type)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, list(filters))

        filter_options = []
        for filter_group in enriched_data['filters']:
            for option in filter_group['options']:
                filter_options.append((
                    filter_group['filterIdentifier'], 
                    option.get('optionLabel', 'No label'),
                    option['optionIdentifier'],
                    option['selected'],
                    option.get('canonicalUrl', 'No URL provided'), 
                    option['numberOfProducts']
                ))

        cur.executemany("""
            INSERT INTO filter_options (filter_id, label, identifier, selected, canonical_url, number_of_products)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, filter_options)


    
      
        cur.executemany("""
            INSERT INTO filter_options (filter_id, label, identifier, selected, canonical_url, number_of_products)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (filter_id, identifier) DO NOTHING;
        """, filter_options)
        
    
        
        for product in enriched_data['products']:
            product_id = product['skuId'] 
            breadcrumbs = []
            
   
            for breadcrumb in enriched_data['breadcrumbs']: 
                breadcrumbs.append((
                    product_id,
                    breadcrumb['name'],
                    breadcrumb['path']
                ))

            cur.executemany("""
                INSERT INTO breadcrumbs (product_id, name, path)
                VALUES (%s, %s, %s)
                ON CONFLICT (product_id, path) DO NOTHING; 
            """, breadcrumbs)


        seo_overrides = []
        for product in enriched_data['products']:
            seo_overrides.append((
                product['skuId'],
                product.get('seo_body', ''), 
                product.get('seo_heading', ''),
                product.get('seo_meta_description', '')
            ))

        cur.executemany("""
            INSERT INTO seo_overrides (product_id, body_copy, heading, meta_description)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING;
        """, seo_overrides)

        price_information = []
        for product in enriched_data['products']:
            price_info = product['priceInformation']
            price_information.append((
                product['skuId'],
                price_info['currentPriceExVat']['amount'],
                price_info['currentPriceIncVat']['amount'],
                price_info['vatRate'],
                price_info['isBulkPricingAvailable'],
                price_info.get('savingsExVat', {}).get('amount', None),
                price_info.get('savingsIncVat', {}).get('amount', None),
                price_info.get('savingsPercent', None),
                price_info.get('wasPriceExVat', {}).get('amount', None),
                price_info.get('wasPriceIncVat', {}).get('amount', None)
            ))

        cur.executemany("""
            INSERT INTO price_information (product_id, current_price_ex_vat, current_price_inc_vat, vat_rate, is_bulk_pricing_available, savings_ex_vat, savings_inc_vat, savings_percent, was_price_ex_vat, was_price_inc_vat)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE SET
            current_price_ex_vat = EXCLUDED.current_price_ex_vat,
            current_price_inc_vat = EXCLUDED.current_price_inc_vat,
            vat_rate = EXCLUDED.vat_rate,
            is_bulk_pricing_available = EXCLUDED.is_bulk_pricing_available,
            savings_ex_vat = EXCLUDED.savings_ex_vat,
            savings_inc_vat = EXCLUDED.savings_inc_vat,
            savings_percent = EXCLUDED.savings_percent,
            was_price_ex_vat = EXCLUDED.was_price_ex_vat,
            was_price_inc_vat = EXCLUDED.was_price_inc_vat;
        """, price_information)

        
        conn.commit()
        print("All data committed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()

total_jobs_count = 0   
    
with open('screwfix_paths.csv', newline='', encoding='utf-8') as csvfile:
    url_reader = csv.reader(csvfile)
    next(url_reader, None)
    for row in url_reader:
        shop_id = row[0]
        category_id = row[1]
        subcategory_id = row[2]
        category_path = row[3]
        total_jobs_count += 1 
        print(f'Job number: {total_jobs_count}')  
        print('Making initial request...')
        response = initial_request(category_path)
        print('Extracting item count...')
        total_results, has_paging = total_page_count(response)
        print(f'Total item count for category: {total_results}')
        if not has_paging:
            missing_paging.append(category_path)
            print(f'Category error')
        else:
            print('Making final request...')
            all_responses = final_request(category_path, total_results)
            if all_responses:
                print('Handling data insertion...')
                for response_data in all_responses:
                    try:  
                        insert_database(response_data, shop_id, category_id, subcategory_id, conn, cur)  
                    except json.JSONDecodeError as e:
                        print(f"Failed to decode JSON from response: {e}")
                    except Exception as e:
                        print(f"An error occurred while processing the response: {e}")
            else:
                print("Failed to retrieve or process detailed data.")

with open('error_categories.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    for cat_id in missing_paging:
        writer.writerow([cat_id])

cur.close()
conn.close()

print(f"Categories without paging data saved to 'error_categories.csv'")