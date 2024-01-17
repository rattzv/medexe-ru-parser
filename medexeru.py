import os
import sys
import random

from concurrent.futures import ThreadPoolExecutor
from utils.exporter import convert_to_json, remove_old_data, save_to_sqlite
from utils.parser import parsing_product, parsing_sitemaps
from utils.utils import check_reports_folder_exist, get_requests, print_template, random_sleep


os.environ['PROJECT_ROOT'] = os.path.dirname(os.path.abspath(__file__))
futures = []


def start():
    try:
        DOMAIN = 'https://medexe.ru'

        reports_folder = check_reports_folder_exist()
        if not reports_folder:
            sys.exit(1)

        remove_old_data(reports_folder)

        print_template("Parse links to categories from the sitemap...")
        products_list = parsing_sitemaps(DOMAIN)
        random.shuffle(products_list)

        if not products_list:
            print_template("Error when parsing links to categories from the sitemap!")
            return False

        print_template(f"Found {len(products_list)} links to products, start parsing...")
        i = 0
        with ThreadPoolExecutor(max_workers=15) as executor:
            results = []
            for product in products_list:
                future = executor.submit(parsing_product, product)
                results.append(future)

            for future in results:
                result = future.result()
                if result:
                    print_template(f"Saving in sqlite new product:  {result['Наименование']} ({result['URL товара']})")
                    save_to_sqlite(result, reports_folder)
                    i += 1

        total_count = convert_to_json(reports_folder)

        print(f"Total count: {total_count}")
    except Exception as ex:
        print_template(f"Unhandleable exception: {ex}")


if __name__ == '__main__':
    start()