"""ALLO.UA XIAOMI PRODUCT SCRAPER ASYNC (HTTPX)"""
# pylint: disable=W3101,locally-disabled, multiple-statements, fixme, line-too-long, W0706, C0116

import re
import os
import glob

from datetime import datetime

from typing import List

import asyncio

from bs4 import BeautifulSoup

import httpx
import aiofiles
from aiocsv import AsyncDictWriter

import pandas as pd

from config import cookies, headers, MIN_SCRAPING_TIME_PERIOD


DATADIR = "data"
LOGDIR = "logs"
DATETIME_FORMAT = '%Y.%m.%d-%H:%M:%S'

PROXY_SETTINGS = os.getenv('PROXY')


# TODO: add max_replies++ params like from http_adapter for requests
# Load html source
async def load_page(url: str) -> str:
    try:
        async with httpx.AsyncClient(headers=headers, cookies=cookies, proxies=PROXY_SETTINGS) as client:
            response = await client.get(url=url)

        print(f"Response status: {response.status_code}")
    except:
        raise

    # errors? proccess it!
    # requests.exceptions.ProxyError  HTTPSConnectionPool(host='allo.ua', port=443): Max retries exceeded with url: ... (Caused by ProxyError('Cannot connect to proxy.', TimeoutError('timed out')))

    return response.text


# Parse all pagination link in catalog from page content
def collect_pagination_links(first_page_link: str, html_src: str) -> List:

    soup = BeautifulSoup(html_src, "lxml")
    page_links = soup.findAll("li", class_="pagination__item")

    # TODO: check if is it right on page without pagination?
    if not page_links or len(page_links) == 1:
        return []

    last_link = page_links[-1].find("a").text

    print(f"Всего страниц: {last_link}")

    page_links = [f"{first_page_link}/p-{num}" for num in range(2, int(last_link)+1)]

    return page_links


# Parse all products from page content
def collect_page_products(html_src: str, page_number, allproductsflag=False) -> List:

    next_parse_flag = True

    soup = BeautifulSoup(html_src, "lxml")

    cards = soup.find_all("div", class_="product-card")

    print(f"На странице {page_number} найдено {len(cards)} товаров")

    markdown_compiled_text = re.compile("Причина")

    all_cards = []

    for card in cards:

        card_title = card.find("a", class_="product-card__title")

        title: str = card_title.text.strip().replace("\n", "").replace("\t", "").replace("  ", " ")
        link: str = card_title["href"]

        sku = card.find(class_="product-sku__value").text.strip()

        old_price = price = markdown_reason = ""

        # if outofstock
        outofstock = False
        try:
            outofstock = card.find("button", class_="v-btn--out-stock")
            if outofstock:
                outofstock = True
        except AttributeError:
            outofstock = True

        # print(outofstock)

        if not allproductsflag and outofstock:
            next_parse_flag = False
            break

        if not outofstock:
            try:
                old_price = card.find("div", class_="v-pb__old").find("span", class_="sum").text.strip()
            except AttributeError:
                pass

            try:
                price = card.find("div", class_="v-pb__cur").find("span", class_="sum").text.strip()
            except AttributeError:
                pass

            # if markdown
            markdown = False
            try:
                markdown = card.find("dt", string=markdown_compiled_text)
                markdown_reason = markdown.find_next_sibling("dd").text.strip()
            except AttributeError:
                pass

        # print(f"{title} | {old_price or '-'} | {price or '-'} | {'УЦЕНКА: ' + markdown_reason if markdown else ' -'} | {link}")

        all_cards.append({
            "sku": sku,
            "title": title,
            "price": price,
            "old_price": old_price,
            "markdown": markdown_reason,
            "link": link,
        })

    return all_cards, next_parse_flag


# Collect all available products data from catalog
async def collect_data(url, parse_all=False):

    all_products = []

    # csvfilename = os.path.abspath(f"data/products_{datetime.now().strftime('%Y.%m.%d')}.csv")
    csvfilename = os.path.abspath(f"data/products_{datetime.now().strftime(DATETIME_FORMAT)}.csv")

    # TODO: add rotatable proxy to all uploads
    firstpage = await load_page(url)

    etc_page_links = collect_pagination_links(url, firstpage)

    _products, _next_parse = collect_page_products(firstpage, 1, allproductsflag=parse_all)

    all_products.append(_products)

    # if we need only vailable products list
    if _next_parse:
        for num, page_link in enumerate(etc_page_links, start=2):
            # sleep(5)
            page = await load_page(url=page_link)
            _products, _next_parse = collect_page_products(page, num, allproductsflag=parse_all)
            all_products.append(_products)
            if not _next_parse:
                break

    async with aiofiles.open(csvfilename, "w", encoding="utf-8") as csvfile:
        header = {"sku": "Артикул", "title": "Название", "price": "Цена", "old_price": "Старая цена", "markdown": "Уценка?", "link": "Ссылка"}

        csvwriter = AsyncDictWriter(csvfile, fieldnames=["sku", "title", "price", "old_price", "markdown", "link"], extrasaction='ignore')
        await csvwriter.writerow(header)

        for page in all_products:
            await csvwriter.writerows(page)

    return csvfilename


# Scrap proccess function
async def scrap_it():

    print("Start new scraping process ...")

    filename = await collect_data("https://allo.ua/ua/products/notebooks/dir-asc/order-price/proizvoditel-xiaomi/")

    print("Scraping process is finished.")

    return filename


# Scrap and Analyze proccess function
async def analyze_it():

    print("Start fresh analyzing process ..."
          )
    print("Analyzing last DATA datetime ...")
    # Take last csv filename from data dir
    # https://stackoverflow.com/a/39327156
    list_of_files = glob.glob('./data/*.csv')  # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
    latest_file = os.path.abspath(latest_file)
    print(f"Latest scraped csv: {latest_file}")
    # Take timestamp from this file (DATETIME_FORMAT = '%Y.%m.%d-%H:%M:%S')
    # https://stackoverflow.com/a/62589532
    match = re.search(r"_((\d+)\.(\d+)\.(\d+)\-(\d+)\:(\d+)\:(\d+))", latest_file)
    latest_datetime = datetime.strptime(match.group(1), DATETIME_FORMAT)
    print(f"Latest scraped datetime: {latest_datetime}")

    # Analyze: if it is too old for scrap new one ? if not - pass and send messege: "Last analyzing isn't too old. Try later."
    current_datetime = datetime.now()
    datetime_diff = int((current_datetime - latest_datetime).total_seconds())
    print(f"Was gone: {datetime_diff/60}m (minimal period is {MIN_SCRAPING_TIME_PERIOD/60}m).")
    # IF MIN_SCRAPING_TIME_PERIOD was NOT gone - just pass this analyzis and msg to user
    if datetime_diff < MIN_SCRAPING_TIME_PERIOD:
        msg = f"SCRAPING/ANALYZYS WAS PASSED: Minimal scraping period was not passed. Wait for {(MIN_SCRAPING_TIME_PERIOD-datetime_diff)/60}m."
        print(msg)
        return False, msg
    # IF not - scrap'n'save new one
    print("Ok. Let's do some work ...")

    # So, load new data to csv
    newest_file = await scrap_it()

    # Load data from this two csv to use with pandas
    # https://www.datasciencelearner.com/compare-two-csv-files-python-pandas/
    # https://softhints.com/python-pandas-compare-csv-files-column/
    df1 = pd.read_csv(latest_file)
    df2 = pd.read_csv(newest_file)
    # Analyze for new products
    c_result = df2[~df2.apply(tuple, 1).isin(df1.apply(tuple, 1))]
    new_products_count = len(c_result)
    # ... if not - send "There is NO new products"
    # TODO: ... (CHECK if it was manual start!!! for analyzing)
    if not new_products_count:
        print("No new products...")
        return False, "No new products..."

    # But if we have new - send links to user (or to all users who is looking for this site category)
    print(f"New products: {new_products_count} pcs")
    print(c_result)

    new_links = []
    for link in c_result['Ссылка']:
        new_links.append(link)

    # IDEA? I can make only http request from cron to inner http-server or just
    # TODO: ... or just pass (return False) if it was started by "cron"

    # TODO: delete all except last 2 csv (if it new was scraped)

    print("Analyzing process is finished.")

    return True, new_links


def prepare_dir():
    """Prepare data directory"""

    datadir = os.path.abspath(DATADIR)

    if not os.path.exists(datadir):
        print(f"There is no DATA DIRECTORY: {datadir}. Making it ...")
        try:
            os.makedirs(datadir)
        # except FileExistsError:
        #     # directory already exists
        #     pass
        except:
            print(f"{datadir} was created.")

    logdir = os.path.abspath(LOGDIR)

    if not os.path.exists(logdir):
        print(f"There is no LOG DIRECTORY: {logdir}. Making it ...")
        try:
            os.makedirs(logdir)
        # except FileExistsError:
        #     # directory already exists
        #     pass
        except:
            print(f"{logdir} was created.")


async def main():

    # await collect_data("https://allo.ua/ua/products/notebooks/dir-asc/order-price/proizvoditel-xiaomi/")

    await scrap_it()
    await analyze_it()


if __name__ == "__main__":

    # test
    # print(load_page('https://api.ipify.org?format=json'))

    asyncio.run(main())
