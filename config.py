"""Config (preset in future) for allo.ua"""


MIN_SCRAPING_TIME_PERIOD = 60*60  # 1 hour

cookies = {}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
}
