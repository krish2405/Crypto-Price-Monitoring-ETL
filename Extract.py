import logging
import requests

logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format="%(asctime)s -%(levelname)s- %(message)s"
)

logging.info("Pipeline Started")
logging.info("Extraction begin")

def extract_crypto_prices():
    url= 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd'
    try:
        response=requests.get(url,timeout=5)
        response.raise_for_status()
        # print(response.json())
        return response.json()
    except Exception as E:
        logging.warning(f'Max time retried {E}')
        return None
    



