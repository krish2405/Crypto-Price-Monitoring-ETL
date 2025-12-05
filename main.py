from Extract import logging,extract_crypto_prices
import json

# Loading metadata
def load_metadata():
    logging.info("Loading metadata")
    with open('crypto_metadata.json','r') as rj:
        metadata=json.load(rj)
        logging.info(f"loaded from file {metadata}")
    return metadata


# Validating data
def validate_data(raw):
    try:
        if not isinstance(raw,list):
            raise ValueError("API did not return a list")
        coin_list=[]
        for coin in raw:
            print(coin)
            if coin.get('id') =="" or coin.get('name')=="" or coin.get('symbol')=="" or coin.get('current_price')==None:
                logging.warning(f"{coin} has mandatory missing value ")
                continue
            else :
                coin_list.append(coin)
        return coin_list

    
    except Exception as E:
        logging.error(f"{E}")


logging.info("hiting api")
data =extract_crypto_prices()
if data :
    validted_data =validate_data(data)
    print(validted_data)
meta=load_metadata()




