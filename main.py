from Extract import logging,extract_crypto_prices
import json
import csv
import datetime

# grouping by price
def pricegroup(price):
    if price>30000:
        return 'High'
    elif price>=5000 and price<= 30000:
        return 'Medium'
    else :
        return 'Low'
    
# transform data
def transform_data(raw,metadata):
    transform_data=[]
    logging.info("Transformation start")
    for coin in raw:
        transform_data.append(
            {
                'crypto_id':coin.get('id'),
                'name':str(coin.get('name')).replace(" ","_"),
                'symbol':str(coin.get('symbol')).upper(),
                "current_price":int(coin.get('current_price')),
                "market_cap":int(coin.get('market_cap')),
                'daily_range':int(coin.get('high_24h')) - int(coin.get('low_24h')),
                'category':metadata.get(coin.get('id'),{}).get('category','Unknown'),
                'launch_year':metadata.get(coin.get('id'),{}).get('launch_year','Unknown'),
                'price_group':pricegroup(int(coin.get('current_price'))),
                'price_change_flag':'Rising' if int(coin.get('current_price')) > (int(coin.get('high_24h'))*.99) else 'No'
            }
        )
    return transform_data

# load to csv

def load_to_csv(transform_data):
    logging.info("Writting in crypto_cleaned.csv")
    with open('crypto_cleaned.csv','w') as cw:
        writer=csv.DictWriter(cw,fieldnames=transform_data[0].keys())
        writer.writeheader()
        writer.writerows(transform_data)
    
    logging.info("Writting in rypto_high_value.cs")
    high_value_coin=[coin for coin in transform_data if coin['price_group']=='High']
    with open('crypto_high_value.csv','w') as hvw:
        writer=csv.DictWriter(hvw,fieldnames=transform_data[0].keys())
        writer.writeheader()
        writer.writerows(high_value_coin)




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

# Archive data

def archive_raw_data(raw):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    with open(f"{timestamp}.json", "w") as wf:
        json.dump(raw, wf, indent=4)


# summary report

def summary_report(transform_data):
    dic = {
        "total_coins": len(transform_data),
        "high_price_count": len([coin for coin in transform_data if coin['price_group'] == 'High']),
        "top_coin_by_market_cap": sorted(transform_data, key=lambda coin: coin['market_cap'], reverse=True)[0]['name'],
        "highest_daily_range_coin": sorted(transform_data, key=lambda coin: coin['daily_range'], reverse=True)[0]['name'],
        "last_updated": datetime.datetime.now().isoformat()
    }

    with open('summary_report.json', 'w') as wf:
        json.dump(dic, wf, indent=4)



def start_etl():
    logging.info("hiting api")
    data =extract_crypto_prices()
    if data :
        validated_data =validate_data(data)
    meta=load_metadata()
    transformed_data=transform_data(validated_data,meta)

    load_to_csv(transformed_data)
    archive_raw_data(data)
    summary_report(transformed_data)

start_etl()

logging.info('Etl Completed')



