import requests
import pandas as pd 


def run_crypto_etl():

    # Cryptocurrency market data
    url = "https://api.coinstats.app/public/v1/markets?coinId=bitcoin"

    response = requests.request("GET", url)

    json_resp = response.json()

    list = []
    limit = 201

    for crypto in json_resp[:limit]:
        refined_crypto = {"price": crypto['price'],
                        'exchange' : crypto['exchange'],
                        'pair' : crypto['pair'],
                        'pairPrice' : crypto['pairPrice'],
                        'volume' : crypto['volume'],
                        }  
        list.append(refined_crypto)

    df = pd.DataFrame(list)
    df.to_csv('refined_crypto.csv')  

    print(list)

def main():
    run_crypto_etl()

if __name__ == "__main__":
    main()

