
import coingecko
import pandas as pd
import datetime


osmosis_coins = ['osmosis','cosmos','terrausd','terra-luna','juno-network','stargaze','secret','comdex','crypto-com-chain','akash-network','ion','sentinel','chihuahua-token','e-money-eur','regen','persistence','lum-network','e-money','bitcanna','iris-network','desmos','ki','bitsong','likecoin','cheqd-network','ixo','starname','vidulum','microtick']
top_coins = ['bitcoin','ethereum', 'binancecoin', 'cardano', 'solana', 'ripple','polkadot','dogecoin','avalanche-2','shiba-inu','matic-network','crypto-com-chain']
all_coins = osmosis_coins+top_coins

all_meta_data = []

for coin in all_coins:
    all_meta_data.append(coingecko.get_coin_metadata(coin))
    
meta_df = pd.DataFrame(all_meta_data)
meta_csv_path = 'data/coin_meta_data.csv'
meta_df.to_csv(meta_csv_path,index=False)

print(f'meta data collected, saved to {meta_csv_path}')

coin_ids = list(meta_df['id'])


start_date = datetime.datetime(2000, 1,1) - datetime.timedelta(hours=8) #to fix strange timezone error
end_date = datetime.datetime.now()

r_count = len(coin_ids)

all_coins_dfs = []

for coin_id in coin_ids:
    response = coingecko.get_hourly_prices(coin_id, 'usd', start_date, end_date, r_count)
    r_count = response['request_count']
    all_coins_dfs.append(response['df'])
        
prices_df = pd.concat(all_coins_dfs)
prices_path = 'data/coin_prices.csv'
prices_df.to_csv(prices_path,index=False)

print(f'price data collected, saved to {prices_path}, total of {len(prices_df)} rows')
