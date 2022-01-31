import requests
import datetime
import time
import copy
import pandas as pd

def get_coin_metadata(coin_id):
    response = requests.get(f'https://api.coingecko.com/api/v3/coins/{coin_id}?tickers=false&market_data=false')
    print(coin_id,':',response.status_code)
    r_dict = response.json()
    new_dict = {}
    top_level_keys = ['id', 'symbol', 'name', 'block_time_in_minutes', 'hashing_algorithm','genesis_date']
    links_keys = ['twitter_screen_name', 'subreddit_url',]
    for key in top_level_keys:
        new_dict[key] = r_dict[key]
    for key in links_keys:
        new_dict[key] = r_dict['links'][key]
    new_dict['description'] = r_dict['description']['en']  
    try: 
        new_dict['github_url'] = r_dict['links']['repos_url']['github'][0]
    except IndexError:
        new_dict['github_url']=None
    return new_dict

def iter_requests(r_count):
    if r_count >= 45:
        print('sleeping for 90 seconds....')
        time.sleep(90)
        return 0
    else:
        return r_count+1

def get_prices(coin_id, currency, start_date, end_date):
    to_unix = lambda x: int(time.mktime(x.timetuple()))

    response = requests.get(f'https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range?vs_currency={currency.lower()}&from={to_unix(start_date)}&to={to_unix(end_date)}')

    r_dict = response.json()
    
    if len(r_dict['prices']) == 0:
        return {'empty_response':True}

    #extract price
    price_df = pd.DataFrame(r_dict['prices'], columns=['date',f'price_{currency}'])
    price_df.set_index('date', inplace=True)

    #extract mcap
    mcap_df = pd.DataFrame(r_dict['market_caps'], columns=['date',f'mcap_{currency}'])
    mcap_df.set_index('date', inplace=True)

    #extract volume
    volume_df = pd.DataFrame(r_dict['total_volumes'], columns=['date',f'volume_{currency}'])
    volume_df.set_index('date', inplace=True)

    merged_df = pd.concat([price_df,mcap_df,volume_df], axis=1).reset_index()
    merged_df['date'] = pd.to_datetime(merged_df['date'],unit='ms')
    merged_df['coin_id'] = coin_id
    
    start_date = merged_df['date'].min()
    end_date = merged_df['date'].max()
    
    if merged_df['date'][1] - merged_df['date'][0] < datetime.timedelta(days=1): frequency = 'hourly'
    else: frequency = 'daily'
    
    print(f'date_range: {start_date}-{end_date}; frequency: {frequency} ; {coin_id}: {response.status_code}')
    
    return {'df': merged_df, 'start_date': start_date, 'end_date':end_date, 'empty_response':False, 'frequency': frequency}


def get_hourly_prices(coin_id, currency, start_date, end_date, start_count=0):
    
    f_start_date = copy.copy(start_date)
    
    df_list = []
    r_count=start_count

    while f_start_date<end_date:
        r = get_prices(coin_id, currency, f_start_date, f_start_date + datetime.timedelta(days=80))
        r_count=iter_requests(r_count)
        if r['empty_response']:
            r = get_prices(coin_id, currency, f_start_date, end_date)    
            r_count=iter_requests(r_count)
        if r['frequency'] == 'daily':
            r = get_prices(coin_id, currency, r['start_date'], r['start_date'] + datetime.timedelta(days=90))
            r_count=iter_requests(r_count)
        if r['frequency'] == 'hourly':
            df_list.append(r['df'])
        f_start_date = r['end_date'] + datetime.timedelta(minutes=1)

    return {'df':pd.concat(df_list), 'request_count':r_count}