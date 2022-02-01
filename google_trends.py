from pytrends.request import TrendReq
import datetime 
import os
import time
import pandas as pd

pytrends = TrendReq(hl='en-US', tz=0) 

def collect_keyword_interest(keyword, start_date, end_date):
    df = pytrends.get_historical_interest([keyword], 
                            year_start=start_date.year, month_start=start_date.month, day_start=start_date.day, hour_start=start_date.hour, 
                            year_end=end_date.year, month_end=end_date.month, day_end=end_date.day, hour_end=end_date.hour, 
                            cat=0, sleep=60)
    df = df.reset_index()[['date',keyword]]
    df.columns = ['date', 'keyword_interest']
    df['keyword'] = keyword
    return df

def coin_keyword_search(keyword, coin_id, start_date, end_date, path_prefix):
    df = collect_keyword_interest(keyword, start_date, end_date)
    df['coin_id'] = coin_id
    clean_kw = keyword.replace(' ','')
    full_path = os.path.join(path_prefix,f'{coin_id}-{clean_kw}-{start_date.year}-{start_date.month}-{start_date.day}.csv')
    df.to_csv(full_path)
    row_count = len(df)
    print(f'**********************************dataframe saved at {full_path} with {row_count} rows')
    return row_count

def main():
    start_date =  datetime.datetime(2021,6,1,0,0,0) 
    end_date = datetime.datetime.now()
    coin_meta_data = pd.read_csv('./input_data/coin_meta_data.csv').to_dict('records')
    out_path_prefix = 'input_data/google_trends_data/'
    total_count = 0
    for coin in coin_meta_data:
        ticker = coin['symbol']
        total_count+=coin_keyword_search(keyword = f'{ticker} token'
                            , coin_id = coin['id']
                            , start_date = start_date
                            , end_date = end_date
                            , path_prefix = out_path_prefix)
        print(f'{total_count} rows collected sleeping for 90 seconds...........................')
        time.sleep(90)
        total_count+=coin_keyword_search(keyword = f'{ticker} coin'
                        , coin_id = coin['id']
                        , start_date = start_date
                        , end_date = end_date
                        , path_prefix = out_path_prefix)
        print(f'{total_count} rows collected sleeping for 90 seconds...........................')
        time.sleep(90)
    print(f'-------------------------***{total_count}***------------------------- total rows collected')

if __name__ == "__main__":
    main()