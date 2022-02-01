from pytrends.request import TrendReq
import datetime 
import pandas as pd

pytrends = TrendReq(hl='en-US', tz=0) 

def collect_keyword_interest(keyword, start_date, end_date):
    df = pytrends.get_historical_interest([keyword], 
                            year_start=start_date.year, month_start=start_date.month, day_start=start_date.day, hour_start=start_date.hour, 
                            year_end=end_date.year, month_end=end_date.month, day_end=end_date.day, hour_end=end_date.hour, 
                            cat=0, sleep=60)
    df = df.reset_index().drop('isPartial', axis=1)
    df.columns = ['date', 'keyword_interest']
    df['keyword'] = keyword
    return df

def coin_keyword_search(keyword, coin_id, start_date, end_date, path_prefix):
    while start_date < end_date:
        df = collect_keyword_interest(keyword, start_date, start_date + datetime.timedelta(days=30))
        df['coin_id'] = coin_id
        full_path = os.path.join(path_prefix,f'{coin_id}-{start_date.year}-{start_date.month}-{start_date.day}.csv')
        df.to_csv(full_path)
        print(f'dataframe saved at {full_path}')
        start_date=start_date + datetime.timedelta(days=30, minutes=1)