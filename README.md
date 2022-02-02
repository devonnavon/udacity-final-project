# udacity-final-project

## Schema
### Fact Tables
**coin_metrics** - records of a coins prices, volume, and marketcap each hour 
- coin_id - string
- recorded_at - timestamp
- currency - string
- price - float
- market_cap - float
- volume - float

**google_trends** - records of google trends scores for different keywords
- coin_id - string
- recorded_at - timestamp
- keyword - string
- trend_value - integer

### Dimension Tables 
**coins** - coin meta data
- coin_id - string
- ticker - string
- name - string
- description - string
- twitter_account - string
- subreddit_url - string
- github_url - string

**time** - timestamps of records in coin_metrics and google_trends broken down into specific units 
- recorded_at - timestamp
- hour - time interval
- day - time interval
- week - time interval
- month - time interval
- year - time interval
- weekday - time interval