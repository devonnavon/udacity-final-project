# Repository Overview

## Set Up

To set up the repository to ingest data and run the etls the following bash command will need to be run. This sets up the directories that that the raw data lands in on ingest and the that the final model lands in after the ETL.

```console
$ mkdir input_data output_data input_data/coin_price_data input_data/google_trends_data
```

## Structure

coingecko.py - this script pulls in price, market, and meta data for a subset of crypto currencies
google_trends.py - this script pulls in google trend data using coin meta data collected in the above step, it searchs two key words per coin ()
etl.py - this script runs the ETL on the raw coingecko and google trends data, using spark to transform them into our final data model
summary.ipynb - because I didn't want to upload the huge data files to github, I've included a summary notebook which shows the final counts and schema of data ingested and our final data model.

To run the ingest scripts and ETL copy the following bash commands (they must be run in this order)

```console
$ python coingecko.py
$ python google_trends.py
$ python etl.py
```

When running the above scripts, it might make sense to run coingecko.py and wait until coin meta data has been ingested (a notification will be printed in the terminal), then run google_trends.py as this will allow them to run in parrallel. This isn't an issue on a single machine because the major bottle neck is API rate limits rather than download speed.

# Projects Steps

## Initial Projet Idea

- My initial project idea is to create a datalake that could be used to build machine learning models with the goal of predicting crypto currency prices
- I'd like to predict prices for a subset of coins that are traded on the Osmosis exchange with the long term goal of building a trading algorithm
  - This is a decentralized exchange that has very low fees
  - I've also decided to collect prices for top coins as these could be used to create general market indicators (more model inputs)

## Project Scope

- After exploring a variety of options for data providers I've decided to use Coingecko's API (documentation can be found <a href='https://www.coingecko.com/en/api/documentation?'>here</a>)
  - Coingeckos API is ideal because it allows me to collect hourly prices as opposed to daily only
  - It also has good documentationa and high rate limits for the free tier allowing me to collect a lot of data quickly
- As a second data source to help inform modeling I've decided to use <a href='https://trends.google.com/trends/?geo=US'>Google Trends</a>
  - This is a good source that tracks overall popularity of search keywords over time
  - There is also a <a href='https://pypi.org/project/pytrends'>python api</a> that makes collecting trend data relatively easy

## Modeling and Technology Decisions

- The purpose of the final data model is to provide this data in an easily accessible form for performing data science research
- As the first step in the modeling process a **data lake** makes more sense than a **data warehouse**, because it will primarly be used for research purposes
- The final tables will be saved in parquet files as their purpose is to be accessed by a data scientist who will likely need to use spark to ingest them anyways
- Once a production ready model is built then it will make more sense to build more excplicit pipelines which take care of feature engineering

## Addressing Other Scenarios

If the data was increased by 100x I'd make the following changes:

- Because I'm ingesting data from APIs it would make sense to upgrade to enterprise tier access so I could have higher rate limits
- Rather than ingesting off one machine, it would make sense to use Airflow to run the ingest scripts and collect data for multiple coins in parrallel
- I'd also have to change my partitioning strategy for saving the data (rather than save data for one coin all together, parition the files by date for example)
- For my ETL process because I wrote it in Spark I wouldn't have to make major changes to the code, but I would want ot run the script on EMR so that I could take advantage of Spark's full power using multiple machines

If the pipelines would be run on a daily basis by 7 am every day I'd make the following changes:

- rather than ingesting all the data at once, I'd want to check the last recorded price or google trend data and fetch only data between the last record and the current timestamp
- I'd need more context on the by 7 am timeline, if data needed to be as up to date as possible (last price 6am) I'd want to run the pipelines in the middle of the night and again at 4am and again at 6am (depending on average run time), since I'm only fetching the most recent data running the pipeline multiple times would allow the process to run quickly and ensure data is as up to date as possible

If the database needed to be accessed by 100+ people I'd make the following changes:

- Context is again important here, it depends who those people are and what they're using the data for
- Its likely there would be a variety of roles accessing it so it would probably make sense to build a warehouse downstream from the datalake
  - this would allow Data and Business Analysts to access the data using SQL
- On top of the data warehouse we'd want to build dashboards using a technology like looker to give non techincal users access to the data

## Schema

### Fact Tables

I chose to use two fact tables. The primary purpose of this data lake is as a research tool, while I could have combined them it would have meant cutting out specific time stamps for coin prices (as we'd need the exact hour to join to google trends). The two tables can be easily joined by using the hour field of the time table. Keeping them separate will allow the data scientists to more easily understand the two data sources on their own. Only after they are understood and some feature engineering decisions have been made does it make sense to combine the two tables into one fact table.

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

### Data Dictionary

#### coin_metrics

- coin_id - a slug that uniquely identifies different crypto currencies
- recorded_at - the time that the corresponding metrics were recorded at
- currency - a three letter currency code that the price, market cap and volume are stated in
- price - the price of the coin at the corresponding recorded_at time
- market_cap - the market cap (total value or price \* supply) of the coin at the corresponding recorded_at time
- volume - the trading volume of the coin at the corresponding recorded_at time

#### google_trends

- coin_id - a slug that uniquely identifies different crypto currencies
- recorded_at - time that the corresponding trend value was recorded at
- keyword - the actual google search keyword
- trend_value - the scaled popularity of the keyword at that time (between 0 and 100)

#### coins

- coin_id - a slug that uniquely identifies different crypto currencies
- ticker - the symbol or code associated to the crypto currency (as seen on exchanges)
- name - the crypto currencies full name
- description - a description of the currencies utility
- twitter_account - the account handle associated with the coin
- subreddit_url - the coin's main subreddit page
- github_url - a url that links to the projects github page

#### time

- recorded_at - the raw timestamp corresponding to either the coin_metrics table or google_trends table
- hour - hour of the recorded_at timestamp
- day - day of the recorded_at timestamp
- week - week of the recorded_at timestamp
- month - month of the recorded_at timestamp
- year - year of the recorded_at timestamp
- weekday - weekday of the recorded_at timestamp
