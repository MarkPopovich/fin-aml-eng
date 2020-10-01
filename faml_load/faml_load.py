import pickle
import requests
import pandas as pd
import sys
import time
from google.cloud import bigquery
from urllib.error import HTTPError
from datetime import datetime

def load_artifacts():
    with open('./tmp/artifacts.pickle', 'rb') as f:
        artifacts = pickle.load(f)

    stocks = artifacts['stocks']
    days = artifacts['days']
    key_map = artifacts['key_map']
    del(artifacts)

    return stocks, days, key_map

def save_artifacts(stocks, day_tracker, key_map):
    with open('./tmp/artifacts.pickle', 'rb') as f:
        artifacts = pickle.load(f)

    artifacts['stocks'] = stocks
    artifacts['days'] = day_tracker
    artifacts['key_map'] = key_map

    with open('./tmp/artifacts.pickle', 'wb') as f:
        pickle.dump(artifacts, f)

def call_historical(ticker, date, time=None, limit=50000, i=None):
    '''
    ticker: instructment symbol, :str:
    date: day, :str:, '2020:05:15'
    time: timestamp
    limit: int, max 50000, default 50000
    '''
    url = f'https://api.polygon.io/v2/ticks/stocks/trades/{ticker}/{date}'
    params = {
        'apiKey': '0URbzTqnNwTsHIe4UPz8AjazYT9vYFNq',
        'timestamp': time,
        'limit': limit,
    }

    try:
        res = requests.get(url, params).json()
    except HTTPError:
        if i == None:
            i = 1
        rng = float(random.randint(1,100)) / 100
        time.sleep(i + rng)
        i = i * 2
        res = call_historical(ticker, date, time, limit, i)

    return res

def format_and_save(json, file_path=None):
    #print(json)
    df = pd.DataFrame(json['results'])

    # map the column names
    df = df.rename(key_map, axis=1)

    # add the ticker symbol
    df['symbol'] = stock

    # ensure correct types for each column
    df['sip_timestamp'] = pd.to_datetime(df['sip_timestamp'])
    df['participant_timestamp'] = pd.to_datetime(df['participant_timestamp'])
    df['id'] = df['id'].astype('int')
    df['sequence_number'] = df['sequence_number'].astype('int')
    df['exchange'] = df['exchange'].astype('int')
    df['size'] = df['size'].astype('int')
    df['price'] = df['price'].astype('float')
    df['tape'] = df['tape'].astype('int')
    df['symbol'] = df['symbol'].astype('O')

    # use only the cols as specified
    df = df[cols]
    if file_path:
        df.to_csv(file_path, index=False)
    elif file_path is None:
        return df


def load_from_csv(file_path, table_id):
    '''
    table_id = "your-project.your_dataset.your_table_name"
    '''
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def load_from_pandas(df, destination_table):
    '''
    Uses pandas-gbq client to load directly to bigquery, skipping the need to save csv's

    Specify a schema to ensure types read correctly

    destination_table: Name of table to be written, in the form dataset.tablename
    project_id: Google BigQuery Account project ID. Optional when available from the environment.

    schema = [
        bigquery.SchemaField("sip_timestamp", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("participant_timestamp", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("sequence_number", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("exchange", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("size", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("price", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("tape", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("symbol", "STRING", mode="NULLABLE"),
    ]
    '''

    schema = [
        {'name': 'sip_timestamp', 'type': 'TIMESTAMP'},
        {'name': 'participant_timestamp', 'type': 'TIMESTAMP'},
        {'name': 'sequence_number', 'type': 'INTEGER'},
        {'name': 'id', 'type': 'INTEGER'},
        {'name': 'exchange', 'type': 'INTEGER'},
        {'name': 'size', 'type': 'INTEGER'},
        {'name': 'price', 'type': 'FLOAT'},
        {'name': 'tape', 'type': 'INTEGER'},
        {'name': 'symbol', 'type': 'STRING'},
    ]

    df.to_gbq(destination_table, table_schema=schema, if_exists='append')

def save_and_reset(df, destination_table):
    '''
    Saves the current df to the BigQuery table and empties it in memory
    '''
    load_from_pandas(df, destination_table)
    save_artifacts(stocks, day_tracker, key_map)
    return pd.DataFrame()

if __name__ == "__main__":

    client = bigquery.Client()
    key = '0URbzTqnNwTsHIe4UPz8AjazYT9vYFNq'

    limit = 50000
    cols = ['sip_timestamp', 'participant_timestamp', 'sequence_number', 'id', 'exchange', 'size', 'price', 'tape',
            'symbol']

    stocks, day_tracker, key_map = load_artifacts()
    destination_table = 'sp500historical.csv_historic'
    df = pd.DataFrame()

    for day in day_tracker['days']:
        if day in day_tracker['days_complete']:
            print(f'Day already complete: {day}')
            continue

        today = datetime.strptime(day, '%Y-%m-%d')
        print(f'Starting to pull {day}')
        for stock in stocks:
            ## complete the next block iteratively until all the data for that day is gathered
            ## complete = False
            ##
            if 'days_complete' in stocks[stock]:
                if day in stocks[stock]['days_complete']:
                    print(f'Already complete: {stock}')
                    continue

            if 'namechange' in stocks[stock]:
                if today < stocks[stock]['namechange']['date']:
                    stock_name = stocks[stock]['namechange']['beforename']
                elif today >= stocks[stock]['namechange']['date']:
                    stock_name = stocks[stock]['namechange']['as_of_name']
            elif 'namechange' not in stocks[stock]:
                stock_name = stock

            while True:
                # grab the starttime if an end_timestamp exists, else set the starttime as None
                if stocks[stock]['timestamp_tracker']['end_timestamp'] is None:
                    starttime = None
                elif stocks[stock]['timestamp_tracker']['end_timestamp']:
                    starttime = stocks[stock]['timestamp_tracker']['end_timestamp']

                # get the json for the stock on the day with the correct symbol and starttime
                print(f'{stock} -> {stock_name}')
                print(today)
                print(starttime)
                print(limit)
                json = call_historical(stock_name, day, time=starttime, limit=limit)

                # format the json and save as csv
                df = pd.concat([df, format_and_save(json, file_path=None)])
                # insert the json in the bigquery table

                #load_from_pandas(df, 'sp500historical.csv_historic')
                # TO DO: BigQuery only accepts 1000 loads per day, so I need to store more information locally before pushing it.
                # What is the best way to do this?
                # I need to keep in mind that I have to track what is happening
                # Currently, there are 3 levels of saves, and 1 push in the very center.
                # I do not know if memory can handle a full day. It would be nice. But its about 1 GB.
                # I could try it, but its a risk, no?
                # If so, I would move the redundant saves out to the day level and only save artifacts once
                # If I don't want to hold it all in memory, I could locally append to a csv, then use the load_from_csv to push a larger csv directly
                # Based on the documnetation, it looks like the best way would be to save each csv individually, then create a load job to load all of them simultaneously, and when that completes delete the local data

                # Abandoning loading from csv, keeping call for posterity
                # load_from_csv('historic_csv.csv', 'sp500historical.csv_historic')

                # set the end_timestamp as the last tick
                stocks[stock]['timestamp_tracker']['end_timestamp'] = json['results'][-1]['t']

                # if the start timestamp is None, create an entry for it
                if stocks[stock]['timestamp_tracker']['start_timestamp'] is None:
                    stocks[stock]['timestamp_tracker']['start_timestamp'] = json['results'][0]['t']

                if len(json['results']) < limit:
                    if 'days_complete' in stocks[stock]:
                        stocks[stock]['days_complete'].append(day)
                    elif 'days_complete' not in stocks[stock]:
                        stocks[stock]['days_complete'] = [day]

                if sys.getsizeof(df) > 50000000:
                    df = save_and_reset(df, destination_table)

                if len(json['results']) < limit:
                    break
                    #save_artifacts(stocks, day_tracker, key_map)
                    #break

                #save_artifacts(stocks, day_tracker, key_map)

        day_tracker['days_complete'].append(day)

        #save_artifacts(stocks, day_tracker, key_map)
