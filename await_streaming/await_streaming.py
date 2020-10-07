import asyncio
import json
import os
import gcsfs
import pickle
from tda import auth
from tda.streaming import StreamClient
from selenium import webdriver
from google.cloud import pubsub_v1
from google.cloud import storage


def get_client():
    if os.path.exists('./tmp/token.pickle'):
        client = auth.client_from_token_file(token_path='./tmp/token.pickle',
                                             api_key=tda_key)
    else:
        driver = webdriver.Chrome()
        client = auth.client_from_login_flow(webdriver=driver,
                                             api_key=tda_key,
                                             redirect_url='https://127.0.0.1',
                                             token_path='./tmp/token.pickle')

    streaming_client = StreamClient(client)
    return streaming_client


def get_stocks():
    store = storage.Client()
    fs = gcsfs.GCSFileSystem(project=store.project)
    with fs.open('fin-aml/ref/artifacts.pickle', 'rb') as file:
        artifacts = pickle.load(file)

    stock_list = list(artifacts['stocks'].keys())
    del artifacts
    return stock_list


def publish_to_equities_pubsub(msg):
    topic_path = publisher.topic_path(project_name, 'live_equities')
    data = json.dumps(msg).encode('utf-8')

    future = publisher.publish(topic_path, data)
    print(f"{msg['service']} for {msg['content'][0]['key']} with future result {future.result()}")


def publish_to_futures_pubsub(msg):
    topic_path = publisher.topic_path(project_name, 'live_futures')
    data = json.dumps(msg).encode('utf-8')

    future = publisher.publish(topic_path, data)
    print(f"{msg['service']} for {msg['content'][0]['key']} with future result {future.result()}")


async def read_stream():
    await stream_client.login()
    await stream_client.quality_of_service(StreamClient.QOSLevel.EXPRESS)
    await stream_client.timesale_equity_subs(symbols=stocks)
    if len(futures) > 0:
        await stream_client.timesale_futures_subs(symbols=futures)

    stream_client.add_timesale_equity_handler(publish_to_equities_pubsub)

    stream_client.add_timesale_futures_handler(publish_to_futures_pubsub)

    while True:
        await stream_client.handle_message()


if __name__ == "__main__":
    # set project and topic
    project_name = os.environ.get('GCS_PROJECT_ID')
    tda_key = os.environ.get('TDAMERITRADE')

    stream_client = get_client()
    publisher = pubsub_v1.PublisherClient()

    # get list of stocks to subscribe to
    stocks = get_stocks()
    futures = []

    asyncio.get_event_loop().run_until_complete(read_stream())
