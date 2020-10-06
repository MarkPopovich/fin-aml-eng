#! python /usr/local/settings/pythonstartup.py
from tda import auth
from tda.streaming import StreamClient
from selenium import webdriver
import asyncio
import json
import os

tda_key = os.environ['TDAMERITRADE']

if os.path.exists('./tmp/token.pickle'):
    Client = auth.client_from_token_file(token_path = './tmp/token.pickle',
                                         api_key = tda_key)
else:
    driver = webdriver.Chrome()
    Client = auth.client_from_login_flow(webdriver = driver,
                                api_key = tda_key,
                                redirect_url = 'https://127.0.0.1',
                                token_path = './tmp/token.pickle')

stream_client = StreamClient(Client)

# TO DO
# load list of equities
# load list of futures
# write publish_to_pub_sub function

async def read_stream():
    await stream_client.login()
    await stream_client.quality_of_service(StreamClient.QOSLevel.EXPRESS)
    await stream_client.timesale_equity_subs(symbols= ['GOOG'])
    await stream_client.timesale_futures_subs(symbols = ['GC'])

    stream_client.add_timesale_futures_handler(
            lambda msg: print(json.dumps(msg, indent=4)))

    stream_client.add_timesale_futures_handler(
            lambda msg: print(json.dumps(msg, indent=4)))

    while True:
        await stream_client.handle_message()

asyncio.get_event_loop().run_until_complete(read_stream())