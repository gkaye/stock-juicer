import threading
import alpaca_trade_api
from alpaca_trade_api.stream import Stream
import time

ALPACA_API_KEY = 'AKCBVDALZTXXE0NJ7LMV'
ALPACA_SECRET_KEY = 'OgCPIyKsZ9g1iLrmU89uKcWUCPW59E8Vrc6bGzwz'


delays = []
async def print_trade(t):
    global delays
    # print('trade', t)
    trade_time = t.timestamp.timestamp()
    current_time = time.time()

    delay = current_time - trade_time

    delays.append(delay)

    # if t.symbol == "CWRK":
    #     print(t)
    if len(delays) % 5000 == 0:
        delays = delays[-5000:]
        print(f'Avg delay: {sum(delays) / len(delays)}')

async def print_quote(q):
    print('quote', q)


async def print_bar(bar):
    print('bar', bar)


def consumer_thread():
    conn = Stream(ALPACA_API_KEY,
                  ALPACA_SECRET_KEY,
                  data_feed='sip')

    alpaca = alpaca_trade_api.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    assets = alpaca.list_assets()
    symbols = [asset.symbol for asset in assets if asset.tradable]
    print(str(len(symbols)) + " symbols")

    # conn.subscribe_quotes(print_quote, 'AAPL')
    # conn.subscribe_bars(print_bar, 'AAPL')
    conn.subscribe_trades(print_trade, *symbols)
    conn.run()

if __name__ == '__main__':
    threading.Thread(target=consumer_thread).start()
