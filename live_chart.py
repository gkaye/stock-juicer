import datetime
import time

import alpaca_trade_api
from alpaca_trade_api import Stream
from dash import Dash, dcc, html, Output, Input
import threading
import pandas

API_KEY = 'AKCBVDALZTXXE0NJ7LMV'
SECRET_KEY = 'OgCPIyKsZ9g1iLrmU89uKcWUCPW59E8Vrc6bGzwz'

app = Dash('Live chart')
app.layout = html.Div([
    dcc.Graph(id='graph', style={'width': '100vh', 'height': '100vh'}),
    dcc.Interval(
        id='chart-interval-component',
        interval=200,
        n_intervals=0
    ),
])


@app.callback(Output('graph', 'figure'), [Input('chart-interval-component', 'n_intervals')])
def update_graph(n_intervals):
    sym = 'TSLA'
    return {
        'data': [{
            'open': bars[sym]['open'],
            'high': bars[sym]['high'],
            'low': bars[sym]['low'],
            'close': bars[sym]['close'],
            'type': 'candlestick'
        }],
        'layout': {
            'title': 'Update #{}'.format(n_intervals)
        }
    }


async def trades_callback(t):
    # print(type(t.timestamp))
    ts = pandas.to_datetime(t.timestamp, unit='ns').replace(second=0, microsecond=0, nanosecond=0).tz_convert('utc') + datetime.timedelta(minutes=1)
    # ts = pandas.to_datetime(t.timestamp, unit='ns').replace(second=0, microsecond=0, nanosecond=0) + datetime.timedelta(minutes=1)
    # print(ts)
    # print(ts.timezone)
    # bars[ts]
    # print(bars.iloc[-1].name.tz)
    # print(f'tz {bars.iloc[-1].index.tz}')

    barz = bars[t.symbol]
    if ts not in barz.index:
        barz.loc[ts, 'open'] = t.price
        barz.loc[ts, 'high'] = t.price
        barz.loc[ts, 'low'] = t.price
        barz.loc[ts, 'close'] = t.price
        barz.loc[ts, 'volume'] = t.size
        barz.loc[ts, 'symbol'] = t.symbol
    else:
        if t.price > barz.loc[ts, 'high']:
            barz.loc[ts, 'high'] = t.price
        if t.price < barz.loc[ts, 'low']:
            barz.loc[ts, 'low'] = t.price
        barz.loc[ts, 'close'] = t.price
        barz.loc[ts, 'volume'] = barz.loc[ts, 'volume'] + t.size

if __name__ == '__main__':
    print('Main thread started')
    threading.Thread(target=lambda: app.run_server(debug=True, use_reloader=False)).start()

    alpaca = alpaca_trade_api.REST(API_KEY, SECRET_KEY)

    now = datetime.datetime.now(datetime.timezone.utc)

    past = now - datetime.timedelta(minutes=50)
    past = past.replace(second=0, microsecond=0)
    past_iso = past.isoformat()

    bars = {}
    bars['UPST'] = alpaca.get_bars('UPST', alpaca_trade_api.TimeFrame(1, alpaca_trade_api.TimeFrameUnit('Min')), past_iso, None).df
    bars['TSLA'] = alpaca.get_bars('TSLA', alpaca_trade_api.TimeFrame(1, alpaca_trade_api.TimeFrameUnit('Min')), past_iso, None).df


    # bars.iloc[-1].at['close'] = 1338
    # print(bars.iloc[-1])
    #
    # # print(bars.at[-1, 'close'])
    # # print(bars.tail(2).to_string())
    # print(type(bars.tail(1).index[0]))
    stream = Stream(API_KEY, SECRET_KEY, data_feed='sip')

    stream.subscribe_trades(trades_callback, 'UPST')
    print('RUNNIN')
    threading.Thread(target=lambda: stream.run()).start()
    print('I CANT GET HERE!')
    time.sleep(1)
    stream.stop()
    time.sleep(1)

    stream.subscribe_trades(trades_callback, 'UPST')
    stream.subscribe_trades(trades_callback, 'TSLA')
    stream.run()
