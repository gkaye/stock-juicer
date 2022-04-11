import datetime
import time

import alpaca_trade_api
from alpaca_trade_api import Stream
from dash import Dash, dcc, html, Output, Input
import threading
import pandas

from BarManager.BarManager import BarManager
from BarManager.Screener import Screener

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
    sym = 'LUV'
    return {
        'data': [{
            'open': bar_manager.get_bars(sym)['open'],
            'high': bar_manager.get_bars(sym)['high'],
            'low': bar_manager.get_bars(sym)['low'],
            'close': bar_manager.get_bars(sym)['close'],
            'type': 'candlestick'
        }],
        'layout': {
            'title': 'Update #{}'.format(n_intervals)
        }
    }


if __name__ == '__main__':
    threading.Thread(target=lambda: app.run_server(debug=True, use_reloader=False)).start()

    screener = Screener(API_KEY, SECRET_KEY)
    screener.initialize()

    bar_manager = BarManager(API_KEY, SECRET_KEY)
    bar_manager.set_symbols(screener.output_symbols)

    bar_manager.initialize()
