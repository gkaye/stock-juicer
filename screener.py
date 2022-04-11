import threading

from BarManager.Screener import Screener
import time
from dash import Dash, dash_table, dcc, html, Output, Input

API_KEY = 'AKCBVDALZTXXE0NJ7LMV'
SECRET_KEY = 'OgCPIyKsZ9g1iLrmU89uKcWUCPW59E8Vrc6bGzwz'


# HARD START TEST
# if __name__ == '__main__':
#     bar_store = BarManager(API_KEY, SECRET_KEY)
#
#     bar_store.set_time_override("2022-03-30T19:59:00+00:00")
#
#     #0
#     start = time.time()
#     bar_store.fetch_symbols()
#     bar_store.fetch_historical_bars(15)
#     bar_store.calc_all_derivative_values()
#     bar_store.filter_by_derivative_values()
#     bar_store.fetch_spread()
#     bar_store.filter_by_spread()
#     end = time.time()
#     print(f'0# took {end - start} seconds')
#
#     #1
#     start = time.time()
#     bar_store.advance_time_override()
#     bar_store.fetch_symbols()
#     bar_store.fetch_historical_bars(1)
#     bar_store.calc_all_derivative_values()
#     bar_store.filter_by_derivative_values()
#     bar_store.fetch_spread()
#     bar_store.filter_by_spread()
#     end = time.time()
#     print(f'1# took {end - start} seconds')
#
#     #2
#     start = time.time()
#     bar_store.advance_time_override()
#     bar_store.fetch_symbols()
#     bar_store.fetch_historical_bars(1)
#     bar_store.calc_all_derivative_values()
#     bar_store.filter_by_derivative_values()
#     bar_store.fetch_spread()
#     bar_store.filter_by_spread()
#     end = time.time()
#     print(f'2# took {end - start} seconds')
#
#     #3
#     start = time.time()
#     bar_store.advance_time_override()
#     bar_store.fetch_symbols()
#     bar_store.fetch_historical_bars(1)
#     bar_store.calc_all_derivative_values()
#     bar_store.filter_by_derivative_values()
#     bar_store.fetch_spread()
#     bar_store.filter_by_spread()
#     end = time.time()
#     print(f'3# took {end - start} seconds')
#
#     #4
#     start = time.time()
#     bar_store.advance_time_override()
#     bar_store.fetch_symbols()
#     bar_store.fetch_historical_bars(1)
#     bar_store.calc_all_derivative_values()
#     bar_store.filter_by_derivative_values()
#     bar_store.fetch_spread()
#     bar_store.filter_by_spread()
#     end = time.time()
#     print(f'4# took {end - start} seconds')
#
#     print(f'Active symbols end length: {len(bar_store.active_symbols)}')
#     print(f'TSLA SPREAD: {bar_store.symbol_to_spread["TSLA"]}')



app = Dash('Screener')
app.layout = html.Div([
    html.H4('Screener Results', id='screener-table-header-text'),
    html.H5(id='screener-last-update-time-text'),
    dash_table.DataTable(id='screener-table'),
    dcc.Interval(
        id='screener-table-interval-component',
        interval=1 * 1000,
        n_intervals=0
    ),
    dcc.Interval(
        id='screener-last-update-time-text-interval-component',
        interval=1 * 1000,
        n_intervals=0
    )
])


@app.callback(Output('screener-table', 'data'),
              Output('screener-table', 'columns'),
              Input('screener-table-interval-component', 'n_intervals'))
def update_metrics(n):
    return bar_store.pretty_output.to_dict('records'), [{"name": i, "id": i} for i in bar_store.pretty_output.columns]


@app.callback(Output('screener-last-update-time-text', 'children'),
              Input('screener-last-update-time-text-interval-component', 'n_intervals'))
def update_metrics(n):
    last_update_time = bar_store.pretty_output_last_update_time
    if not last_update_time:
        elapsed_string = 'Never updated'
    else:
        elapsed_string = f'{(time.time() - bar_store.pretty_output_last_update_time):.0f} seconds ago'
    return f'Last update time: {elapsed_string}'


if __name__ == '__main__':
    print('Main thread started')
    threading.Thread(target=lambda: app.run_server(debug=True, use_reloader=False)).start()

    # BAR MANAGER START --------------------------------------------
    bar_store = Screener(API_KEY, SECRET_KEY)

    # bar_store.set_time_override("2022-04-01T19:50:00+00:00")

    bar_store.initialize()

    # #0
    # start = time.time()
    # bar_store.fetch_symbols()
    # bar_store.fetch_historical_bars(50)
    # bar_store.calc_all_derivative_values()
    # bar_store.filter_by_derivative_values()
    # bar_store.fetch_spread()
    # bar_store.filter_by_spread()
    # bar_store.generate_pretty_output()
    # end = time.time()
    # print(f'0# took {end - start} seconds')
    #
    # #1
    # start = time.time()
    # bar_store.advance_time_override()
    # bar_store.fetch_symbols()
    # bar_store.fetch_historical_bars(1)
    # bar_store.calc_all_derivative_values()
    # bar_store.filter_by_derivative_values()
    # bar_store.fetch_spread()
    # bar_store.filter_by_spread()
    # bar_store.generate_pretty_output()
    # end = time.time()
    # print(f'1# took {end - start} seconds')
    # BAR MANAGER END ----------------------------------------------











# BAR MANAGER START --------------------------------------------
#     bar_store = BarManager(API_KEY, SECRET_KEY)
#
#     bar_store.set_time_override("2022-03-30T19:59:00+00:00")
#
#     #0
#     start = time.time()
#     bar_store.fetch_symbols()
#     bar_store.fetch_historical_bars(15)
#     bar_store.calc_all_derivative_values()
#     bar_store.filter_by_derivative_values()
#     bar_store.fetch_spread()
#     bar_store.filter_by_spread()
#     bar_store.generate_pretty_output()
#     end = time.time()
#     print(f'0# took {end - start} seconds')
# BAR MANAGER END ----------------------------------------------


# Dash table

# if __name__ == '__main__':
#     bar_store = BarManager(API_KEY, SECRET_KEY)
#
#     bar_store.set_time_override("2022-03-30T19:59:00+00:00")
#
#     #0
#     start = time.time()
#     bar_store.fetch_symbols()
#     bar_store.fetch_historical_bars(15)
#     bar_store.calc_all_derivative_values()
#     bar_store.filter_by_derivative_values()
#     bar_store.fetch_spread()
#     bar_store.filter_by_spread()
#     bar_store.generate_pretty_output()
#     end = time.time()
#     print(f'0# took {end - start} seconds')
#
#     #1
#     start = time.time()
#     bar_store.advance_time_override()
#     bar_store.fetch_symbols()
#     bar_store.fetch_historical_bars(1)
#     bar_store.calc_all_derivative_values()
#     bar_store.filter_by_derivative_values()
#     bar_store.fetch_spread()
#     bar_store.filter_by_spread()
#     bar_store.generate_pretty_output()
#     end = time.time()
#     print(f'1# took {end - start} seconds')
#
#     #2
#     start = time.time()
#     bar_store.advance_time_override()
#     bar_store.fetch_symbols()
#     bar_store.fetch_historical_bars(1)
#     bar_store.calc_all_derivative_values()
#     bar_store.filter_by_derivative_values()
#     bar_store.fetch_spread()
#     bar_store.filter_by_spread()
#     bar_store.generate_pretty_output()
#     end = time.time()
#     print(f'2# took {end - start} seconds')
#
#     #3
#     start = time.time()
#     bar_store.advance_time_override()
#     bar_store.fetch_symbols()
#     bar_store.fetch_historical_bars(1)
#     bar_store.calc_all_derivative_values()
#     bar_store.filter_by_derivative_values()
#     bar_store.fetch_spread()
#     bar_store.filter_by_spread()
#     bar_store.generate_pretty_output()
#     end = time.time()
#     print(f'3# took {end - start} seconds')
#
#     #4
#     start = time.time()
#     bar_store.advance_time_override()
#     bar_store.fetch_symbols()
#     bar_store.fetch_historical_bars(1)
#     bar_store.calc_all_derivative_values()
#     bar_store.filter_by_derivative_values()
#     bar_store.fetch_spread()
#     bar_store.filter_by_spread()
#     bar_store.generate_pretty_output()
#     end = time.time()
#     print(f'4# took {end - start} seconds')
#
#     print(f'Active symbols end length: {len(bar_store.active_symbols)}')
#     print(f'TSLA SPREAD: {bar_store.symbol_to_spread["TSLA"]}')






##############################################################################################
# HIST BARS
##############################################################################################
# bar_store = BarManager(API_KEY, SECRET_KEY)
# # bar_store = BarManager(API_KEY, SECRET_KEY, "2022-03-25T13:30:00+00:00")
# # bar_store = BarManager(API_KEY, SECRET_KEY, "2022-03-27T20:00:00+00:00")
# bar_store.set_symbols(["TSLA", ])
# bar_store.fetch_historical_bars(2)
# # bar_store.set_time_override("2022-03-25T13:38:00Z")
# # bar_store.fetch_historical_bars(4)
# # bar_store.set_time_override("2022-03-25T13:32:00Z")
# # bar_store.fetch_historical_bars(3)
#
#
# for k in bar_store.symbol_to_bars:
#     print(f'THIS IS: {k}')
#     print(bar_store.symbol_to_bars[k].to_string())
#     print('\r\n')
#
# # bar_store.prune_bars(3)
# # for k in bar_store.symbol_to_bars:
# #     print(f'THIS IS: {k}')
# #     print(bar_store.symbol_to_bars[k].to_string())
# #     print('\r\n')
##############################################################################################

##############################################################################################
# STREAM
##############################################################################################
# async def trade_callback(t):
#     print('trade', t)
#
# async def quote_callback(q):
#     print('quote', q)
#
# bar_store = BarManager(API_KEY, SECRET_KEY)
# import time
# async def bars_callback(q):
#     # time.sleep(2)
#     # global bar_store
#     # bar_store.set_symbols(["TSLA", ])
#     # bar_store.fetch_historical_bars(2)
#     # print(f'BARSTORE:')
#     # for k in bar_store.symbol_to_bars:
#     #     print(f'BARSTORE: {k}')
#     #     print(bar_store.symbol_to_bars[k].to_string())
#     #     print('\r\n')
#     # print(f'STREAM:')
#     # print(q)
#     # print()
#     # h_ts = str(bar_store.symbol_to_bars['TSLA'][bar_store.symbol_to_bars['TSLA']['symbol'] == "TSLA"].index[0])
#     # print(f'stream: {q.timestamp} vs')
#     # print(f'hist:   {h_ts}')
#     print(q)
#
# # Initiate Class Instance
# stream = Stream(API_KEY,
#                 SECRET_KEY,
#                 data_feed='sip')  # <- replace to SIP if you have PRO subscription
#
# # subscribing to event
# stream.subscribe_bars(bars_callback, '*')
#
# stream.run()

##############################################################################################










# def random_worker(datastore):
#     while True:
#         datastore.append(random())
#         sleep(0.5)
#
#
# datastore = [1]
# random_worker = Thread(target=random_worker, args=(datastore, ))
# random_worker.start()
#
# app = Dash(__name__)
#
# start_fig = go.Figure(go.Candlestick(
#     x=[],
#     open=[],
#     high=[],
#     low=[],
#     close=[],
# ))
#
# # App layout
# app.layout = html.Div([
#     html.H1(f'Random Value: {datastore[-1]} TTL: {len(datastore)}', id="live-header", style={'text-align': 'center'}),
#     html.Br(),
#
#     # dcc.Dropdown(id="slct_year",
#     #              options=[
#     #                  {"label": "2015", "value": 2015},
#     #                  {"label": "2016", "value": 2016},
#     #                  {"label": "2017", "value": 2017},
#     #                  {"label": "2018", "value": 2018}],
#     #              multi=False,
#     #              value=2015,
#     #              style={'width': "40%"}
#     #              ),
#     html.Br(),
#     dcc.Interval(
#         id='interval-component',
#         interval=250,  # in milliseconds
#         n_intervals=0
#     ),
#     dcc.Graph(id="graph", figure=start_fig)
# ])
#
#
# @app.callback(Output('live-header', 'children'),
#               Input('interval-component', 'n_intervals'))
# def interval(n):
#     return f'Random Value: {datastore[-1]}   TTL: {len(datastore)}   ATTEMPTS: {n}'
#
#
#
# bar_store = BarManager(API_KEY, SECRET_KEY)
# bar_store.set_symbols(["AAPL", ])
# bar_store.fetch_historical_bars(240)
#
# print(f"open {bar_store.symbol_to_bars['AAPL']['open'][1]}")
# print(f"open {bar_store.symbol_to_bars['AAPL'].shape}")
#
# import math
# jits = 100
# start_point = 60 * jits
# @app.callback(
#     Output("graph", "figure"),
#     Input('interval-component', 'n_intervals'))
# def display_candlestick(value):
#     global bar_store
#     global start_point
#     offset = math.floor((start_point + value % start_point) / jits)
#
#     bar_store.fetch_historical_bars(240)
#
#     df = bar_store.symbol_to_bars['AAPL'][:offset]
#
#     current_index = df.index[-1]
#     # dist = df.at[current_index, 'high'] - df.at[current_index, 'low']
#     dist = (df['high'] - df['low']).mean()
#     print("greg ", dist)
#
#     df.at[current_index, 'close'] = df.at[current_index, 'close'] + ((random() - 0.5) * dist * 3)
#     # df.at[current_index, 'high'] = if df.at[current_index, 'close'] > df.at[current_index, 'high'] : df.at[current_index, 'close'] + dist else df.at[current_index, 'high']
#     df.at[current_index, 'high'] = df.at[current_index, 'close'] + dist if df.at[current_index, 'close'] >= df.at[current_index, 'high'] else df.at[current_index, 'high']
#     df.at[current_index, 'low'] = df.at[current_index, 'close'] - dist if df.at[current_index, 'close'] <= df.at[current_index, 'low'] else df.at[current_index, 'low']
#     # bar_store.symbol_to_bars['AAPL'].at[current_index, 'close'] = bar_store.symbol_to_bars['AAPL'].at[current_index, 'close'] = 1000
#
#     fig = go.Figure(go.Candlestick(
#         x=df.index,
#         open=df['open'],
#         high=df['high'],
#         low=df['low'],
#         close=df['close'],
#     ))
#
#     return fig
#
#
# if __name__ == '__main__':
#     app.run_server(debug=True)
