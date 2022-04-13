import random
import time

from dash import Dash, dash_table, dcc, html, Output, Input
import threading

from BarManager.BarManager import BarManager
from BarManager.Screener import Screener
import dash
import dash_bootstrap_components as dbc
import dash_daq as daq


API_KEY = 'AKCBVDALZTXXE0NJ7LMV'
SECRET_KEY = 'OgCPIyKsZ9g1iLrmU89uKcWUCPW59E8Vrc6bGzwz'

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], title='Juicer', update_title=None)
app.config.suppress_callback_exceptions = True

graph_config = {'staticPlot': True, 'displaylogo': False, 'frameMargins': 0.0, 'autosizable': False, 'modeBarButtonsToRemove': ['zoom2d', 'pan2d', 'select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d']}
graph_style = {"height": "330px", 'minWidth': '425px', 'maxWidth': '600px'}

ttl_live_charts = 3


def create_graph(i):
    return html.Div(
        [
            html.Div([
                html.H6('Symbol', id=f'symbol_{i}'),
                html.H6('📌', id=f'pin-button_{i}', n_clicks=0, style={'cursor': 'pointer'})
            ], style={'display': 'inline-flex', 'justifyContent': 'space-between', 'alignItems': 'flex-end', 'margin': '3px 7px -4px 7px'}),
            html.Div(dcc.Graph(id=f'graph_{i}', responsive=True, config=graph_config, style=graph_style), style={}),
            html.Div(daq.GraduatedBar(
                id=f'volume-acceleration_{i}',
                label="",
                value=1,
                size=425,
                color={"gradient": True, "ranges": {"gray": [0, 3], "yellow": [3, 6], "red": [6, 10]}},
            ), style={'height': '15px'})
        ], id=f'container_{i}', style={'display': 'inline-flex', 'flexDirection': 'column', 'borderStyle': 'solid', 'borderRadius': '12px', 'margin': '2px', 'overflow': 'hidden'})


def create_graphs(n):
    return [create_graph(i) for i in range(n)]


app.layout = html.Div([
    *create_graphs(ttl_live_charts),
    dcc.Interval(
        id='chart-interval-component',
        interval=200,
        n_intervals=0
    ),
    dcc.Interval(
        id='pin-interval-component',
        interval=1000,
        n_intervals=0
    ),
], style={'overflow-y': 'hidden', 'overflow-x': 'hidden', 'textAlign': 'center'})


@app.callback([Output(f'pin-button_{i}', 'children') for i in range(ttl_live_charts)],
              [Input(f'pin-button_{i}', 'n_clicks') for i in range(ttl_live_charts)],
               Input('pin-interval-component', 'n_intervals'))
def update_pin(*n_clicks):
    if dash.callback_context.triggered and dash.callback_context.triggered[0]['prop_id'].split('.')[0] != 'pin-interval-component':
        chart_id_triggered = int(dash.callback_context.triggered[0]['prop_id'].split('.')[0].replace('pin-button_', ''))
        toggled_symbol = bar_manager.get_active_symbols()[chart_id_triggered]
        print(f'Pin toggled on: {toggled_symbol}')
        bar_manager.toggle_pinned(toggled_symbol)

    ret = []
    if bar_manager is not None:
        for symbol in bar_manager.get_active_symbols():
            if symbol in bar_manager.pinned_symbols:
                ret.append('📍')
            else:
                ret.append('📌')

    while len(ret) < ttl_live_charts:
        ret.append('📌')

    return ret



@app.callback([Output(f'container_{i}', 'hidden') for i in range(ttl_live_charts)],
              [Input('chart-interval-component', 'n_intervals')])
def update_hidden(n_intervals):
    ret = []
    if bar_manager is not None:
        for symbol in bar_manager.get_active_symbols():
            ret.append(False)

    while len(ret) < ttl_live_charts:
        ret.append(True)

    return ret


@app.callback([Output(f'symbol_{i}', 'children') for i in range(ttl_live_charts)],
              [Input('chart-interval-component', 'n_intervals')])
def update_symbol(n_intervals):
    ret = []
    if bar_manager is not None:
        for symbol in bar_manager.get_active_symbols():
            ret.append(symbol)

    while len(ret) < ttl_live_charts:
        ret.append('Symbol')

    return ret


@app.callback([Output(f'volume-acceleration_{i}', 'value') for i in range(ttl_live_charts)],
              [Input('chart-interval-component', 'n_intervals')])
def update_volume_acceleration(n_intervals):
    ret = []
    if bar_manager is not None:
        for symbol in bar_manager.get_active_symbols():

            ret.append(bar_manager.get_volume_acceleration(symbol))

    while len(ret) < ttl_live_charts:
        ret.append(0)

    return ret


@app.callback([Output(f'graph_{i}', 'figure') for i in range(ttl_live_charts)],
              [Input('chart-interval-component', 'n_intervals')])
def update_graph(n_intervals):
    ret = []
    if bar_manager is not None:
        for symbol in bar_manager.get_active_symbols():

            ret.append({
                'data': [{
                    'open': bar_manager.get_bars(symbol)['open'],
                    'high': bar_manager.get_bars(symbol)['high'],
                    'low': bar_manager.get_bars(symbol)['low'],
                    'close': bar_manager.get_bars(symbol)['close'],
                    'type': 'candlestick'
                }],
                'layout': {
                    'showlegend': False,
                    'margin': {'l': 5, 'r': 50, 't': 0, 'b': 0},
                    'xaxis': {'rangeslider': {'visible': False}, 'zeroline': False, 'visible': False},
                    'yaxis': {'side': 'right'},
                    'template': 'plotly_dark'
                }
            })

    while len(ret) < ttl_live_charts:
        ret.append({
            'data': [{
                'open': [],
                'high': [],
                'low': [],
                'close': [],
                'type': 'candlestick'
            }],
            'layout': {
                'title': {'text': f'', 'x': .05},
                'showlegend': False,
                'margin': {'l': 5, 'r': 50, 't': 50, 'b': 25},
                'xaxis': {'rangeslider': {'visible': False}, 'zeroline': False, 'visible': False},
                'yaxis': {'side': 'right'},
            }
        })

    return ret


if __name__ == '__main__':
    bar_manager = None
    threading.Thread(target=lambda: app.run_server(debug=True, use_reloader=False)).start()

    bar_manager = BarManager(API_KEY, SECRET_KEY, pinned_symbols=['SPY'], num_active_charts=3)
    bar_manager.set_symbols(['SPY', 'QQQ', 'TSLA'])
    bar_manager.initialize()

