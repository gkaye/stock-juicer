import random
import time
import datetime

from dash import Dash, dash_table, dcc, html, Output, Input
import threading

from BarManager.BarManager import BarManager
from BarManager.Screener import Screener
import dash
import dash_daq as daq



API_KEY = 'AKCBVDALZTXXE0NJ7LMV'
SECRET_KEY = 'OgCPIyKsZ9g1iLrmU89uKcWUCPW59E8Vrc6bGzwz'

app = Dash(__name__, title='Juicer', update_title=None)
app.config.suppress_callback_exceptions = True

graph_config = {'staticPlot': True, 'displaylogo': False, 'frameMargins': 0.0, 'autosizable': False, 'modeBarButtonsToRemove': ['zoom2d', 'pan2d', 'select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d']}
graph_style = {"height": "320px", 'minWidth': '425px', 'maxWidth': '600px'}

sub_graph_config = {'staticPlot': True, 'displaylogo': False, 'frameMargins': 0.0, 'autosizable': False, 'modeBarButtonsToRemove': ['zoom2d', 'pan2d', 'select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d']}
sub_graph_style = {"height": "75px", 'minWidth': '425px', 'maxWidth': '600px'}

sub_graph_2_config = {'staticPlot': True, 'displaylogo': False, 'frameMargins': 0.0, 'autosizable': False, 'modeBarButtonsToRemove': ['zoom2d', 'pan2d', 'select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d']}
sub_graph_2_style = {"height": "75px", 'minWidth': '425px', 'maxWidth': '600px'}

ttl_live_charts = 10


def create_graph(i):
    return html.Div(
        [
            html.Div([
                html.H6('Symbol', id=f'symbol_{i}'),
                html.H6('📌', id=f'pin-button_{i}', n_clicks=0, style={'cursor': 'pointer'})
            ], style={'display': 'inline-flex', 'justifyContent': 'space-between', 'alignItems': 'flex-end', 'margin': '3px 7px -4px 7px'}),
            html.Div(dcc.Graph(id=f'graph_{i}', responsive=True, config=graph_config, style=graph_style), style={}),
            html.Div(dcc.Graph(id=f'sub_graph_{i}', responsive=True, config=sub_graph_config, style=sub_graph_style), style={'border-top': '2px solid black'}),
            html.Div(dcc.Graph(id=f'sub_graph_2_{i}', responsive=True, config=sub_graph_2_config, style=sub_graph_2_style), style={'border-top': '2px solid black'}),
            html.Div(daq.GraduatedBar(
                id=f'volume-acceleration_{i}',
                className='rvol_bar',
                label="",
                value=1,
                max=10,
                size=425,
                color={"gradient": True, "ranges": {"gray": [0, 4], "orange": [4, 7], "red": [7, 10]}},
                style={'opacity': '0.75', 'background-color': 'rgb(230, 230, 230)'}
            ), style={'height': '15px', 'margin': '-1px', 'background-color': 'white'})
        ], id=f'container_{i}', style={'display': 'inline-flex', 'flexDirection': 'column', 'borderStyle': 'solid', 'borderRadius': '12px', 'margin': '2px', 'overflow': 'hidden'})


def create_graphs(n):
    return [create_graph(i) for i in range(n)]


graphs_layout = html.Div([
    *create_graphs(ttl_live_charts),
    dcc.Interval(
        id='chart-interval-component',
        interval=1000,
        n_intervals=0
    ),
    dcc.Interval(
        id='sub-chart-interval-component',
        interval=2500,
        n_intervals=0
    ),
    dcc.Interval(
        id='pin-interval-component',
        interval=1000,
        n_intervals=0
    ),
], style={'overflow-y': 'hidden', 'overflow-x': 'hidden', 'textAlign': 'center'})

raw_layout = html.Div([
    html.H5(id='screener-info-text-1'),
    html.H5(id='screener-info-text-2'),
    dash_table.DataTable(id='screener-table'),
    dcc.Interval(
        id='screener-table-interval-component',
        interval=1 * 1000,
        n_intervals=0
    ),
    dcc.Interval(
        id='screener-info-text-interval-component',
        interval=1 * 1000,
        n_intervals=0
    )
], style={'padding': '10px'})



app.layout = html.Div([
    dcc.Tabs(id="tabs", value='tab-2', children=[
        dcc.Tab(label='Graphs', value='tab-1'),
        dcc.Tab(label='Raw Output', value='tab-2'),
    ]),
    html.Div(id='tabs-content-example-graph')
])


@app.callback(Output('tabs-content-example-graph', 'children'),
              Input('tabs', 'value'))
def render_content(tab):
    if tab == 'tab-1':
        return graphs_layout
    elif tab == 'tab-2':
        return raw_layout


@app.callback(Output('screener-table', 'data'),
              Output('screener-table', 'columns'),
              Input('screener-table-interval-component', 'n_intervals'))
def update_metrics(n):
    return screener.pretty_output.to_dict('records'), [{"name": i, "id": i} for i in screener.pretty_output.columns]


@app.callback(Output('screener-info-text-1', 'children'),
              Input('screener-info-text-interval-component', 'n_intervals'))
def update_metrics(n):
    last_update_time = screener.pretty_output_last_update_time
    if not last_update_time:
        elapsed_string = 'Never updated'
    else:
        elapsed_string = f'{(time.time() - screener.pretty_output_last_update_time):.0f} seconds ago'

    return f'Last update time: {elapsed_string}'


@app.callback(Output('screener-info-text-2', 'children'),
              Input('screener-info-text-interval-component', 'n_intervals'))
def update_metrics(n):
    return f'Number of Records: {screener.pretty_output.shape[0]}'


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


@app.callback([Output(f'sub_graph_{i}', 'figure') for i in range(ttl_live_charts)],
              [Input('sub-chart-interval-component', 'n_intervals')])
def update_sub_graph(n_intervals):
    start_time = time.time()

    sub_graph_window_minutes = 3
    ret = []
    if bar_manager is not None:
        for symbol in bar_manager.get_active_symbols():
            dataframe = bar_manager.get_ticker_momentum(symbol)

            ret.append({
                'data': [{
                    'x': dataframe.index,
                    'y': dataframe['oscillator_ratio'],
                    'type': 'scatter'
                }],
                'layout': {
                    'showlegend': False,
                    'margin': {'l': 5, 'r': 50, 't': 0, 'b': 0},
                    'xaxis': {'rangeslider': {'visible': False}, 'zeroline': False, 'visible': False, 'range': [dataframe.tail(1).index[0] - datetime.timedelta(minutes=sub_graph_window_minutes), dataframe.tail(1).index[0]]},
                    'yaxis': {'side': 'right', 'range': [-1.05, 1.05]},
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

    print(f'{(time.time() - start_time):.4f} seconds elapsed processing all momentum sub graphs.')

    return ret


@app.callback([Output(f'sub_graph_2_{i}', 'figure') for i in range(ttl_live_charts)],
              [Input('sub-chart-interval-component', 'n_intervals')])
def update_sub_graph_2(n_intervals):
    start_time = time.time()

    sub_graph_window_minutes = 3
    ret = []
    if bar_manager is not None:
        for symbol in bar_manager.get_active_symbols():
            dataframe = bar_manager.get_friction(symbol)

            ret.append({
                'data': [{
                    'x': dataframe.index,
                    'y': dataframe['friction_ma'],
                    'type': 'scatter'
                }],
                'layout': {
                    'showlegend': False,
                    'margin': {'l': 5, 'r': 50, 't': 0, 'b': 0},
                    'xaxis': {'rangeslider': {'visible': False}, 'zeroline': False, 'visible': False, 'range': [dataframe.tail(1).index[0] - datetime.timedelta(minutes=sub_graph_window_minutes), dataframe.tail(1).index[0]]},
                    'yaxis': {'side': 'right', 'range': [-1.05, 1.05]},
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

    print(f'{(time.time() - start_time):.4f} seconds elapsed processing all friction sub graphs.')

    return ret


if __name__ == '__main__':
    historical_mode = False
    historical_time = "2022-04-08T19:00:00+00:00"

    bar_manager = None
    threading.Thread(target=lambda: app.run_server(debug=True, use_reloader=False)).start()

    screener = Screener(API_KEY, SECRET_KEY, should_filter_by_spread=not historical_mode)
    if historical_mode:
        screener.set_time_override(historical_time)
    screener.initialize()

    bar_manager = BarManager(API_KEY, SECRET_KEY, pinned_symbols=['SPY'], num_active_charts=ttl_live_charts, aggregation_period_minutes=1)
    bar_manager.set_symbols(screener.output_symbols)
    if historical_mode:
        bar_manager.set_get_time_override_function(lambda: screener.time_override)
    bar_manager.initialize()