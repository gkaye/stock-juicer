import datetime
import statistics
import time
from concurrent.futures.process import ProcessPoolExecutor
import dateutil
import alpaca_trade_api
import numpy
import pandas
from apscheduler.schedulers.background import BackgroundScheduler


class Screener:
    def __init__(self, api_key, api_secret, should_filter_by_spread=True):
        self.symbol_to_bars = {}
        self.symbol_to_spreads = {}
        self.symbol_to_spread = {}
        self.symbol_to_spread_acr_ratio = {}
        self.symbols = []
        self.active_symbols = []
        self.output_symbols = []
        self.symbols_override = None
        self.time_override = None
        self.alpaca = alpaca_trade_api.REST(api_key, api_secret)
        self.should_filter_by_spread = should_filter_by_spread
        self.pretty_output = pandas.DataFrame()
        self.pretty_output_last_update_time = None

    def fetch_symbols(self):
        start = time.time()
        if self.symbols_override:
            self.symbols = self.symbols_override
            return

        assets = self.alpaca.list_assets()
        self.symbols = [asset.symbol for asset in assets if asset.tradable]
        end = time.time()
        print(f'fetch_symbols() took {end - start} seconds')

    def set_symbols_override(self, input_symbols):
        self.symbols_override = input_symbols

    def set_time_override(self, time_override):
        self.time_override = dateutil.parser.parse(time_override)

    def advance_time_override(self, advance_minutes=1):
        self.time_override = self.time_override + datetime.timedelta(minutes=advance_minutes)

    def get_offset_time(self, offset_minutes):
        if not self.time_override and offset_minutes == 0:
            return None

        current_time = self.time_override if self.time_override else datetime.datetime.now(datetime.timezone.utc)
        t = current_time - datetime.timedelta(minutes=offset_minutes)
        t = t.replace(second=0, microsecond=0)
        return t.isoformat()

    def fetch_spread(self, quotes_back=20):
        start = time.time()
        quotes_window_size = 10

        for symbol in self.active_symbols:

            quotes = self.alpaca.get_quotes(symbol, self.get_offset_time(2), self.get_offset_time(0), limit=quotes_back).df

            if quotes.empty:
                continue

            mean_spread = (quotes['ask_price'] - quotes['bid_price']).mean()

            if symbol not in self.symbol_to_spreads:
                self.symbol_to_spreads[symbol] = []

            self.symbol_to_spreads[symbol].append(mean_spread)

            # Prune
            self.symbol_to_spreads[symbol] = self.symbol_to_spreads[symbol][-quotes_window_size:]

            self.symbol_to_spread[symbol] = statistics.mean(self.symbol_to_spreads[symbol])
        end = time.time()
        print(f'fetch_spread() took {end - start} seconds')

    def fetch_historical_bars(self, bars_back=1):
        start = time.time()
        symbol_query_chunk_size = 3000

        new_data_list = []
        for i in range(0, len(self.symbols), symbol_query_chunk_size):
            symbols_subset = self.symbols[i:i + symbol_query_chunk_size]
            new_data_list.append(self.alpaca.get_bars(symbols_subset,
                                                      alpaca_trade_api.TimeFrame(1,
                                                      alpaca_trade_api.TimeFrameUnit('Min')),
                                                      self.get_offset_time(bars_back + 1),
                                                      self.get_offset_time(0)).df)

        new_data = pandas.concat(new_data_list)

        if new_data.empty:
            return

        for t in new_data.groupby(new_data['symbol']):
            symbol = t[0]
            df = t[1]
            if symbol not in self.symbol_to_bars:
                self.symbol_to_bars[symbol] = df.sort_values(by=['timestamp'])
            else:
                self.symbol_to_bars[symbol] = pandas.concat([self.symbol_to_bars[symbol], df]).reset_index() \
                    .drop_duplicates(subset=['timestamp'], keep='last') \
                    .set_index('timestamp') \
                    .sort_values(by=['timestamp'])

        end = time.time()
        print(f'fetch_historical_bars() took {end - start} seconds')

    def prune_bars(self, n=390):
        for k in self.symbol_to_bars:
            self.symbol_to_bars[k] = self.symbol_to_bars[k].tail(n)

    def calc_all_derivative_values(self):
        start = time.time()
        with ProcessPoolExecutor() as executor:
            futures = []
            for symbol, df in self.symbol_to_bars.items():
                future = executor.submit(self.calc_derivative_values, symbol, df)
                futures.append(future)

            # Process results
            for future in futures:
                result = future.result()
                self.symbol_to_bars[result['symbol']] = result['bars']
        end = time.time()
        print(f'calc_all_derivative_values() took {end - start} seconds')

    @staticmethod
    def calc_derivative_values(symbol, df):
        liquidity_dollars_risk = 100
        liquidity_acr_period = 15
        liquidity_volume_period = 10

        acr_period = 15

        min_volume_period = 15

        linearity_period = 40
        linearity_window = 6

        df[f'liquidity.{liquidity_dollars_risk}.{liquidity_acr_period}.{liquidity_volume_period}'] = (liquidity_dollars_risk / (((df['high'] - df['low']).rolling(window=liquidity_acr_period, min_periods=1).mean()) / 2)) / df['volume'].rolling(window=liquidity_volume_period, min_periods=1).mean()

        df[f'acr_MA.{acr_period}'] = (df['high'] - df['low']).rolling(window=acr_period, min_periods=1).mean()

        df[f'volume_MIN.{min_volume_period}'] = df['volume'].rolling(window=min_volume_period, min_periods=1).min()

        linearity_numerator = (df['close'] - df['open'].shift(linearity_window)).abs()
        linearity_denominator = ((2 * (df['high'] - df['low'])) - (df['close'] - df['open']).abs() + (df['open'] - df['close'].shift(1)).abs()).rolling(window=linearity_window + 1, min_periods=1).sum() - (df['open'].shift(linearity_window) - df['close'].shift(linearity_window + 1)).abs()
        df[f'linearity.{linearity_period}.{linearity_window}'] = (linearity_numerator / linearity_denominator).rolling(window=linearity_period, min_periods=1).mean()

        return {'symbol': symbol, 'bars': df}

    def filter_by_derivative_values(self):
        liquidity_max = 0.10
        volume_min = 2000
        self.active_symbols = []

        new_active_symbols = []
        for symbol in self.symbol_to_bars:
            if self.symbol_to_bars[symbol]['liquidity.100.15.10'].iloc[-1] >= liquidity_max or numpy.isnan(self.symbol_to_bars[symbol]['liquidity.100.15.10'].iloc[-1]):
                continue

            if self.symbol_to_bars[symbol]['volume_MIN.15'].iloc[-1] < volume_min or numpy.isnan(self.symbol_to_bars[symbol]['volume_MIN.15'].iloc[-1]):
                continue

            new_active_symbols.append(symbol)

        self.active_symbols = new_active_symbols

    def filter_by_spread(self):
        new_active_symbols = []
        spread_acr_ratio_threshold = 0.2
        for symbol in self.symbol_to_spreads:
            mean_spread = self.symbol_to_spread[symbol]
            spread_acr_ratio = mean_spread / self.symbol_to_bars[symbol]['acr_MA.15'].iloc[-1]

            self.symbol_to_spread_acr_ratio[symbol] = spread_acr_ratio

            if numpy.isnan(spread_acr_ratio) or spread_acr_ratio > spread_acr_ratio_threshold:
                continue

            new_active_symbols.append(symbol)

        self.active_symbols = new_active_symbols

    def generate_symbols(self):
        self.output_symbols.clear()
        if not self.pretty_output.empty:
            self.output_symbols.extend(self.pretty_output['symbol'].to_list())

    def safe_get_symbol_to_spread(self, symbol):
        if symbol in self.symbol_to_spread:
            return self.symbol_to_spread[symbol]
        return 0.0

    def safe_get_symbol_to_spread_acr_ratio(self, symbol):
        if symbol in self.symbol_to_spread_acr_ratio:
            return self.symbol_to_spread_acr_ratio[symbol]
        return 0.0

    def generate_pretty_output(self):
        results_length = 30
        prioritized_columns = ['symbol', 'linearity.40.6', 'liquidity.100.15.10', 'volume_MIN.15', 'spread_acr_ratio']

        series_list = []
        for symbol in self.active_symbols:
            series = self.symbol_to_bars[symbol].iloc[-1].to_dict()
            series['spread'] = self.safe_get_symbol_to_spread(symbol)
            series['spread_acr_ratio'] = self.safe_get_symbol_to_spread_acr_ratio(symbol)
            series_list.append(series)

        df = pandas.DataFrame(series_list)

        # Sort and prune
        if df.empty:
            print('No results found')
        else:
            df.sort_values(by=['linearity.40.6', 'liquidity.100.15.10', 'volume_MIN.15'], ascending=[False, True, False], inplace=True)
            df = df[:results_length]

            # Downselect and reorder columns
            leftover_columns = [col for col in list(df.columns.values) if col not in prioritized_columns]
            df = df[prioritized_columns + leftover_columns]

        self.pretty_output = df
        self.pretty_output_last_update_time = time.time()

    def initialize(self):
        initial_candles = 50

        start = time.time()
        self.lifecycle(initial_candles, initialize=True)
        end = time.time()

        print(f'initialize() took {end - start} seconds')

        self.start_lifecycle()

    def start_lifecycle(self):
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.lifecycle, 'interval', minutes=1)
        scheduler.start()

    def lifecycle(self, lookback_candles=2, initialize=False):
        start = time.time()
        if not initialize and self.time_override:
            self.advance_time_override()

        self.fetch_symbols()
        self.fetch_historical_bars(lookback_candles)
        self.calc_all_derivative_values()
        self.filter_by_derivative_values()
        if self.should_filter_by_spread:
            self.fetch_spread()
            self.filter_by_spread()
        self.generate_pretty_output()
        self.generate_symbols()
        end = time.time()

        print(f'lifecycle() took {end - start} seconds')
