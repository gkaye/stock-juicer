import datetime
import math
import statistics
import threading
import time
from concurrent.futures.process import ProcessPoolExecutor
import dateutil
import alpaca_trade_api
import numpy
import pandas
from alpaca_trade_api import Stream
from apscheduler.schedulers.background import BackgroundScheduler


class BarManager:
    def __init__(self, api_key, api_secret, num_active_charts=15, symbols_buffer=15, data_feed='sip', max_bars=100, aggregation_period_minutes=2, pinned_symbols=[]):
        self.api_key = api_key
        self.api_secret = api_secret
        self.data_feed = data_feed
        self.symbol_to_bars = {}
        self.symbols = []
        self.max_bars = max_bars
        self.num_active_charts = num_active_charts
        self.symbols_buffer = symbols_buffer
        self.subscription_symbols = []
        self.stream_thread = None
        self.get_time_override_function = None
        self.aggregation_period_minutes = aggregation_period_minutes
        self.pinned_symbols = pinned_symbols

        self.alpaca = alpaca_trade_api.REST(api_key, api_secret)
        self.stream = Stream(api_key, api_secret, data_feed=data_feed)

    def set_get_time_override_function(self, get_time_override_function):
        self.get_time_override_function = get_time_override_function

    def get_bars(self, symbol):
        if symbol in self.symbol_to_bars:
            return self.symbol_to_bars[symbol]

        return pandas.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])

    def set_symbols(self, symbols):
        self.symbols = symbols
        print(f'Symbols set to: {self.symbols}')

    def toggle_pinned(self, symbol):
        if symbol in self.pinned_symbols:
            self.pinned_symbols.remove(symbol)
        else:
            self.pinned_symbols.append(symbol)

    def get_active_symbols(self):
        unpinned_symbols_subset = [x for x in self.symbols if x not in self.pinned_symbols]

        active_symbols = (self.pinned_symbols + unpinned_symbols_subset)[:self.num_active_charts]
        return active_symbols

    def generate_subscription_symbols(self):
        unpinned_symbols_subset = [x for x in self.symbols if x not in self.pinned_symbols]
        self.subscription_symbols = (self.pinned_symbols + unpinned_symbols_subset)[:(self.num_active_charts + self.symbols_buffer)]
        print(f'Subscribing to symbols: {self.subscription_symbols}')

    def subscriptions_out_of_sync(self):
        active_symbols = self.get_active_symbols()
        subscription_symbols = self.subscription_symbols
        for symbol in active_symbols:
            if symbol not in self.subscription_symbols:
                print(f'Active symbols out of sync, missing {symbol}')
                print(f'Active:               {active_symbols}')
                print(f'Currently Subscribed: {subscription_symbols}')
                return True

    def prune_dead_symbols(self):
        symbols_for_deletion = [symbol for symbol in self.symbol_to_bars if symbol not in self.subscription_symbols]
        for symbol in symbols_for_deletion:
            self.symbol_to_bars.pop(symbol, None)

    def maybe_reinitialize_job(self):
        stream_stop_wait_time = 0.1
        if not self.subscriptions_out_of_sync():
            return

        self.stream.stop()

        if self.stream_thread is not None:
            while self.stream_thread.is_alive():
                print(f'Stream thread still alive, waiting {stream_stop_wait_time} seconds for termination...')
                time.sleep(stream_stop_wait_time)

        self.update_stream()

    def get_offset_time(self, offset_minutes):
        if not self.get_time_override_function and offset_minutes == 0:
            return None

        current_time = self.get_time_override_function() if self.get_time_override_function else datetime.datetime.now(datetime.timezone.utc)
        t = current_time - datetime.timedelta(minutes=offset_minutes)
        t = t.replace(second=0, microsecond=0)
        return t.isoformat()

    def historical_mode_job(self):
        for symbol in self.get_active_symbols():
            self.symbol_to_bars[symbol] = self.alpaca.get_bars([symbol],
                                          alpaca_trade_api.TimeFrame(self.aggregation_period_minutes, alpaca_trade_api.TimeFrameUnit('Min')),
                                          self.get_offset_time(self.max_bars + 1),
                                          self.get_offset_time(0)).df

    def initialize(self):
        if self.get_time_override_function is not None:
            self.historical_mode_job()
            scheduler = BackgroundScheduler()
            scheduler.add_job(self.historical_mode_job, 'interval', seconds=30)
            scheduler.start()
            return

        self.update_stream()
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.maybe_reinitialize_job, 'interval', seconds=15)
        scheduler.start()

    def update_stream(self):
        self.generate_subscription_symbols()
        self.prune_dead_symbols()
        self.stream.subscribe_trades(self.trades_callback, *self.subscription_symbols)

        self.stream_thread = threading.Thread(target=lambda: self.stream.run())
        self.stream_thread.start()

    def initialize_historical_bars(self, symbol):
        self.update_historical_bars(symbol, self.max_bars + 1)

    def update_historical_bars(self, symbol, minutes_back):
        now = datetime.datetime.now(datetime.timezone.utc)

        past = now - datetime.timedelta(minutes=minutes_back)
        past = past.replace(second=0, microsecond=0)
        past_iso = past.isoformat()

        if symbol not in self.symbol_to_bars:
            self.symbol_to_bars[symbol] = self.alpaca.get_bars([symbol], alpaca_trade_api.TimeFrame(self.aggregation_period_minutes, alpaca_trade_api.TimeFrameUnit('Min')), past_iso, None).df
        else:
            # Potentially remove last candle due to poor rendering on alpacas behalf
            now_offset = (now - datetime.timedelta(minutes=(self.aggregation_period_minutes + 1))).replace(second=0, microsecond=0).isoformat()
            df = self.alpaca.get_bars([symbol], alpaca_trade_api.TimeFrame(self.aggregation_period_minutes, alpaca_trade_api.TimeFrameUnit('Min')), past_iso, now_offset).df

            self.symbol_to_bars[symbol] = pandas.concat([self.symbol_to_bars[symbol], df]).reset_index() \
                .drop_duplicates(subset=['timestamp'], keep='last') \
                .set_index('timestamp') \
                .sort_values(by=['timestamp'])

    def nearest_candle(self, dt):
        nearest_candle_minute = math.floor(dt.minute / self.aggregation_period_minutes) * self.aggregation_period_minutes
        return dt.replace(minute=nearest_candle_minute)

    async def trades_callback(self, t):
        if t.symbol not in self.symbol_to_bars:
            self.initialize_historical_bars(t.symbol)

        bars = self.symbol_to_bars[t.symbol]

        ts = pandas.to_datetime(t.timestamp, unit='ns').replace(second=0, microsecond=0, nanosecond=0).tz_convert('utc')
        ts = self.nearest_candle(ts)

        if ts not in self.symbol_to_bars[t.symbol].index:
            # Fill in data gaps with historical api
            self.update_historical_bars(t.symbol, 3 * self.aggregation_period_minutes)

            self.symbol_to_bars[t.symbol].loc[ts, 'open'] = t.price
            self.symbol_to_bars[t.symbol].loc[ts, 'high'] = t.price
            self.symbol_to_bars[t.symbol].loc[ts, 'low'] = t.price
            self.symbol_to_bars[t.symbol].loc[ts, 'close'] = t.price
            self.symbol_to_bars[t.symbol].loc[ts, 'volume'] = t.size
            self.symbol_to_bars[t.symbol].loc[ts, 'symbol'] = t.symbol

            # Maintain maximum length
            self.symbol_to_bars[t.symbol] = self.symbol_to_bars[t.symbol][-(self.max_bars + 1):]

        else:
            if t.price > bars.loc[ts, 'high']:
                bars.loc[ts, 'high'] = t.price
            if t.price < bars.loc[ts, 'low']:
                bars.loc[ts, 'low'] = t.price
            bars.loc[ts, 'close'] = t.price
            bars.loc[ts, 'volume'] = bars.loc[ts, 'volume'] + t.size

    def set_get_time_override_function(self, get_time_override_function):
        self.get_time_override_function = get_time_override_function