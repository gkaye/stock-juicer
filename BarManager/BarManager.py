import asyncio
import datetime
import math
import threading
import time
from concurrent.futures.process import ProcessPoolExecutor

import alpaca_trade_api
import pandas
from alpaca_trade_api import Stream
from apscheduler.schedulers.background import BackgroundScheduler
from tornado.platform.asyncio import AnyThreadEventLoopPolicy


class BarManager:
    def __init__(self, api_key, api_secret, num_active_charts=15, symbols_buffer=0, data_feed='sip', max_bars=100, aggregation_period_minutes=1, pinned_symbols=[],
                 rvol_sample_window_seconds=50, rvol_multiplier=1):
        self.api_key = api_key
        self.api_secret = api_secret
        self.data_feed = data_feed
        self.symbol_to_bars = {}
        self.symbol_to_ticker_momentum = {}
        self.symbol_to_friction = {}
        self.symbol_to_quotes = {}
        self.symbol_to_trade_timestamps = {}
        self.symbols = []
        self.max_bars = max_bars
        self.num_active_charts = num_active_charts
        self.symbols_buffer = symbols_buffer
        self.subscription_symbols = []
        self.stream_thread = None
        self.get_time_override_function = None
        self.aggregation_period_minutes = aggregation_period_minutes
        self.pinned_symbols = pinned_symbols
        self.rvol_sample_window_seconds = rvol_sample_window_seconds
        self.rvol_multiplier = rvol_multiplier

        self.alpaca = alpaca_trade_api.REST(api_key, api_secret)
        self.stream = Stream(api_key, api_secret, data_feed=data_feed)

    def reset_subgraphs(self):
        self.symbol_to_ticker_momentum = {}
        self.symbol_to_friction = {}


    def set_get_time_override_function(self, get_time_override_function):
        self.get_time_override_function = get_time_override_function


    def prune_symbol_to_trade_timestamps(self, symbol):
        buffer_seconds = 15

        for symbol in self.symbol_to_trade_timestamps:
            timestamps = self.symbol_to_trade_timestamps[symbol]
            minimum_time = time.time() - self.rvol_sample_window_seconds - buffer_seconds
            self.symbol_to_trade_timestamps[symbol] = [ts for ts in timestamps if ts > minimum_time]


    def get_friction(self, symbol, period='10s'):
        start_time = time.time()
        if symbol not in self.symbol_to_friction:
            return []

        dictionary_list = self.symbol_to_friction[symbol]

        dataframe = pandas.DataFrame(dictionary_list)
        dataframe = dataframe.set_index('index')
        dataframe = dataframe.sort_index()

        dataframe['friction_ma'] = dataframe['friction'].rolling(period).mean()

        # print(f'{(time.time() - start_time):.4f} seconds elapsed processing get_friction({symbol})')
        return dataframe


    def get_ticker_momentum(self, symbol, period='15s'):
        start_time = time.time()
        if symbol not in self.symbol_to_ticker_momentum:
            return []

        dictionary_list = self.symbol_to_ticker_momentum[symbol]

        dataframe = pandas.DataFrame(dictionary_list)
        dataframe = dataframe.set_index('index')
        dataframe = dataframe.sort_index()

        dataframe['oscillator_ratio'] = dataframe['oscillator'].rolling(period).sum() / dataframe['oscillator'].abs().rolling(period).sum()

        # print(f'{(time.time() - start_time):.4f} seconds elapsed processing get_ticker_momentum({symbol})')
        return dataframe


    def get_volume_acceleration(self, symbol):
        if symbol not in self.symbol_to_trade_timestamps:
            return 0

        timestamps = self.symbol_to_trade_timestamps[symbol]

        current_time = time.time()
        samples_large_window = self.timestamps_count_within(current_time, timestamps, self.rvol_sample_window_seconds)
        samples_small_window = self.timestamps_count_within(current_time, timestamps, self.rvol_sample_window_seconds / 2)
        samples_small_window_2 = samples_large_window - samples_small_window

        if samples_small_window_2 == 0 or samples_small_window == 0 or samples_large_window == 0:
            return 0

        rvol = (samples_small_window / samples_small_window_2)

        return rvol * self.rvol_multiplier


    @staticmethod
    def timestamps_count_within(current_time, timestamps_list, within_seconds):
        minimum_time = current_time - within_seconds

        count = 0
        for ts in timestamps_list:
            if ts > minimum_time:
                count += 1

        return count


    def get_bars(self, symbol):
        if symbol in self.symbol_to_bars:
            return self.symbol_to_bars[symbol]

        return pandas.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])


    def set_symbols(self, symbols):
        self.symbols = symbols
        just_symbols = [row['symbol'] for row in self.symbols]
        print(f'Symbols set to: {just_symbols}')


    def toggle_pinned(self, symbol):
        if symbol in self.pinned_symbols:
            self.pinned_symbols.remove(symbol)
        else:
            self.pinned_symbols.append(symbol)

    def get_metadata(self, symbol):
        for row in self.symbols:
            if row['symbol'] == symbol:
                return row

    def get_active_symbols(self):
        unpinned_symbols_subset = [x['symbol'] for x in self.symbols if x['symbol'] not in self.pinned_symbols]

        active_symbols = (self.pinned_symbols + unpinned_symbols_subset)[:self.num_active_charts]
        return active_symbols


    def generate_subscription_symbols(self):
        old_subscription_symbols = self.subscription_symbols
        unpinned_symbols_subset = [x['symbol'] for x in self.symbols if x['symbol'] not in self.pinned_symbols]
        self.subscription_symbols = (self.pinned_symbols + unpinned_symbols_subset)[:(self.num_active_charts + self.symbols_buffer)]

        if set(old_subscription_symbols) != set(self.subscription_symbols):
            print(f'Subscription symbols: {self.subscription_symbols}')


    # def subscriptions_out_of_sync(self):
    #     active_symbols = self.get_active_symbols()
    #     subscription_symbols = self.subscription_symbols
    #     for symbol in active_symbols:
    #         if symbol not in self.subscription_symbols:
    #             print(f'Active symbols out of sync, missing {symbol}')
    #             print(f'Active:                             {active_symbols}')
    #             print(f'Currently Subscribed:               {subscription_symbols}')
    #             return True


    def prune_dead_symbols(self):
        symbols_for_deletion = [symbol for symbol in self.symbol_to_bars if symbol not in self.subscription_symbols]
        for symbol in symbols_for_deletion:
            self.symbol_to_bars.pop(symbol, None)
            self.symbol_to_trade_timestamps.pop(symbol, None)


    # def maybe_reinitialize_job(self):
    #     if not self.subscriptions_out_of_sync():
    #         return
    #
    #     self.update_stream()


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
        scheduler.add_job(self.update_stream, 'interval', seconds=5)
        scheduler.start()


    def update_stream(self):
        old_subscription_symbols = self.subscription_symbols
        self.generate_subscription_symbols()
        self.prune_dead_symbols()


        symbols_to_add = []
        for symbol in self.subscription_symbols:
            if symbol not in old_subscription_symbols:
                symbols_to_add.append(symbol)

        if len(symbols_to_add) > 0:
            print(f'Adding symbols:   {symbols_to_add}')


        symbols_to_remove = []
        for symbol in old_subscription_symbols:
            if symbol not in self.subscription_symbols:
                symbols_to_remove.append(symbol)

        if len(symbols_to_remove) > 0:
            print(f'Removing symbols: {symbols_to_remove}')

        if self.stream_thread is None:
            asyncio.set_event_loop_policy(AnyThreadEventLoopPolicy())

            self.stream_thread = threading.Thread(target=lambda: self.stream.run())
            self.stream_thread.start()

        if self.stream_thread is not None and len(symbols_to_remove) > 0:
            self.stream.unsubscribe_trades(*symbols_to_remove)
            self.stream.unsubscribe_quotes(*symbols_to_remove)

        if self.stream_thread is not None and len(symbols_to_add) > 0:
            self.stream.subscribe_trades(self.trades_callback, *symbols_to_add)
            self.stream.subscribe_quotes(self.quotes_callback, *symbols_to_add)


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


    def get_latest_quote(self, symbol, before_timestamp):
        if symbol not in self.symbol_to_quotes:
            print(f'Quotes not found for {symbol}')
            return None

        for quote in reversed(self.symbol_to_quotes[symbol]):
            if quote['timestamp'] <= before_timestamp:
                return quote

        print(f'Quote not found at or before {before_timestamp} for {symbol}')
        return None

    async def quotes_callback(self, q):
        max_quotes = 1000
        if q.symbol not in self.symbol_to_quotes:
            self.symbol_to_quotes[q.symbol] = []

        container = {'ask_price': q.ask_price, 'bid_price': q.bid_price, 'timestamp': q.timestamp}
        self.symbol_to_quotes[q.symbol].append(container)

        # Prune
        self.symbol_to_quotes[q.symbol] = self.symbol_to_quotes[q.symbol][-max_quotes:]


        # # Friction
        # spread = q.ask_price - q.bid_price
        # if spread <= 0.0:
        #     # print(f"Retrieved 0 or negative spread for {t.symbol}")
        #     return
        #
        # if q.symbol not in self.symbol_to_friction:
        #     self.symbol_to_friction[q.symbol] = []
        #
        # ask_size = q.ask_size
        # bid_size = q.bid_size
        # friction = ((bid_size / (ask_size + bid_size)) - 0.5) * 2.0
        #
        # container = {'index': q.timestamp, 'friction': friction}
        # self.symbol_to_friction[q.symbol].append(container)
        #
        # # Prune
        # max_friction_length = 20000
        # max_time_minutes = 3
        # check_time_minutes = 4
        # self.symbol_to_friction[q.symbol] = self.symbol_to_friction[q.symbol][-max_friction_length:]
        #
        # current_time = datetime.datetime.now(datetime.timezone.utc)
        # max_time = current_time - datetime.timedelta(minutes=max_time_minutes)
        # check_time = current_time - datetime.timedelta(minutes=check_time_minutes)
        #
        # if self.symbol_to_friction[q.symbol][0]['index'] < check_time:
        #     print(f'Friction prune initiated for {q.symbol}...')
        #     reduced_set = [r for r in self.symbol_to_friction[q.symbol] if r['index'] > max_time]
        #     print(f'Friction prune results for {q.symbol}: {len(self.symbol_to_friction[q.symbol])} -> {len(reduced_set)}')
        #     self.symbol_to_friction[q.symbol] = reduced_set


    async def trades_callback(self, t):
        # Manage symbol_to_trade_timestamps
        if t.symbol not in self.symbol_to_trade_timestamps:
            self.symbol_to_trade_timestamps[t.symbol] = []
        self.symbol_to_trade_timestamps[t.symbol].append(t.timestamp.timestamp())


        if t.symbol not in self.symbol_to_bars:
            self.initialize_historical_bars(t.symbol)

        bars = self.symbol_to_bars[t.symbol]

        ts = pandas.to_datetime(t.timestamp, unit='ns').replace(second=0, microsecond=0, nanosecond=0).tz_convert('utc')
        ts = self.nearest_candle(ts)

        if ts not in self.symbol_to_bars[t.symbol].index:
            self.prune_symbol_to_trade_timestamps(t.symbol)

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


        # # Ticker momentum
        # latest_quote = self.get_latest_quote(t.symbol, t.timestamp)
        # if not latest_quote:
        #     return
        #
        # ask = latest_quote['ask_price']
        # bid = latest_quote['bid_price']
        # spread = ask - bid
        # midpoint = (ask + bid) / 2
        # fill = t.price
        #
        # if spread <= 0.0:
        #     # print(f"Retrieved 0 or negative spread for {t.symbol}")
        #     return
        #
        # burst = (fill - midpoint) / (spread / 2)
        # weighted_burst = t.size * burst
        #
        # if t.symbol not in self.symbol_to_ticker_momentum:
        #     self.symbol_to_ticker_momentum[t.symbol] = []
        #
        # container = {'index': t.timestamp, 'oscillator': weighted_burst}
        # self.symbol_to_ticker_momentum[t.symbol].append(container)
        #
        # # Prune
        # max_ticker_momentum_length = 20000
        # max_time_minutes = 3
        # check_time_minutes = 4
        # self.symbol_to_ticker_momentum[t.symbol] = self.symbol_to_ticker_momentum[t.symbol][-max_ticker_momentum_length:]
        #
        # current_time = datetime.datetime.now(datetime.timezone.utc)
        # max_time = current_time - datetime.timedelta(minutes=max_time_minutes)
        # check_time = current_time - datetime.timedelta(minutes=check_time_minutes)
        #
        # if self.symbol_to_ticker_momentum[t.symbol][0]['index'] < check_time:
        #     print(f'Prune initiated for {t.symbol}...')
        #     reduced_set = [r for r in self.symbol_to_ticker_momentum[t.symbol] if r['index'] > max_time]
        #     print(f'Prune results for {t.symbol}: {len(self.symbol_to_ticker_momentum[t.symbol])} -> {len(reduced_set)}')
        #     self.symbol_to_ticker_momentum[t.symbol] = reduced_set


    def set_get_time_override_function(self, get_time_override_function):
        self.get_time_override_function = get_time_override_function
