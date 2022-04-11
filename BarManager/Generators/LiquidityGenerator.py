
class LiquidityGenerator:
    def __init__(self, dollars_risk=100, acr_period=15, volume_period=10):
        self.dollars_risk = dollars_risk
        self.acr_period = acr_period
        self.volume_period = volume_period

    def generate(self, df):
        df[self.__generate_name()] = (self.dollars_risk / (((df['high'] - df['low']).rolling(window=self.acr_period).mean()) / 2)) / df['volume'].rolling(window=self.volume_period).mean()

    def __generate_name(self):
        return
