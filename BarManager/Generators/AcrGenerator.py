
class AcrGenerator:
    def __init__(self, period):
        self.period = period

    def generate(self, df):
        df[self.__generate_name()] = (df['high'] - df['low']).rolling(window=self.period).mean()

    def __generate_name(self):
        return f'ACR.{self.period}'
