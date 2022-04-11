
class MaGenerator:
    def __init__(self, field, period):
        self.field = field
        self.period = period

    def generate(self, df):
        df[self.__generate_name()] = df[self.field].rolling(window=self.period).mean()

    def __generate_name(self):
        return f'{self.field}_MA.{self.period}'
