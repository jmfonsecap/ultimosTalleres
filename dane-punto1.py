from mrjob.job import MRJob

class MinMaxStockPrice(MRJob):
    def mapper(self, _, line):
        company, price, date = line.split(',')
        yield company, (float(price), date)
    
    def reducer(self, key, values):
        prices = list(values)
        min_price = min(prices[0])
        max_price = max(prices[0])
        yield key, (min_price[1], max_price[1])

if __name__ == '__main__':
    MinMaxStockPrice.run()