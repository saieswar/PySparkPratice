from pyspark import SparkContext

sc = SparkContext(master="local", appName="broadcastvariable")

nyse_daily_rdd = sc.textFile("./inputData/nyse_daily.tsv")
nyse_dividend_rdd = sc.textFile("./inputData/nyse_dividends.tsv")

def getParsedNyseDailyRecord(record):
    """
    results in parsing NYSE daily Record
    :param nyse_daily_record:
    :return: ((exchange, symbol, date), openPrice,  closePrice)
    """
    daily_record = record.split("\t")
    exchange = str(daily_record[0])
    symbol = str(daily_record[1])
    date = str(daily_record[2])
    open = float(daily_record[3])
    #high = float(daily_record[4])
    close = float(daily_record[6])
    return ((exchange, symbol, date), open,  close)

def getParsedDividendRecord(record):
    """
    Dividend Rdd with parsed format

    :param dividend record:
    :return: ((exchange, symbol, date), dividend)
    """
    daily_dividend_list = record.split("\t")
    exchange = str(daily_dividend_list[0])
    symbol = str(daily_dividend_list[1])
    date = str(daily_dividend_list[2])
    dividend = float(daily_dividend_list[3])
    return ((exchange, symbol, date), dividend)


def getDiffAndDividends(daily_pair_iter):
    """
    Genereates close price and open price for the exchange on given date along with dividend
    :param nyse_daily_rdd:
    :return: exchange, symbol, date, price_diff, dividend
    """
    for daily_pair in daily_pair_iter:
        key, open, close = daily_pair
        dividend = dividend_brct.value.get(key)
        if dividend is None:
            continue
        else:
            exchange, symbol, date = key
            yield "{} {} {} {} {}".format(exchange, symbol, date, close - open, dividend)


parsed_nyse_daily_rdd = nyse_daily_rdd.map(getParsedNyseDailyRecord)
parsed_nyse_dividend = nyse_dividend_rdd.map(getParsedDividendRecord)
dividends_dict = dict(parsed_nyse_dividend.collect())
dividend_brct = sc.broadcast(dividends_dict)
results_rdd = parsed_nyse_daily_rdd.mapPartitions(getDiffAndDividends)

#printing few records
for result in results_rdd.take(5):
    print(result)

