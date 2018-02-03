# Holds all data manipulation and calculation functions

import pymongo
import gdax, time

mongo_client = pymongo.MongoClient()
db = mongo_client.algodb_test
BTC_collection = db.algoHistTable

# cur = BTC_collection.find().skip(1).limit(1)
# print cur
#
# for doc in cur:
#     print doc


# MACD 12 period moving average
def msema(n):
    curSum = 0
    new = BTC_collection.find().skip(n-11).limit(11)
    first = BTC_collection.find().skip(n-12).limit(1)
    print new
    print first
    for doc in new:
        print doc
        curVal = new.get(doc['hclose'])
        curSum = curSum + curVal
    ema = (2 * first['hclose'] + curSum) / 13
    print ema
    return ema

def msema2(n):
    curSum = 0
    new = BTC_collection.find().skip(n-11).limit(11)
    first = BTC_collection.find_one().skip(n-12)
    print new
    print first
    for doc in new:
        print doc
        curVal = new.get(doc['hclose'])
        curSum = curSum + curVal
    ema = (2 * first['hclose'] + curSum) / 13
    print ema
    return ema

# MACD 26 period moving average
def mlema(n):
    new = BTC_collection.find().skip(n-25).limit(25)
    first = BTC_collection.find().skip(n-26).limit(1)
    for doc in new:
        curVal = float(new["hclose"])
        curSum = curSum + curVal
    ema = (2 * float(first["hclose"]) + curSum) / 27
    print ema
    return ema

# MACD average indicator
def mave(n):
    ave = m12ema(n) - m26ema(n)
    print ave
    return ave

# MACD signal 9 period moving average indicator
def msig(n):
    sig = mave(n-35)
    return sig
