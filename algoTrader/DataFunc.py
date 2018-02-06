# Holds all data manipulation and calculation functions

import pymongo
import gdax, time

mongo_client = pymongo.MongoClient()
db = mongo_client.algodb_test
histData = db.algoHistTable

def calcPopulate(n):
    


# MACD 12 period moving average
def m12ema(n):
    curSum = 0
    if n < 12:
        return
    new = histData.find().skip(n-11).limit(11)
    first = histData.find().skip(n-12).limit(1)
#    print new
#    print first
    for doc in new:
#        print doc
        curVal = float(doc['hclose'])
        curSum = curSum + curVal
    for i in first:
        firstVal = float(i['hclose'])
    #print firstVal
    ema = (2 * firstVal + curSum) / 13
    #print ema
    return ema

# MACD 26 period moving average
def m26ema(n):
    curSum = 0
    new = histData.find().skip(n-25).limit(25)
    first = histData.find().skip(n-26).limit(1)
    for doc in new:
        curVal = float(doc["hclose"])
        curSum = curSum + curVal
    for i in first:
        firstVal = float(i['hclose'])
#    print firstVal
    ema = (2 * firstVal + curSum) / 27
#    print ema
    return ema

# MACD average indicator
def mave(n):
    ave = m12ema(n) - m26ema(n)
#    print ave
    return ave

# MACD signal 9 period moving average indicator
def msig(n):
    sig = mave(n-35)
    return sig
