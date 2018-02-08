# Holds all data manipulation and calculation functions

import pymongo, sys
import gdax

mongo_client = pymongo.MongoClient()
db = mongo_client.algodb_test
histData = db.algoHistTable
calcData = db.calcData

# Slowwwwwwww. Use calcPopulateBulk().
def calcPopulate():
#   Set db collection titles to be added
    dictTitles = ['m12ema','m26ema','mave','msig']

    nCount = histData.count()
    batchSize = 100
    nCur = 0
    nSize = 100
    if (nCount < 100):
        nSize = nCount - 1
    try:
        while (nCur <= nCount):
            batchCursor = histData.find().sort('htime', pymongo.ASCENDING).skip(nCur).limit(batchSize)
            for doc in batchCursor:
                calcArray = [m12ema(nCur), m26ema(nCur), mave(nCur), msig(nCur)]
                calcUpdate = dict(zip(dictTitles,calcArray))
                histData.update_one(doc, {'$push': calcUpdate}, upsert = True)
                nCur = nCur + 1
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: calcPopulate2')
        print (sys.exc_info())

def calcPopulateBulk():
#   Set db collection titles to be added
    dictTitles = ['m12ema','m26ema','mave','msig']
    calcPush = []
    nCount = histData.count()
    batchSize = 1000
    nCur = 0
    batch = 0
    try:
        while (batch <= nCount/batchSize):
            batchCursor = histData.find().sort('htime', pymongo.ASCENDING).skip(nCur).limit(batchSize)
            for doc in batchCursor:
                calcArray = [m12ema(nCur), m26ema(nCur), mave(nCur), msig(nCur)]
                calcUpdate = dict(zip(dictTitles,calcArray))
                calcPush.append(pymongo.UpdateOne(doc,{'$push': calcUpdate}))
            batch = batch + 1
            print batch
            histData.bulk_write(calcPush)
            print 'push ' + str(batch) + ' complete'
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: calcPopulate2')
        print (sys.exc_info())

# MACD 12 period moving average
def m12ema(n):
    curSum = 0
    if n < 12:
        return ''
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
    if ( n < 26 ):
        return ''
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
    if (m12ema(n) == '' or m26ema(n) == ''):
        return
    ave = m12ema(n) - m26ema(n)
#    print ave
    return ave

# MACD signal 9 period moving average indicator
def msig(n):
    if (n < 35):
        return ''
    sig = mave(n-35)
    return sig
