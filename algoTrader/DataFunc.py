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
                histData.update_one(doc, {'$set': calcUpdate}, upsert = True)
                nCur = nCur + 1
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: calcPopulate2')
        print (sys.exc_info())

# Populate or update data from
def calcPopulateBulk():
#   Set db collection titles to be added
    dictTitles = ['m12ema','m26ema','mave','msig']
    calcPush = []
#    if (nCount == -1):
    nCount = histData.count()
    batchSize = 1000
    nCur = 0
    batch = 0
    try:
        while (batch <= nCount/batchSize):
            batchCursor = histData.find().sort('htime', pymongo.ASCENDING).skip(nCur).limit(batchSize)
            for doc in batchCursor:
                try:
                    test = doc['m12ema']
                except KeyboardInterrupt:
                    break
                except:
                    calcArray = [m12ema(nCur), m26ema(nCur), mave(nCur), msig(nCur)]
                    calcUpdate = dict(zip(dictTitles,calcArray))
                    calcPush.append(pymongo.UpdateOne(doc,{'$set': calcUpdate}))
                    nCur = nCur + 1
                    sys.exc_clear()
            batch = batch + 1
            histData.bulk_write(calcPush)
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: calcPopulateBulk')
        print (sys.exc_info())
    finally:
        print('calcPopulate complete')

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
    ema = (2 * firstVal + curSum) / 13
    return ema

# MACD 26 period moving average
def m26ema(n):
    curSum = 0
    if ( n < 26 ):
         return
    new = histData.find().skip(n-25).limit(25)
    first = histData.find().skip(n-26).limit(1)
    for doc in new:
        curVal = float(doc["hclose"])
        curSum = curSum + curVal
    for i in first:
        firstVal = float(i['hclose'])
    ema = (2 * firstVal + curSum) / 27
    return ema

# MACD average indicator
def mave(n):
    if (n < 26):
        return
    ave = m12ema(n) - m26ema(n)
    return ave

# MACD signal 9 period moving average indicator
def msig(n):
    maveSum = 0
    if (n < 35+26+9):
        return
    for i in range(1,10):
        maveSum += mave(n-35-i)
    sig = mave(n-35)*.2 + (maveSum/9)*.8
    return sig
