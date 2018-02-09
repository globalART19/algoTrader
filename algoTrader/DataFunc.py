# Holds all data manipulation and calculation functions

import pymongo, sys
import gdax

mongo_client = pymongo.MongoClient()
db = mongo_client.algodb_test
histData = db.algoHistTable
calcData = db.calcData

# Delete all current calculations to allow repopulate
def deleteCalcs():
    try:
        histData.update({}, {'$unset': {'m12ema': 1, 'm26ema': 1, 'mave': 1, 'msig': 1}}, multi=True)
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: calcPopulateBulk')
        print (sys.exc_info())
    finally:
        print (sys.exc_info())
        print('deleteCalcs complete')

# Populate or update data from
def calcPopulateBulk():
#   Set db collection titles to be added
    dictTitles = ['m12ema','m26ema','mave','msig']
    calcPush = []
#    if (nCount == -1):
    nCount = histData.count()
    batchSize = 1000
    intData = histData.find().limit(2)
    tInt = []
    for doc in intData:
        tInt.append(doc['htime'])
    tStart = tInit = tInt[0]
    tInt = tInt[1] - tInt[0]
    #nCur = 0
    nCur = tStart
    batch = 1
    try:
        while (batch <= nCount/batchSize):
            tEnd = tStart + tInt * batchSize
            while (not(histData.find_one({'htime': tEnd}) > 0)):
                   tEnd = tEnd + tInt
            #batchCursor = histData.find().sort('htime', pymongo.ASCENDING).skip(nCur).limit(batchSize)
            batchCursor = histData.find({'htime': {'$gte': tStart, '$lte': tEnd}})
            for doc in batchCursor:
                try:
                    test = doc['m12ema']
                    #nCur = nCur + 1
                    nCur += tInt
                except KeyboardInterrupt:
                    sys.exc_info()
                    sys.exc_clear()
                    break
                except:
                    nCur = doc['htime']
                    #print 'pre calc'
                    m12 = m12ema(nCur,tInt,tInit)
                    m26 = m26ema(nCur,tInt,tInit)
                    try:
                        ave = m12 - m26
                    except:
                        ave = None
                        sys.exc_clear()
                    calcArray = [m12, m26, ave, msig(nCur,tInt,tInit)]
                    #print 'calced'
                    calcUpdate = dict(zip(dictTitles,calcArray))
                    calcPush.append(pymongo.UpdateOne(doc,{'$set': calcUpdate}))
                    #nCur = nCur + 1
                    nCur += tInt
                    #print (nCur - tStart)/tInt
                    sys.exc_clear()
            #print 'pre push'
            if (len(calcPush)>0):
                histData.bulk_write(calcPush)
            tStart = tEnd
            print batch
            batch += 1
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: calcPopulateBulk')
        print (sys.exc_info())
    finally:
        print (sys.exc_info())
        print('calcPopulate complete')


# MACD 12 period moving average
def m12ema(n,i,init):
    curSum = 0
    if n - init < 12 * i:
        return
    f = n - 12 * i
    while (not (histData.find_one({'htime': f}) > 0)):
        f -= i
        if f < init:
            return
    #new = histData.find().skip(n-11).limit(11)
    #first = histData.find().skip(n-12).limit(1)
    new = histData.find({'htime': {'$gte': f, '$lt': n}})
    while (new.count() < 11):
        f -= i
        if f < init:
            return
        new = histData.find({'htime': {'$gt': f, '$lte': n}})
    first = histData.find_one({'htime': f})
#    print new
#    print first
    for doc in new:
#        print doc
        curVal = float(doc['hclose'])
        curSum = curSum + curVal
    firstVal = float(first['hclose'])
    ema = (2 * firstVal + curSum) / 13
    return ema

# MACD 26 period moving average
def m26ema(n,i,init):
    curSum = 0
    if (n - init < 26 * i):
        return
    f = n - 26 * i
    while (not (histData.find_one({'htime': f}) > 0)):
        f -= i
        if f < init:
            return
    #new = histData.find().skip(n-25).limit(25)
    #first = histData.find().skip(n-26).limit(1)
    new = histData.find({'htime': {'$gte': f, '$lt': n}})
    while (new.count() < 25):
        f -= i
        if f < init:
            return
        new = histData.find({'htime': {'$gt': f, '$lte': n}})
    first = histData.find_one({'htime': f})
    for doc in new:
        curVal = float(doc["hclose"])
        curSum = curSum + curVal
    #for i in first:
    #    firstVal = float(i['hclose'])
    firstVal = float(first['hclose'])
    ema = (2 * firstVal + curSum) / 27
    return ema

# MACD average indicator
def mave(n,):
    ave = m12ema(n) - m26ema(n)
    return ave

# MACD signal 9 period moving average indicator
def msig(n,i,init):
    maveSum = 0
    if (n - init < (35+26+9) * i):
        return
    m26 = m26ema(n-35*i,i,init)
    try:
        m26 = m26 + 1
    except:
        sys.exc_clear()
        return
    for j in range(1,10):
        f = (n - (35 + j) * i)
        maveSum += m12ema(f,i,init) - m26ema(f,i,init)
    sig = (m12ema(n-35*i,i,init) - m26)*.2 + (maveSum/9)*.8
    return sig

'''
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
'''
