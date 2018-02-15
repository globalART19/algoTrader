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
    intData = histData.find().sort({'htime': pymongo.DESCENDING}).limit(2)
    tInt = []
    for doc in intData:
        tInt.append(doc['htime'])
    tStart = tInit = tInt[0]
    tInt = tInt[0] - tInt[1]
    #nCur = 0
    nCur = tStart
    batch = 1
    try:
        while (batch <= nCount/batchSize):
            tEnd = tStart - tInt * batchSize
            while (not(histData.find_one({'htime': tEnd}) > 0)):
                   tEnd = tEnd - tInt
            #batchCursor = histData.find().sort('htime', pymongo.ASCENDING).skip(nCur).limit(batchSize)
            if tStart == tInit:
                batchCursor = histData.find({'htime': {'$gte': tEnd, '$lte': tStart}})
            else:
                batchCursor = histData.find({'htime': {'$gte': tEnd, '$lte': tStart}})
                while(batchCursor.count() < 70):
                    tEnd = tEnd + tInt
                    batchCursor = histData.find({'htime': {'$gte': tEnd, '$lte': tStart}})
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

# Populate or update data from
def calcPopulateBulk2():
#   Set db collection titles to be added
    dictTitles = ['m12ema','m26ema','mave','msig']
    calcPush = []
#    if (nCount == -1):
    nCount = histData.count()
    batchSize = 1000
    intData = histData.find().sort({'htime': pymongo.ASCENDING}).limit(2)
    tInt = []
    for doc in intData:
        tInt.append(doc['htime'])
    tInit = tInt[0]
    tInt = tInt[0] - tInt[1]
    init = histData.find().sort({'htime': pymongo.DESCENDING}).limit(1)
    for doc in init:
        tStart = doc['htime']
    nCur = tStart
    batch = 1
    try:
        while (batch <= nCount/batchSize):
            updateMarker = 0
            array12 = []
            array26 = []
            arrayAve = []
            #arraySig = []
            tEnd = tStart - tInt * batchSize
            batchCursor = histData.find({'htime': {'$gte': tEnd, '$lte': tStart}}).sort({'htime': pymongo.DESCENDING})
            while(batchCursor.count() < 700):
                tEnd = tEnd - 300 * tInt
                batchCursor = histData.find({'htime': {'$gte': tEnd, '$lte': tStart}}).sort({'htime': pymongo.DESCENDING})
            for doc in batchCursor:
                try:
                    test = doc['mave']
                    nCur -= tInt
                    if updateMarker == 0:
                        array12 = [0] * 11
                        array26 = [0] * 25
                        arrayAve = [0] * 34
                        updateMarker = 1
                except KeyboardInterrupt:
                    sys.exc_info()
                    sys.exc_clear()
                    break
                except:
                    nCur = doc['htime']
                    curPrice = doc['hclose']
                    if nCur - array12[0][0] > tInt:
                        array12 = []
                        array26 = []
                        arrayAve = []
                        #arraySig = []
                    array12.insert([nCur,curPrice])
                    array26.insert([nCur,curPrice])
                    arrayAve.insert([nCur,curPrice])
                    #arraySig.insert([nCur,curPrice])
                    if updateMarker == 1:
                        prevEntry = histData.find_one({'htime': (doc['htime'] + tInt)})
                        array12[0].append(prevEntry['m12ema'])
                        array26[0].append(prevEntry['m26ema'])
                        arrayAve[0].append(prevEntry['mave'])
                        #arraySig[0].append(prevEntry['msig'])
                        updateMarker = 0
                    if len(array12) == 12:
                        m12 = m12ema(array12)
                        del array12[-1]
                    else:
                        m12 = None
                    if len(array26) == 26:
                        m26 = m26ema(array26)
                        mA = m12 - m26
                        del array26[-1]
                        #del arrayAve[-1]
                    else:
                        m26 = None
                        mA = None
                    if len(array35) == 35:
                        mS = msig(arrayAve)
                        del arrayAve[-1]
                    else:
                        mS = None
                    array12[0].append(m12)
                    array26[0].append(m26)
                    arrayAve[0].append(mA)
                    #arraySig[0].append(mS)
                    calcArray = [m12, m26, mA, mS]
                    calcUpdate = dict(zip(dictTitles,calcArray))
                    calcPush.append(pymongo.UpdateOne(doc,{'$set': calcUpdate}))
                    nCur -= tInt
                    sys.exc_clear()
            histData.bulk_write(calcPush)
            tStart = tEnd
            print batch + ' complete!'
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
def m12ema(dataArray):
    try:
        lastM12 = dataArray[1][2]
        ema = dataArray[0][1] * 2/13 + lastM12 * 11/13
    except:
        curSum = 0
        for item in range(1,len(dataArray)):
            curSum = curSum + item[1]
        ema = dataArray[0][1] * 2/13 + curSum * 11/13
        sys.exc_clear()
    return ema


# MACD 26 period moving average
def m26ema(dataArray):
    try:
        lastM26 = dataArray[1][2]
        ema = dataArray[0][1] * 2/13 + lastM26 * 11/13
    except:
        curSum = 0
        for item in range(1,len(dataArray)):
            curSum = curSum + item[1]
        ema = dataArray[0][1] * 2/27 + curSum * 11/27
        sys.exc_clear()
    finally:
        return ema

# MACD signal 9 period moving average indicator
def msig(dataArray):
    maveSum = 0
    try:
        lastMAve = dataArray[1][2]
        mS = dataArray[0][1] * 2/10 + lastMAve * 8/10
    except:
        for item in range(1,len(dataArray)):
            curSum = curSum + item[1]
        mS = dataArray[0][1] * 2/27 + curSum * 11/27
        sys.exc_clear()
    finally:
        return mS


# # Slowwwwwwww. Use calcPopulateBulk().
# def calcPopulate():
# #   Set db collection titles to be added
#     dictTitles = ['m12ema','m26ema','mave','msig']
#
#     nCount = histData.count()
#     batchSize = 100
#     nCur = 0
#     nSize = 100
#     if (nCount < 100):
#         nSize = nCount - 1
#     try:
#         while (nCur <= nCount):
#             batchCursor = histData.find().sort('htime', pymongo.DESCENDING).skip(nCur).limit(batchSize)
#             for doc in batchCursor:
#                 calcArray = [m12ema(nCur), m26ema(nCur), mave(nCur), msig(nCur)]
#                 calcUpdate = dict(zip(dictTitles,calcArray))
#                 histData.update_one(doc, {'$set': calcUpdate}, upsert = True)
#                 nCur = nCur + 1
#     except (KeyboardInterrupt, SystemExit):
#         pass
#     except:
#         print ('Unknown exception: calcPopulate2')
#         print (sys.exc_info())

# # MACD 12 period moving average
# def m12ema(n,interval,init):
#     curSum = 0
#     if n - init < 12 * interval:
#         return
#     f = n - 12 * interval
#     while (not (histData.find_one({'htime': f}) > 0)):
#         f -= interval
#         if f < init:
#             return
#     #new = histData.find().skip(n-11).limit(11)
#     #first = histData.find().skip(n-12).limit(1)
#     new = histData.find({'htime': {'$gte': f, '$lt': n}})
#     while (new.count() < 11):
#         f -= interval
#         if f < init:
#             return
#         new = histData.find({'htime': {'$gt': f, '$lte': n}})
#     first = histData.find_one({'htime': f})
# #    print new
# #    print first
#     for doc in new:
# #        print doc
#         curVal = float(doc['hclose'])
#         curSum = curSum + curVal
#     firstVal = float(first['hclose'])
#     ema = (2 * firstVal + curSum) / 13
#     return ema

# # MACD 26 period moving average
# def m26ema(n,interval,init):
#     curSum = 0
#     if (n - init < 26 * interval):
#         return
#     f = n - 26 * interval
#     while (not (histData.find_one({'htime': f}) > 0)):
#         f -= interval
#         if f < init:
#             return
#     #new = histData.find().skip(n-25).limit(25)
#     #first = histData.find().skip(n-26).limit(1)
#     new = histData.find({'htime': {'$gte': f, '$lt': n}})
#     while (new.count() < 25):
#         f -= interval
#         if f < init:
#             return
#         new = histData.find({'htime': {'$gt': f, '$lte': n}})
#     first = histData.find_one({'htime': f})
#     for doc in new:
#         curVal = float(doc["hclose"])
#         curSum = curSum + curVal
#     #for i in first:
#     #    firstVal = float(i['hclose'])
#     firstVal = float(first['hclose'])
#     ema = (2 * firstVal + curSum) / 27
#     return ema

# MACD average indicator
# def mave(n,m12='a',m26='a'):
#     if m12 == 'a':
#         m12 = m12ema(n)
#     if m26 == 'a':
#         m26 = m26ema(n)
#     return (m12 - m26)

# # MACD signal 9 period moving average indicator
# def msig(n,interval,init,m12='a',m26='a'):
#     maveSum = 0
#     if (n - init < (35+26+9) * interval):
#         return
#     m26 = m26ema(n-35*interval,interval,init)
#     try:
#         m26 = m26 + 1
#     except:
#         sys.exc_clear()
#         return
#     for j in range(1,10):
#         f = (n - (35 + j) * interval)
#         maveSum += m12ema(f,interval,init) - m26ema(f,interval,init)
#     sig = (m12ema(n-35*interval,interval,init) - m26)*.2 + (maveSum/9)*.8
#     return sig
