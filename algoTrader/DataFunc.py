# Holds all data manipulation and calculation functions

import pymongo, sys
import gdax

db = pymongo.MongoClient().algodb_test
histData = db.algoHistTable
calcData = db.calcData

# Delete all current calculations to allow repopulate
def deleteCalcs():
    try:
        histData.update({}, {'$unset': {'m12ema': 1, 'm26ema': 1, 'mave': 1, 'msig': 1, 'rsi': 1}}, multi=True)
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
    dictTitles = ['m12ema', 'm26ema', 'mave', 'msig', 'rsi']
    calcPush = []
#    if (nCount == -1):
    nCount = histData.count()
    batchSize = 1000
    intData = histData.find().sort('htime', pymongo.ASCENDING).limit(2)
    tInt = []
    for doc in intData:
        tInt.append(doc['htime'])
    tOldest = tInt[0]
    tInt = abs(tInt[1] - tInt[0])
    print tInt
    init = histData.find().sort('htime', pymongo.DESCENDING).limit(1)
    for doc in init:
        tStart = doc['htime']
    nCur = tStart
    batch = 1
    print 'calc initialized'
    try:
        while (nCur >= tOldest):
            # loopNum = 1
            # updateMarker = 0
            # array12 = [[0]]
            # array26 = []
            # arrayAve = []
            dataArray = [[0]]
            #arraySig = []
            tEnd = tStart - tInt * batchSize
            # print tEnd
            batchCursor = histData.find({'htime': {'$gte': tEnd, '$lte': tStart}}).sort('htime', pymongo.DESCENDING)
            bcCount = batchCursor.count()
            while(bcCount < 700):
                tEnd = tEnd - 300 * tInt
                print '<700 datapoints', batch, tEnd
                batchCursor = histData.find({'htime': {'$gte': tEnd, '$lte': tStart}}).sort('htime', pymongo.DESCENDING)
                bcCount = batchCursor.count()
                if tEnd <= tOldest:
                    tEnd = tOldest
                    bcCount = 1000
            # print 'batch ' + str(batch) + ' set up. starting for loop'
            for doc in batchCursor:
                try:
                    test = doc['mave']
                    #print 'try 1 pass'
                    nCur -= tInt
                    # if (updateMarker == 0 and test != None):
                    #     print 'update'
                    #     array12 = [0] * 11
                    #     array26 = [0] * 25
                    #     arrayAve = [0] * 34
                    #     updateMarker = 1
                except KeyboardInterrupt:
                    sys.exc_info()
                    sys.exc_clear()
                    break
                except KeyError:
                    nCur = doc['htime']
                    curPrice = doc['hclose']
                    # if nCur - array12[0][0] > tInt:
                    #     array12 = []
                    #     array26 = []
                    #     arrayAve = []
                    #     #arraySig = []
                    # array12.insert(0,[nCur,curPrice])
                    # array26.insert(0,[nCur,curPrice])
                    # arrayAve.insert(0,[nCur,curPrice])
                    if nCur - dataArray[0][0] > tInt:
                        dataArray = []
                    dataArray.insert(0,[nCur,curPrice])
                    arrLength = len(dataArray)
                    if arrLength >= 35:
                        m12 = m12ema(dataArray)
                        m26 = m26ema(dataArray)
                        mA = m12 - m26
                        dataArray[0].extend([m12, m26, mA])
                        mS = msig(dataArray)
                        dataArray[0].extend([mS, rsi])
                    elif arrLength >= 26:
                        m12 = m12ema(dataArray)
                        m26 = m26ema(dataArray)
                        mA = m12 - m26
                        mS = None
                        rsi = rsiFunc(dataArray)
                        dataArray[0].extend([m12, m26, mA, mS, rsi])
                    elif arrLength >= 15:
                        m12 = m12ema(dataArray)
                        m26 = None
                        mA = None
                        mS = None
                        rsi = rsiFunc(dataArray)
                        dataArray[0].extend([m12, m26, mA, mS, rsi])
                    elif arrLength >= 12:
                        m12 = m12ema(dataArray)
                        m26 = None
                        mA = None
                        mS = None
                        rsi = None
                        dataArray[0].extend([m12, m26, mA, mS, rsi])
                    else:
                        m12 = None
                        m26 = None
                        mA = None
                        mS = None
                        rsi = None
                        dataArray[0].extend([m12, m26, mA, mS, rsi])
                    #arraySig.insert([nCur,curPrice])
                    # if updateMarker == 1:
                    #     print 'update'
                    #     prevEntry = histData.find_one({'htime': (nCur + tInt)})
                    #     array12[0].append(prevEntry['m12ema'])
                    #     array26[0].append(prevEntry['m26ema'])
                    #     arrayAve[0].append(prevEntry['mave'])
                    #     #arraySig[0].append(prevEntry['msig'])
                    #     updateMarker = 0
                    # if len(array12) == 12:
                    #     #print 'm12'
                    #     m12 = m12ema(array12)
                    #     del array12[-1]
                    # else:
                    #     m12 = None
                    # if len(array26) == 26:
                    #     #print 'm26'
                    #     m26 = m26ema(array26)
                    #     mA = m12 - m26
                    #     del array26[-1]
                    #     #del arrayAve[-1]
                    # else:
                    #     m26 = None
                    #     mA = None
                    # array12[0].append(m12)
                    # array26[0].append(m26)
                    # arrayAve[0].append(mA)
                    # #arraySig[0].append(mS)
                    # if len(arrayAve) == 36:
                    #     #print 'mS'
                    #     mS = msig(arrayAve)
                    #     del arrayAve[-1]
                    # else:
                    #     mS = None
                    calcArray = [m12, m26, mA, mS, rsi]
                    calcUpdate = dict(zip(dictTitles,calcArray))
                    calcPush.append(pymongo.UpdateOne(doc,{'$set': calcUpdate}))
                    nCur -= tInt
                    sys.exc_clear()
                # finally:
                #     print loopNum
                #     loopNum += 1
            # print 'pre-batch push'
            histData.bulk_write(calcPush)
            calcPush = []
            tStart = tEnd
            # print 'batch ' + str(batch) + ' complete!'
            batch += 1
    except (KeyboardInterrupt, SystemExit):
        pass
    # except:
    #     print ('Unknown exception: calcPopulateBulk')
    #     print (sys.exc_info())
    finally:
        print (sys.exc_info())
        print('calcPopulate complete')


# MACD 12 period moving average
def m12ema(dataArray):
    lastM12 = dataArray[1][2]
    if lastM12 != None:
        ema = dataArray[0][1] * 2/13 + lastM12 * 11/13
    else:
        curSum = 0
        for i in range(1,12):
            curSum = curSum + dataArray[i][1]
        ema = dataArray[0][1] * 2/13 + curSum / 13
    return ema


# MACD 26 period moving average
def m26ema(dataArray):
    lastM26 = dataArray[1][3]
    if lastM26 != None:
        ema = dataArray[0][1] * 2/27 + lastM26 * 25/27
    else:
        curSum = 0
        for i in range(1,26):
            curSum = curSum + dataArray[i][1]
        ema = dataArray[0][1] * 2/27 + curSum / 27
    return ema

# MACD signal 9 period moving average indicator
def msig(dataArray):
    maveSum = 0
    lastMAve = dataArray[1][4]
    if lastMAve != None:
        mS = dataArray[0][4] * 2/10 + lastMAve * 8/10
    else:
        for i in range(1,10):
            curSum = curSum + dataArray[i][4]
        mS = dataArray[0][2] * 2/10 + curSum / 10
    return mS

# Relative Strength indicator
def rsiFunc(dataArray):
    gain = 0
    numGain = 0
    loss = 0
    numLoss = 0
    curPrice = dataArray[14][1]
    for i in range(13, -1, -1):
        lastPrice = curPrice
        curPrice = dataArray[i][1]
        glValue = curPrice - lastPrice
        if glValue >= 0:
            gain += glValue
            numGain += 1
        else:
            loss += glValue
            numLoss += 1
    if numGain == 0:
        numGain = 1
        print 'gain: ' + str(gain)
        return 0
    if numLoss == 0:
        print 'loss: ' + str(loss)
        return 100
    rsi = 100 - 100/(1+(gain/numGain)/(abs(loss)/numLoss))
    return rsi
