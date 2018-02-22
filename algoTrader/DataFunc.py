# Holds all data manipulation and calculation functions

import pymongo, sys, gdax, time, datetime
import plotly.offline as po
import plotly.graph_objs as go

db = pymongo.MongoClient().algodb_test
histData = db.algoHistTable

# Delete all current calculations to allow repopulate
def deleteCalcs():
    try:
        histData.update({}, {'$unset': {'m12ema': 1, 'm26ema': 1, 'mave': 1, 'msig': 1, 'rsi': 1}}, multi=True)
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: deleteCalcs')
        print (sys.exc_info())
    finally:
        print (sys.exc_info())
        print('deleteCalcs complete')

# Populate or update indicator calculations from historical data.
# Loop runs from the oldest datapoint to the newest datapoint.
def calcPopulateBulk():
    # Set db collection titles to be added
    dictTitles = ['m12ema', 'm26ema', 'mave', 'msig', 'rsi']
    # Initialize variables
    calcPush = []
    batch = 1
    quitTrigger = 0
    dataArray = [[0]]
    nCount = histData.count()
    # Set size of batch to be pushed to mongo database
    batchSize = 1000
    # Find most recent db entries and use them to determine the time interval
    intData = histData.find().sort('htime', pymongo.DESCENDING).limit(2)
    tInt = []
    for doc in intData:
        tInt.append(doc['htime'])
    tNewest = tInt[0]
    tInt = abs(tInt[1] - tInt[0])
    # print tInt #----------------------------------------------------------------------
    # Find and store earliest historical datapoint
    init = histData.find().sort('htime', pymongo.ASCENDING).limit(1)
    for doc in init:
        tStart = doc['htime']
    nCur = tStart
    print 'calc initialized'
    try:
        # Loop over historical dataset until last entry (quitTrigger) submitted.
        while (quitTrigger != 1):
            # Determine last entry time for current batch
            tEnd = tStart + tInt * batchSize
            # print tEnd # ------------------------------------------------------------
            batchCursor = histData.find({'htime': {'$gte': tStart, '$lte': tEnd}}).sort('htime', pymongo.ASCENDING)
            bcCount = batchCursor.count()
            # Signal to quit loop if last entry is pulled. Set batch as full
            if tEnd >= tNewest:
                tEnd = tNewest
                bcCount = batchSize
                quitTrigger = 1
            # Count the size of the batch pull. If too small, add more entries until large enough to justify run. +++++++++
            while(bcCount < batchSize * .7):
                tEnd = tEnd + batchSize * .3 * tInt
                # print '<700 datapoints', batch, tEnd # -----------------------------------------------------------------------
                batchCursor = histData.find({'htime': {'$gte': tStart, '$lte': tEnd}}).sort('htime', pymongo.ASCENDING)
                bcCount = batchCursor.count()
                # Signal to quit loop if last entry is pulled.
                if tEnd >= tNewest:
                    tEnd = tNewest
                    bcCount = batchSize
                    quitTrigger = 1
            # print 'batch ' + str(batch) + ' set up. starting for loop' # ---------------------------------------
            # Loop over documents in batch. Run calculations. Build batch data to push to db.
            for doc in batchCursor:
                # Test is calculations already exist. If they do, skip to next entry.
                try:
                    test = doc['mave']
                    # print 'try 1 pass' # ------------------------------------------
                    nCur -= tInt
                except KeyboardInterrupt:
                    sys.exc_info()
                    sys.exc_clear()
                    break
                # If calcs do not exist, run calculations.
                except KeyError:
                    nCur = doc['htime']
                    curPrice = doc['hclose']
                    # If the data skips an entry, clear calculation data storage and restart. Prevents averaging over missing data points.
                    if nCur - dataArray[0][0] > tInt:
                        dataArray = []
                    # Insert data into the calculation data array
                    dataArray.insert(0,[nCur,curPrice])
                    arrLength = len(dataArray)
                    # Determine which indicators to calculate based on how many data points exist (dataArray size) and store them in the data array
                    if arrLength >= 35:
                        m12 = m12ema(dataArray)
                        m26 = m26ema(dataArray)
                        mA = m12 - m26
                        dataArray[0].extend([m12, m26, mA])
                        mS = msig(dataArray)
                        rsi = rsiFunc(dataArray)
                        dataArray[0].extend([mS, rsi])
                        # Remove oldest (unneeded) data point to prevent size creep
                        del dataArray[-1]
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
                    # Create array/dict to be pushed to database
                    calcArray = [m12, m26, mA, mS, rsi]
                    calcUpdate = dict(zip(dictTitles,calcArray))
                    # Append push command to bulk push command
                    calcPush.append(pymongo.UpdateOne(doc,{'$set': calcUpdate}))
                    nCur -= tInt
                    sys.exc_clear()
            # print 'pre-batch push'
            histData.bulk_write(calcPush)
            calcPush = []
            tStart = tEnd + tInt
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
    # If previous tic has M12 data, calculate simply.
    if lastM12 != None:
        ema = dataArray[0][1] * 2/13 + lastM12 * 11/13
    # Else average over previous stored datapoints.
    else:
        curSum = 0
        for i in range(1,12):
            curSum = curSum + dataArray[i][1]
        ema = dataArray[0][1] * 2/13 + curSum / 13
    return ema


# MACD 26 period moving average
def m26ema(dataArray):
    lastM26 = dataArray[1][3]
    # If previous tic has M26 data, calculate simply.
    if lastM26 != None:
        ema = dataArray[0][1] * 2/27 + lastM26 * 25/27
    # Else average over previous stored datapoints.
    else:
        curSum = 0
        for i in range(1,26):
            curSum = curSum + dataArray[i][1]
        ema = dataArray[0][1] * 2/27 + curSum / 27
    return ema

# MACD signal 9 period moving average indicator
def msig(dataArray):
    maveSum = 0
    # If previous tic has MAve data, calculate simply.
    lastMAve = dataArray[1][4]
    if lastMAve != None:
        mS = dataArray[0][4] * 2/10 + lastMAve * 8/10
    # Else average over previous stored datapoints.
    else:
        for i in range(1,10):
            curSum = curSum + dataArray[i][4]
        mS = dataArray[0][2] * 2/10 + curSum / 10
    return mS

# Relative Strength Indicator - SMA version, less accurate? Switch to SMMA?
def rsiFunc(dataArray):
    # Initialize variables as proper data types
    gain = 0.0
    numGain = 0
    loss = 0.0
    numLoss = 0
    curPrice = dataArray[0][1]
    # Calculate gain/loss between current/previous price and total gains and losses
    for i in range(1,15):
        lastPrice = curPrice
        curPrice = dataArray[i][1]
        glValue = curPrice - lastPrice
        if glValue >= 0:
            gain += glValue
            numGain += 1
            # print 'gain: ', gain, numGain # -----------------------------------------
        else:
            loss += glValue
            numLoss += 1
            # print 'loss: ', loss, numLoss # ------------------------------------------
    # Handle edge cases
    if numGain == 0:
        # print 'gain: ', gain, dataArray[0][0] # -----------------------------------------
        return 0
    if numLoss == 0:
        # print 'loss: ', loss, dataArray[0][0] # -----------------------------------------
        return 100
    # Calculate RSI
    rsi = 100 - 100/(1+(gain/numGain)/(abs(loss)/numLoss))
    # print gain, numGain, loss, numLoss, (gain/numGain)/(abs(loss)/numLoss)
    return rsi

# Print combined graph of price and indicators to visualize what is happening.
def cGraph(tStart = 1515000000):
    # Initialize variables
    dTime = []
    price = []
    m12 = []
    m26 = []
    mave = []
    msig = []
    rsi = []
    tradesBuyTime = []
    tradesBuy = []
    tradesSellTime = []
    tradesSell = []
    # Handle data missing
    if("algoHistTable" not in db.collection_names()):
        print('Historical data does not exist. Quitting...')
        raise Exception
    # Build datasets for graph
    for doc in histData.find({'htime': {'$gte': tStart}}).sort('htime',pymongo.ASCENDING):
        dTime.append(datetime.datetime.fromtimestamp(doc['htime']))
        price.append(doc['hclose'])
        m12.append(doc['m12ema'])
        m26.append(doc['m26ema'])
        mave.append(doc['mave'])
        msig.append(doc['msig'])
        rsi.append(doc['rsi'])
        try:
            if doc['action'] == 'buy':
                tradesBuy.append(doc['hclose'])
                tradesBuyTime.append(datetime.datetime.fromtimestamp(doc['htime']))
            if doc['action'] == 'sell':
                tradesSell.append(doc['hclose'])
                tradesSellTime.append(datetime.datetime.fromtimestamp(doc['htime']))
        except:
            sys.exc_clear()
    # Set up each trace data points, label, and style
    tracePrice = go.Scatter(
        x = dTime,
        y = price,
        name = 'price',
        # mode = 'lines+markers',
        line = dict(color = ('rgb(0,0,0)'), width = 3)
    )
    traceM12 = go.Scatter(
        x = dTime,
        y = m12,
        name = 'm12ema',
        line = dict(color = ('rgb(255,0,0)'), width = 2, dash = 'dash')
    )
    traceM26 = go.Scatter(
        x = dTime,
        y = m26,
        name = 'm26ema',
        line = dict(color = ('rgb(255,127,0)'), width = 2, dash = 'dot')
    )
    traceMAVE = go.Scatter(
        x = dTime,
        y = mave,
        name = 'mAve',
        yaxis = 'y2',
        line = dict(color = ('rgb(0,255,0)'), width = 2)
    )
    traceMSIG = go.Scatter(
        x = dTime,
        y = msig,
        name = 'mSig',
        yaxis = 'y2',
        line = dict(color = ('rgb(0,0,255)'), width = 2, dash = 'dash')
    )
    traceRSI = go.Scatter(
        x = dTime,
        y = rsi,
        name = 'RSI',
        yaxis = 'y3',
        line = dict(color = ('rgb(255,0,255)'), width = 1)
    )
    traceBuy = go.Scatter(
        x = tradesBuyTime,
        y = tradesBuy,
        name = 'Buys',
        mode = 'markers',
        marker = dict(symbol = 'triangle-up', size = 20, color = ('rgb(0,127,0)'))
    )
    traceSell = go.Scatter(
        x = tradesSellTime,
        y = tradesSell,
        name = 'Sells',
        mode = 'markers',
        marker = dict(symbol = 'triangle-down', size = 20, color = ('rgb(127,0,0)'))
    )
    # Set graph traces to be included in (each) graph
    # graph1 = [tracePrice, traceM12, traceM26]
    # graph2 = [traceMAVE, traceMSIG]
    # graph3 = [traceRSI]
    graph4 = [traceRSI, traceMSIG, traceMAVE, traceM26, traceM12, tracePrice, traceBuy, traceSell]
    # Set up layout for graphs
    # layout1 = dict(title = 'Calculated data over time',
    #               xaxis = dict(title = 'Time (s since epoch)'),
    #               yaxis = dict(title = 'Price ($)'))
    # layout2 = dict(title = 'Calculated data over time',
    #               xaxis = dict(title = 'Time (s since epoch)'),
    #               yaxis = dict(title = 'MACD Average/Signal 9'))
    # layout3 = dict(title = 'Calculated data over time',
    #               xaxis = dict(title = 'Time (s since epoch)'),
    #               yaxis = dict(title = 'RSI Index'))
    layout4 = dict(title = 'Calculated data over time',
                  xaxis = dict(title = 'Time (s since epoch)', domain=[0,.95], type="date"),
                  yaxis = dict(title = 'Price ($)'),
                  yaxis2 = dict(title = 'MACD Average/Signal 9',
                                anchor='x',
                                overlaying='y',
                                side='right'),
                  yaxis3 = dict(title = 'Relative Strength Index',
                                anchor='free',
                                side='right',
                                overlaying='y',
                                position = 1))
    # plot graphs
    # po.plot(dict(data = graph1, layout = layout1), filename = 'm12m26Graph')
    # po.plot(dict(data = graph2, layout = layout2), filename = 'mavemsigGraph')
    # po.plot(dict(data = graph3, layout = layout3), filename = 'rsiGraph')
    po.plot(dict(data = graph4, layout = layout4))
    # except (KeyboardInterrupt, SystemExit):
    #     pass
    # except:
    #     print ('Unknown exception: l2Graph')
    #     print sys.exc_info()
    # finally:
    #     sys.exc_clear()
