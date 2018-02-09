# Trading algorithm start point. May break out functions to new files later.

import gdax, pymongo, collections, sys
from DataFunc import calcPopulateBulk
from datetime import datetime
from time import sleep

#DB labeling
db = pymongo.MongoClient().algodb_test
algoHist = db.algoHistTable

def updateHistory(prod=''):
#   Determine latest data time. Set time range and end points.
    last = algoHist.find().sort('htime',pymongo.DESCENDING).limit(2)
    j = 0
    for i in last:
        if (j == 0):
            tStart = float(i['htime'])
        else:
            histGranularity = tStart - float(i['htime'])
        j += 1
    tEnd = float(gdax.PublicClient().get_time()['epoch'])
    print tEnd, tStart
#   calc loop spans
    dataInterval = 349 * histGranularity
    updateQty = (tEnd-tStart)/histGranularity
#   define database keys
    keys = ['htime','hlow','hhigh','hopen','hclose','hvolume']
#   loop for data immport over time range. Start at tStart and append to db.
    try:
        tsCursor = tStart
        if (tEnd < tStart + dataInterval):
            teCursor = tEnd
        else:
            teCursor = tStart + dataInterval
        while (teCursor < tEnd):
#           Convert time cursors to iso format for GDAX API
            tsCursorISO = datetime.isoformat(datetime.utcfromtimestamp(tsCursor))
            teCursorISO = datetime.isoformat(datetime.utcfromtimestamp(teCursor))
#           Call data from gdax and store locally
            histData = gdax.PublicClient().get_product_historic_rates(
                prod,start=tsCursorISO,end=teCursorISO,granularity=histGranularity)
#           Build dictionary document and push to history collection
            for i in range(0,len(histData)-1):
                histDataDoc = dict(zip(keys,histData[len(histData)-1-i]))
                algoHist.insert_one(histDataDoc)
#           Set cursors for next loop
            tsCursor = teCursor
            teCursor = teCursor + dataInterval
#           Check for last loop condition to prevent overdraw
            if teCursor > tEnd:
                teCursor = tEnd
        calcPopulateBulk(updateQty)
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: updateHistory')
        print sys.exc_info()
    finally:
        print('updateHistory complete')



def popHistory(prod, timeRange, timeInt):
#   drop history table on start
    algoHist.drop()

#   set time range and convert inputs to seconds
    now = gdax.PublicClient().get_time()
    tEnd = float(now['epoch'])
    tStart = tEnd - timeRange * 86400
    histGranularity = timeInt * 60
    projTot = (tEnd - tStart)/histGranularity
    actTot = 0
#   calc loop span
    dataInterval = 349 * histGranularity
#   define database keys
    keys = ['htime','hlow','hhigh','hopen','hclose','hvolume']
#   loop for data immport over time range. Start at tStart and append to db.
    try:
        tsCursor = tStart
        tryCount = 0
        if (tEnd < tStart + dataInterval):
            teCursor = tEnd
        else:
            teCursor = tStart + dataInterval
        while (teCursor < tEnd):
            dataPush = []
#           Convert time cursors to iso format for GDAX API
            tsCursorISO = datetime.isoformat(datetime.utcfromtimestamp(tsCursor))
            teCursorISO = datetime.isoformat(datetime.utcfromtimestamp(teCursor))
#           Call data from gdax and store locally
            histData = gdax.PublicClient().get_product_historic_rates(
                prod,start=tsCursorISO,end=teCursorISO,granularity=histGranularity)
            if (len(histData) < 345 and teCursor != tEnd):
                print 'Failed to complete pull. Trying again: ',tryCount
                tryCount += 1
                sleep(6)
            else:
#           Build dictionary document and push to history collection
                tryCount = 0
                for i in range(0,len(histData)-1):
                    histDataDoc = dict(zip(keys,histData[len(histData)-1-i]))
                    #algoHist.insert_one(histDataDoc)
                    dataPush.append(histDataDoc)
                algoHist.insert_many(dataPush)
                actTot += len(dataPush)
#               Set cursors for next loop
                tsCursor = teCursor
                teCursor = teCursor + dataInterval
#           Check for last loop condition to prevent overdraw
            if teCursor > tEnd:
                teCursor = tEnd
            if tryCount > 10:
                print 'HistData pull failed due to crap servers. Try again later'
                print algoHist.find_one()
                tryCount = 0
                #cont = raw_input('Continue anyway? (y/n) >>> ')
                cont = 'y'
                if cont == 'y':
                    tryCount = 0
                    for i in range(0,len(histData)-1):
                        histDataDoc = dict(zip(keys,histData[len(histData)-1-i]))
                        #algoHist.insert_one(histDataDoc)
                        dataPush.append(histDataDoc)
                    if len(dataPush) > 0:
                        algoHist.insert_many(dataPush)
                        actTot += len(dataPush)
                        print len(dataPush), algoHist.find_one()
    #               Set cursors for next loop
                    tsCursor = teCursor
                    teCursor = teCursor + dataInterval
                else:
                    break
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: popHistory')
        print sys.exc_info()
    finally:
        print('popHistory complete')
        print('# docs for full report vs actually pulled: ', )
