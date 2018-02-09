# Trading algorithm start point. May break out functions to new files later.

import gdax, pymongo, collections, sys
from DataFunc import calcPopulateBulk
from datetime import datetime

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
#   calc loop span
    dataInterval = 349 * histGranularity
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
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: popHistory')
        print sys.exc_info()
    finally:
        print('popHistory complete')
