# Trading algorithm structure and initialization file.

import gdax
import pymongo
import collections

db = pymongo.MongoClient().algodb_test

print "Welcome to the Trahan Autonomous Trading Program!"

while True:
    print ('''Choose File: (h = history, w = websocket, l2 = level2 feed,
           l2g = graph level 2 data, t = ticker feed, c = clear dbs''')
    selFile = raw_input(">>> ")

#   Import historical data
    if selFile == 'h' or selFile == 'H':
        import HistData
#   Clear database
    elif selFile == 'c' or selFile == 'C':
        db.algoHistTable.drop()
        db.algoWebsocketTable.drop()
        db.algoWStest.drop()
        db.level2col.drop()
        db.tickercol.drop()
        db.level2current.drop()

        print ('Collections cleared: \n   algoHistTable \n'
              '   algoWebsocketTable \n   algoWStest \n   level2col \n'
              '   level2current \n   tickercol ')
#   Start generic feed data draw
    elif selFile == 'w' or selFile == 'W':
        import WStoMongo
        WStoMongo.initDataDraw(prods, chans)
#   Start Level 2 feed data draw
    elif selFile == 'l2' or selFile == 'l2':
        import WStoMongo
        WStoMongo.initLevel2DataDraw()
#   Start ticker feed data draw
    elif selFile == 't' or selFile == 'T':
        import WStoMongo
        WStoMongo.initTickerDataDraw()
#   Graph current level2 data
    elif selFile == 'l2g' or selFile == 'L2G':
        import Level2Data
        prange = float(raw_input('Price range ($): '))
        Level2Data.lGraph(prange)
#   Handler for no match
    else:
        print 'Selection not valid'
        break
