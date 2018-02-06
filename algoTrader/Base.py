# Trading algorithm structure and initialization file.

import gdax, pymongo, collections, threading, sys
import WStoMongo, Level2Data, HistData

db = pymongo.MongoClient().algodb_test
quitCall = False

print "Welcome to the Trahan Autonomous Trading Program!"

while True:
    print ('''Choose File: (h = history, w = websocket, l2 = level2 feed,
           l2g = graph level 2 data, t = ticker feed, c = clear dbs,
           End threads (q**): ie. ql2 quit l2 thread''')
    selFile = raw_input(">>> ")

#   Import historical data
    if selFile == 'h' or selFile == 'H':
#       State format for data draw
        print ('Format for initTickerDataDraw(products, tRange, tInterval): \n   prod: BTC-USD')
        hProd = raw_input('Product (ex. BTC-USD): ')
        tRange = int(raw_input('Time range (in days): '))
        tInterval = int(raw_input('Data point interval in mins (1, 5, 15, 60, 360, 1440): '))
        try:
            h = threading.Thread(target = HistData.popHistory,args=(hProd, tRange, tInterval))
            h.start()
        except EOFError:
            sys.exc_info()
            print('End of file')
        except:
            print(sys.exc_info())
            print("Error: unable to start thread")
        finally:
            sys.exc_clear()
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
        try:
            w = threading.Thread(target = WStoMongo.initDataDraw())
            w.setDaemon(True)
            w.start()
        except:
            print(sys.exc_info())
            print("Error: unable to start thread")
        finally:
            sys.exc_clear()
#   Start Level 2 feed data draw
    elif selFile == 'l2' or selFile == 'l2':
#       State format for data draw
        print ('Format for initLevel2DataDraw(products): \n   prod: BTC-USD')
        l2Prod = raw_input('Product: ')
        try:
            ll = threading.Thread(target = WStoMongo.initLevel2DataDraw,args=(l2Prod,))
            ll.start()
        except EOFError:
            print(sys.exc_info())
            print('End of file')
        except:
            print(sys.exc_info())
            print("Error: unable to start thread")
        finally:
            sys.exc_clear()
#   Start ticker feed data draw
    elif selFile == 't' or selFile == 'T':
        #   State format for data draw
        print ('Format for initTickerDataDraw(products, limiter): \n   prod: BTC-USD')
        #   Define products
        tProd = [raw_input('Product: ')]
        n = int(raw_input('Limiter qty: '))
        try:
            t = threading.Thread(target = WStoMongo.initTickerDataDraw,args=(tProd, n))
            t.start()
        except EOFError:
            sys.exc_info()
            print('End of file')
        except:
            print(sys.exc_info())
            print("Error: unable to start thread")
        finally:
            sys.exc_clear()
#   Graph current level2 data
    elif selFile == 'l2g' or selFile == 'L2G':
        prange = float(raw_input('Price range ($): '))
        Level2Data.lGraph(prange)
    elif selFile == 'ql2':
        print ('# of Threads: ', threading.activeCount())
        quitCall = True
#   Handler for no match
    else:
        print 'Selection not valid'
        break
