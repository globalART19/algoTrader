# Websocket access directly to Mongo database

from pymongo import MongoClient
import gdax, time, threading, sys
import Level2Data
#from Base import quitCall
quitCall = False

# specify the database and collection
mongo_client = MongoClient()
db = mongo_client.algodb_test
tickercol = db.tickercol
level2col = db.level2col

def initTickerDataDraw(prod, limiter):
#   Define products and cahnnels
    prodf = [prod]
#   Set up websocket class to reference
    class MyWebsocketClient(gdax.WebsocketClient):
        def on_open(self):
            self.url = "wss://ws-feed.gdax.com/"
            self.products = prodf
            self.channels = ['ticker']
            self.message_count = 0
            self.mongo_collection = tickercol
            self.should_print = False
        def on_message(self, msg):
            self.message_count += 1
            self.mongo_collection.insert_one(msg)
        def on_close(self):
            print("-- Data draw complete! --")
#   Call websocket class and initiate websocket
    wsClient = MyWebsocketClient(prod)
    wsClient.start()
    print(wsClient.url, wsClient.products, wsClient.channels,
          wsClient.mongo_collection) #debug
    print '\ndata draw started\n'
#   Handles waiting for data to complete.
    try:
#       Handles waiting for connection and data to hit database
        while (tickercol.find_one() is None):
            pass
#       Handles failed subscription to websocket
        cur = tickercol.find_one()
        if(cur["type"] == "error"):
            raise Exception
        else:
            pass
#       Update current state collection every __ messages
        while (limiter < nLoops):
            if wsClient.message_count > 1:
                oldTic = tickerFeed.find().sort('sequence',pymongo.DESCENDING).limit(1)
                tickerFeed.delete_one(oldTic)
#           Check if "quit websockets" has been called
            if quitCall:
                raise Exception
            else:
                pass
    except Exception:
        print(sys.exc_info())
        print('Ticker data draw failed or quit')
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        sys.exc_clear()

#   Close websocket client and end data draw
    wsClient.close()
    print 'Ticker data draw complete'


def initLevel2DataDraw(prod):
#   Set function variables
    updateCount = 100
    uNum = 0
#   Define products and cahnnels
    prodf = [prod]
#   Set up websocket class to reference
    class MyWebsocketClient(gdax.WebsocketClient):
        def on_open(self):
            self.url = "wss://ws-feed.gdax.com/"
            self.products = prodf
            self.channels = ['level2']
            self.message_count = 0
            self.mongo_collection = level2col
            self.should_print = False
        def on_message(self, msg):
            self.message_count += 1
            self.mongo_collection.insert_one(msg)
        def on_close(self):
            print("-- Data draw complete! --")

#   Call websocket class and initiate websocket
    wsClient = MyWebsocketClient()
    wsClient.start()
    print(wsClient.url, wsClient.products, wsClient.channels,
          wsClient.mongo_collection) #debug
    print '\ndata draw started\n'

#   Handles waiting for data to complete.
    try:
#       Handles waiting for connection and data to hit database
        while (level2col.find_one() is None):
            pass
#       Handles failed subscription to websocket
        cur = level2col.find_one()
        if(cur["type"] == "error"):
            raise Exception
#       If successful subscription, call collection current state setup
        ss = threading.Thread(target=Level2Data.lSnapshotSplit,args=(), name="snapshot")
        try:
            #ss.setDaemon(True)
            #ss.start()
            Level2Data.lSnapshotSplit()
        except:
            print(sys.exc_info())
        print('End of snapshot breakout')
#       Update current state collection every updateCount messages
        lu = threading.Thread(target = Level2Data.lUpdateData,args=())
        while True:
#            if ss.isAlive():
#                pass
            if wsClient.message_count >= updateCount:
            #and not lu.isAlive() and not ss.isAlive():
                try:
                    #lu.setDaemon(True)
                    #lu.start()
                    Level2Data.lUpdateData(updateCount)
                    wsClient.message_count = wsClient.message_count - updateCount
                except:
                    print("Update " + str(uNum) + " complete")
                    print(sys.exc_info())
                finally:
                    sys.exc_clear()
                uNum = uNum + 1
                if uNum > 1000:
                    print("1000 loops complete (Probably broken)!!")
                    raise Exception
#           Check if "quit websockets" has been called
            elif quitCall:
                raise Exception
            else:
                pass
#   Exceptions to quit try loop
    except EOFError:
        print(sys.exc_info())
        print('End of file')
    except Exception:
        print(sys.exc_info())
        print('Level 2 data draw failed or quit')
        sys.exc_clear()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        sys.exc_clear()
#   Close websocket client and end data draw
    wsClient.close()
    print('Level2 data draw complete')


# Draw data for specified products and channels
def initDataDraw():
    # State format for data draw
    print 'Format for initDataDraw(products, channels): \n   prod: ["BTC-USD", "LTC-USD:] \n   chan: ["level2"]'
    prod = raw_input('prod: ')
    chan = raw_input('chan: ')

    # Set up websocket class to reference
    class MyWebsocketClient(gdax.WebsocketClient):
        def on_open(self):
            self.url = "wss://ws-feed.gdax.com/"
            self.products = prod
            self.channels = chan
            self.message_count = 0
            self.mongo_collection = procCol
            self.should_print = False
            #print("Lets count the messages!")
        def on_message(self, msg):
            self.message_count += 1
            self.mongo_collection.insert_one(msg)
            #if 'price' in msg and 'type' in msg:
            #    print ("Message type:", msg["type"],
            #           "\t@ {:.3f}".format(float(msg["price"])))
        def on_close(self):
            print("-- Data draw complete! --")

    n = int(raw_input('Number of records to pull: '))
    wsClient = MyWebsocketClient(prod, chan)
    wsClient.start()
    print(wsClient.url, wsClient.products, wsClient.channels, wsClient.mongo_collection)
    print '\ndata draw started\n'

    try:
        while (BTC_collection.find_one() is None):
            pass
        cur = BTC_collection.find_one()
        if(cur["type"] == "error"):
            raise Exception
        while (wsClient.message_count < n):
#           Check if "quit websockets" has been called
            if quitCall:
                raise Exception
            else:
                pass
    except Exception:
        print(sys.exc_info())
        print('Websocket data draw failed or quit')
        sys.exc_clear()
    except (KeyboardInterrupt, SystemExit):
        pass

    wsClient.close()
    print 'data draw complete'

    # cur = BTC_collection.find().sort([('sequence', -1)]).limit(1)
    # print cur
    # for doc in cur:
    #     print doc
