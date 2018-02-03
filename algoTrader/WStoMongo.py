# Websocket access directly to Mongo database

from pymongo import MongoClient
import gdax, time, threading
import Level2Data

# specify the database and collection
mongo_client = MongoClient()
db = mongo_client.algodb_test
tickercol = db.tickercol
level2col = db.level2col

def initTickerDataDraw():
#   State format for data draw
    print ('Format for initTickerDataDraw(products): \n   prod: BTC-USD')

#   Define products and cahnnels
    prod = [raw_input('Product: ')]
#   Set up websocket class to reference
    class MyWebsocketClient(gdax.WebsocketClient):
        def on_open(self):
            self.url = "wss://ws-feed.gdax.com/"
            self.products = prod
            self.channels = ['ticker']
            self.message_count = 0
            self.mongo_collection = tickercol
            self.should_print = False
        def on_message(self, msg):
            self.message_count += 1
            self.mongo_collection.insert_one(msg)
        def on_close(self):
            print("-- Data draw complete! --")

#   testing cycle limiter
    n = int(raw_input('Max number of records to pull: '))
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
        while (wsClient.message_count != n):
            pass
    except Exception:
        print cur["reason"]
    except:
        KeyboardInterrupt

#   Close websocket client and end data draw
    wsClient.close()
    print 'Level2 data draw complete'


def initLevel2DataDraw():
#   State format for data draw
    print ('Format for initLevel2DataDraw(products): \n   prod: BTC-USD')

#   Define products and cahnnels
    prod = [raw_input('Product: ')]
#   Set up websocket class to reference
    class MyWebsocketClient(gdax.WebsocketClient):
        def on_open(self):
            self.url = "wss://ws-feed.gdax.com/"
            self.products = prod
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
    wsClient = MyWebsocketClient(prod)
    wsClient.start()
    print(wsClient.url, wsClient.products, wsClient.channels,
          wsClient.mongo_collection) #debug
    print '\ndata draw started\n'

#   Handles waiting for data to complete.
    try:
#       Handles waiting for connection and data to hit database
        while (level2col.find_one() is None):
            pass
#       Call collection current state setup in new thread
        t = threading.Thread(Target=Level2Data.lSnapshotSplit())
        t.setDaemon(True)
        t.start()
#       Handles failed subscription to websocket
        cur = level2col.find_one()
        if(cur["type"] == "error"):
            raise Exception
        else:
            pass
#       Update current state collection every __ messages
        while True:
            pass
            # if message_count == 100:
            #     Level2Data.lUpdateData()
            #     message_count = 0
            # else:
            #     pass

#   Exceptions to quit try loop
    except Exception:
        print 'cur["reason"]'
    except:
        KeyboardInterrupt

#   Close websocket client and end data draw
    wsClient.close()
    print 'Level2 data draw complete'

# debug
#     if chan[0] == 'level2':
#         cur = tickercol.find().skip(2).limit(1)
#     else:
#         cur = tickercol.find().sort([('sequence', -1)]).limit(1)
#
#     print cur
#     for doc in cur:
#         print doc


# Draw data for specified products and channels
def initDataDraw(prod, chan):
    # State format for data draw
    print 'Format for initDataDraw(products, channels): \n   prod: ["BTC-USD", "LTC-USD:] \n   chan: ["level2"]'

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
            pass
    except Exception:
        print cur["reason"]
    except:
        KeyboardInterrupt

    wsClient.close()
    print 'data draw complete'

    cur = BTC_collection.find().sort([('sequence', -1)]).limit(1)
    print cur
    for doc in cur:
        print doc
