# Websocket access directly to Mongo database

from pymongo import MongoClient
import gdax, time
import Level2Data

# specify the database and collections
mongo_client = MongoClient()
db = mongo_client.algodb_test
tickercol = db.tickerdata
level2col = db.level2data

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

#debug
    if chan[0] == 'level2':
        cur = tickercol.find().skip(2).limit(1)
    else:
        cur = tickercol.find().sort([('sequence', -1)]).limit(1)

    print cur
    for doc in cur:
        print doc


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
#       Call collection current state setup
        Level2Data.lSnapshotSplit()
#       Handles failed subscription to websocket
        cur = level2col.find_one()
        if(cur["type"] == "error"):
            raise Exception
        else:
            pass
#       Update current state collection every __ messages
        while True:
            if message_count == 100:
                Level2Data.lUpdateData()
                message_count = 0
            else:
                pass
#   Exceptions to quit try loop
    except Exception:
        print cur["reason"]
    except:
        KeyboardInterrupt

#   Close websocket client and end data draw
    wsClient.close()
    print 'Level2 data draw complete'

#debug
    if chan[0] == 'level2':
        cur = level2col.find().skip(2).limit(1)
    else:
        cur = level2col.find().sort([('sequence', -1)]).limit(1)

    print cur
    for doc in cur:
        print doc


# Draw data for specified products and channels
def initDataDraw():
#   State format for data draw
    print ('''Format for initDataDraw(products, channels): \n   prod: BTC-USD
           \n   chan: level2''')

#   Define products and cahnnels
    prod = [raw_input('Products: ')]
    chan = [raw_input('Channels: ')]
#   Set up websocket class to reference
    class MyWebsocketClient(gdax.WebsocketClient):
        def on_open(self):
            self.url = "wss://ws-feed.gdax.com/"
            self.products = prod
            self.channels = chan
            self.message_count = 0
            self.mongo_collection = procCol
            self.should_print = False
        def on_message(self, msg):
            self.message_count += 1
            self.mongo_collection.insert_one(msg)
        def on_close(self):
            print("-- Data draw complete! --")

#   Set loop to infinity
    n = -1

#   Channel selection handler
    if chan[0] == 'level2':
        procCol = level2data
    elif chan[0] == 'ticker':
        procCol = tickerdata
        n = int(raw_input('Number of records to pull: '))
    else:
        print 'Incompatible channel'
        quit()

#   Call websocket class and initiate websocket
    wsClient = MyWebsocketClient(prod, chan)
    wsClient.start()
    print(wsClient.url, wsClient.products, wsClient.channels,
          wsClient.mongo_collection) #debug
    print '\ndata draw started\n'

#   Handles waiting for data to complete.
    try:
#       Handles waiting for connection and data to hit database
        while (procCol.find_one() is None):
            pass
#       Handles failed subscription to websocket
        cur = procCol.find_one()
        if(cur["type"] == "error"):
            raise Exception
        else:
            pass
#       Wait for message count to reach limiter, then quit loop
        while (wsClient.message_count != n):
            pass
    except Exception:
        print cur["reason"]
    except:
        KeyboardInterrupt

#   Close websocket client and end data draw
    wsClient.close()
    print 'data draw complete'

#debug
    if chan[0] == 'level2':
        cur = procCol.find().skip(2).limit(1)
    else:
        cur = procCol.find().sort([('sequence', -1)]).limit(1)

    print cur
    for doc in cur:
        print doc
