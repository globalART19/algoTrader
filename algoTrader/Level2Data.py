# level2 data handling
import pymongo, gdax, plotly, threading, sys
from plotly.graph_objs import Scatter, Layout

db = pymongo.MongoClient().algodb_test
fullData = db.level2col
curColl = db.level2current
tickerFeed = db.tickercol

# Update level 2 dataset to maintain current state
def lUpdateData(updateCount):
#   Pull sorted cursor data from db
    updateData = fullData.find({'type':'l2update'}).sort('time',pymongo.ASCENDING).limit(updateCount)
#   Loop over cursor data to update/insert/delete items in level2current coll
    try:
        for i in updateData:
            if float(i['changes'][0][2]) == 0:
                curColl.delete_one({'price': float(i['changes'][0][1])})
            else:
                curColl.update_one({'price': float(i['changes'][0][1])},
                                            {'$set':{'volume':float(i['changes'][0][2])}},
                                            upsert = True)
#           Delete doc from level2col once level2current has been updated
            fullData.delete_one({'_id': i['_id']})
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: lUpdateData')
        print sys.exc_info()
    finally:
        sys.exc_clear()


# Slower than non-bulk op? Update level 2 dataset to maintain current state
def lUpdateDataBulk(updateCount):
    updatePush = []
    deletePush = []
#   Pull sorted cursor data from db
    updateData = fullData.find({'type':'l2update'}).sort('time',pymongo.ASCENDING).limit(updateCount)
#   Loop over cursor data to update/insert/delete items in level2current coll
    try:
        for i in updateData:
            if float(i['changes'][0][2]) == 0:
                updatePush.append(pymongo.DeleteOne({'price': float(i['changes'][0][1])}))
            else:
                updatePush.append(pymongo.UpdateOne({'price': float(i['changes'][0][1])},
                                            {'$set':{'volume':float(i['changes'][0][2])}},
                                            upsert = True))
#           Delete doc from level2col once level2current has been updated
            deletePush.append(pymongo.DeleteOne(i['_id']))
        curColl.bulk_write(updatePush)
        fullData.bulk_write(deletePush)
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: lUpdateData')
        print sys.exc_info()
    finally:
        sys.exc_clear()


# Split snapshot into its own collections
def lSnapshotSplit():
#   define document in db to process from
    snap = fullData.find_one()
#   define coordinate variables
    tags = ['price','volume']
    newins = []
    splitSnap = []
#   iterate across (price, volume) tuples to build x/y coordinate lists
    try:
#      ---bids portion
        for i in snap['bids']:
            newins = dict(zip(tags,[float(i[0]),float(i[1])]))
            splitSnap.insert(0, newins)
#       ---asks portion
        for j in snap['asks']:
            newins = dict(zip(tags,[float(j[0]),float(j[1])]))
            splitSnap.append(newins)
#       ---insert into new database collection
        for k in splitSnap:
            curColl.insert_one(k)
#       delete snapshot documents from incoming messages collections
        fullData.delete_one({'type': 'snapshot'})
        fullData.delete_one({'type': 'subscriptions'})
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: lSnapshotSplit')
        print sys.exc_info()
    finally:
        sys.exc_clear()


# Graph level2 snapshot of order book (from current state l2 data)
def lGraph(priceRange):
#   store latest ticker price
#    tickerCur = tickerFeed.find().sort('sequence',pymongo.DESCENDING).limit(1)
#    tickerList = list(tickerCur)
#    tickerPrice = float(tickerList[0]['price'])
#   define required variables
    tickerPrice = float(gdax.PublicClient().get_product_ticker(product_id='BTC-USD')['price'])
    vBidTot = 0
    vAskTot = 0
    x = []
    y = []
    n = 0
    try:
#       Check if l2data is present
        if("level2current" not in db.collection_names()):
            print('Level2 data does not exist. Quitting...')
            raise Exception
#       Check if current state is currently processing and wait for completion
        while ("snapshot" in threading.enumerate()):
            if(n == 0):
                print('Level2 data collection in process, waiting for completion')
                n = 1
            pass
#       calculate bid side data and create y-axis points
        for doc in curColl.find().sort('price',pymongo.DESCENDING):
            if tickerPrice - priceRange < doc['price'] <= tickerPrice:
                vBidTot = vBidTot + doc['volume']
                y.insert(0, vBidTot)
#       calculate ask side data and append to y-axis points and store x-axis points
        for doc in curColl.find().sort('price',pymongo.ASCENDING):
            if tickerPrice - priceRange < doc['price'] <= tickerPrice + priceRange:
                x.append(doc['price'])
            if tickerPrice < doc['price'] < tickerPrice + priceRange:
                vAskTot = vAskTot + doc['volume']
                y.append(vAskTot)
#       plot graph with title and axis labels
        plotly.offline.plot({"data": [Scatter(x=x,y=y)],
                         "layout": Layout(title="Open Trades Volume Chart<br>Cur Price: "+str(tickerPrice),
                                          xaxis=dict(title=('Price (USD/BTC)')),
                                          yaxis=dict(title=('Volume (BTC)'))
                                          )
                         })
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: l2Graph')
        print sys.exc_info()
    finally:
        sys.exc_clear()
