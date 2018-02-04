# level2 data handling
import pymongo, gdax, plotly
from plotly.graph_objs import Scatter, Layout

db = pymongo.MongoClient().algodb_test
fullData = db.level2col
snapColl = db.level2current
tickerFeed = db.tickercol

# Update level 2 dataset to maintain current state
def lUpdateData(updateCount):
#   Pull sorted cursor data from db
    updateData = fullData.find({'type':'l2update'}).sort('time',pymongo.ASCENDING).limit(updateCount)
#   Loop over cursor data to update/insert/delete items in level2current coll
    try:
        for i in updateData:
            if float(i['changes'][0][2]) == 0:
                rresult = snapColl.delete_one({'price': float(i['changes'][0][1])})
            else:
                result = snapColl.update_one({'price': float(i['changes'][0][1])},
                                            {'$set':{'volume':float(i['changes'][0][2])}},
                                            upsert = True)
#           Delete doc from level2col once level2current has been updated
            fullData.delete_one(i)
    except:
        KeyboardInterrupt


# Split snapshot into its own collections
def lSnapshotSplit():
#   define document in db to process from
    snap = fullData.find_one()
#   define coordinate variables
    tags = ['price','volume']
    newins = []
    splitSnap = []
#   iterate across (price, volume) tuples to build x/y coordinate lists
#   ---bids portion
    for i in snap['bids']:
        newins = dict(zip(tags,[float(i[0]),float(i[1])]))
        splitSnap.insert(0, newins)
#   ---asks portion
    for j in snap['asks']:
        newins = dict(zip(tags,[float(j[0]),float(j[1])]))
        splitSnap.append(newins)
#   ---insert into new database collection
    for k in splitSnap:
        snapColl.insert_one(k)
#   delete snapshot documents from incoming messages collections
    fullData.delete_one({'type': 'snapshot'})
    fullData.delete_one({'type': 'subscriptions'})


# Graph level2 snapshot of order book (from current state l2 data)
def lGraph(priceRange):
#   store latest ticker price
#    tickerCur = tickerFeed.find().sort('sequence',pymongo.DESCENDING).limit(1)
#    tickerList = list(tickerCur)
#    tickerPrice = float(tickerList[0]['price'])
#   define required variables
    tickerPrice = gdax.get_product_ticker(product_id='BTC-USD')
    vBidTot = 0
    vAskTot = 0
    x = []
    y = []
#   calculate bid side data and create y-axis points
    for doc in snapColl.find().sort('price',pymongo.DESCENDING):
        if tickerPrice - priceRange < doc['price'] <= tickerPrice:
            vBidTot = vBidTot + doc['volume']
            #print doc['price'], vBidTot
            y.insert(0, vBidTot)
#   calculate ask side data and append to y-axis points and store x-axis points
    for doc in snapColl.find().sort('price',pymongo.ASCENDING):
        if tickerPrice - priceRange < doc['price'] <= tickerPrice + priceRange:
            x.append(doc['price'])
        if tickerPrice < doc['price'] < tickerPrice + priceRange:
            #print doc['price']
            vAskTot = vAskTot + doc['volume']
            y.append(vAskTot)
#   plot graph with title and axis labels
    plotly.offline.plot({"data": [Scatter(x=x,y=y)],
                         "layout": Layout(title="Open Trades Volume Chart<br>Cur Price: "+str(tickerPrice),
                                          xaxis=dict(title=('Price (USD/BTC)')),
                                          yaxis=dict(title=('Volume (BTC)'))
                                          )
                         })
