# level2 data handling
import pymongo
import plotly
from plotly.graph_objs import Scatter, Layout

db = pymongo.MongoClient().algodb_test
fullData = db.level2col
snapColl = db.level2current
tickerFeed = db.tickercol

# Update level 2 dataset to maintain current state
def lUpdateData():
    # iterate snapshot process over l2update documents
    # update + upsert=True for all options
    # understand buy vs bid and sell vs ask
    pass



# Split snapshot into its own collections
def lSnapshotSplit():
# define document in db to process from
    snap = fullData.find_one()

#    print base['bids'][0][0]
#    print base['asks'][0][0]

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
#    fullData.delete_one({'type': 'snapshot'})
#    fullData.delete_one({'type': 'subscriptions'})


# Graph level2 snapshot of order book (from current state l2 data)
def lGraph(priceRange):
#   store latest ticker price
    tickerCur = tickerFeed.find().sort('sequence',pymongo.DESCENDING).limit(1)
    tickerList = list(tickerCur)
    tickerPrice = float(tickerList[0]['price'])

#   define required variables
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


# Graph level2 snapshot of order book (from original snapshot)
def lGraph2():
# define document in db to process from
    base = fullData.find_one()

#    print base['bids'][0][0]
#    print base['asks'][0][0]

#   define coordinate variables
    x = []
    y = []
    xb = []
    yb = []
    vbtot = 0
    xa = []
    ya = []
    vatot = 0

#   iterate across (price, volume) tuples to build x/y coordinate lists
#   ---bids portion
    for pb,vb in base['bids']:
        #print p
        #print v
        if float(pb) > 8000:
            vbtot = vbtot + float(vb)
            x.insert(0, pb)
            y.insert(0, vbtot)
            xb.insert(0, pb)
            yb.insert(0, vbtot)
#   ---asks portion
    for pa,va in base['asks']:
        if float(pa) < 9800:
            vatot = vatot + float(va)
            x.append(pa)
            y.append(vatot)
            xa.append(pa)
            ya.append(vatot)

    #print x
    #print y

    plotly.offline.plot({"data": [Scatter(x=x,y=y)],
                         "layout": Layout(title="Open Trades Volume Chart",
                                          xaxis=dict(title=('Price (USD/BTC)')),
                                          yaxis=dict(title=('Volume (BTC)'))
                                          )
                         })
