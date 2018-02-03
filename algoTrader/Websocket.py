# Trading algorithm start point. May break out functions to new files later.

import gdax
import pymongo
import mongoengine
import collections

#~~~~~~~~~ Add MongoDB launch on startup
"""
NEXT STEPS:
1. subscribe to websocket
1.1. determine whether only need subscribe once or if on each startup
2. Set up pull data direct websocket to db
3. determin how to parse data from db
. look into "cache" used in Zenbot

"""

#set up mongoengine for algo DB
mongoengine.connect('algodb_test', host='localhost', port=27017)

#DB labeling
algoDB = pymongo.MongoClient().algodb_test
#pymongo.collection = algoDB.algoHistTable
algoWebsocketTable = algoDB.algoHistTable

# drop history table on start
algoWebsocketTable.drop()

"""
#DB define document
class histDataDoc(mongoengine.Document):
    htime = mongoengine.IntField(required=True)
    hlow = mongoengine.IntField(required=True)
    hhigh = mongoengine.IntField(required=True)
    hopen = mongoengine.IntField(required=True)
    hclose = mongoengine.IntField(required=True)
    hvolume = mongoengine.IntField(required=True)
    """
#Historical data test functionality
histDataStart = 1
histDataEnd = 2
#granularity= 60,300,900,3600,21600,86400 candle sizes. Max 350 per pull.
histDataGranularity = 900

#pull and print most recent historical data
histData1 = gdax.PublicClient().get_product_historic_rates('BTC-USD', granularity=histDataGranularity)
print histData1[0]
print histData1[-1]

#format histData1 for database
keys = ['htime','hlow','hhigh','hopen','hclose','hvolume']
# histData2 = []
# histData2.append(zip(keys,histData1[0]))
# print histData2

for i in range(0,len(histData1)-1):
    histDataZ = dict(zip(keys,histData1[i]))
    histData2 = algoWebsocketTable.insert_one(histDataZ)

print('All Stored: {0}'.format(histData2.inserted_id))

print histDataZ
cur = algoWebsocketTable.find_one({'htime': histDataZ.get('htime')})
print cur
for doc in cur:
    print cur.get(doc)
print histData1[len(histData1)-1]

# create index on time key
# algoHistTable.create_index([( 'time', pymongo.ASCENDING )])
