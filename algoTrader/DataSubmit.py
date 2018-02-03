# Input all data calculations into database

import pymongo, DataFunc

mongo_client = pymongo.MongoClient()
db = mongo_client.algodb_test
histData = db.algoHistTable
calcData = db.calcData

dictTitles = ['price','m12ema','m26ema','mave','msig']

#for n in range(histData.count()):
#    for doc in histData.update_one().skip(n).limit(1)

n=0
m12 = DataFunc.m12ema(n)
cur = histData.find().skip(n).limit(1)
vm12ema = m12ema(n)
vm26ema = m26ema(n)
newUp = calcData.insert_one(cur,['m12ema', vm12ema])
print newUp
