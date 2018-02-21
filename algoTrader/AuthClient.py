#Start the authenticated client
import gdax, pymongo, sys

db = pymongo.MongoClient().algodb_test
histData = db.algoHistTable

def authClient(sOrR, key, b64secret, passphrase):
    # Determine sandbox or real API (requires different set of API access credentials)
    if sOrR == 'r':
        apiurl = "https://api.gdax.com/"
    else:
        apiurl = "https://api-public.sandbox.gdax.com"

    auth_client = gdax.AuthenticatedClient(key, b64secret, passphrase,
                                  api_url=apiurl)

# Store buy in dataset
def testBuy(doc):
    histData.update_one({'htime': doc['htime']},{'$set':{'action': 'buy'}})

# Store sell in dataset
def testSell(doc):
    histData.update_one({'htime': doc['htime']},{'$set':{'action': 'sell'}})

def testAlgorithm(tStart = 0, tEnd = 0):
    state = 'out'
    if tEnd == 0:
        tEnd = histData.find().sort({'htime': pymongo.DESCENDING}).limit(1)
        for doc in tEnd:
            tEnd = doc['htime']
    for doc in cursor:
        if 'buy condition' and state = 'out':
            testBuy(doc)
            state = 'in'
        if 'sell condition' and state == 'in':
            testSell(doc)
            state = 'out'
