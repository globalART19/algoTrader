#Start the authenticated client
import gdax, pymongo, sys
from Algorithm import algorithm

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

# Remove previous test data
def deleteTests():
    try:
        histData.update({}, {'$unset': {'action': 1, 'qty': 1, 'netChange': 1, 'plusMinus': 1}}, multi=True)
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print ('Unknown exception: deleteTests')
        print (sys.exc_info())
    finally:
        print (sys.exc_info())
        print('deleteTests complete')

# Store buy in dataset
def testBuy(doc, qty, reason):
    histData.update_one({'htime': doc['htime']},{'$set':{'action': 'buy', 'qty': qty, 'reason': reason}})
    # print 'buy: ' + str(doc['htime']) # -------------------------------------------------------------

# Store sell in dataset
def testSell(doc, net, plusMinus, reason):
    histData.update_one({'htime': doc['htime']},{'$set':{'action': 'sell', 'netChange': net, 'plusMinus': plusMinus, 'reason': reason}})
    # print 'sell: ' + str(doc['htime']) # -------------------------------------------------------------

def testAlgorithm(startBalance = 10000, tStart = 0, tEnd = 0):
    data = []
    algoData = []
    priceBuy = 0
    plusMinus = 0
    totGain = 0
    totLoss = 0
    numGain = 0
    numLoss = 0
    state = 'out'
    curBalance = startBalance
    if tEnd == 0:
        tEnd = histData.find().sort('htime', pymongo.DESCENDING).limit(1)
        for doc in tEnd:
            tEnd = doc['htime']
    dataCursor = histData.find({'htime': {'$gte': tStart, '$lte': tEnd}}).sort('htime', pymongo.ASCENDING)
    for doc in dataCursor:
        result = algorithm(doc, state, algoData)
        try:
            tradeCondition = result[0]
            reason = result[2]
            # print result # -------------------------------------------------------------
            algoData = result[1]
        except TypeError:
            tradeCondition = None
            sys.exc_clear()
        if tradeCondition == 'buy' and state == 'out':
            state = 'in'
            priceBuy = doc['hclose']
            qty = curBalance / priceBuy
            testBuy(doc, qty, reason)
        if tradeCondition == 'sell' and state == 'in':
            state = 'out'
            priceSell = doc['hclose']
            curBalance = qty * priceSell
            net = qty * (priceSell - priceBuy)
            plusMinus = plusMinus + net
            testSell(doc, net, plusMinus, reason)
        # print 'buy/sell checked'  # -------------------------------------------------------------
    netGain = plusMinus
    mgDoc = histData.find({'action': 'sell'}).sort('netChange',pymongo.DESCENDING).limit(1)
    for doc in mgDoc:
        maxGain = doc['netChange']
    mlDoc = histData.find({'action': 'sell'}).sort('netChange',pymongo.ASCENDING).limit(1)
    for doc in mlDoc:
        maxLoss = doc['netChange']
    tradesCursor = histData.find({'action': 'sell'})
    for doc in tradesCursor:
        netChange = doc['netChange']
        if netChange >= 0:
            totGain = totGain + netChange
            numGain += 1
        else:
            totLoss = totLoss + netChange
            numLoss += 1
    if numGain == 0:
        numGain = 1
    if numLoss == 0:
        numLoss = 1
    avgGain = totGain/numGain
    avgLoss = totLoss/numLoss
    totalTrades = numGain + numLoss
    print ('Net Gain: ', netChange, '\n',
           'Max Gain: ', maxGain, 'Average Gain: ', avgGain, '\n',
           'Max Loss: ', maxLoss, 'Average Loss: ', avgLoss, '\n',
           'Total Trades: ', totalTrades)
