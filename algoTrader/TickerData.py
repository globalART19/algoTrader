# Start ticker websocket feed and gather data to db

from pymongo import MongoClient
import gdax, time
import Level2Data

# specify the database and collections
mongo_client = MongoClient()
db = mongo_client.algodb_test
tickercol = db.tickercol
level2col = db.level2col
