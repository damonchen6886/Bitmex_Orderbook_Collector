import json
from pymongo import MongoClient
import websocket
import redis

# author: Chen Gu  Date: 3/17/2021


MONGODB_NAME = "order_book_data"
MONGO_USER_NAME = "test1"
MONGO_USER_PWD = "test1"

REDIS_HOST = "redis-11962.c10.us-east-1-3.ec2.cloud.redislabs.com"
REDIS_PWD = ""

'''
sample obj to save the latest timestamp
'''


class Quote(object):
    def __init__(self):
        self.timestamp = ""


class Tick(object):
    def __init__(self):
        self.bid = []
        self.ask = []
        self.bid_id = []
        self.ask_id = []


quote = Quote()
tick = Tick()

'''
    set up mongoDB connection 
'''


def connect_mongoDB():
    client = MongoClient(
        "mongodb://" + MONGO_USER_NAME + ":" + MONGO_USER_PWD + "" + MONGODB_NAME + "?ssl=true&replicaSet=atlas-1101jk-shard-0&authSource=admin&retryWrites=true&w=majority")
    db = client["bitmex"]
    collection = db[MONGODB_NAME]
    # refresh the collection for test propose
    collection.delete_many({})
    return collection


'''
    set up redis connection 
'''


def connection_redis():
    r = redis.Redis(
        host=REDIS_HOST,
        port=11962,
        password=REDIS_PWD,
        decode_responses=True
    )
    r.flushall()
    return r


mongoConnection = connect_mongoDB()
redisConnection = connection_redis()


def on_open(ws):
    data = {"op": "subscribe", "args": ["orderBookL2_25:XBTUSD", "quote:XBTUSD"]}
    ws.send(json.dumps(data))


def on_close(ws):
    print("closed")


def on_message(ws, message):
    print(message)
    msg = json.loads(message)

    if "table" in msg:
        if msg["table"] == "orderBookL2_25":
            data = msg['data']
            # process_data_to_mongoDB(data, msg['action'])
            precess_to_display(data, msg["action"])
        elif msg["table"] == 'quote':
            data = msg['data']
            # process_to_redis(data)


'''
    save the latest quote data to the redis, if exists one in redis, updates then
'''


def precess_to_display(data, action):
    for item in data:
        if action == "partial":
            if item["side"] == 'Sell':
                tick.ask.append(item)
                tick.ask_id.append(item["id"])
            elif item["side"] == "Buy":
                tick.bid.append(item)
                tick.bid_id.append(item["id"])


        elif action == "update":
            if item["side"] == 'Sell':
                if item["id"] in tick.ask_id:
                    for t in tick.ask:
                        if t["id"] == item["id"]:
                            # print()
                            t["size"] = item["size"]

            elif item["side"] == "Buy":
                if item["id"] in tick.bid_id:
                    for t in tick.bid:
                        if t["id"] == item["id"]:
                            t["size"] = item["size"]
        elif action == "insert":
            if item['side'] == "Sell":
                tick.ask.append(item)
            if item['side'] == "Buy":
                tick.bid.append(item)
        elif action == "delete":
            if item['side'] == "Sell":
                tick.ask = list(filter(lambda i: i["id"] != item["id"], tick.ask))
                tick.ask_id.remove(item["id"])
            if item['side'] == "Buy":
                tick.bid = list(filter(lambda i: i["id"] != item["id"], tick.bid))
                tick.bid_id.remove(item["id"])

    # print(tick.ask)
    sorted(tick.ask, key=lambda i:  int(i['price']))
    print("*************",tick.ask)
    sorted(tick.bid, key=lambda i: int(i['price']))
    print_tick()


def print_tick():

    print("%12s %3s %s" % ("price", " | ", "size"))
    for i in range(len(tick.ask)):
        print("%12s %3s %s %12s" % (tick.ask[i]["price"], " | ", tick.ask[i]["size"] , tick.ask[i]["id"]))
    print("*" * 30)
    for i in range(len(tick.bid)):
        print("%12s %3s %s %12s" % (tick.bid[i]["price"], " | ", tick.bid[i]["size"],tick.ask[i]["id"]))

    # print(tick.bid)
    # print(tick.ask)
    print("\n" * 3)



def process_to_redis(data):
    for item in data:
        cur_timestamp = item["timestamp"]
        redis_data = redisConnection.get('XBTUSD')
        print("redis_data = ", redis_data)
        if redis_data is None:
            quote.timestamp = cur_timestamp
            redisConnection.set("XBTUSD", str(item))
        else:
            if quote.timestamp < cur_timestamp:
                redisConnection.set("XBTUSD", str(item))
                quote.timestamp = cur_timestamp


'''
    process CRUD to mongoDB
    @:param: connection : mongoDB connection
    @:param: data : dictionary data from socket
    @:param: action : specific action from dictionary, either one of : partial, update, insert, delete
'''


def process_data_to_mongoDB(data, action):
    for item in data:
        if action == "partial":
            print("adding item to database *********")
            # print(item)
            mongoConnection.insert_one(item)
            print("************success***********")
        elif action == "update":
            print("before", mongoConnection.find_one({'id': item['id']}))
            mongoConnection.update_one({'id': item['id'], "side": item['side']},
                                       {"$set":
                                            {"size": item["size"]}
                                        })
            print("after", mongoConnection.find_one({'id': item['id']}))

        elif action == "delete":
            print("deleting *******************")
            print("before", mongoConnection.find_one({'id': item['id']}))
            mongoConnection.delete_one({'id': item['id']})
            print("after", mongoConnection.find_one({'id': item['id']}))
            print("done deleting ****************")
        elif action == "insert":
            print("inserting *******************")
            print("before", mongoConnection.find_one({'id': item['id']}))
            mongoConnection.insert_one(item)
            print("after", mongoConnection.find_one({'id': item['id']}))
            print("done inserting *************")


def on_error(ws, error):
    print(f"error:{error}")


if __name__ == '__main__':
    url = "wss://www.bitmex.com/realtime"
    ws = websocket.WebSocketApp(url, on_open=on_open, on_close=on_close, on_message=on_message, on_error=on_error)
    ws.run_forever(ping_interval=30)
