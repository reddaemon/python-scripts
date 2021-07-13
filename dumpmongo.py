from os.path import join
from pymongo import MongoClient
from bson.json_util import dumps
import logging
from time import time



DUMPDIR = "/data/backup"
DB_HOST = '127.0.0.1'
DB_PORT = 27017
PERIOD = '2020_11'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = True
sh = logging.StreamHandler()
formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%b %d %H:%M:%S")
sh.setFormatter(formatter)
logger.addHandler(sh)

def get_collections(db_host, db_port, period):
    dump_collections = []
    client = MongoClient(db_host, db_port)
    database = client['db_prod']
    collections = database.collection_names()
    for item in collections:
        if period in item:
            dump_collections.append(item)
    return database,dump_collections


def backup_db(dumpdir, db_host, db_port, period):
    database,dump_collections = get_collections(db_host, db_port, period)
    counter = 0
    collections_count = len(dump_collections)
    collections_left = collections_count
    for i, collection_name in enumerate(dump_collections):
        col = getattr(database,dump_collections[i])
        collection = col.find()
        jsonpath = collection_name + ".json"
        jsonpath = join(dumpdir, jsonpath)
        logger.info("Dumping collection %s" % collection_name)
        start_time = time()
        with open(jsonpath, 'wb') as jsonfile:
            jsonfile.write(dumps(collection).encode())
        counter += 1
        end_time = time()
        elapsed_time = end_time - start_time
        logger.info("[%d / %d] Collection %s done in %d seconds" % (counter, collections_count, collection_name, elapsed_time))


backup_db(DUMPDIR, DB_HOST, DB_PORT, PERIOD)