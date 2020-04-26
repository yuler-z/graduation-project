#! /usr/bin/env python3

import json
import argparse
import logging
import queue
from datetime import datetime

from cassandra.cluster import Cluster,ExecutionProfile,EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel
from elasticsearch import Elasticsearch

import sys
sys.path.append('../')
from utils import ThreadPoolExecutorWithQueueSizeLimit, getLogger

## Script args and Help
parser = argparse.ArgumentParser(add_help=True)

parser.add_argument('-s', action="store", dest="DSE_IP", default="10.10.9.2,10.10.9.3,10.10.9.4")

opts = parser.parse_args()

DSE_IP = opts.DSE_IP.split(',')

# get logger
class Statement:
    def __init__(self,name):
        switcher = { 'reddit': self.reddit_stat, 'amazon': self.amazon_stat}
        self.create_ks, self.create_tb, self.cql = switcher[name]

    def reddit_stat(self):
        create_ks = ("CREATE KEYSPACE IF NOT EXISTS reddit"
                    " WITH replication = {"
                        "'class' : 'NetworkTopologyStrategy',"
                        " 'datacenter1' : 2"
                    "}"
                )
        create_tb = ("CREATE TABLE IF NOT EXISTS reddit.comment "
                        " (id text,"
                        " name text,"
                        " link_id text,"
                        " parent_id text,"
                        " subreddit_id text,"
                        " author text,"
                        " body text,"
                        " PRIMARY KEY(id, name))"
                    )
        cql = ("INSERT INTO reddit.comment "
                "(id, name, link_id, parent_id, subreddit_id, author, body)"
                " VALUES(?,?,?,?,?,?,?)"
                " USING TIMESTAMP ?"
                )
        create_index = ("CREATE SEARCH INDEX IF NOT EXISTS ON reddit.comment"
                    " WITH COLUMNS author,body {indexed:true}")
    def amazon_stat(self):
         create_ks = ("CREATE KEYSPACE IF NOT EXISTS amazon"
                    " WITH replication = {"
                        "'class' : 'NetworkTopologyStrategy',"
                        " 'datacenter1' : 2"
                    "}"
                )
        create_tb = ("CREATE TABLE IF NOT EXISTS amazon.comment "
                        " (id int,"
                        " user_id int,"
                        " product_id text,"
                        " rating float,"
                        " title text,"
                        " body text,"
                        " PRIMARY KEY(id, name))"
                    )
        cql = ("INSERT INTO reddit.comment "
                "(id, user_id, product_id, rating_id, title, body)"
                " VALUES(?,?,?,?,?,?,?)"
                " USING TIMESTAMP ?"
                )
        create_index = ("CREATE SEARCH INDEX IF NOT EXISTS ON amazon.comment"
                    " WITH COLUMNS author,body {indexed:true}")
            

class Loader:

    start = 0

    def __init__(self,name):
        # set log
        logging.getLogger("elasticsearch").setLevel(logging.WARNING)
        self.log = getLogger('dse', 'dse.log')
        # start timer
        Loader.start = datetime.now()

        self.index = name
        self.stat = Statement(name)
        self.filename = mapper[name][load] 
        
        self.pool = ThreadPoolExecutorWithQueueSizeLimit(max_workers=10, queue_size=30)

        self.__init_dse()
        


    # init dse, create keyspace and table, prepare cql and set consistency level
    def __init_dse(self):
        self.session = Cluster(DSE_IP).connect()

        self.session.execute(self.stat.create_ks)
        self.session.execute(self.stat.create_tb)

        self.cql_prepared = self.session.prepare(self.stat.cql)
        self.cql_prepared.consistency_level = ConsistencyLevel.LOCAL_QUORUM

    def load(self):

        if self.index == 'reddit': 
            g = self.__line_generator()
            while True:
                try:
                    line = json.loads(next(g))
                    self.pool.submit(self.__insert_data,line)
                except StopIteration:
                    break
                except Exception:
                    continue 
        elif self.index == 'amazon':
            df = pd.read_csv(self.filename)
            
            end = df.index.max()
            try:
                for line in zip(range(0,end+1),df['userId'],df['productId'],
                                    df['rating'],df['title'],df['comment'],df['timestamp']):
                    self.pool.submit(self.__insert_data,line)
            except Exception as e:
                print(e)
                continue

        # shutdown
        self.pool.shutdown() 

        # create search index
        end = datetime.now() 
        self.log.info('Insert data cost time: {}'.format(end - Loader.start))
        self.log.info('create search index')
        self.session.execute(self.stat.create_index, timeout=60000)

        self.session.shutdown()

        self.log.info('## Session closed')

        # print information
        self.log.info('Create search index cost time: {}'.format(datetime.now() - end))
        self.log.info('Total cost time: {}'.format(datetime.now() - Loader.start))
        self.log.info('## Inserts completed')

    def __line_generator(self):

        with open(self.filename, 'r', encoding='utf-8') as f:
            for counter,line in enumerate(f):
                try:
                    if counter%100000 == 0:
                        self.log.info(str(counter))
                        if counter%1000000 == 0:
                            self.log.info('{}'.format(datetime.now()-Loader.start))
                            self.log.info('')
                    yield line
                except Exception as e:
                    self.log.warn(e)
                    continue



       
    def __insert_data(self, line):
        if self.index == 'reddit'
            data = [line['id'], line['name'], line['link_id'], line['parent_id'],
                    line['subreddit_id'],line['author'], line['body'], int(line['created_utc'])]
        elif self.index == 'amazon':
            pass

        res = self.session.execute(self.cql_prepared,data)
        return 


if __name__ == '__main__':
    loader = Loader('reddit')
    loader.load()
