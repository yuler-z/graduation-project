#! /usr/bin/env python3

import json
import argparse
import logging
import queue
from datetime import datetime
import pandas as pd

from cassandra.cluster import Cluster,ExecutionProfile,EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel
from elasticsearch import Elasticsearch

import sys
sys.path.append('../')
from utils import ThreadPoolExecutorWithQueueSizeLimit,getLogger,mapper

## Script args and Help
parser = argparse.ArgumentParser(add_help=True)
parser.add_argument('-s', action="store", dest="SCYLLA_IP", default="10.10.10.2,10.10.10.3,10.10.10.4")
parser.add_argument('-e', action="store", dest="ES_IP", default="127.0.0.1")
opts = parser.parse_args()

SCYLLA_IP = opts.SCYLLA_IP.split(',')
ES_IP = opts.ES_IP.split(',')

class Statement:
    def __init__(self,name):
        switcher = { 'reddit': self.reddit_stat, 'amazon': self.amazon_stat}
        switcher[name]()

    def reddit_stat(self):
        self.create_ks = ("CREATE KEYSPACE IF NOT EXISTS reddit"
                    " WITH replication = {"
                        "'class' : 'NetworkTopologyStrategy',"
                        " 'datacenter1' : 2"
                    "}"
                )
        self.create_tb = ("CREATE TABLE IF NOT EXISTS reddit.comment "
                        " (id text,"
                        " name text,"
                        " link_id text,"
                        " parent_id text,"
                        " subreddit_id text,"
                        " author text,"
                        " body text,"
                        " PRIMARY KEY(id, name))"
                    )
        self.cql = ("INSERT INTO reddit.comment "
                "(id, name, link_id, parent_id, subreddit_id, author, body)"
                " VALUES(?,?,?,?,?,?,?)"
                " USING TIMESTAMP ?"
                )
    def amazon_stat(self):
        self.create_ks = ("CREATE KEYSPACE IF NOT EXISTS amazon"
                    " WITH replication = {"
                        "'class' : 'NetworkTopologyStrategy',"
                        " 'datacenter1' : 2"
                    "}"
                )
        self.create_tb = ("CREATE TABLE IF NOT EXISTS amazon.comment "
                        " (id int,"
                        " user_id int,"
                        " product_id int,"
                        " rating float,"
                        " title text,"
                        " body text,"
                        " PRIMARY KEY(id, user_id))"
                    )
        self.cql = ("INSERT INTO amazon.comment "
                "(id, user_id, product_id, rating, title, body)"
                " VALUES(?,?,?,?,?,?)"
                " USING TIMESTAMP ?"
                )
            

class Loader:
    start = 0
    def __init__(self,name):
        logging.getLogger("elasticsearch").setLevel(logging.WARNING)
        self.log = getLogger('scy+es','load.log')
        
        Loader.start = datetime.now()

    
        self.index = name
        self.stat = Statement(name)

        self.filename = mapper[name]['load']

        self.__init_scy()
        self.__init_es()
        
        self.pool_scy = ThreadPoolExecutorWithQueueSizeLimit(max_workers=10)
        self.pool_es = ThreadPoolExecutorWithQueueSizeLimit(max_workers=10)

    def load(self):

        if self.index == 'reddit':
            # main loop
            g = self.__line_generator()
            while True:
                try:
                    line = json.loads(next(g))
                    self.pool_scy.submit(self.__insert_data,line)
                    self.pool_es.submit(self.__insert_index,line)
                except StopIteration:
                    break
                except Exception:
                    continue 
        elif self.index == 'amazon':
            df = pd.read_csv(self.filename,encoding='utf-8')
            end = df.index.max()
            df = df.fillna('missing')
            try:
                for line in zip(range(0,end+1),df['userId'],df['productId'],
                                    df['rating'],df['title'],df['comment'],df['timestamp']):
                    counter = line[0]
                    if counter%100000 == 0:
                        self.log.info(str(counter))
                        if counter%1000000 == 0:
                            self.log.info('{}'.format(datetime.now()-Loader.start))
                            self.log.info('')
                    self.pool_scy.submit(self.__insert_data,line)
                    self.pool_es.submit(self.__insert_index,line)
            except Exception as e:
                print(e)
            
        self.pool_scy.shutdown()
        self.pool_es.shutdown()
        
        self.es.indices.refresh(index=self.index)
        # shutdown
        self.session.shutdown()

        # print information
        self.log.info('## Total cost time: {}'.format(datetime.now() - Loader.start))
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
                    print(e)
                    continue


    # get scylladb connect, create ks and tb, return session
    def __init_scy(self):
        # session = Cluster(contact_points=SCYLLA_IP,execution_profiles={EXEC_PROFILE_DEFAULT:ep}).connect()
        self.session = Cluster(contact_points=SCYLLA_IP).connect()
        # create a schema
        self.session.execute(self.stat.create_ks)
        # create a tb
        self.session.execute(self.stat.create_tb)

        self.cql_prepared = self.session.prepare(self.stat.cql)
        self.cql_prepared.consistency_level = ConsistencyLevel.LOCAL_QUORUM



    # get es connect, create index
    def __init_es(self):

        with open('mapping.json','r') as f:
           mapping = json.load(f)[self.index] 

        self.es = Elasticsearch(ES_IP, timeout=30)
        # create es index
        self.es.indices.create(index=self.index,body=mapping ignore=400,timeout=30)



    # insert data into scylladb
    def __insert_data(self,line):

        data = list()
        # TODO: format your data
        if self.index == 'reddit':
            data = [line['id'], line['name'], line['link_id'], line['parent_id'], line['subreddit_id'],line['author'], line['body'], int(line['created_utc'])]
        elif self.index == 'amazon':
            #       id | user_id | product_id | rating | title | body | timestamp
            data = [line[0], int(line[1]), int(line[2]), line[3], line[4], line[5], line[6]]
        self.log.info(data)
        res = self.session.execute(self.cql_prepared,data,timeout=60000)
        return 
    


    # insert data into elasticsearch
    def __insert_index(self,line): 

        # TODO: format your data
        if self.index == 'reddit':
            data = { k:v for k,v in line.items() if k in ['id', 'name', 'author', 'body'] }
        elif self.index == 'amazon':
            data = {}
            data['id'] = line[0]
            data['title'] = line[4]
            data['body'] = line[5]

        res = self.es.index(index=self.index, doc_type="comment", id=data['id'], body=data)
        return 



if __name__ == '__main__':
    loader = Loader('amazon')
    loader.load()
