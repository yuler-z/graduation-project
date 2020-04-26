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
from utils import ThreadPoolExecutorWithQueueSizeLimit

## Script args and Help
parser = argparse.ArgumentParser(add_help=True)
parser.add_argument('-s', action="store", dest="SCYLLA_IP", default="10.10.10.2,10.10.10.3,10.10.10.4")
parser.add_argument('-e', action="store", dest="ES_IP", default="127.0.0.1")
opts = parser.parse_args()

SCYLLA_IP = opts.SCYLLA_IP.split(',')
ES_IP = opts.ES_IP.split(',')

# statement
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


class Loader:
    start = 0
    def __init__(self,filename):
        logging.getLogger("elasticsearch").setLevel(logging.WARNING)
        self.log = getLogger('scy+es','load.log')
        
        Loader.start = datetime.now()

        self.__init_scy()
        self.__init_es()
        
        self.pool_scy = ThreadPoolExecutorWithQueueSizeLimit(max_workers=10)
        self.pool_es = ThreadPoolExecutorWithQueueSizeLimit(max_workers=10)

    def load_data(filename):

        # main loop
        g = self.__line_generator(filename)
        while True:
            try:
                line = json.loads(next(g))
                self.pool_scy.submit(self.__insert_data,line)
                self.pool_es.submit(self.__insert_index,line)
            except StopIteration:
                break
            except Exception:
                continue 

        self.pool_scy.shutdown()
        self.pool_es.shutdown()

        self.es.indices.refresh(index='reddit')
        # shutdown
        self.session.shutdown()

        # print information
        self.log.info('## Total cost time: {}'.format(datetime.now() - Loader.start))
        self.log.info('## Inserts completed')



    def __line_generator(self,filename):

        with open(filename, 'r', encoding='utf-8') as f:
            for counter,line in enumerate(f):
                try:
                    if counter%100000 == 0:
                        log.info(str(counter))
                        if counter%1000000 == 0:
                            log.info('{}'.format(datetime.now()-Loader.start))
                            log.info('')
                    yield line
                except Exception as e:
                    print(e)
                    continue


    # get scylladb connect, create ks and tb, return session
    def __init_scylladb(self):
        # session = Cluster(contact_points=SCYLLA_IP,execution_profiles={EXEC_PROFILE_DEFAULT:ep}).connect()
        self.session = Cluster(contact_points=SCYLLA_IP).connect()
        # create a schema
        self.session.execute(create_ks)
        # create a tb
        self.session.execute(create_tb)

        self.cql_prepared = self.session.prepare(cql)
        self.cql_prepared.consistency_level = ConsistencyLevel.LOCAL_QUORUM



    # get es connect, create index
    def __init_es(self):

        slef.es = Elasticsearch(ES_IP)
        # create es index
        self.es.indices.create(index="reddit", ignore=400)

        return es


    # insert data into scylladb
    def __insert_data(self,line):

        # TODO: format your data
        data = [line['id'], line['name'], line['link_id'], line['parent_id'],
        line['subreddit_id'],line['author'], line['body'], int(line['created_utc'])]

        # prepare
        # cql_prepared.consistency_level = ConsistencyLevel.LOCAL_ONE if random.random() < 0.2 else ConsistencyLevel.LOCAL_QUORUM
        
        res = self.session.execute(self.cql_prepared,data,timeout=60000)
        return 
    


    # insert data into elasticsearch
    def __insert_index(self,line): 

        # TODO: format your data
        data = { k:v for k,v in line.items() if k in ['id', 'name', 'author', 'body'] }
        res = self.es.index(index="reddit", doc_type="comment", id=line['id'], body=data)
        return 



if __name__ == '__main__':
    loader = Loader('/home/zyh/graduation-project/data/reddit_comment')
    loader.load()
