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


# statement
create_ks = ("CREATE KEYSPACE IF NOT EXISTS reddit "
              " WITH replication = {"
                " 'class' : 'NetworkTopologyStrategy',"
                " 'datacenter1' : 2}"
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
create_index = ("CREATE SEARCH INDEX IF NOT EXISTS ON reddit.comment"
                " WITH COLUMNS author,body {indexed:true}")

cql = ("INSERT INTO reddit.comment"
       " (id, name, link_id, parent_id, subreddit_id, author, body)"
       " VALUES(?,?,?,?,?,?,?)"
       " USING TIMESTAMP ?"
        )


class Loader:

    start = 0

    def __init__(self,filename):
        # set log
        logging.getLogger("elasticsearch").setLevel(logging.WARNING)
        self.log = getLogger('dse', 'dse.log')

        Loader.start = datetime.now()

        self.pool = ThreadPoolExecutorWithQueueSizeLimit(max_workers=10, queue_size=30)

        self.__init_dse()
        
        self.filename = filename



    def __init_dse(self):
        self.session = Cluster(DSE_IP).connect()

        self.session.execute(create_ks)
        self.session.execute(create_tb)

        self.cql_prepared = self.session.prepare(cql)
        self.cql_prepared.consistency_level = ConsistencyLevel.LOCAL_QUORUM

    def load(self):

        g = self.__line_generator()
        while True:
            try:
                line = json.loads(next(g))
                self.pool.submit(self.__insert_data,line)
            except StopIteration:
                break
            except Exception:
                continue 

        # shutdown
        self.pool.shutdown() 

        # create search index
        end = datetime.now() 
        self.log.info('Insert data cost time: {}'.format(end - Loader.start))
        self.log.info('create search index')
        self.session.execute(create_index, timeout=60000)

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
        data = [line['id'], line['name'], line['link_id'], line['parent_id'],
        line['subreddit_id'],line['author'], line['body'], int(line['created_utc'])]

        res = self.session.execute(self.cql_prepared,data)
        return 


if __name__ == '__main__':
    loader = Loader('/home/zyh/graduation-project/data/reddit_comment')
    loader.load()
