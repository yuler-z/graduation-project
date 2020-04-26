#! /usr/bin/env python3
# -*- coding: latin-1 -*-
#

## Using elasticsearch-py ###
import argparse
import concurrent.futures
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from elasticsearch import Elasticsearch

import sys
sys.path.append('../')
from utils import ThreadPoolExecutorWithQueueSizeLimit, getLogger


## Script args and help
parser = argparse.ArgumentParser(add_help=True)

parser.add_argument('-s', action="store", dest="DSE_IP", default="10.10.9.2,10.10.9.3,10.10.9.4")

opts = parser.parse_args()

DSE_IP = opts.DSE_IP.split(',')



class Searcher:
    start = 0
    def __init__(self,filename):

        self.filename = filename 
        Searcher.start = datetime.now()

        self.log1 = getLogger('time', 'query_time.log')
        self.log2 = getLogger('result', 'query_result.log')

        self.log2.info('## connecting to cassandra cluster')
        self.session = Cluster(DSE_IP).connect()

        cql = "SELECT * FROM reddit.comment WHERE solr_query=?"
        self.solr_query = '{{"q":"body:*{0}*"}}'
        self.cql_prepared = self.session.prepare(cql)
        self.cql_prepared.consistency_level = ConsistencyLevel.ONE
        
     
    def query(self):
        counter = 0

        with open(self.filename, 'r', encoding='utf-8') as f:
            for counter,line in enumerate(f):
                try:
                    line = line.strip('\n')
                    if counter%100 == 0:
                        self.log1.info(str(counter))
                        if counter%1000 == 0:
                            self.log1.info('{}'.format(datetime.now()- Searcher.start))
                            self.log1.info('')
                    self.log2.info('({})'.format(line))
                    
                    # format query body
                    data = self.solr_query.format(line)
                    
                    res = self.session.execute(self.cql_prepared,[data],timeout=60000)
                    
                    self.log2.info(res[0])
                except Exception as e:
                    self.log2.warn(e)


        self.log1.info('total time:{}'.format(datetime.now() - Searcher.start))

if __name__ == '__main__':
    searcher = Searcher('/home/zyh/graduation-project/data/word')
    searcher.query()

