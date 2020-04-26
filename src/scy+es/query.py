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
from utils import ThreadPoolExecutorWithQueueSizeLimit, getLogger,mapper


## Script args and help
parser = argparse.ArgumentParser(add_help=True)

parser.add_argument('-s', action="store", dest="SCYLLA_IP", default="10.10.10.2,10.10.10.3,10.10.10.4")
parser.add_argument('-e', action="store", dest="ES_IP", default="127.0.0.1")

opts = parser.parse_args()

SCYLLA_IP = opts.SCYLLA_IP.split(',')
ES_IP = opts.ES_IP.split(',')

# counter


class Searcher:
    def __init__(self,name):

        self.index = name
        self.filename = mapper[name][query]

        self.log1 = getLogger('time', 'search_time.log')
        self.log2 = getLogger('result', 'search_result.log')

        self.log2.info('## connecting to elasticsearch cluster')
        self.es = Elasticsearch(ES_IP)
        self.log2.info('## connecting to scylladb cluster')
        self.session = Cluster(SCYLLA_IP).connect()

#        self.query_body = '''
#                    {
#                        "query":{
#                            "match":{
#                                "body":"{}"
#                            }
#                        }
#                    }
#                    '''
        if self.index == 'reddit':
            self.query_body = {"query":{"match":{"body":""}}}
            cql = "SELECT * FROM reddit.comment WHERE id=?"
        elif self.index == 'amazon':
            self.query_body = {"query":{"match":{"body":""}}}
            cql = "SELECT * FROM amazon.comment WHERE id=?"
            
        self.cql_prepared = self.session.prepare(cql)
        self.cql_prepared.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        
     
    def query(self):
        counter = 0
        start = datetime.now()

        with open(self.filename, 'r', encoding='utf-8') as f:
            for counter,line in enumerate(f):
                try:
                    line = line.strip('\n')
                    if counter%100 == 0:
                        self.log1.info(str(counter))
                        if counter%1000 == 0:
                            self.log1.info('{}'.format(datetime.now()-start))
                            self.log1.info('')
                    self.log2.info('({})'.format(line))
                    
                    # format query body
                    self.query_body['query']['match']['body'] = line
                        
                    res = self.es.search(index=self.index, doc_type="comment", body=self.query_body, size=3)
                    
                    es_results = [doc['_id'] for doc in res['hits']['hits']]

                    for r in es_results:
                        scylla_res = self.session.execute(self.cql_prepared, (r,))
                        self.log2.info("{}".format([list(row) for row in scylla_res]))

                except Exception as e:
                    self.log2.warn(e)

        
        self.log1.info('total time:{}'.format(datetime.now() - start))

                     

        
#    for doc in res['hits']['hits']:
        ## Print all columns in Elasticsearch result set
        #print("id: %s | name: %s | author: %s | body: %s" % (doc['_id'], doc['_source']['name'],doc['_source']['author'], doc['_source']['body']))
if __name__ == '__main__':
    searcher = Searcher('reddit')
    searcher.query()

