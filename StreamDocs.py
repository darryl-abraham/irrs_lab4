"""
.. module:: StreamDocs

StreamDocs
******

:Description: StreamDocs

    Different Auxiliary functions used for different purposes

:Authors:
    bejar

:Version: 

:Date:  14/07/2017
"""

import sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from elasticsearch.exceptions import NotFoundError

import argparse

__author__ = 'bejar'

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--index', default=None, required=True, help='Index to search')

    args = parser.parse_args()

    index = args.index
    try:
        client = Elasticsearch(hosts=["http://localhost:9200"], request_timeout=1000)
        sc = scan(client, index=index, query={"query": {"match_all": {}}})
        for r in sc:
            print(r['_source']['path'], '\t', r['_source']['text'].encode('ascii','replace'))
    except BrokenPipeError:
        sys.exit(0)
    except NotFoundError:
        raise(NameError(f'Index {index} does not exists'))

