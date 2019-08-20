# -*- coding: utf-8 -*-

import json
import requests
from elasticsearch import Elasticsearch

_DEFAULT_PAGE_SIZE = 30
_DEFAULT_FROM = 0

# es connection
_HOST = "127.0.0.1"
_PORT = 9200
_INDEX = "bank"
_CONNECTION_TIMEOUT = 5000
_QUERY_TIMEOUT = '5s'
_SCROLL_SESSION_TIMEOUT = '10s'

_ES = Elasticsearch(
    [{
        'host': _HOST,
        'port': _PORT
    }],
    timeout=_CONNECTION_TIMEOUT
)


# -------------------------------------------------------------------
# use java api for data retrieval
def query(page_size=_DEFAULT_PAGE_SIZE, from_index=_DEFAULT_FROM):
    q = {
        "size": page_size,
        "from": from_index,
        "query": {
            "exists": {
                "field": "educations.majorName"
            }
        },
        "_source": ["es_id", "contacts"],
        "sort": [
            "_score",
            {
                "_script": {
                    "type": "number",
                    "script": "doc['educations.majorName.keyword'].length",
                    "order": "desc"
                }
            }
        ],
        "timeout": "5s"
    }
    return json.dumps(q)


def retrieve_data(q):
    es_url = "http://api.hitalent.us:38088/api/v2/search-talents"
    response = requests.post(es_url, data=q, headers={'Content-Type': 'application/json'})

    return response.json()
    pass
# -------------------------------------------------------------------


# -------------------------------------------------------------------
# use es scroll for data retrieval
def init_scroll(page_size=_DEFAULT_PAGE_SIZE):
    q = {
        "size": page_size,
        "query": {
            "exists": {
                "field": "educations.majorName"
            }
        },
        "_source": ["es_id", "contacts"],
        "sort": [
            "_score",
            {
                "_script": {
                    "type": "number",
                    "script": "doc['educations.majorName.keyword'].length",
                    "order": "desc"
                }
            }
        ],
        "timeout": _QUERY_TIMEOUT
    }

    data = _ES.search(
        index=_INDEX,
        scroll=_SCROLL_SESSION_TIMEOUT,
        size=page_size,
        body=q
    )

    return data['_scroll_id'], data['hits']['hits']


def scroll(scroll_id):
    data = _ES.scroll(
        scroll_id=scroll_id,
        scroll=_SCROLL_SESSION_TIMEOUT
    )
    return data['_scroll_id'], data['hits']['hits']
    pass
