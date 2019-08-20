# -*- coding: utf-8 -*-

import es
import db

ES_PAGE_SIZE = 30
DEBUG = False


def process():
    # initialize the scroll query
    # get the scroll id and init data
    scrollId, data = es.init_scroll(ES_PAGE_SIZE)
    es_total = len(data)

    # set a var for count
    count = es_total

    # handle the init data
    data_handler(data)

    # infinite pagination loop as long as there exists data returned
    # from current page
    while es_total > 0 and (not DEBUG or (DEBUG and count < 100)):
        scrollId, data = es.scroll(scrollId)
        es_total = len(data)

        count += es_total
        data_handler(data)

    print('----ES data retrieval completed and all data have been in MongoDB----')
    pass


def data_handler(data):
    new_list = []
    for d in data:
        info = {}
        info['id'] = d['_id']

        if 'es_id' in d['_source'].keys():
            info['es_id'] = d['_source']['es_id']
        else:
            info['es_id'] = ''

        contacts = list(filter(lambda c: c['type'] == 'LinkedIn', d['_source']['contacts']))
        for c in contacts:
            c['isValid'] = False
        info['contacts'] = contacts
        info['processed'] = False

        new_list.append(info)

    x = 0
    try:
        x = db.COLLECTION.insert_many(new_list, ordered=False)
    except:
        print('Probably duplicated records found.')
    else:
        print('%d inserted.' % x.inserted_ids)

    count = db.COLLECTION.count_documents({})
    print('number of docs: %d' % count)


# --------------------------------------------

if __name__ == "__main__":
    process()
