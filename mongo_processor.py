# -*- coding: utf-8 -*-

import requests
import db
import json
import time


MONGO_PAGE_SIZE = 10
DEBUG = False


def process():
    # initialize the parameters of pagination for data retrieval
    page_num = 0
    skip = 0

    # initialize a positive value for loop to work and updated in the loop
    mongo_total = 1

    while mongo_total > 0 and (not DEBUG or (DEBUG and skip < 50)):
        mongo_data, skip = get_data(page_num)
        mongo_total = len(mongo_data)

        page_num += 1

        # -------------------------------------------------------------
        # process the linkedIn url
        updated_list = data_handler(mongo_data)
        # update_talents(updated_list)

    print('----MongoDB updated----')


def get_data(page_num):
    skip = page_num * MONGO_PAGE_SIZE
    talents = list(db.COLLECTION.find({"processed": False}, skip=skip, limit=MONGO_PAGE_SIZE))
    return talents, skip


def data_handler(data):
    for t in data:
        contacts = t['contacts']
        for c in contacts:
            identifier = c['contact']

            # call api to check the url validation
            q = {"identifier": identifier}
            linkedin_res = api_linkedIn(json.dumps(q))

            if 'talent' in linkedin_res.keys():
                c['isValid'] = True

            print('contact: %s verified' % identifier)
            time.sleep(30)

        update_one_talent(t)

        print('-----------------------------------------------')

    return data


def api_linkedIn(q):
    url = "http://staging.api.hitalent.us:3010/api/linkedin/syncProfile"
    response = requests.post(url, data=q, headers={'Content-Type': 'application/json'})

    return response.json()
    pass


def update_talents(talents):
    for t in talents:
        db.COLLECTION.update_one(
            {"_id": t['_id']},
            {
                "$set": {
                    "contacts": t['contacts'],
                    "processed": True
                }
            })
        print(t["_id"])

    pass


def update_one_talent(t):
    db.COLLECTION.update_one(
        {"_id": t['_id']},
        {
            "$set": {
                "contacts": t['contacts'],
                "processed": True
            }
        })

    print('talent %s updated' % t["_id"])
    pass


# --------------------------------------------

if __name__ == "__main__":
    process()
