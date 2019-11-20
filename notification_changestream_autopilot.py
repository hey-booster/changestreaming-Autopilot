# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 23:25:29 2019

@author: altun
"""
from database import db
import requests as rq
import os
import logging
from bson.json_util import dumps


AUTOPILOT_APIKEY = os.environ.get('AUTOPILOT_APIKEY').strip()

post_headers = {
  'autopilotapikey': AUTOPILOT_APIKEY,
  'Content-Type': 'application/json'
}

get_headers = {
  'autopilotapikey': AUTOPILOT_APIKEY,
}


addContactLink = "https://api2.autopilothq.com/v1/contact"
addToListLink = "https://api2.autopilothq.com/v1/list/{list_id}/contact/{contact_id}"

NOTIFICATIONS_LIST_ID = "contactlist_8c31a81b-bb7e-40f8-83fb-75269b064d94"


if __name__ == "__main__":
    logging.basicConfig(filename="changestreaming-user-collection.log", filemode='a',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    db.init()
    change_stream = db.DATABASE['notification'].watch()
    for change in change_stream:
        logging.info(dumps(change))
        operationType = change['operationType']
        documentId = change['documentKey']['_id']['$oid']
        if  operationType == 'insert':
            notification = change['fullDocument']
            Email = notification['email']
            #For every type of notification add user into Notifications List
            resp = rq.post(addToListLink.format(list_id = NOTIFICATIONS_LIST_ID,
                                        contact_id = Email), headers = get_headers)

