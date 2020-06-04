# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 23:25:29 2019

@author: altun
"""
from database import db
import requests as rq
import json
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

GOOGLE_SIGN_UP_LIST_ID = "contactlist_1cd91374-0ab7-40d8-9883-4b9416bf0d3d"
ANALYTICS_LINKED_LIST_ID = "contactlist_edcdbe6d-a0ee-46e7-888f-b644106c7254"
SLACK_LINKED_LIST_ID = "contactlist_1642457e-684b-43e2-84a3-c7cc59eeac90"

if __name__ == "__main__":
    db.init()
    change_stream = db.DATABASE['user'].watch()
    logging.basicConfig(filename="changestreaming-user-collection.log", filemode='a',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    for change in change_stream:
        logging.info(dumps(change))
        operationType = change['operationType']
        if  operationType == 'insert':
            #User is added Google Sign-up List
            user = change['fullDocument']
            FirstName = user['firstname']
            LastName = user['lastname']
            Email = user['email']
            Segment = user['segment'] if 'segment' in user.keys() else ''
            data = {
                    "contact": {
                                'FirstName': FirstName,
                                'LastName': LastName,
                                'Email': Email,
                                "custom": { "string--Segment": Segment,
                                            "integer--Analytics--Audit--Score": 0,
                                            "string--Datasource--Name": "",
                                            'boolean--eCommerce--Activity': False}
                                
                            }
                    }
            #Add contact and get contact id
            resp = rq.post(addContactLink, data = json.dumps(data), headers=post_headers)
            contact_id = resp.json()['contact_id']
            #Add this to Google-Sign-Up Contact List (all users - there is no other condition)
            resp = rq.post(addToListLink.format(list_id = GOOGLE_SIGN_UP_LIST_ID,
                                                contact_id = contact_id), headers = get_headers)
            if resp.status_code == 200:
                logging.info("SUCCESS - {} is inserted into Google Sign Up List for Autopilot".format(Email))
            else:
                logging.info("ERROR - {} cannot be inserted into Google Sign Up List for Autopilot - Status Code: {}".format(Email, resp.status_code))
            del Email
        elif operationType == 'update':
            updateFields = change['updateDescription']['updatedFields']
            documentId = change["documentKey"]["_id"]
            doc = db.find_one("user", {"_id":documentId})
            Email = doc['email']
            if 'ga_accesstoken' in updateFields.keys() or 'ga_refreshtoken' in updateFields.keys():
                #User is added to Analytics Linked List 
                resp = rq.post(addToListLink.format(list_id = ANALYTICS_LINKED_LIST_ID,
                                        contact_id = Email), headers = get_headers)
                if resp.status_code == 200:
                    logging.info("SUCCESS - {} is inserted into Analytics Linked List for Autopilot".format(Email))
                else:
                    logging.info("ERROR - {} cannot be inserted into Analytics Linked List for Autopilot - Status Code: {}".format(Email, resp.status_code))
            elif 'sl_accesstoken' in updateFields.keys():
                #User is added to Slack Linked List
                resp = rq.post(addToListLink.format(list_id = SLACK_LINKED_LIST_ID,
                                        contact_id = Email), headers = get_headers)
                if resp.status_code == 200:
                    logging.info("SUCCESS - {} is inserted into Slack Linked List for Autopilot".format(Email))
                else:
                    logging.info("ERROR - {} cannot be inserted into Slack Linked List for Autopilot - Status Code: {} ".format(Email, resp.status_code))
        else:
            logging.info("INFO - It is not important change - Operation Type: {}".format(operationType))
