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
import json


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

with open('autopilot_analyticsaudit_emails.txt', 'r') as f:
    autopilot_analyticsaudit_emails = [ x.strip() for x in f.readlines()]

if __name__ == "__main__":
    logging.basicConfig(
        filename="changestreaming-report-collection.log",
        filemode='a',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO)
    db.init()
    change_stream = db.DATABASE['reports'].watch()
    for change in change_stream:
        logging.info(dumps(change))
        operationType = change['operationType']
        if operationType == 'insert':
            report = change['fullDocument']
            Email = report['email']
            if report.get('package') == 'analytics-audit' and (not Email in autopilot_analyticsaudit_emails):
                score = report.get("totalScore")
                datasource = db.find_one("datasource", {"_id":report.get("datasourceID")})
                datasourceName = datasource.get("dataSourceName")
                data = {
                        "contact": {
                                    'Email': Email,
                                    "custom": { "integer--Analytics--Audit--Score": score,
                                                "string--Datasource--Name": datasourceName}
                                }
                        }
                resp = rq.post(addContactLink, data = json.dumps(data), headers=post_headers)
                if resp.status_code == 200:
                    logging.info(
                        "SUCCESS - Analytics Audit score for {} is added/updated"
                        .format(Email))
                    with open('autopilot_analyticsaudit_emails.txt', 'a') as f:
                        f.write(f"{Email}\n")
                    autopilot_analyticsaudit_emails += [Email]
                else:
                    logging.info(
                        "ERROR - Analytics Audit score for {} cannot be added/updated - Status Code: {}"
                        .format(Email, resp.status_code))
            else:
                logging.info(
                "INFO - It is not important change - Report Type: {} - Email: {}".
                format(report.get('package'), Email))
        else:
            logging.info(
                "INFO - It is not important change - Operation Type: {}".
                format(operationType))
