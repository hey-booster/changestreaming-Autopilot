from database import db
import logging
from bson.json_util import dumps
from slack import WebClient
from bson.objectid import ObjectId


if __name__ == "__main__":
    logging.basicConfig(filename="changestreaming-feedback-collection.log", filemode='a',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    db.init()
    change_stream = db.DATABASE['feedback'].watch()
    for change in change_stream:
        logging.info(dumps(change))
        operationType = change['operationType']
        if operationType == 'insert':
            feedback = change['fullDocument']
            #Get user's slack token for openening Slack Web Client
            email = feedback['email']
            user = db.find_one('user', query={"email": email})
            sl_token = user['sl_accesstoken']
            #Get channel where feedback is given
            dataSourceID = feedback['datasourceID']
            datasource = db.find_one('datasource', query={"_id": ObjectId(dataSourceID)})
            channel = datasource['channelID']
            #Open Slack Client
            slack_client = WebClient(token=sl_token)
            #Send Message to the Client
            slack_client.chat_postMessage(channel=channel, text="Thank you for your support, we are working hard to improve your experience. :muscle:")