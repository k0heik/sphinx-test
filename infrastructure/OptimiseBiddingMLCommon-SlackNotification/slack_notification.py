import json
import logging
import os

from base64 import b64decode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    message = json.loads(event['Records'][0]['Sns']['Message'])
    logger.info("Message: " + str(message))

    HOOK_URL = os.environ['HookUrl']

    if 'slack-channel' in message:
        SLACK_CHANNEL = message['slack-channel']
    else:
        SLACK_CHANNEL = os.environ['slackChannel']

    dict = {}
    if message['detail']['build-status'] == 'Failed':
        dict['color'] = "#FF0000"
    else:
        dict['color'] = "#00FF00"
    dict['title'] = message['title']

    fields = []
    for k, v in message['detail'].items():
        print(k, v)
        field = {}
        field['title'] = k
        field['value'] = v
        fields.append(field)

    dict['fields'] = fields

    attachments = []
    attachments.append(dict)

    slack_message_append = {}
    slack_message_append['channel'] = SLACK_CHANNEL
    slack_message_append['attachments'] = attachments

    logger.info("slack_message_append: " + str(slack_message_append))

    slack_message = slack_message_append

    req = Request(HOOK_URL, json.dumps(slack_message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        logger.info("Message posted to %s", slack_message['channel'])
    except HTTPError as e:
        logger.error("Request failed: %d %s", e.code, e.reason)
    except URLError as e:
        logger.error("Server connection failed: %s", e.reason)

    return {
        'statusCode': 200,
    }
