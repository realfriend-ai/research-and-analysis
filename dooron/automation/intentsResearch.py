import pandas as pd

from constants.importantDates import last_week
from constants.mongoConnectDooron import tyntec_webhooks

dooron_whatsapp_numbers = ['972542324236', '972502020220']
intents_of_requests = ['SET_REQUEST', 'SET_BUDGET', 'SET_AREA', 'SET_CITY', 'SET_OTHER_REQUEST', 'NEW_REQUEST',
                       'SET_NUM_OF_ROOMS']


def isByUs(webhook):
    if str(int(webhook.get('from'))) in dooron_whatsapp_numbers:
        return True
    else:
        return False


def get_unique_user_phone(webhook):
    isWebHookByUs = webhook.get('isByUs')
    if isWebHookByUs:
        return webhook.get('to')
    else:
        return webhook.get('from')


def is_found_in_webhook(webhook, kind):
    if type(webhook.get(kind)) == float:
        return False
    if len(webhook.get(kind)) == 0:
        return False
    else:
        return True


def is_content_from_type_text(content):
    if type(content) == dict:
        contentType = content.get('contentType')
        if contentType == 'text':
            return True
    return False


def is_intent_related_to_request(webhook):
    if webhook.get('foundIntents'):
        for intent in webhook.get('intents'):
            if intent in intents_of_requests:
                return True
    return False


def get_respond_after_request(whatsappMessagesDf: pd.DataFrame):
    whatsappMessagesDf.sort_values(by=['userPhoneNumber', 'createdAt'], ignore_index=True, inplace=True,
                                   ascending=False)
    content = ''
    userPhone = ''
    for i in whatsappMessagesDf.index:
        if whatsappMessagesDf.at[i, 'isIntentRelatedToRequest']:
            if whatsappMessagesDf.at[i, 'userPhoneNumber'] == userPhone:
                if type(content) == dict:
                    contentType = content.get('contentType')
                    whatsappMessagesDf.at[i, 'contentAfterType'] = contentType
                    if contentType == 'text':
                        whatsappMessagesDf.at[i, 'contentAfterText'] = content.get('text')
                    if contentType == 'template':
                        whatsappMessagesDf.at[i, 'contentAfterTemplateId'] = content.get('template').get(
                            'templateId')
        content = whatsappMessagesDf.at[i, 'content']
        userPhone = whatsappMessagesDf.at[i, 'userPhoneNumber']
    return whatsappMessagesDf


def get_webhooks_df():
    whatsappMessagesList = list(tyntec_webhooks.find({
        'createdAt': {'$gte': last_week},
        'content': {'$exists': True}
    }, {'from': 1, 'to': 1, 'createdAt': 1, 'content': 1, 'intents': 1, 'entities': 1}))
    whatsappMessagesDf = pd.DataFrame(whatsappMessagesList)
    whatsappMessagesDf['isByUs'] = whatsappMessagesDf.apply(lambda webhook: isByUs(webhook), axis=1)
    whatsappMessagesDf['userPhoneNumber'] = whatsappMessagesDf.apply(lambda webhook: get_unique_user_phone(webhook),
                                                                     axis=1)
    whatsappMessagesDf['foundIntents'] = whatsappMessagesDf.apply(
        lambda webhook: is_found_in_webhook(webhook, 'intents'), axis=1)
    whatsappMessagesDf['isIntentRelatedToRequest'] = whatsappMessagesDf.apply(
        lambda webhook: is_intent_related_to_request(webhook), axis=1)
    whatsappMessagesDf['foundEntities'] = whatsappMessagesDf.apply(
        lambda webhook: is_found_in_webhook(webhook, 'entities'), axis=1)
    whatsappMessagesDf['isText'] = whatsappMessagesDf['content'].apply(
        lambda x: is_content_from_type_text(x))
    whatsappMessagesDf = get_respond_after_request(whatsappMessagesDf)
    userSendingMessages = whatsappMessagesDf[
        (whatsappMessagesDf['isByUs'] == False) & (whatsappMessagesDf['isText'] == True)]
    return {'messagesDf': whatsappMessagesDf, 'userSendingMessagesDf': userSendingMessages}


def main_func():
    output = get_webhooks_df()
    total_messages_sent = output.get('messagesDf')
    print(f'Num of message looked into {len(total_messages_sent.index)}')
    userMessagesSent = output.get('userSendingMessagesDf')
    print(f'Num of user message looked into {len(userMessagesSent.index)}')
    print(userMessagesSent['foundIntents'].value_counts(normalize=True))
    print(userMessagesSent['foundEntities'].value_counts(normalize=True))
    print(userMessagesSent['isIntentRelatedToRequest'].value_counts(normalize=True))
    userMessagesSentWithRequests = userMessagesSent[userMessagesSent['isIntentRelatedToRequest']]
    print(userMessagesSentWithRequests['contentAfterType'].value_counts())
    print(userMessagesSentWithRequests['contentAfterTemplateId'].value_counts())
    print(userMessagesSentWithRequests['contentAfterText'].value_counts())
    return userMessagesSentWithRequests


# user_msg_with_req = main_func()
