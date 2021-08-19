import pandas as pd
from datetime import timedelta
from constants import lukeFbUserIds
from constants.importantDates import last_day
from constants.mongoConnectDooron import fb_users, tyntec_webhooks


def get_received_msgs_from_whatsapp(limit):
    list1 = list(tyntec_webhooks.find({
        'createdAt': {'$gte': last_day - timedelta(days=1), '$lte': last_day},
        'content.contentType': 'text',
    }, {'createdAt': 1, 'from': 1, 'content': 1, 'intents': 1, 'entities': 1}).limit(limit))
    whDf = pd.DataFrame(list1)
    whDf['text'] = whDf['content'].apply(lambda x: x.get('text') if type(x) == dict else None)
    whDf.dropna(subset=['text'], inplace=True)
    whDf.rename(columns={'from': 'whatsappPhoneNumber', '_id': 'webhookId'}, inplace=True)
    return whDf


def get_whatsapp_users(phone_list):
    list1 = list(fb_users.find({
        '_id': {'$nin': lukeFbUserIds.luke_fb_user_ids},
        'mediums.preferredMedium': 'whatsapp',
        'mediums.whatsapp.phone': {'$in': phone_list}
    }, {'_id': 1, 'mediums': 1}))
    df = pd.DataFrame(list1)
    df.rename(columns={'_id': 'fbUserId'}, inplace=True)
    df['whatsappPhoneNumber'] = df['mediums'].apply(
        lambda x: x.get('whatsapp').get('phone') if (type(x) == dict) & (type(x.get('whatsapp')) == dict) else None)
    return df


def main_func():
    user_text_whatsapp = get_received_msgs_from_whatsapp(limit=100)
    users_whatsapp_df = get_whatsapp_users(user_text_whatsapp['whatsappPhoneNumber'].tolist())
    merged_df = pd.merge(users_whatsapp_df, user_text_whatsapp, how='inner', left_on='whatsappPhoneNumber',
                         right_on='whatsappPhoneNumber')
    merged_df['userLink'] = merged_df['fbUserId'].apply(
        lambda x: f'https://adooron.realfriend.ai/admin/users/{x}/request?collapsed=false')
    merged_df.sort_values(by='createdAt', ascending=False, inplace=True)
    merged_df.to_clipboard()
    print('hello')


