from datetime import datetime

import pandas as pd

from constants import lukeFbUserIds
from constants.importantDates import last_six_months
from constants.mongoConnectDooron import reply, app_webhooks, fb_users


def get_app_users():
    list1 = list(fb_users.find({
        '_id': {'$nin': lukeFbUserIds.luke_fb_user_ids},
        'createdAt': {'$gt': last_six_months},
        '$or': [{'mediums.preferredMedium': 'app'}, {'mediums.group.preferredMedium': 'app'}],
    }, {'_id': 1}))
    df = pd.DataFrame(list1)
    listOfFbUsers = df['_id'].tolist()
    return listOfFbUsers


def get_replies(user_list):
    list1 = list(reply.find({
        'chosen.kind': 'Manual',
        'fbUserId': {'$in': user_list},
        'createdAt': {'$gte': last_six_months},
    }).limit(100000))
    df = pd.DataFrame(list1)
    return df


def get_user_text(user_list):
    user_list_str = list(map(lambda user: str(user), user_list))
    start_time = datetime.now()
    list1 = list(app_webhooks.find({
        'text': {'$exists': True},
        'from': {'$in': user_list_str},
    }, {'_id': 0, 'text': 1, 'from': 1, 'createdAt': 1, 'intents': 1}))
    print(f'Time for query: {datetime.now() - start_time}')
    general_txt = pd.DataFrame(list1)
    if len(general_txt.index) > 0:
        general_txt['fbUserId'] = general_txt['from']
        general_txt.sort_values(by=['fbUserId', 'createdAt'], ascending=False, inplace=True)
        return general_txt.reset_index(drop=True)


def get_last_msgs(general_txt, row):
    fbUserId = row['fbUserId']
    manual_answer_time = row['createdAt']
    textBefore = row['userText']
    num = row['num']
    if num % 100 == 0:
        print(f'Finished {num}')
    user_txt_df = general_txt[general_txt['fbUserId'] == str(fbUserId)]
    user_txt_df = user_txt_df[user_txt_df['createdAt'] < manual_answer_time]
    if len(user_txt_df.index) > 1:
        user_txt_df.reset_index(inplace=True)
        user_txt_df = user_txt_df.head(1)
        text = user_txt_df.at[0, 'text']
        if text == textBefore:
            return {'intents': user_txt_df.at[0, 'intents'], 'text': text}


def get_intents_histogram(df):
    data = pd.get_dummies(df['intentsBefore'].apply(pd.Series).stack()).sum(level=0)
    data = data.T
    data['sum'] = data.apply(lambda row: row.sum(), axis=1)
    data.reset_index(inplace=True)
    data = data[['index', 'sum']]
    data.rename(columns={'index': 'intentsBefore', 'sum': 'count'}, inplace=True)
    data.sort_values(by='count', ignore_index=True, ascending=False, inplace=True)
    print(data)


replies_df = get_replies(get_app_users())
user_messages = get_user_text(replies_df['fbUserId'].tolist())
replies_df['num'] = replies_df.index
replies_df['messageBeforeIntentAndText'] = replies_df.apply(
    lambda row: get_last_msgs(user_messages, row), axis=1)
replies_df.dropna(subset=['messageBeforeIntentAndText'], inplace=True)
replies_df.reset_index(inplace=True, drop=True)
replies_df['intentBefore'] = replies_df['messageBeforeIntentAndText'].apply(lambda x: x.get('intents'))
replies_df['textBefore'] = replies_df['messageBeforeIntentAndText'].apply(lambda x: x.get('text'))
