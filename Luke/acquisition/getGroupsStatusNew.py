import pandas as pd

from constants.importantDates import last_month
from constants.lukeFbUserIds import luke_fb_user_ids
from constants.mongoConnectLuke import fb_users_collection, send_action_collection


def get_last_groups_users_of_luke(start):
    query = {
        'isAgent': {'$ne': True},
        'tags': {'$ne': 'ADMIN'},
        'requestsKind': 'APT_SALE',
        'kind': 'GROUP',
        '_id': {'$nin': luke_fb_user_ids},
        'createdAt': {'$gte': start},
    }
    projection = {'_id': 1, 'createdAt': 1, 'mediums': 1, 'requestsKind': 1, 'initialRequest': 1, 'tags': 1,
                  'status': 1}
    fbUsersList = list(fb_users_collection.find(query, projection))
    fbUserDf = pd.DataFrame(fbUsersList)
    print(fbUserDf['status'].value_counts())
    return fbUserDf


groups_df = get_last_groups_users_of_luke(start=last_month)
new_df = groups_df[groups_df['status'] == 'NEW']
not_new_df = groups_df[groups_df['status'] != 'NEW']


def get_properties_sent(df):
    sendList = list(send_action_collection.find({
        'fbUserId': {'$in': df['_id'].tolist()},
        'rfKind': {'$in': ['SEND_PROPERTIES', 'SEND_FEED']}}))
    print(f'Num of feed items found: {len(sendList)}')
    sendListDf = pd.DataFrame(sendList)
    return sendListDf
import pandas as pd

from constants.importantDates import last_month
from constants.lukeFbUserIds import luke_fb_user_ids
from constants.mongoConnectLuke import fb_users_collection, send_action_collection


def get_last_groups_users_of_luke(start):
    query = {
        'isAgent': {'$ne': True},
        'tags': {'$ne': 'ADMIN'},
        'requestsKind': 'APT_SALE',
        'kind': 'GROUP',
        '_id': {'$nin': luke_fb_user_ids},
        'createdAt': {'$gte': start},
    }
    projection = {'_id': 1, 'createdAt': 1, 'mediums': 1, 'requestsKind': 1, 'initialRequest': 1, 'tags': 1,
                  'status': 1}
    fbUsersList = list(fb_users_collection.find(query, projection))
    fbUserDf = pd.DataFrame(fbUsersList)
    print(fbUserDf['status'].value_counts())
    return fbUserDf


groups_df = get_last_groups_users_of_luke(start=last_month)
new_df = groups_df[groups_df['status'] == 'NEW']
not_new_df = groups_df[groups_df['status'] != 'NEW']


def get_properties_sent(df):
    sendList = list(send_action_collection.find({
        'fbUserId': {'$in': df['_id'].tolist()},
        'rfKind': {'$in': ['SEND_PROPERTIES', 'SEND_FEED']}}))
    print(f'Num of feed items found: {len(sendList)}')
    sendListDf = pd.DataFrame(sendList)
    return sendListDf


new_properties_Sent = get_properties_sent(new_df)
not_new_properties_Sent = get_properties_sent(not_new_df)
