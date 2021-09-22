import pandas as pd
from bson import ObjectId

from constants.helpers import get_intents_and_fill_na_luke
from constants.importantDates import last_six_months
from constants.mongoConnectLuke import app_webhoooks, mutation_logger, fb_users_collection
from displaySetting import display_settings

run_conf = {'startTime': last_six_months}
display_settings()


def get_app_webhooks(limit):
    list1 = list(app_webhoooks.find({
        'createdAt': {'$gte': run_conf['startTime']}}
        , {'text': 1, 'sentByUs': 1, 'to': 1, 'from': 1, 'intents': 1, 'createdAt': 1,
           'entities': 1}).limit(limit))
    app_df = pd.DataFrame(list1)
    app_df['groupFbUserId'] = app_df['to']
    app_df.sort_values(by=['groupFbUserId', 'createdAt'], ignore_index=True, inplace=True)
    app_df.dropna(subset=['groupFbUserId'], inplace=True)
    return app_df


def filter_admins(app_webhook_df):
    user_list = app_webhook_df['groupFbUserId'].tolist()
    user_list_object_ids = list(map(lambda x: ObjectId(x), user_list))
    print(len(app_webhook_df.index))
    admin_users_list = list(fb_users_collection.find({
        '_id': {'$in': user_list_object_ids},
        'tags': 'ADMIN'
    }))
    admin_users_ids = list(map(lambda x: str(x.get('_id')), admin_users_list))
    app_webhook_df['isAdmin'] = app_webhook_df['groupFbUserId'].apply(lambda x: True if x in admin_users_ids else False)
    app_webhook_df = app_webhook_df[app_webhook_df['isAdmin'] == False]
    app_webhook_df.drop(columns=['isAdmin'], inplace=True)
    print(len(app_webhook_df.index))
    return app_webhook_df


def get_mutation_logger_df(fb_user_id):
    list1 = list(mutation_logger.find({
        'mutationName': {'$in': ['sendMessageToUser', 'sendPropertiesBatchToFBUser']},
        'createdAt': {'$gte': run_conf['startTime']},
        'params.fbUserId': {'$in': fb_user_id}}
        , {'_id': 0, 'params': 1, 'mutationName': 1}))
    mut_df = pd.DataFrame(list1)
    mut_df['text'] = mut_df['params'].apply(lambda x: x.get('text'))
    mut_df['groupFbUserId'] = mut_df['params'].apply(lambda x: x.get('fbUserId'))
    mut_df.drop(columns=['params'], inplace=True)
    return mut_df


def is_auto_sent(row):
    if row.get('sentByUs') is True:
        if pd.isna(row.get('mutationName')) is True:
            return True
        else:
            return False


def get_last_message_data(sent_by_us_row, sent_by_user_df):
    groupFbUserId = sent_by_us_row.get('groupFbUserId')
    messageSentAt = sent_by_us_row.get('createdAt')
    sent_by_user_df_temp = sent_by_user_df[sent_by_user_df['groupFbUserId'] == groupFbUserId]
    sent_by_user_df_temp = sent_by_user_df_temp[sent_by_user_df_temp['createdAt'] < messageSentAt]
    if len(sent_by_user_df_temp.index) > 0:
        sent_by_user_df_temp.reset_index(drop=True, inplace=True)
        return {'intentsBefore': sent_by_user_df_temp.at[0, 'intents'],
                'textBefore': sent_by_user_df_temp.at[0, 'text'],
                'messageBeforeReceivedAt': sent_by_user_df_temp.at[0, 'createdAt']}
    return {'intentsBefore': None,
            'textBefore': None,
            'messageBeforeReceivedAt': None}


def is_auto_initiated_by_us(row):
    if row.get('isAuto') is True:
        messageBeforeReceivedAt = row.get('messageBeforeReceivedAt')
        systemMessageSentAt = row.get('createdAt')
        if pd.isna(messageBeforeReceivedAt) is False:
            time_delta = (systemMessageSentAt - messageBeforeReceivedAt).total_seconds()
            if time_delta > 60:
                return True
        if pd.isna(messageBeforeReceivedAt):
            return True
    return False


def get_previous_messages_data(sent_by_us_df, sent_by_user_df):
    sent_by_us_df['prevMessageData'] = sent_by_us_df.apply(lambda row: get_last_message_data(row, sent_by_user_df),
                                                           axis=1)
    sent_by_us_df['intentsBefore'] = sent_by_us_df['prevMessageData'].apply(lambda x: x.get('intentsBefore'))
    sent_by_us_df['textBefore'] = sent_by_us_df['prevMessageData'].apply(lambda x: x.get('textBefore'))
    sent_by_us_df['messageBeforeReceivedAt'] = sent_by_us_df['prevMessageData'].apply(
        lambda x: x.get('messageBeforeReceivedAt'))


def main_func():
    app_df = get_app_webhooks(100000)
    app_df = filter_admins(app_df)
    mut_df = get_mutation_logger_df(app_df['groupFbUserId'].tolist())
    # performed a left join to find mutation loggers when we have those then they will be considered as manual
    merged_df = pd.merge(app_df, mut_df, how='left', left_on=['text', 'groupFbUserId'],
                         right_on=['text', 'groupFbUserId'])
    merged_df['isAuto'] = merged_df.apply(lambda row: is_auto_sent(row), axis=1)
    merged_df.sort_values(by=['groupFbUserId', 'createdAt'], ascending=False, ignore_index=True)
    sent_by_us_df = merged_df[merged_df['sentByUs'] == True]
    print(f'Num of total auto messages:\n'
          f'{sent_by_us_df["isAuto"].value_counts()}\n'
          f'{sent_by_us_df["isAuto"].value_counts(normalize=True)}'
          f'\nfor {sent_by_us_df["to"].nunique()} users')
    sent_by_user_df = merged_df[merged_df['sentByUs'] != True]
    get_previous_messages_data(sent_by_us_df, sent_by_user_df)
    sent_by_us_df['isAutoInitiatedByUs'] = sent_by_us_df.apply(lambda row: is_auto_initiated_by_us(row), axis=1)
    sent_by_us_df = sent_by_us_df[sent_by_us_df['isAutoInitiatedByUs'] == False]
    print(f'Num of total auto messages not initiated by us:\n'
          f'{sent_by_us_df["isAuto"].value_counts()}\n'
          f'{sent_by_us_df["isAuto"].value_counts(normalize=True)}'
          f'\nfor {sent_by_us_df["to"].nunique()} users')
    sent_by_us_df['intentsBeforeCat'] = sent_by_us_df.apply(lambda row: get_intents_and_fill_na_luke(row), axis=1)
    return sent_by_us_df


sent_by_us_df = main_func()


def get_percentage_of_auto_per_group(sent_by_us_df, intent):
    sent_by_us_df_temp = sent_by_us_df[sent_by_us_df['intentsBeforeCat'] == intent]
    number_of_intents_found = len(sent_by_us_df_temp)
    auto_merged_df_temp = sent_by_us_df_temp[sent_by_us_df_temp['isAuto'] == True]
    number_of_auto_answered = len(auto_merged_df_temp)
    return {'intent': intent,
            'numberOfTimesInTimeFrame': number_of_intents_found,
            'autoCount': number_of_auto_answered,
            'autoPercentage': number_of_auto_answered / number_of_intents_found}


intents_df = []
for intnet in sent_by_us_df['intentsBeforeCat'].unique():
    intents_df.append(get_percentage_of_auto_per_group(sent_by_us_df, intnet))

intents_df = pd.DataFrame(intents_df)
