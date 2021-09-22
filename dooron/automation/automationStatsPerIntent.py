from datetime import timedelta

import pandas as pd

from constants.helpers import get_intents_and_fill_na
from constants.importantDates import last_week
from constants.mongoConnectDooron import tyntec_webhooks, users_collection

run_conf = {'startTime': last_week, 'recreateData': True}

dooron_whatsapp_numbers = ['972542324236', '972502020220', 972542324236, 972502020220]


def get_sent_msgs_from_whatsapp(limit):
    list1 = list(tyntec_webhooks.find(
        {'from': {'$in': dooron_whatsapp_numbers},
         'createdAt': {'$gte': run_conf['startTime']}}
        , {'createdAt': 1, 'to': 1, 'content': 1, 'MetaData': 1}).limit(limit))
    whDf = pd.DataFrame(list1)
    whDf['analystUserId'] = whDf['MetaData'].apply(lambda x: x.get('adminUserId') if type(x) == dict else None)
    whDf.rename(columns={'to': 'whatsappPhoneNumber', '_id': 'webhookId', 'createdAt': 'systemMessageSentAt'},
                inplace=True)
    return whDf


def get_users_from_analyst_ids(analyst_id_list):
    analyst_id_list = list(filter(lambda x: pd.isna(x) is False, analyst_id_list))
    list1 = list(users_collection.find({
        '_id': {'$in': analyst_id_list},
    }, {'firstName': 1, 'lastName': 1, 'ticketingUserID': 1}))
    users_df = pd.DataFrame(list1)
    users_df.rename(columns={'_id': 'analystUserId'}, inplace=True)
    return users_df


def get_received_msgs_from_whatsapp_from_users(tickets_phone_numbers):
    list1 = list(tyntec_webhooks.find({
        'createdAt': {'$gte': run_conf['startTime'] - timedelta(days=30)},
        'from': {'$in': tickets_phone_numbers},
        'content.contentType': 'text',
    }, {'createdAt': 1, 'from': 1, 'content': 1, 'intents': 1, 'entities': 1}))
    whDf = pd.DataFrame(list1)
    whDf['text'] = whDf['content'].apply(lambda x: x.get('text') if type(x) == dict else None)
    whDf.dropna(subset=['text'], inplace=True)
    whDf.rename(columns={'from': 'whatsappPhoneNumber', '_id': 'webhookId', 'createdAt': 'messageReceivedAt'},
                inplace=True)
    whDf.sort_values(by=['whatsappPhoneNumber', 'messageReceivedAt'], ascending=False, inplace=True, ignore_index=True)
    whDf.drop(columns='content', inplace=True)
    return whDf


def get_last_message_before_ticket(row, whatsapp_df):
    whatsapp_phone_num = row.get('whatsappPhoneNumber')
    sent_msg_time = row.get('systemMessageSentAt')
    whatsapp_df_temp = whatsapp_df[whatsapp_df['whatsappPhoneNumber'] == whatsapp_phone_num]
    whatsapp_df_temp = whatsapp_df_temp[whatsapp_df_temp['messageReceivedAt'] < sent_msg_time]
    whatsapp_df_temp.reset_index(inplace=True)
    if len(whatsapp_df_temp.index) > 0:
        return {'intentsBefore': whatsapp_df_temp.at[0, 'intents'],
                'textBefore': whatsapp_df_temp.at[0, 'text'],
                'messageBeforeReceivedAt': whatsapp_df_temp.at[0, 'messageReceivedAt']}
    return None


def get_messages_before_sent_by_system(merged_df):
    wh_df_from_user = get_received_msgs_from_whatsapp_from_users(merged_df['whatsappPhoneNumber'].tolist())
    merged_df['messageBeforeData'] = merged_df.apply(lambda row: get_last_message_before_ticket(row, wh_df_from_user),
                                                     axis=1)
    merged_df.dropna(subset=['messageBeforeData'], inplace=True)
    print(merged_df['isAuto'].value_counts())

    merged_df['intentsBefore'] = merged_df['messageBeforeData'].apply(lambda x: x.get('intentsBefore'))
    merged_df['textBefore'] = merged_df['messageBeforeData'].apply(lambda x: x.get('textBefore'))
    merged_df['messageBeforeReceivedAt'] = merged_df['messageBeforeData'].apply(
        lambda x: x.get('messageBeforeReceivedAt'))


def is_auto_initiated_by_us(row):
    if row.get('isAuto') is True:
        messageBeforeReceivedAt = row.get('messageBeforeReceivedAt')
        systemMessageSentAt = row.get('systemMessageSentAt')
        if (messageBeforeReceivedAt is not None) & (messageBeforeReceivedAt is not None):
            time_delta = (systemMessageSentAt - messageBeforeReceivedAt).total_seconds()
            if time_delta > 60:
                return True
    return False


def main_func():
    if run_conf['recreateData']:
        wh_df_from_system = get_sent_msgs_from_whatsapp(limit=1000000)
        admin_user_df = get_users_from_analyst_ids(wh_df_from_system['analystUserId'].tolist())
        # with left join we can find places where there's no analyst id which means auto
        merged_df = pd.merge(wh_df_from_system, admin_user_df, how='left', right_on='analystUserId',
                             left_on='analystUserId')
        merged_df['isAuto'] = merged_df['ticketingUserID'].apply(lambda x: True if pd.isna(x) is True else False)
        print(
            f'Total Messages sent in time frame:'
            f'\n{merged_df["isAuto"].value_counts()}'
            f'\n{merged_df["isAuto"].value_counts(normalize=True)}ֿֿֿ'
            f'\nTo {merged_df["whatsappPhoneNumber"].nunique()} users')
        get_messages_before_sent_by_system(merged_df)
        merged_df['isAutoInitiatedByUs'] = merged_df.apply(lambda row: is_auto_initiated_by_us(row), axis=1)
        merged_df = merged_df[merged_df['isAutoInitiatedByUs'] != True]
        merged_df.to_pickle('sentBySystemStatsDf.pkl')
        print(
            f'Total Messages not initiated by Dooron sent in time frame:'
            f'\n{merged_df["isAuto"].value_counts()}'
            f'\n{merged_df["isAuto"].value_counts(normalize=True)}'
            f'\nTo {merged_df["whatsappPhoneNumber"].nunique()} users')
        merged_df.to_pickle('./whatsappLastWeekAutoVsMan.pkl')
    else:
        merged_df = pd.read_pickle('./whatsappLastWeekAutoVsMan.pkl')
    return merged_df


merged_df = main_func()
merged_df['intentsBeforeCat'] = merged_df.apply(lambda row: get_intents_and_fill_na(row), axis=1)
merged_df.to_pickle('./whatsappLastWeekAutoVsMan.pkl')


def get_percentage_of_auto_per_group(merged_df, intent):
    merged_df_temp = merged_df[merged_df['intentsBeforeCat'] == intent]
    number_of_intents_found = len(merged_df_temp)
    auto_merged_df_temp = merged_df_temp[merged_df_temp['isAuto'] == 1]
    number_of_auto_answered = len(auto_merged_df_temp)
    return {'intent': intent,
            'numberOfTimesInTimeFrame': number_of_intents_found,
            'autoCount': number_of_auto_answered,
            'autoPercentage': number_of_auto_answered / number_of_intents_found}


intents_df = []
for intnet in merged_df['intentsBeforeCat'].unique():
    intents_df.append(get_percentage_of_auto_per_group(merged_df, intnet))

intents_df = pd.DataFrame(intents_df)
