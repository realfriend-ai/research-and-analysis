import pandas as pd
from bson import ObjectId

from constants.helpers import intersection, remove_outliers_per_column
from constants.importantDates import last_week
from constants.intents import set_request_related, intents_for_research_dooron
from constants.mongoConnectDooron import users_collection, tickets_collection, fb_users, mutationLogger_collection, \
    tyntec_webhooks, feed
from dooron.automation.sqPercentageDailyMessages import find_sq_answers

run_conf = {'createFinalTicketDf': False, 'startTime': last_week, 'findPrevIntents': False,
            'getFirstTimeSentProperties': False}

pd.set_option('display.min_rows', 100)
seen_by_admin = 'updateLastSeenByAdmin'


def get_resolved_time_in_seconds(resolvedTime):
    if pd.isnull(resolvedTime) is not True:
        return resolvedTime.total_seconds()


def get_resolved_tickets_by_period(start_date):
    list1 = list(tickets_collection.find({
        'isAssigned': True,
        'isResolved': True,
        'createdAt': {'$gte': start_date}
    }, {'_id': 0, 'createdAt': 1, 'resolvedAt': 1, 'userId': 1, 'analystId': 1}))
    ticketsDf = pd.DataFrame(list1)
    ticketsDf.rename(columns={'createdAt': 'ticketCreatedAt', 'userId': 'fbUserId', 'analystId': 'ticketingUserID'},
                     inplace=True)
    ticketsDf['fbUserId'] = ticketsDf['fbUserId'].apply(lambda x: ObjectId(x))
    return ticketsDf


def get_users_from_analyst_ids(analyst_id_list):
    list1 = list(users_collection.find({
        'ticketingUserID': {'$in': analyst_id_list},
    }, {'firstName': 1, 'lastName': 1, 'ticketingUserID': 1}))
    users_df = pd.DataFrame(list1)
    users_df.rename(columns={'_id': 'analystUserId'}, inplace=True)
    return users_df


def get_fb_users_per_ticket_opened(fb_user_id_list):
    list1 = list(fb_users.find({
        '_id': {'$in': fb_user_id_list},
    }, {'_id': 1, 'mediums': 1}))
    fb_user_df = pd.DataFrame(list1)
    fb_user_df.dropna(subset=['mediums'], inplace=True)
    fb_user_df['preferredMedium'] = fb_user_df['mediums'].apply(
        lambda x: x.get('preferredMedium'))
    fb_user_df.rename(columns={'_id': 'fbUserId'}, inplace=True)
    fb_user_df = fb_user_df[fb_user_df['preferredMedium'] == 'whatsapp']
    fb_user_df['whatsappPhoneNumber'] = fb_user_df['mediums'].apply(
        lambda x: x.get('whatsapp').get('phone') if (type(x) == dict) & (type(x.get('whatsapp')) == dict) else None)
    fb_user_df.drop(columns=['mediums'], inplace=True)
    return fb_user_df


def try_Object_id(fb_user_id):
    try:
        fb_user_obj = ObjectId(fb_user_id)
        return fb_user_obj
    except:
        return None


def get_mutation_df(fb_user_id_list):
    fb_user_id_list_str = list(map(lambda x: str(x), fb_user_id_list))
    list1 = list(mutationLogger_collection.find({
        'mutationName': 'updateLastSeenByAdmin',
        'params.fbUserId': {'$in': fb_user_id_list_str},
        'createdAt': {'$gte': run_conf['startTime']}
    }, {'params': 1, 'userId': 1, 'createdAt': 1}))
    mutDf = pd.DataFrame(list1)
    mutDf['fbUserId'] = mutDf['params'].apply(lambda x: try_Object_id(x['fbUserId']) if 'fbUserId' in x else None)
    mutDf.dropna(subset=['fbUserId'], inplace=True)
    mutDf.rename(columns={'userId': 'analystUserId', 'createdAt': 'seenByAdminAt'}, inplace=True)
    mutDf.sort_values(by=['analystUserId', 'fbUserId', 'seenByAdminAt'], inplace=True)
    return mutDf


def get_analyst_seen_time_per_ticket(mut_df, row):
    fbUserId = row.get('fbUserId')
    analystUserId = row.get('analystUserId')
    ticketCreatedAt = row.get('ticketCreatedAt')
    mut_df_temp = mut_df[(mut_df['analystUserId'] == analystUserId) & (mut_df['fbUserId'] == fbUserId)]
    mut_df_temp = mut_df_temp[mut_df_temp['seenByAdminAt'] > ticketCreatedAt]
    mut_df_temp.reset_index(inplace=True, drop=True)
    if len(mut_df_temp.index) > 0:
        return mut_df_temp.at[0, 'seenByAdminAt']
    else:
        return None


def get_tickets_df():
    if run_conf['createFinalTicketDf']:
        ticket_df = get_resolved_tickets_by_period(run_conf['startTime'])
        users_df = get_users_from_analyst_ids(ticket_df['ticketingUserID'].tolist())
        ticket_df_with_analyst_names = pd.merge(ticket_df, users_df, how='inner', left_on='ticketingUserID',
                                                right_on='ticketingUserID')
        fb_user_df = get_fb_users_per_ticket_opened(ticket_df_with_analyst_names['fbUserId'].tolist())
        ticket_df_final_whatsapp = pd.merge(ticket_df_with_analyst_names, fb_user_df, how='inner', left_on='fbUserId',
                                            right_on='fbUserId')
        mutation_df = get_mutation_df(ticket_df_final_whatsapp['fbUserId'].tolist())
        ticket_df_final_whatsapp['seenByAdminAt'] = ticket_df_final_whatsapp.apply(
            lambda row: get_analyst_seen_time_per_ticket(mutation_df, row), axis=1)
        ticket_df_final_whatsapp.dropna(subset=['seenByAdminAt'], inplace=True)
        ticket_df_final_whatsapp['timeToResolveTicket'] = ticket_df_final_whatsapp['resolvedAt'] - \
                                                          ticket_df_final_whatsapp['seenByAdminAt']
        ticket_df_final_whatsapp['timeToResolveTicketSeconds'] = ticket_df_final_whatsapp['timeToResolveTicket'].apply(
            lambda x: get_resolved_time_in_seconds(x))
        ticket_df_final_whatsapp.to_pickle('tickets_final_whatsapp.pkl')
    else:
        ticket_df_final_whatsapp = pd.read_pickle('tickets_final_whatsapp.pkl')
    return ticket_df_final_whatsapp


ticket_df_final_whatsapp = get_tickets_df()


def get_time_describe_and_totals(tickets_df):
    describeOfTime = tickets_df['timeToResolveTicketSeconds'].describe(
        percentiles=[.05, .1, .15, .2, .25, .3, .35, .4, .45, .5, .55, .6, .65, .7, .75, .8, .85, .9, .95])
    describeOfTime.to_clipboard()


# get_time_describe_and_totals(ticket_df_final_whatsapp)
def get_received_msgs_from_whatsapp_from_users(tickets_phone_numbers):
    list1 = list(tyntec_webhooks.find({
        'createdAt': {'$gte': run_conf['startTime']},
        'from': {'$in': tickets_phone_numbers},
        'content.contentType': 'text',
    }, {'createdAt': 1, 'from': 1, 'content': 1, 'intents': 1, 'entities': 1}))
    whDf = pd.DataFrame(list1)
    whDf['text'] = whDf['content'].apply(lambda x: x.get('text') if type(x) == dict else None)
    whDf.dropna(subset=['text'], inplace=True)
    whDf.rename(columns={'from': 'whatsappPhoneNumber', '_id': 'webhookId', 'createdAt': 'messageReceivedAt'},
                inplace=True)
    whDf.sort_values(by=['whatsappPhoneNumber', 'messageReceivedAt'], ascending=False, inplace=True, ignore_index=True)
    return whDf


def get_last_message_before_ticket(row, whatsapp_df):
    whatsapp_phone_num = row.get('whatsappPhoneNumber')
    ticket_open_time = row.get('ticketCreatedAt')
    whatsapp_df_temp = whatsapp_df[whatsapp_df['whatsappPhoneNumber'] == whatsapp_phone_num]
    whatsapp_df_temp = whatsapp_df_temp[whatsapp_df_temp['messageReceivedAt'] < ticket_open_time]
    whatsapp_df_temp.reset_index(inplace=True)
    if len(whatsapp_df_temp.index) > 0:
        return {'textBefore': whatsapp_df_temp.at[0, 'text'], 'intentsBefore': whatsapp_df_temp.at[0, 'intents']}
    return None


def get_intents_and_fill_na(row):
    intents_before = row.get('intentsBefore')
    if type(intents_before) == list:
        if len(intents_before) > 0:
            if intersection(intents_before, set_request_related):
                return 'SET_REQUEST_RELATED'
            else:
                return intents_before
        else:
            return 'NO_INTENT'
    elif find_sq_answers(row.get('textBefore')):
        return 'SQ'
    else:
        return 'NO_INTENT'


def get_tickets_df_with_intent_before(ticket_df):
    if run_conf['findPrevIntents']:
        wh_df = get_received_msgs_from_whatsapp_from_users(ticket_df['whatsappPhoneNumber'].tolist())
        ticket_df['msgBefore'] = ticket_df.apply(lambda row: get_last_message_before_ticket(row, wh_df),
                                                 axis=1)
        ticket_df.dropna(subset=['msgBefore'], inplace=True)
        ticket_df['textBefore'] = ticket_df['msgBefore'].apply(lambda x: x.get('textBefore'))
        ticket_df['intentsBefore'] = ticket_df['msgBefore'].apply(lambda x: x.get('intentsBefore'))
        ticket_df['intentsBeforeCat'] = ticket_df.apply(lambda row: get_intents_and_fill_na(row), axis=1)
        ticket_df.to_pickle('tickets_final_whatsapp_w_intent_before.pkl')
    else:
        ticket_df = pd.read_pickle('tickets_final_whatsapp_w_intent_before.pkl')
    return ticket_df


def get_intents_histogram(df):
    data = pd.get_dummies(df['intentsBefore'].apply(pd.Series).stack()).sum(level=0)
    data = data.T
    data['sum'] = data.apply(lambda row: row.sum(), axis=1)
    data.reset_index(inplace=True)
    data = data[['index', 'sum']]
    data.rename(columns={'index': 'intent', 'sum': 'count'}, inplace=True)
    data.sort_values(by='count', ignore_index=True, ascending=False, inplace=True)
    print(data['intent'].tolist())
    data.to_clipboard()


ticket_df_final = get_tickets_df()
wh_inc_intent_before = get_tickets_df_with_intent_before(ticket_df_final)
wh_inc_intent_before_no_intent = wh_inc_intent_before[wh_inc_intent_before['intentsBeforeCat'] == 'NO_INTENT']


def get_solving_time_per_ticket_type(intent, tickets_df_with_intents: pd.DataFrame):
    tickets_df_with_intents['hasIntent'] = tickets_df_with_intents['intentsBeforeCat'].apply(
        lambda x: True if intent in x else False)
    tickets_df_with_intents_temp = tickets_df_with_intents[tickets_df_with_intents['hasIntent']]
    tickets_df_with_intents_temp = remove_outliers_per_column(threshold=0.05, df=tickets_df_with_intents_temp,
                                                              col='timeToResolveTicketSeconds')
    return {'intent': intent, 'numberOfTimesFound': len(tickets_df_with_intents_temp.index),
            'mediantimeToResolveTicket': tickets_df_with_intents_temp['timeToResolveTicketSeconds'].median(),
            '60': tickets_df_with_intents_temp['timeToResolveTicketSeconds'].quantile(0.6),
            '70': tickets_df_with_intents_temp['timeToResolveTicketSeconds'].quantile(0.7),
            '80': tickets_df_with_intents_temp['timeToResolveTicketSeconds'].quantile(0.8),
            '90': tickets_df_with_intents_temp['timeToResolveTicketSeconds'].quantile(0.9),
            'totalResolvedTimeInHours': tickets_df_with_intents_temp['timeToResolveTicketSeconds'].sum() / 3600
            }


def get_ticket_times_per_intent():
    res_df = []
    found_set_request_stats = False
    for intent in intents_for_research_dooron:
        if intent not in set_request_related:
            res_df.append(
                get_solving_time_per_ticket_type(intent=intent, tickets_df_with_intents=wh_inc_intent_before))
        elif found_set_request_stats is False:
            res_df.append(get_solving_time_per_ticket_type(intent='SET_REQUEST_RELATED',
                                                           tickets_df_with_intents=wh_inc_intent_before))
            found_set_request_stats = True

    res_df = pd.DataFrame(res_df)
    return res_df


tickets_time_per_intent = get_ticket_times_per_intent()


# def get_val_counts_of_intents_over_one_minute(ticket_df):
#     ticket_df = remove_outliers_per_column(threshold=0.05, df=ticket_df, col='timeToResolveTicket')
#     sum_of_time_all_tickets = ticket_df['timeToResolveTicket'].sum()
#     ticket_df_one_minute = ticket_df[ticket_df['timeToResolveTicket'] > 60]
#     print(ticket_df_one_minute['timeToResolveTicket'].sum() / sum_of_time_all_tickets)
#     print(len(ticket_df_one_minute.index) / len(ticket_df.index))
#     get_intents_histogram(ticket_df_one_minute)
#     return ticket_df_one_minute
#
#
def get_first_time_sent_properties_to_user(ticket_df):
    if run_conf['getFirstTimeSentProperties']:
        fb_user_id_list = ticket_df['fbUserId'].unique().tolist()
        i = 0
        users_feed_list = []
        while i < len(fb_user_id_list):
            list1 = list(feed.find({
                'fbUserId': {'$in': fb_user_id_list[i:i + 20]},
            }, {'createdAt': 1, 'fbUserId': 1}))
            users_feed_list = users_feed_list + list1
            i += 20
            print(f'Finished {i} out of {len(fb_user_id_list)}')
        sent_first_df = pd.DataFrame(users_feed_list)
        sent_first_df.sort_values(by=['fbUserId', 'createdAt'], ascending=True, inplace=True, ignore_index=True)
        sent_first_df.rename(columns={'createdAt': 'firstPropertySentAt'}, inplace=True)
        sent_first_df.drop_duplicates(subset=['fbUserId'], keep='first', inplace=True, ignore_index=True)
        tickets_df_with_first_sent_time = pd.merge(ticket_df, sent_first_df, how='inner', left_on='fbUserId',
                                                   right_on='fbUserId')
        tickets_df_with_first_sent_time.to_pickle('tickets_final_whatsapp_w_intent_before_and_first_sent.pkl')
    else:
        tickets_df_with_first_sent_time = pd.read_pickle('tickets_final_whatsapp_w_intent_before_and_first_sent.pkl')
    return tickets_df_with_first_sent_time
#
#
# wh_inc_intent_before_and_first_send_time = get_first_time_sent_properties_to_user(wh_inc_intent_before)
#
#
# def get_val_counts_of_intents_of_initial_dialog(ticket_df):
#     ticket_df = remove_outliers_per_column(threshold=0.05, df=ticket_df, col='timeToResolveTicket')
#     sum_of_time_all_tickets = ticket_df['timeToResolveTicket'].sum()
#     initial_dialog_df = ticket_df[ticket_df['ticketCreatedAt'] < ticket_df['firstPropertySentAt']]
#     print(initial_dialog_df['timeToResolveTicket'].sum() / sum_of_time_all_tickets)
#     print(len(initial_dialog_df.index) / len(ticket_df.index))
#     # get_intents_histogram(ticket_df_one_minute)
#     return initial_dialog_df
#
#
# initial_dialog_df = get_val_counts_of_intents_of_initial_dialog(wh_inc_intent_before_and_first_send_time)
#
#
# def check_main_patterns_in_dooron_no_intent_rec(dooron_no_intent):
#     dooron_no_intent['hasNewReq'] = dooron_no_intent['textBefore'].apply(lambda x: True if 'חדש' in x else False)
#     dooron_no_intent['hasStopReq'] = dooron_no_intent['textBefore'].apply(lambda x: True if 'פסק' in x else False)
#     dooron_no_intent_new_req = dooron_no_intent[dooron_no_intent['hasNewReq']]
#
#     print(
#         f'len df size: {len(dooron_no_intent_new_req.index)}, solving time: {dooron_no_intent_new_req["timeToResolveTicket"].sum()}')
#     dooron_no_intent_stop_req = dooron_no_intent[dooron_no_intent['hasStopReq']]
#     print(
#         f'len df size: {len(dooron_no_intent_stop_req.index)}, solving time: {dooron_no_intent_stop_req["timeToResolveTicket"].sum()}')
#     return dooron_no_intent_new_req, dooron_no_intent_stop_req
#
#
# new_req, stop_req = check_main_patterns_in_dooron_no_intent_rec(wh_inc_intent_before_no_intent)
