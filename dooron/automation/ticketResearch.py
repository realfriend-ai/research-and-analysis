import pandas as pd
from bson import ObjectId

from constants.helpers import intersection
from constants.importantDates import last_week
from constants.intents import intents_for_research_dooron, set_request_related
from constants.mongoConnectDooron import users_collection, tickets_collection, fb_users, tyntec_webhooks
from dooron.automation.sqPercentageDailyMessages import find_sq_answers

run_conf = {'createFinalTicketDf': False, 'startTime': last_week, 'findPrevIntents': False}

pd.set_option('display.min_rows', 100)


def get_resolved_time_in_seconds(resolvedTime):
    if pd.isnull(resolvedTime) is not True:
        return resolvedTime.total_seconds()


def get_resolved_tickets_by_period(start_date):
    list1 = list(tickets_collection.find({
        'isAssigned': True,
        'isResolved': True,
        'createdAt': {'$gte': start_date}
    }, {'_id': 0, 'createdAt': 1, 'isResolved': 1, 'resolvedAt': 1, 'userId': 1, 'analystId': 1, 'notWaitingAt': 1}))
    ticketsDf = pd.DataFrame(list1)
    ticketsDf.rename(columns={'createdAt': 'ticketCreatedAt', 'userId': 'fbUserId', 'analystId': 'ticketingUserID'},
                     inplace=True)
    ticketsDf['fbUserId'] = ticketsDf['fbUserId'].apply(lambda x: ObjectId(x))
    ticketsDf['timeToResolveTicket'] = ticketsDf['notWaitingAt'] - ticketsDf['ticketCreatedAt']
    ticketsDf['timeToResolveTicket'] = ticketsDf['timeToResolveTicket'].apply(
        lambda x: get_resolved_time_in_seconds(x))
    ticketsDf.query('timeToResolveTicket > 0', inplace=True)
    return ticketsDf


def get_users_from_analyst_ids(analystIdList):
    list1 = list(users_collection.find({
        'ticketingUserID': {'$in': analystIdList},
    }, {'firstName': 1, 'lastName': 1, 'ticketingUserID': 1}))
    users_df = pd.DataFrame(list1)
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
    return fb_user_df


def get_tickets_df():
    if run_conf['createFinalTicketDf']:
        ticket_df = get_resolved_tickets_by_period(run_conf['startTime'])
        users_df = get_users_from_analyst_ids(ticket_df['ticketingUserID'].tolist())
        ticket_df_with_analyst_names = pd.merge(ticket_df, users_df, how='inner', left_on='ticketingUserID',
                                                right_on='ticketingUserID')
        fb_user_df = get_fb_users_per_ticket_opened(ticket_df_with_analyst_names['fbUserId'].tolist())
        ticket_df_final = pd.merge(ticket_df_with_analyst_names, fb_user_df, how='inner', left_on='fbUserId',
                                   right_on='fbUserId')
        ticket_df_final_whatsapp = ticket_df_final[ticket_df_final['preferredMedium'] == 'whatsapp']
        ticket_df_final_whatsapp['whatsappPhoneNumber'] = ticket_df_final_whatsapp['mediums'].apply(
            lambda x: x.get('whatsapp').get('phone') if (type(x) == dict) & (type(x.get('whatsapp')) == dict) else None)
        ticket_df_final_whatsapp.to_pickle('tickets_final_whatsapp.pkl')
    else:
        ticket_df_final_whatsapp = pd.read_pickle('tickets_final_whatsapp.pkl')
    return ticket_df_final_whatsapp


def get_time_describe_and_totals(tickets_df):
    describeOfTime = tickets_df['timeToResolveTicket'].describe(
        percentiles=[.05, .1, .15, .2, .25, .3, .35, .4, .45, .5, .55, .6, .65, .7, .75, .8, .85, .9, .95])
    ticket_df_with_user_names_no_outliers = tickets_df.query('timeToResolveTicket < 196')
    ticket_df_with_user_names_between_percentage_65_90 = tickets_df.query('16 < timeToResolveTicket < 74')
    print(ticket_df_with_user_names_between_percentage_65_90['timeToResolveTicket'].sum())
    print(ticket_df_with_user_names_no_outliers['timeToResolveTicket'].sum())


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


def get_intent_of_sq_and_fill_na(row):
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
        ticket_df['intentsBefore'] = ticket_df.apply(lambda row: get_intent_of_sq_and_fill_na(row), axis=1)
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
get_intents_histogram(wh_inc_intent_before)
wh_inc_intent_before_no_intent = wh_inc_intent_before[wh_inc_intent_before['intentsBefore'] == 'NO_INTENT']


def get_median_solving_time_per_ticket_type(intent, tickets_df_with_intents: pd.DataFrame):
    tickets_df_with_intents['hasIntent'] = tickets_df_with_intents['intentsBefore'].apply(
        lambda x: True if intent in x else False)
    tickets_df_with_intents_temp = tickets_df_with_intents[tickets_df_with_intents['hasIntent']]
    return {'intent': intent, 'numberOfTimesFound': len(tickets_df_with_intents_temp.index),
            'medianTimeToResolveTicket': tickets_df_with_intents_temp['timeToResolveTicket'].median(),
            '60': tickets_df_with_intents_temp['timeToResolveTicket'].quantile(0.6),
            '70': tickets_df_with_intents_temp['timeToResolveTicket'].quantile(0.7),
            '80': tickets_df_with_intents_temp['timeToResolveTicket'].quantile(0.8),
            '90': tickets_df_with_intents_temp['timeToResolveTicket'].quantile(0.9),
            }


res_df = []
found_set_stats = False
for intent in intents_for_research_dooron:
    if intent not in set_request_related:
        res_df.append(
            get_median_solving_time_per_ticket_type(intent=intent, tickets_df_with_intents=wh_inc_intent_before))
    elif found_set_stats is False:
        res_df.append(get_median_solving_time_per_ticket_type(intent='SET_REQUEST_RELATED',
                                                              tickets_df_with_intents=wh_inc_intent_before))
        found_set_stats = True

res_df = pd.DataFrame(res_df)
