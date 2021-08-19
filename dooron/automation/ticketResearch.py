import pandas as pd
from bson import ObjectId

from constants.mongoConnectDooron import users_collection, tickets_collection, fb_users
from getEngagementByUsers import get_preferred_medium_by_user_cleaning_groups
from importantDates import last_week


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
    fb_user_df['preferredMedium'] = fb_user_df['mediums'].apply(
        lambda x: get_preferred_medium_by_user_cleaning_groups(x))
    fb_user_df.rename(columns={'_id': 'fbUserId'}, inplace=True)
    return fb_user_df


ticket_df = get_resolved_tickets_by_period(last_week)
users_df = get_users_from_analyst_ids(ticket_df['ticketingUserID'].tolist())
ticket_df_with_user_names = pd.merge(ticket_df, users_df, how='inner', left_on='ticketingUserID',
                                     right_on='ticketingUserID')
fb_user_df = get_fb_users_per_ticket_opened(ticket_df_with_user_names['fbUserId'].tolist())


def get_time_describe_and_totals():
    describeOfTime = ticket_df_with_user_names['timeToResolveTicket'].describe(
        percentiles=[.05, .1, .15, .2, .25, .3, .35, .4, .45, .5, .55, .6, .65, .7, .75, .8, .85, .9, .95])
    ticket_df_with_user_names_no_outliers = ticket_df.query('timeToResolveTicket < 196')
    ticket_df_with_user_names_between_percentage_65_90 = ticket_df.query('16 < timeToResolveTicket < 74')
    print(ticket_df_with_user_names_between_percentage_65_90['timeToResolveTicket'].sum())
    print(ticket_df_with_user_names_no_outliers['timeToResolveTicket'].sum())
