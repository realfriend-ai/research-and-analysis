from datetime import datetime

import pandas as pd

from Luke.acquisition.getGroupesToIgnore import groups_created_between_dates
from Luke.acquisition.getNumOfUsersCreatedByMediumByPeriod import get_users_created_by_medium_and_date, \
    get_preferred_medium_by_user_cleaning_groups
from Luke.acquisition.mergeGroupAndFbUserForActions import map_group_id_id_to_his_origin_fb_user_id
from constants.importantDates import first_of_nov
from constants.mongoConnectLuke import feed_item_event_collection, fb_users_collection


def get_lead_sent_from_users(buyer_user_list):
    send_lead_list = list(feed_item_event_collection.find(
        {
            'fbUserId': {'$in': buyer_user_list},
            'eventKind': 'SEND_LEAD_FORM'
        }, {'fbUserId': 1, 'createdAt': 1}
    ))
    sendLeadDf = pd.DataFrame(send_lead_list)
    sendLeadDf.rename(columns={'createdAt': 'lead_send_at'}, inplace=True)
    return sendLeadDf


def get_lead_per_day_in_service(fb_user_df, send_lead_df):
    merged_df = pd.merge(fb_user_df, send_lead_df, how='inner', left_on='fbUserId', right_on='fbUserId')
    merged_df['deltaUntilLeadSent'] = merged_df.apply(
        lambda row: row['lead_send_at'].date() - row['userCreatedAt'].date(), axis=1)
    merged_df['dayOfLeadSent'] = merged_df['deltaUntilLeadSent'].apply(lambda x: x.days + 1)
    merged_df['count'] = 1
    return merged_df


def get_first_sent_lead_per_user(merged_df):
    merged_df.sort_values(by=['fbUserId', 'lead_send_at'], ignore_index=True)
    only_first_df = merged_df.drop_duplicates(subset=['fbUserId'], keep='first')
    return only_first_df


def get_sent_lead_stats_by_date_and_medium(start, end, medium):
    """Summary: get lead sent stats of users between dates in specific medium

    Parameters:
         start (date): the beginning date -  we want to find user created after
         end (date): the final date - we want to find users created before
         medium (str): indicates which medium stats we would like

    """
    users_created_by_dates = get_users_created_by_medium_and_date(start, end, only_user_did_pw=True,
                                                                  for_action=True)
    users_created_by_dates.rename(columns={'_id': 'fbUserId', 'createdAt': 'userCreatedAt'}, inplace=True)
    medium_users_df = users_created_by_dates[users_created_by_dates['preferredMedium'] == medium]
    user_list = medium_users_df['fbUserId'].tolist()
    send_lead_df = get_lead_sent_from_users(user_list)
    groups_df = groups_created_between_dates(start, end)
    send_lead_df['fbUserId'] = send_lead_df['fbUserId'].apply(
        lambda x: map_group_id_id_to_his_origin_fb_user_id(x, groups_df))
    send_lead_by_day_df = get_lead_per_day_in_service(medium_users_df, send_lead_df)
    only_first_lead_df = get_first_sent_lead_per_user(send_lead_by_day_df)
    print(only_first_lead_df['dayOfLeadSent'].describe(percentiles=[.1, .2, .3, .4, .5, .6, .7, .8, .9]))
    return only_first_lead_df


# imsg_engagement = get_sent_lead_stats_by_date_and_medium(start=first_of_nov, end=datetime.now(), medium='imessage')


def is_agent_in_members(member_list):
    for member in member_list:
        if member.get('role') == 'AGENT':
            return True
    return False


def get_origin_fb_user_id(member_list):
    for member in member_list:
        if member.get('role') == 'ADMIN':
            return member.get('fbUserId')


def get_origin_fb_users(user_list):
    query = {
        'requestsKind': 'APT_SALE',
        '_id': {'$in': user_list},
    }
    projection = {'_id': 1, 'createdAt': 1, 'mediums': 1, 'requestsKind': 1, 'initialRequest': 1}
    fbUsersList = list(fb_users_collection.find(query, projection))
    fbUserDf = pd.DataFrame(fbUsersList)
    fbUserDf.rename(columns={'createdAt': 'userCreatedAt', '_id': 'fbUserId'}, inplace=True)
    fbUserDf.dropna(subset=['mediums'], inplace=True)
    fbUserDf['preferredMedium'] = fbUserDf['mediums'].apply(lambda x: get_preferred_medium_by_user_cleaning_groups(x))
    fbUserDf = fbUserDf[fbUserDf['preferredMedium'] == 'app']
    return fbUserDf


def get_sent_lead_for_app_users(start, end):
    groups_df = groups_created_between_dates(start, end)
    groups_df['hasAgentInMembers'] = groups_df['members'].apply(lambda x: is_agent_in_members(x))
    leads_df = groups_df[groups_df['hasAgentInMembers']]
    leads_df.rename(columns={'createdAt': 'lead_send_at', 'fbUserId': 'groupFbUserId'}, inplace=True)
    leads_df['fbUserId'] = leads_df['members'].apply(lambda x: get_origin_fb_user_id(x))
    app_user_df = get_origin_fb_users(leads_df['fbUserId'].tolist())
    send_lead_by_day_df = get_lead_per_day_in_service(app_user_df, leads_df)
    only_first_lead_df = get_first_sent_lead_per_user(send_lead_by_day_df)
    print(only_first_lead_df['dayOfLeadSent'].describe(percentiles=[.1, .2, .3, .4, .5, .6, .7, .8, .9]))
    return only_first_lead_df


get_sent_lead_for_app_users(start=first_of_nov, end=datetime.now())
