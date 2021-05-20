import pandas as pd

from Luke.acquisition.getNumOfUsersCreatedByMediumByPeriod import get_users_created_by_medium_and_date
from constants.importantDates import first_of_apr_21, first_of_may_21, first_of_nov
from constants.mongoConnectLuke import feed_item_event_collection


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
    """Summary: get engagement of users between dates in specific medium

    Parameters:
         start (date): the beginning date -  we want to find user created after
         end (date): the final date - we want to find users created before
         medium (str): indicates which medium stats we would like

    """
    users_created_by_dates = get_users_created_by_medium_and_date(start, end, only_user_did_pw=True)
    users_created_by_dates.rename(columns={'_id': 'fbUserId', 'createdAt': 'userCreatedAt'}, inplace=True)
    medium_users_df = users_created_by_dates[users_created_by_dates['preferredMedium'] == medium]
    user_list = medium_users_df['fbUserId'].tolist()
    send_lead_df = get_lead_sent_from_users(user_list)
    send_lead_by_day_df = get_lead_per_day_in_service(medium_users_df, send_lead_df)
    only_first_lead_df = get_first_sent_lead_per_user(send_lead_by_day_df)
    return only_first_lead_df


imsg_engagement = get_sent_lead_stats_by_date_and_medium(start=first_of_nov, end=first_of_may_21, medium='imessage')
# app_engagement = get_sent_lead_stats_by_date_and_medium(start=first_of_apr_21, end=first_of_may_21, medium='app')
print(imsg_engagement['dayOfLeadSent'].describe())