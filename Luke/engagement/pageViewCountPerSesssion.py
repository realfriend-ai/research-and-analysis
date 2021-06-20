from datetime import datetime, timedelta

import pandas as pd

from Luke.acquisition.getNumOfUsersCreatedByMediumByPeriod import get_users_created_by_medium_and_date
from Luke.engagement.pageViewsMetrics import get_page_views_from_users
from constants.importantDates import first_of_mar_21


def divide_page_view_to_session(pw_df_input: pd.DataFrame):
    pw_df_input.sort_values(by=['fbUserId', 'pageViewCreatedAt'], inplace=True)
    last_fb_user = ''
    last_time_of_pw = ''
    session = 0
    for i in pw_df_input.index:
        rowFbUser = pw_df_input.at[i, 'fbUserId']
        rowPageViewCreatedAt = pw_df_input.at[i, 'pageViewCreatedAt']
        rowDuration = pw_df_input.at[i, 'duration']
        if rowFbUser == last_fb_user:
            if pd.isna(rowDuration) is False:
                sameSessionTime = last_time_of_pw + timedelta(seconds=rowDuration) + timedelta(minutes=5)
            else:
                sameSessionTime = last_time_of_pw + timedelta(minutes=1) + timedelta(minutes=5)
            if rowPageViewCreatedAt < sameSessionTime:
                pw_df_input.at[i, 'sessionNumber'] = session
                last_time_of_pw = rowPageViewCreatedAt

            else:
                session += 1
                pw_df_input.at[i, 'sessionNumber'] = session
                last_time_of_pw = rowPageViewCreatedAt
        else:
            last_fb_user = rowFbUser
            last_time_of_pw = rowPageViewCreatedAt
            session = 0
    return pw_df_input


def get_counts_stats_per_session(pw_df_input):
    pw_df_input['count'] = 1
    pw_df_input = pd.DataFrame(pw_df_input.groupby(['fbUserId', 'sessionNumber'])['count'].count()).reset_index()
    return pw_df_input


def get_pw_count_per_session(start, end, medium, page_type):
    """Summary: get engagement of users between dates in specific medium

    Parameters:
         start (date): the beginning date -  we want to find user created after
         end (date): the final date - we want to find users created before
         medium (str): indicates which medium stats we would like

    """
    users_created_by_dates = get_users_created_by_medium_and_date(start, end, only_user_did_pw=True, for_action=True)
    users_created_by_dates.rename(columns={'_id': 'fbUserId', 'pageViewCreatedAt': 'userpageViewCreatedAt'},
                                  inplace=True)
    medium_users_df = users_created_by_dates[users_created_by_dates['preferredMedium'] == medium]
    user_list = medium_users_df['fbUserId'].tolist()
    pw_df = get_page_views_from_users(user_list, page_type)
    pw_df = divide_page_view_to_session(pw_df)
    pw_df_grouped = get_counts_stats_per_session(pw_df)
    return pw_df_grouped['count'].describe(percentiles=[.1, .2, .3, .4, .5, .6, .7, .8, .9])


pw_df_session_counts_stats_app = get_pw_count_per_session(start=first_of_mar_21, end=datetime.now(), medium='app',
                                                          page_type='PROPERTY_MORE_DETAILS')
pw_df_session_counts_stats_imsg = get_pw_count_per_session(start=first_of_mar_21, end=datetime.now(), medium='imessage',
                                                           page_type='PROPERTY_MORE_DETAILS')
