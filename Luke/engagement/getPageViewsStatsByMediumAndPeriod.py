import pandas as pd

from Luke.acquisition.getNumOfUsersCreatedByMediumByPeriod import get_users_created_by_medium_and_date
from constants.importantDates import first_of_apr_21, first_of_may_21
from constants.mongoConnectLuke import page_view_collection


def get_page_views_from_users(buyer_user_list):
    pageViewsList = list(page_view_collection.find(
        {
            'fbUserId': {'$in': buyer_user_list},
            'pageName': 'PROPERTY_MORE_DETAILS',
        }, {'createdAt': 1, 'fbUserId': 1, 'duration': 1}
    ))
    pageViewsDf = pd.DataFrame(pageViewsList)
    pageViewsDf.rename(columns={'createdAt': 'pageViewCreatedAt'}, inplace=True)
    return pageViewsDf


def get_pw_per_day_of_page_view(fb_user_df, pw_df):
    merged_df = pd.merge(fb_user_df, pw_df, how='inner', left_on='fbUserId', right_on='fbUserId')
    merged_df['deltaUntilPW'] = merged_df.apply(
        lambda row: row['pageViewCreatedAt'].date() - row['userCreatedAt'].date(), axis=1)
    merged_df['dayOfPageView'] = merged_df['deltaUntilPW'].apply(lambda x: x.days + 1)
    merged_df['count'] = 1
    return merged_df


def get_pw_metrics_per_day(day, pw_df_by_day_df):
    pw_df_by_day_df_temp = pw_df_by_day_df[pw_df_by_day_df['dayOfPageView'] <= day]
    pw_df_by_day_df_grouped = pd.DataFrame(pw_df_by_day_df_temp.groupby(['fbUserId'])['count'].count()).reset_index()
    return {'day': day, 'numOfUserDidPw': pw_df_by_day_df_grouped['fbUserId'].nunique(),
            'meanPageViews': pw_df_by_day_df_grouped['count'].mean(),
            'medianPageViews': pw_df_by_day_df_grouped['count'].median()}


def get_pw_stats_by_date_and_medium(start, end, medium):
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
    pw_df = get_page_views_from_users(user_list)
    pw_by_day_df = get_pw_per_day_of_page_view(medium_users_df, pw_df)
    pw_by_day_list = []
    for i in [1, 3, 7, 14, 21, 28]:
        pw_by_day_list.append(get_pw_metrics_per_day(i, pw_by_day_df))
        pw_stats_by_day_df = pd.DataFrame(pw_by_day_list)
    return pw_stats_by_day_df


app_engagement = get_pw_stats_by_date_and_medium(start=first_of_apr_21, end=first_of_may_21, medium='app')
imsg_engagement = get_pw_stats_by_date_and_medium(start=first_of_apr_21, end=first_of_may_21, medium='imessage')
