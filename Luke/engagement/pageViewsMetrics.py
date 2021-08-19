import pandas as pd

from Luke.acquisition.getNumOfUsersCreatedByMediumByPeriod import get_users_created_by_medium_and_date
from constants.importantDates import first_of_may_21, first_of_mar_21
from constants.mongoConnectLuke import page_view_collection


def get_page_views_from_users(buyer_user_list, page_type):
    pageViewsList = list(page_view_collection.find(
        {
            'fbUserId': {'$in': buyer_user_list},
            'pageName': page_type,
            'adminUserId': {'$exists': False},
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


def get_pw_metrics_per_day(day, pw_df_by_day_df, prev_day):
    pw_df_by_day_df_temp = pw_df_by_day_df[
        (pw_df_by_day_df['dayOfPageView'] <= day) & (pw_df_by_day_df['dayOfPageView'] > prev_day)]
    pw_df_by_day_df_grouped = pd.DataFrame(pw_df_by_day_df_temp.groupby(['fbUserId'])['count'].count()).reset_index()
    return {'day': day, 'numOfUserDidPw': pw_df_by_day_df_grouped['fbUserId'].nunique(),
            'meanPageViews': pw_df_by_day_df_grouped['count'].mean(),
            'medianPageViews': pw_df_by_day_df_grouped['count'].median()}


def get_pw_stats_by_date_and_medium(start, end, medium, page_type, only_user_sent_lead):
    """Summary: get engagement of users between dates in specific medium

    Parameters:
         start (date): the beginning date -  we want to find user created after
         end (date): the final date - we want to find users created before
         medium (str): indicates which medium stats we would like

    """
    users_created_by_dates = get_users_created_by_medium_and_date(start, end, only_user_did_pw=True,
                                                                  only_user_sent_lead=only_user_sent_lead,
                                                                  for_action=True)
    users_created_by_dates.rename(columns={'_id': 'fbUserId', 'createdAt': 'userCreatedAt'}, inplace=True)
    medium_users_df = users_created_by_dates[users_created_by_dates['preferredMedium'] == medium]
    user_list = medium_users_df['fbUserId'].tolist()
    pw_df = get_page_views_from_users(user_list, page_type)
    pw_by_day_df = get_pw_per_day_of_page_view(medium_users_df, pw_df)
    return pw_by_day_df


def get_page_view_num_per_days(pw_by_day_df):
    pw_by_day_list = []
    prev_day = 0
    for i in [1, 3, 7, 14, 21, 28]:
        pw_by_day_list.append(get_pw_metrics_per_day(i, pw_by_day_df, prev_day))
        prev_day = i
    pw_stats_by_day_df = pd.DataFrame(pw_by_day_list)
    return pw_stats_by_day_df


def get_unique_page_views_days_in_first_week(pw_by_day_df):
    pwFirstWeek = pw_by_day_df[pw_by_day_df['dayOfPageView'] <= 7]
    numOfUniquePageViewsFirstWeekPerUser = pd.DataFrame(
        pwFirstWeek.groupby(['fbUserId'])['dayOfPageView'].nunique()).reset_index()
    return numOfUniquePageViewsFirstWeekPerUser['dayOfPageView'].describe()


def get_num_of_pws_per_user(pw_by_day_df):
    pw_per_user = pd.DataFrame(pw_by_day_df.groupby(['fbUserId'])['count'].count()).reset_index()
    return pw_per_user['count'].describe()


def get_page_view_data(start, end, medium, page_type, only_user_sent_lead):
    pw_by_day_df = get_pw_stats_by_date_and_medium(start, end, medium, page_type, only_user_sent_lead)
    unique_days_pw = get_unique_page_views_days_in_first_week(pw_by_day_df)
    pw_num_per_days = get_page_view_num_per_days(pw_by_day_df)
    pw_per_user = get_num_of_pws_per_user(pw_by_day_df)
    return unique_days_pw, pw_num_per_days, pw_per_user

