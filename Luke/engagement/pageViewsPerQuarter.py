import pandas as pd

from constants.importantDates import *
from constants.lukeFbUserIds import luke_fb_user_ids, bug_users_ids
from constants.mongoConnectLuke import page_view_collection, fb_users_collection

quarters = {'firstQuarter': {'start': first_of_jan_21, 'end': first_of_apr_21},
            'secondQuarter': {'start': first_of_apr_21, 'end': first_of_july_21}}


def get_page_views_by_date(start, end, page_type):
    pageViewsList = list(page_view_collection.find(
        {
            'pageName': page_type,
            'createdAt': {'$gte': start, '$lte': end},
            'adminUserId': {'$exists': False},
        }, {'createdAt': 1, 'fbUserId': 1, 'duration': 1}
    ))
    pageViewsDf = pd.DataFrame(pageViewsList)
    pageViewsDf.rename(columns={'createdAt': 'pageViewCreatedAt'}, inplace=True)
    return pageViewsDf


def get_only_buyers_page_views(fb_user_list, page_view_df):
    users_to_ignore = luke_fb_user_ids + bug_users_ids
    print(f'Before filter {len(fb_user_list)}')
    fb_user_list = list(filter(lambda x: x not in users_to_ignore, fb_user_list))
    print(f'After filter {len(fb_user_list)}')
    query = {
        'isAgent': {'$ne': True},
        'tags': {'$ne': 'ADMIN'},
        'requestsKind': 'APT_SALE',
        '_id': {'$in': fb_user_list},
    }
    projection = {'_id': 1}
    fbUsersList = list(fb_users_collection.find(query, projection))
    fbUserDf = pd.DataFrame(fbUsersList)
    fbUserDf.rename(columns={'_id': 'fbUserId'}, inplace=True)
    print(f'Before merge {len(page_view_df.index)}')
    page_view_df = pd.merge(page_view_df, fbUserDf, how='inner', left_on='fbUserId', right_on='fbUserId')
    print(f'After merge {len(page_view_df.index)}')
    return page_view_df


def get_page_view_metrics_by_quarter(quarter):
    page_views_df = get_page_views_by_date(start=quarters.get(quarter).get('start'),
                                           end=quarters.get(quarter).get('end')
                                           , page_type='PROPERTY_MORE_DETAILS')
    page_views_df = get_only_buyers_page_views(fb_user_list=page_views_df['fbUserId'].tolist(),
                                               page_view_df=page_views_df)
    total_users = page_views_df['fbUserId'].nunique()
    page_views_df['count'] = 1
    page_views_df_grouped_by_user = pd.DataFrame(page_views_df.groupby('fbUserId')['count'].count()).reset_index()
    return {'Q': quarter, 'NumOfUserDidPageView': total_users, 'totalNumOfPageViews': len(page_views_df.index),
            'describe': page_views_df_grouped_by_user['count'].describe()}


final_df_list = []
final_df_list.append(get_page_view_metrics_by_quarter('firstQuarter'))
final_df_list.append(get_page_view_metrics_by_quarter('secondQuarter'))
df = pd.DataFrame(final_df_list)


def get_page_view_metrics_by_month(start, end):
    page_views_df = get_page_views_by_date(start=start,
                                           end=end
                                           , page_type='PROPERTY_MORE_DETAILS')
    page_views_df = get_only_buyers_page_views(fb_user_list=page_views_df['fbUserId'].tolist(),
                                               page_view_df=page_views_df)
    total_users = page_views_df['fbUserId'].nunique()
    page_views_df['count'] = 1
    page_views_df_grouped_by_user = pd.DataFrame(page_views_df.groupby('fbUserId')['count'].count()).reset_index()
    return {'month': start.month, 'NumOfUserDidPageView': total_users, 'totalNumOfPageViews': len(page_views_df.index),
            'describe': page_views_df_grouped_by_user['count'].describe()}


# final_df_list = []
# final_df_list.append(get_page_view_metrics_by_month(start=first_of_jan_21, end=first_of_feb_21))
# final_df_list.append(get_page_view_metrics_by_month(start=first_of_feb_21, end=first_of_mar_21))
# final_df_list.append(get_page_view_metrics_by_month(start=first_of_mar_21, end=first_of_apr_21))
# final_df_list.append(get_page_view_metrics_by_month(start=first_of_apr_21, end=first_of_may_21))
# final_df_list.append(get_page_view_metrics_by_month(start=first_of_may_21, end=first_of_jun_21))
# final_df_list.append(get_page_view_metrics_by_month(start=first_of_jun_21, end=first_of_july_21))
# df = pd.DataFrame(final_df_list)
