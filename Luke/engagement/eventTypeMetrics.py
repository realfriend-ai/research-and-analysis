import pandas as pd

from Luke.acquisition.getNumOfUsersCreatedByMediumByPeriod import get_users_created_by_medium_and_date
from constants.importantDates import first_of_apr_21, first_of_may_21
from constants.mongoConnectLuke import feed_item_event_collection


def get_event_from_users(buyer_user_list, event):
    pageViewsList = list(feed_item_event_collection.find(
        {
            'fbUserId': {'$in': buyer_user_list},
            'eventKind': event,
        }, {'createdAt': 1, 'fbUserId': 1, 'duration': 1}
    ))
    eventsDf = pd.DataFrame(pageViewsList)
    eventsDf.rename(columns={'createdAt': 'eventCreatedAt'}, inplace=True)
    return eventsDf


def get_event_per_day_in_service(fb_user_df, event_df):
    merged_df = pd.merge(fb_user_df, event_df, how='inner', left_on='fbUserId', right_on='fbUserId')
    merged_df['deltaUntilEvent'] = merged_df.apply(
        lambda row: row['eventCreatedAt'].date() - row['userCreatedAt'].date(), axis=1)
    merged_df['dayOfEvent'] = merged_df['deltaUntilEvent'].apply(lambda x: x.days + 1)
    merged_df['count'] = 1
    return merged_df


def get_events_metrics_per_day(day, event_df_by_day_in_service, prev_day):
    event_df_by_day_in_service_temp = event_df_by_day_in_service[(event_df_by_day_in_service['dayOfEvent'] <= day) & (event_df_by_day_in_service['dayOfEvent'] > prev_day)]
    event_df_by_day_in_service_grouped = pd.DataFrame(
        event_df_by_day_in_service_temp.groupby(['fbUserId'])['count'].count()).reset_index()
    return {'day': day, 'numOfUserDidEvent': event_df_by_day_in_service_grouped['fbUserId'].nunique(),
            'meanEvents': event_df_by_day_in_service_grouped['count'].mean(),
            'medianEvents': event_df_by_day_in_service_grouped['count'].median()}


def get_events_num_per_days(events_df_by_day_in_service):
    event_by_day_list = []
    prev_day = 0
    for i in [1, 3, 7, 14, 21, 28]:
        event_by_day_list.append(get_events_metrics_per_day(i, events_df_by_day_in_service, prev_day))
        prev_day = i
    events_stats_by_day_df = pd.DataFrame(event_by_day_list)
    return events_stats_by_day_df


def get_event_stats_by_date_and_medium(start, end, event, medium):
    """Summary: get engagement of users between dates in specific medium

    Parameters:
         start (date): the beginning date -  we want to find user created after
         end (date): the final date - we want to find users created before
         event (str): indicates which event we are looking for

    """
    users_created_by_dates = get_users_created_by_medium_and_date(start, end, only_user_did_pw=True, for_action=True)
    users_created_by_dates = users_created_by_dates[users_created_by_dates['preferredMedium'] == medium]
    users_created_by_dates.rename(columns={'_id': 'fbUserId', 'createdAt': 'userCreatedAt'}, inplace=True)
    events_df = get_event_from_users(buyer_user_list=users_created_by_dates['fbUserId'].tolist(),
                                     event=event)
    events_df_by_day_in_service = get_event_per_day_in_service(users_created_by_dates, events_df)
    return events_df_by_day_in_service


interested_df_by_day_in_service_app = get_event_stats_by_date_and_medium(start=first_of_apr_21, end=first_of_may_21,
                                                                     event='FEED_ITEM_STATUS_INTERESTED', medium='imessage')
int_stats = get_events_num_per_days(interested_df_by_day_in_service_app)
