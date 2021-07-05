import pandas as pd

from Luke.acquisition.getGroupesToIgnore import get_admin_group_members_fb_user_ids
from Luke.acquisition.getNumOfUsersCreatedByMediumByPeriod import get_users_created_by_medium_and_date
from Luke.acquisition.removeGroupsWithMembersInDIfferentInitalMedium import \
    get_fbUser_df_without_groups_with_member_from_different_medium
from constants.importantDates import first_of_jun_21, first_of_july_21
from mongoConnect import group_chat_collection, fb_users_collection


def get_groups_id_from_medium(mediums):
    if mediums.get('preferredMedium') == 'group':
        return mediums.get('group').get('id')


def get_groups_origin_fb_user_ids(group_ids):
    groupsList = list(group_chat_collection.find(
        {
            '_id': {'$in': group_ids}
        }, {'_id': 1, 'fbUserId': 1, 'members': 1, 'createdAt': 1}
    ))
    groups_df = pd.DataFrame(groupsList)
    groups_df['originFbUserId'] = groups_df['members'].apply(lambda x: get_admin_group_members_fb_user_ids(x))
    groups_df['numOfUserIds'] = groups_df['originFbUserId'].apply(lambda x: len(x))
    members_ids = groups_df['originFbUserId'].tolist()
    members_ids_flatten = [i for member in members_ids for i in member]
    return members_ids_flatten


def get_app_users_created_between_dates_and_approved_notifications(start, end):
    """Summary: get users created by APP between dates and approved notificatios

    Parameters:
         start (date): the beginning date -  we want to find user created after
         end (date): the final date - we want to find users created before
     """

    app_users_df = get_users_created_by_medium_and_date(start=start, end=end, medium='app',
                                                        for_action_insights=False, only_user_sent_lead=False,
                                                        only_user_did_pw=False)
    app_users_df = get_fbUser_df_without_groups_with_member_from_different_medium(app_users_df, start, end, 'app')
    app_users_df['groupId'] = app_users_df['mediums'].apply(lambda x: get_groups_id_from_medium(x))
    fb_users_ids = get_groups_origin_fb_user_ids(app_users_df['groupId'].tolist())
    query = {
        '_id': {'$in': fb_users_ids},
        'mediums.app.apnToken': {
            '$exists': True
        },
        'createdAt': {'$gte': start, '$lte': end},
        'isAgent': {'$ne': True},
        'tags': {'$ne': 'ADMIN'},
    }
    projection = {'_id': 1, 'mediums': 1}
    app_user_with_token_list = list(fb_users_collection.find(query, projection))
    app_user_with_token_df = pd.DataFrame(app_user_with_token_list)
    app_user_with_token_df['token'] = app_user_with_token_df['mediums'].apply(lambda x: x.get('app').get('apnToken'))
    return app_user_with_token_df


app_users_with_token_df = get_app_users_created_between_dates_and_approved_notifications(start=first_of_jun_21,
                                                                                         end=first_of_july_21)
