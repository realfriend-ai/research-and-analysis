from datetime import datetime

import pandas as pd

from constants.lukeFbUserIds import luke_fb_user_ids
from constants.mongoConnectLuke import fb_users_collection, group_chat_collection


def get_admin_group_members_fb_user_ids(members):
    res = []
    for member in members:
        if member.get('role') == 'ADMIN':
            res.append(member.get('fbUserId'))
    return res


def is_member_in_list(fb_user_list, members):
    for member in members:
        if (member in fb_user_list) & (member not in luke_fb_user_ids):
            return True
    return False


def groups_created_between_dates(start, end):
    groupsList = list(group_chat_collection.find(
        {
            'createdAt': {'$gte': start, '$lte': end},
        }, {'_id': 1, 'fbUserId': 1, 'members': 1, 'createdAt': 1}
    ))
    groups_df = pd.DataFrame(groupsList)
    groups_df['membersFbUserIds'] = groups_df['members'].apply(lambda x: get_admin_group_members_fb_user_ids(x))
    return groups_df


def get_fb_users_who_created_before_start_date_from_ids_list(members_ids_flatten, start):
    fbUsersList = list(fb_users_collection.find(
        {
            '_id': {'$in': members_ids_flatten},
            'createdAt': {'$lte': start},
        }, {'_id': 1, 'createdAt': 1, 'mediums': 1}
    ))
    fbUserDf = pd.DataFrame(fbUsersList)
    return fbUserDf['_id'].tolist()


def find_groups_with_user_created_between_date_and_user_member_before(start, end):
    """Summary: cleaning groups which were created between two dates but the origin user was not

     Parameters:
             start (date): the beginning date -  we want to find user created after
             end (date): the final date - we want to find users created before

     """
    groups_df = groups_created_between_dates(start, end)
    members_ids = groups_df['membersFbUserIds'].tolist()
    members_ids_flatten = [i for member in members_ids for i in member]
    fb_user_ids_created_before_start_date = get_fb_users_who_created_before_start_date_from_ids_list(
        members_ids_flatten, start)
    groups_df['shouldBeIgnored'] = groups_df['membersFbUserIds'].apply(
        lambda x: is_member_in_list(fb_user_ids_created_before_start_date, x))
    groups_df = groups_df[groups_df['shouldBeIgnored'] == True]
    return groups_df['fbUserId'].tolist()


listOfUsers = find_groups_with_user_created_between_date_and_user_member_before(start=datetime(2021, 5, 1), end=datetime(2021, 5, 31))
