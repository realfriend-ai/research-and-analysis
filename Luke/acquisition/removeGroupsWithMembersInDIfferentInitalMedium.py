import pandas as pd

from Luke.acquisition.getGroupesToIgnore import groups_created_between_dates
from Luke.acquisition.getNumOfUsersCreatedByMediumByPeriod import get_preferred_medium_by_user_cleaning_groups
from constants.mongoConnectLuke import fb_users_collection


def find_groups_created_for_mediums(start, end, medium):
    groups_df = groups_created_between_dates(start, end)
    groups_df_fb_user_ids = groups_df['fbUserId'].tolist()
    groups_fb_users_df = pd.DataFrame(
        list(fb_users_collection.find({'_id': {'$in': groups_df_fb_user_ids}}, {'mediums': 1})))
    groups_fb_users_df['preferredMedium'] = groups_fb_users_df['mediums'].apply(
        lambda x: get_preferred_medium_by_user_cleaning_groups(x))
    groups_fb_users_df_by_medium = groups_fb_users_df[groups_fb_users_df['preferredMedium'] == medium]
    list_of_group_user_using_medium = groups_fb_users_df_by_medium['_id'].tolist()
    groups_df['isFbUserNeeded'] = groups_df['fbUserId'].apply(
        lambda x: True if x in list_of_group_user_using_medium else False)
    groups_df = groups_df[groups_df['isFbUserNeeded']]
    return groups_df


def is_have_initial_medium_tag_different_than_medium(medium, tags):
    mediumUpper = medium.upper()
    initial_medium_tags = ['INITIAL_MEDIUM_IMESSAGE', 'INITIAL_MEDIUM_APP']
    for i in initial_medium_tags:
        if i in tags:
            if i != f'INITIAL_MEDIUM_{mediumUpper}':
                return True
    return False


def find_members_with_different_initial_medium_than_group(groups_df, medium):
    members_ids = groups_df['membersFbUserIds'].tolist()
    members_ids_flatten = [i for member in members_ids for i in member]
    members_fb_user_df = pd.DataFrame(
        list(fb_users_collection.find({'_id': {'$in': members_ids_flatten}}, {'tags': 1})))
    members_fb_user_df['isInitialMediumDiffThanGiven'] = members_fb_user_df['tags'].apply(
        lambda x: is_have_initial_medium_tag_different_than_medium(tags=x, medium=medium))
    members_fb_user_df = members_fb_user_df[members_fb_user_df['isInitialMediumDiffThanGiven']]
    return members_fb_user_df['_id'].tolist()


def find_groups_to_ignore(groups_df, member_list_to_ignore):
    groups_df['groupWeShouldIgnore'] = False
    for i in groups_df.index:
        members = groups_df.at[i, 'membersFbUserIds']
        for member in members:
            if member in member_list_to_ignore:
                groups_df.at[i, 'groupWeShouldIgnore'] = True
    groups_df_should_ignore = groups_df[groups_df['groupWeShouldIgnore']]
    return groups_df_should_ignore


def find_groups_contains_users_created_in_different_medium(start, end, medium):
    groups_df_by_medium = find_groups_created_for_mediums(start, end, medium)
    user_df_with_different_initial_medium = find_members_with_different_initial_medium_than_group(groups_df_by_medium,
                                                                                                  medium)
    groups_df_should_ignore = find_groups_to_ignore(groups_df_by_medium, user_df_with_different_initial_medium)
    return groups_df_should_ignore['fbUserId'].tolist()


def get_fbUser_df_without_groups_with_member_in_different_medium(fb_user_df, start, end, medium):
    list_of_groups_we_should_ignore = find_groups_contains_users_created_in_different_medium(start, end, medium)
    fb_user_df['isFbUserIdShouldBeIgnored'] = fb_user_df['fbUserId'].apply(
        lambda x: True if x in list_of_groups_we_should_ignore else False)
    fb_user_df = fb_user_df[fb_user_df['isFbUserIdShouldBeIgnored'] == False]
    return fb_user_df
