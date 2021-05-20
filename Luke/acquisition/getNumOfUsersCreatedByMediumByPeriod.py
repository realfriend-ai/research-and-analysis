import pandas as pd

from Luke.acquisition.getFbUserIdsWeShouldIgnore import get_user_we_should_ignore
from Luke.acquisition.getGroupesToIgnore import find_groups_with_user_created_between_date_and_user_member_before, \
    groups_created_between_dates
from constants.importantDates import first_of_apr_21, first_of_may_21
from constants.lukeFbUserIds import luke_fb_user_ids
from constants.mongoConnectLuke import fb_users_collection


def get_preferred_medium_by_user_cleaning_groups(mediums):
    preferred = mediums.get('preferredMedium')
    if preferred == 'group':
        return mediums.get('group').get('preferredMedium')
    else:
        return preferred


def remove_fb_user_we_should_ignore_on_counting(start, end, fbUserDf):
    groups_df = groups_created_between_dates(start, end)
    user_ids_we_should_ignore_in_counting = get_user_we_should_ignore(groups_df, fbUserDf)
    fbUserDf['isIgnoreUser'] = fbUserDf['fbUserId'].apply(
        lambda x: True if x in user_ids_we_should_ignore_in_counting else False)
    fbUserDf = fbUserDf[fbUserDf['isIgnoreUser'] == False]
    return fbUserDf


def get_users_created_by_medium_and_date(start, end, only_user_did_pw):
    """Summary: get users created by medium between dates

     Parameters:
             start (date): the beginning date -  we want to find user created after
             end (date): the final date - we want to find users created before

     """
    groups_ids_we_should_ignore = find_groups_with_user_created_between_date_and_user_member_before(start, end)
    query = {
        'isAgent': {'$ne': True},
        'tags': {'$ne': 'ADMIN'},
        'requestsKind': 'APT_SALE',
        '_id': {'$nin': luke_fb_user_ids + groups_ids_we_should_ignore},
        'createdAt': {'$gte': start, '$lte': end},
    }
    projection = {'_id': 1, 'createdAt': 1, 'mediums': 1, 'requestsKind': 1, 'initialRequest': 1}
    if only_user_did_pw:
        query.update({'lastPageViewAt': {'$exists': True}})
    fbUsersList = list(fb_users_collection.find(query, projection))
    fbUserDf = pd.DataFrame(fbUsersList)
    fbUserDf.dropna(subset=['mediums'], inplace=True)
    fbUserDf.rename(columns={'_id': 'fbUserId'}, inplace=True)
    fbUserDf = remove_fb_user_we_should_ignore_on_counting(start, end, fbUserDf)
    fbUserDf['preferredMedium'] = fbUserDf['mediums'].apply(lambda x: get_preferred_medium_by_user_cleaning_groups(x))
    print(fbUserDf['preferredMedium'].value_counts())
    return fbUserDf


