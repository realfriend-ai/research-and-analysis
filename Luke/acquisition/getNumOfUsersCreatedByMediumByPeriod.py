import pandas as pd

from Luke.acquisition.getGroupesToIgnore import find_groups_with_user_created_between_date_and_user_member_before
from constants.importantDates import *
from constants.lukeFbUserIds import luke_fb_user_ids
from constants.mongoConnectLuke import fb_users_collection


def get_preferred_medium_by_user_cleaning_groups(mediums):
    preferred = mediums.get('preferredMedium')
    if preferred == 'group':
        return mediums.get('group').get('preferredMedium')
    else:
        return preferred


def get_number_of_users_created_by_medium_and_date(start, end, only_user_did_pw):
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
    fbUserDf['preferredMedium'] = fbUserDf['mediums'].apply(lambda x: get_preferred_medium_by_user_cleaning_groups(x))
    print(fbUserDf['preferredMedium'].value_counts())
    return fbUserDf


get_number_of_users_created_by_medium_and_date(start=first_of_apr_21, end=first_of_may_21, only_user_did_pw=False)
